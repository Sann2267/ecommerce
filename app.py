"""
Flask Dashboard

Routes:
  GET  /              → Dashboard (tabel transaksi, stats, webhook log)
  GET  /report        → Athena analytics report
  POST /webhook       → Terima notifikasi SNS (HTTP subscription)
  GET  /api/transactions → JSON API untuk frontend
  GET  /api/stats     → JSON API untuk statistik
  GET  /health        → Health check
"""

import json
import os
import time
import logging
import hmac
import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from functools import wraps

import boto3
from flask import Flask, render_template, request, jsonify
from boto3.dynamodb.conditions import Attr
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-change-me')

AWS_REGION        = os.environ.get('AWS_REGION',          'ap-southeast-1')
DYNAMO_TABLE_NAME = os.environ.get('DYNAMO_TABLE_NAME',   'sistematoko_transactions')
ATHENA_DB         = os.environ.get('ATHENA_DATABASE',     'sistematoko_db')
ATHENA_S3_OUTPUT  = os.environ.get('ATHENA_S3_OUTPUT',    's3://sistematoko-datalake/athena-results/')
SNS_TOPIC_ARN     = os.environ.get('SNS_TOPIC_ARN',       '')

dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
athena   = boto3.client('athena',    region_name=AWS_REGION)
table    = dynamodb.Table(DYNAMO_TABLE_NAME)

webhook_log: list[dict] = []
MAX_WEBHOOK_LOG = 100

def decimal_to_json(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def format_rupiah(amount) -> str:
    try:
        return f"Rp {float(amount):,.0f}".replace(',', '.')
    except Exception:
        return str(amount)


def run_athena_query(query: str) -> list[dict]:
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DB},
        ResultConfiguration={'OutputLocation': ATHENA_S3_OUTPUT}
    )
    exec_id = resp['QueryExecutionId']

    for _ in range(30):
        time.sleep(1)
        status = athena.get_query_execution(
            QueryExecutionId=exec_id
        )['QueryExecution']['Status']['State']

        if status == 'SUCCEEDED':
            break
        if status in ('FAILED', 'CANCELLED'):
            logger.error(f"Athena query {status}: {exec_id}")
            return []

    results = athena.get_query_results(QueryExecutionId=exec_id)
    rows    = results['ResultSet']['Rows']
    cols    = [c['Name'] for c in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    return [
        {cols[i]: cell.get('VarCharValue', '') for i, cell in enumerate(row['Data'])}
        for row in rows[1:]
    ]


def get_dynamo_transactions(limit: int = 50) -> list[dict]:
    """Scan DynamoDB untuk semua transaksi (untuk tabel kecil)."""
    resp  = table.scan(Limit=limit)
    items = resp.get('Items', [])
    items.sort(key=lambda x: x.get('processed_at', ''), reverse=True)
    return items


def get_stats(items: list[dict]) -> dict:
    """Hitung statistik dari list transaksi."""
    total     = len(items)
    fraud     = sum(1 for i in items if i.get('is_fraud'))
    valid     = total - fraud
    total_rev = sum(float(i.get('total_billing', 0)) for i in items if not i.get('is_fraud'))
    fraud_amt = sum(float(i.get('total_billing', 0)) for i in items if i.get('is_fraud'))
    ai_count  = sum(1 for i in items if i.get('decision_source') == 'AI')
    rule_count= sum(1 for i in items if i.get('decision_source') == 'RULE_FALLBACK')

    return {
        'total':            total,
        'fraud':            fraud,
        'valid':            valid,
        'fraud_rate':       round(fraud / total * 100, 1) if total else 0,
        'total_revenue':    round(total_rev, 2),
        'total_fraud_amount': round(fraud_amt, 2),
        'ai_decisions':     ai_count,
        'rule_decisions':   rule_count,
    }

@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now(timezone.utc).isoformat()})


@app.route('/')
def index():
    """Dashboard utama: tabel transaksi + statistik + webhook log."""
    try:
        items = get_dynamo_transactions(limit=100)
        stats = get_stats(items)
    except Exception as e:
        logger.error(f"Error loading dashboard: {e}")
        items, stats = [], {}

    return render_template('index.html',
                           transactions=items,
                           stats=stats,
                           webhook_log=webhook_log[-20:],
                           format_rupiah=format_rupiah)


@app.route('/report')
def report():
    """Halaman laporan analitik dari Athena."""
    daily_data, top_merchants, fraud_by_category = [], [], []
    error = None

    try:
        daily_data = run_athena_query("""
            SELECT
                SUBSTR(processed_at, 1, 10)          AS tanggal,
                COUNT(*)                             AS total_trx,
                SUM(CAST(total_billing AS DOUBLE))   AS total_revenue,
                COUNT(CASE WHEN is_fraud = 'true' THEN 1 END) AS fraud_count
            FROM sistematoko_db.transactions
            GROUP BY SUBSTR(processed_at, 1, 10)
            ORDER BY tanggal DESC
            LIMIT 30
        """)

        top_merchants = run_athena_query("""
            SELECT
                merchant_name,
                COUNT(*)                             AS total_trx,
                SUM(CAST(total_billing AS DOUBLE))   AS total_revenue,
                COUNT(CASE WHEN is_fraud = 'true' THEN 1 END) AS fraud_count
            FROM sistematoko_db.transactions
            GROUP BY merchant_name
            ORDER BY total_revenue DESC
            LIMIT 10
        """)

        fraud_by_category = run_athena_query("""
            SELECT
                category,
                COUNT(CASE WHEN is_fraud = 'true' THEN 1 END)  AS fraud_count,
                COUNT(*)                                         AS total_count,
                ROUND(
                    COUNT(CASE WHEN is_fraud = 'true' THEN 1 END) * 100.0 / COUNT(*),
                1)                                               AS fraud_rate
            FROM sistematoko_db.transactions
            GROUP BY category
            ORDER BY fraud_count DESC
        """)

    except Exception as e:
        logger.error(f"Athena error: {e}")
        error = str(e)

    return render_template('report.html',
                           daily_data=daily_data,
                           top_merchants=top_merchants,
                           fraud_by_category=fraud_by_category,
                           error=error,
                           format_rupiah=format_rupiah)


@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Terima notifikasi dari SNS HTTP subscription.
    SNS kirim:
      - SubscriptionConfirmation → harus confirm URL
      - Notification             → data fraud alert
    """
    global webhook_log

    sns_type = request.headers.get('x-amz-sns-message-type', '')
    content_type = request.headers.get('Content-Type', '')

    try:
        if 'json' in content_type:
            payload = request.get_json(force=True) or {}
        else:
            payload = json.loads(request.data.decode('utf-8'))
    except Exception:
        payload = {}

    logger.info(f"Webhook received: type={sns_type}")

    if sns_type == 'SubscriptionConfirmation':
        confirm_url = payload.get('SubscribeURL')
        if confirm_url:
            try:
                import urllib.request
                urllib.request.urlopen(confirm_url, timeout=5)
                logger.info(f"SNS subscription confirmed: {confirm_url[:60]}...")
            except Exception as e:
                logger.error(f"Confirm gagal: {e}")
        return jsonify({'confirmed': True}), 200

    if sns_type == 'Notification':
        message_str = payload.get('Message', '{}')
        try:
            message = json.loads(message_str)
        except Exception:
            message = {'raw': message_str}

        log_entry = {
            'received_at':    datetime.now(timezone.utc).isoformat(),
            'subject':        payload.get('Subject', ''),
            'transaction_id': message.get('transaction_id', 'unknown'),
            'amount':         message.get('amount', 0),
            'total_billing':  message.get('total_billing', 0),
            'fraud_verdict':  message.get('fraud_verdict', ''),
            'decision_source':message.get('decision_source', ''),
            'user_id':        message.get('user_id', ''),
            'merchant_name':  message.get('merchant_name', ''),
        }

        webhook_log.insert(0, log_entry)
        if len(webhook_log) > MAX_WEBHOOK_LOG:
            webhook_log = webhook_log[:MAX_WEBHOOK_LOG]

        logger.info(f"Fraud alert logged: {log_entry['transaction_id']}")
        return jsonify({'received': True}), 200

    return jsonify({'received': True, 'type': sns_type}), 200


@app.route('/api/transactions')
def api_transactions():
    """JSON API: list transaksi untuk AJAX request."""
    limit = min(int(request.args.get('limit', 50)), 200)
    only_fraud = request.args.get('fraud_only', '').lower() == 'true'

    try:
        items = get_dynamo_transactions(limit=limit)
        if only_fraud:
            items = [i for i in items if i.get('is_fraud')]
        serialized = json.loads(json.dumps(items, default=decimal_to_json))
        return jsonify({'status': 'ok', 'count': len(serialized), 'transactions': serialized})
    except Exception as e:
        logger.error(f"API transactions error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/stats')
def api_stats():
    """JSON API: statistik untuk chart."""
    try:
        items = get_dynamo_transactions(limit=500)
        stats = get_stats(items)
        return jsonify({'status': 'ok', **stats})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/webhook-log')
def api_webhook_log():
    """JSON API: webhook log terbaru."""
    return jsonify({'status': 'ok', 'logs': webhook_log[:50]})

if __name__ == '__main__':
    port  = int(os.environ.get('FLASK_PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    logger.info(f"SistemToko Dashboard starting on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug)