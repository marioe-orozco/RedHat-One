import os
import time
import json
import random
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from prometheus_client import start_http_server, Counter, Gauge

# --- CONFIGURACIÓN DESDE VARIABLES DE ENTORNO (OCP ConfigMap/Secrets) ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redhat-amq-kafka-bootstrap.spei-streaming.svc:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spei.raw.inbound")
TARGET_TPS = int(os.getenv("TARGET_TPS", "1000"))  # Transacciones por segundo deseadas
FRAUD_RATE = float(os.getenv("FRAUD_RATE", "0.05")) # 5% de tráfico con anomalías/ataques
PROMETHEUS_PORT = int(os.getenv("METRICS_PORT", "8000"))

# --- MÉTRICAS PARA OPENSHIFT MONITORING (PROMETHEUS) ---
SENT_TRANSACTIONS = Counter('spei_traffic_gen_sent_total', 'Total de transacciones enviadas', ['type'])
CURRENT_TPS_GAUGE = Gauge('spei_traffic_gen_current_tps', 'TPS actuales generados por el simulador')

# --- CONFIGURACIÓN DEL PRODUCER DE KAFKA ---
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'spei-traffic-generator-pod',
    'acks': 1,
    'queue.buffering.max.messages': 500000
}
producer = Producer(conf)

def delivery_report(err, msg):
    """Callback de entrega de Kafka para métricas."""
    if err is not None:
        SENT_TRANSACTIONS.labels(type='failed').inc()
    else:
        SENT_TRANSACTIONS.labels(type='success').inc()

def generate_iso20022_payload(is_fraud: bool) -> dict:
    """Genera una trama sintética ISO 20022 (pacs.008) de transferencia SPEI."""
    tx_id = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).isoformat()
    
    # Datos base legítimos
    amount = round(random.uniform(50.0, 15000.0), 2)
    device_ip = f"192.168.1.{random.randint(2, 250)}"
    user_location = "CDMX_MX"
    
    # Inyección de patrones de fraude/anomalías si se activa la bandera
    if is_fraud:
        fraud_type = random.choice(["HIGH_AMOUNT", "SUSPICIOUS_IP", "RAPID_VELOCITY"])
        if fraud_type == "HIGH_AMOUNT":
            amount = round(random.uniform(500000.0, 5000000.0), 2)  # Monto inusualmente alto
        elif fraud_type == "SUSPICIOUS_IP":
            device_ip = "185.220.101.5"  # IP catalogada en Tor/Lista Negra
            user_location = "TOR_NETWORK"

    payload = {
        "Document": {
            "FIToFICstmrCdtTrf": {
                "GrpHdr": {
                    "MsgId": f"REDLINE-{tx_id[:8]}",
                    "CreDtTm": now_iso,
                    "NbOfTxs": "1"
                },
                "CdtTrfTxInf": {
                    "PmtId": {"EndToEndId": tx_id},
                    "IntrBkSttlmAmt": {"@Ccy": "MXN", "#text": str(amount)},
                    "Dbtr": {"Nm": "Cliente Ordenante Demo"},
                    "DbtrAcct": {"Id": {"Othr": {"Id": "012180015012345678"}}},
                    "Cdtr": {"Nm": "Beneficiario Destino"},
                    "CdtrAcct": {"Id": {"Othr": {"Id": "044180015087654321"}}}
                }
            }
        },
        "ContextMetadata": {
            "DeviceIP": device_ip,
            "Location": user_location,
            "IsSyntheticAttack": is_fraud
        }
    }
    return payload

def main():
    # Iniciar servidor de métricas para Prometheus en OpenShift
    start_http_server(PROMETHEUS_PORT)
    print(f"[TrafficGen] Servidor de métricas escuchando en el puerto {PROMETHEUS_PORT}")
    print(f"[TrafficGen] Iniciando generación de tráfico a {TARGET_TPS} TPS en tópico '{KAFKA_TOPIC}'...")

    interval = 1.0 / TARGET_TPS if TARGET_TPS > 0 else 0.001
    count = 0
    start_time = time.time()

    try:
        while True:
            tx_start = time.time()
            
            # Determinar si la transacción actual es fraudulenta
            is_fraud = random.random() < FRAUD_RATE
            payload = generate_iso20022_payload(is_fraud)
            
            # Enviar mensaje al tópico de Kafka
            producer.produce(
                topic=KAFKA_TOPIC,
                key=payload["Document"]["FIToFICstmrCdtTrf"]["CdtTrfTxInf"]["PmtId"]["EndToEndId"],
                value=json.dumps(payload),
                callback=delivery_report
            )
            producer.poll(0)

            count += 1
            if is_fraud:
                SENT_TRANSACTIONS.labels(type='fraud_injected').inc()

            # Cálculo y actualización periódica de TPS reales en Prometheus
            elapsed = time.time() - start_time
            if elapsed >= 1.0:
                CURRENT_TPS_GAUGE.set(count / elapsed)
                count = 0
                start_time = time.time()

            # Control preciso de ritmo (Pacing) para mantener los TPS deseados
            processing_time = time.time() - tx_start
            sleep_time = interval - processing_time
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("[TrafficGen] Deteniendo el generador de tráfico...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()