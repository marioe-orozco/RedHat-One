Ejecutar Postgress
Crear tablas
oc exec -i deployment/postgres -n redhat-one-rf-demo -- psql -U fraud_user -d frauddb <<EOF
CREATE TABLE IF NOT EXISTS clients (
    client_id VARCHAR(50) PRIMARY KEY,
    full_name VARCHAR(150) NOT NULL,
    email VARCHAR(100),
    account_status VARCHAR(20) DEFAULT 'ACTIVE',
    risk_score INT DEFAULT 0,
    daily_limit DECIMAL(15, 2) DEFAULT 50000.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    client_id VARCHAR(50) REFERENCES clients(client_id),
    amount DECIMAL(15, 2) NOT NULL,
    target_account VARCHAR(50) NOT NULL,
    status VARCHAR(30) DEFAULT 'PENDING',
    fraud_reason VARCHAR(255),
    transaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO clients (client_id, full_name, email, risk_score, daily_limit) 
VALUES 
    ('CLI-001', 'Mario Orozco', 'mario.orozco@example.com', 5, 100000.00),
    ('CLI-002', 'Juan Pérez', 'juan.perez@example.com', 85, 20000.00)
ON CONFLICT (client_id) DO NOTHING;
EOF

Consultar los registros de ejemplo
oc exec -it deployment/postgres -n redhat-one-rf-demo -- psql -U fraud_user -d frauddb -c "SELECT * FROM clients;"

mariooro@iMac-de-Mario Repositorios % oc exec -it deployment/postgres -n redhat-one-rf-demo -- psql -U fraud_user -d frauddb -c "SELECT * FROM clients;"
 client_id |  full_name   |          email           | account_status | risk_score | daily_limit |         created_at         |         updated_at         
-----------+--------------+--------------------------+----------------+------------+-------------+----------------------------+----------------------------
 CLI-001   | Mario Orozco | mario.orozco@example.com | ACTIVE         |          5 |   100000.00 | 2026-10-16 00:03:55.677267 | 2026-10-16 00:03:55.677267
 CLI-002   | Juan Pérez   | juan.perez@example.com   | ACTIVE         |         85 |    20000.00 | 2026-10-16 00:03:55.677267 | 2026-10-16 00:03:55.677267

instalar redis
oc apply -f redis.yaml -n redhat-one-rf-demo

Cargar la DB con 200,000 clientes y 1,000,000 de transacciones

oc exec -it deployment/postgres -n redhat-one-rf-demo -- psql -U fraud_user -d frauddb -c "
INSERT INTO clients (client_id, full_name, email, account_status, risk_score, daily_limit)
SELECT 
    'CLI-' || LPAD(i::TEXT, 6, '0'),
    'Cliente ' || i,
    'cliente' || i || '@example.com',
    CASE WHEN i % 10 = 0 THEN 'BLOCKED' ELSE 'ACTIVE' END,
    (RANDOM() * 100)::INT,
    (5000 + (RANDOM() * 95000))::DECIMAL(15,2)
FROM generate_series(3, 200000) AS t(i)
ON CONFLICT (client_id) DO NOTHING;
"


Subir los datos a redis
ejecutar el Job temporal en OpenShift (redis-sync-job), para la migración masiva de los 200,000 registros desde PostgreSQL hacia Redis
oc port-forward svc/postgres-service 5432:5432 -n redhat-one-rf-demo
oc port-forward svc/redis-service 6379:6379 -n redhat-one-rf-demo
pip3 install redis psycopg2-binary
python3 sync.py



validar que los datos estan en redis
# Consultar puntaje de riesgo
oc exec -it deployment/redis -n redhat-one-rf-demo -- redis-cli GET client:CLI-000100:risk_score

# Consultar límite diario
oc exec -it deployment/redis -n redhat-one-rf-demo -- redis-cli GET client:CLI-000100:daily_limit


Instalar la instancia del broker de kafka

oc apply -f broker.yaml -n kafka
oc apply -f kafka-connect.yaml -n kafka

topicos

Instalar el conector de kafka para postgress
oc apply -f debezium-connector.yaml -n kafka

oc create imagestream kafka-connect-debezium -n kafka


oc exec -it postgres-6b7b84d4d5-f4vtg -n redhat-one-rf-demo -- psql -U fraud_user -d frauddb -c "ALTER SYSTEM SET wal_level = logical;"



oc rollout restart deployment/postgres-service -n redhat-one-rf-demo

oc exec -it postgres-7866575766-t9nfl -n redhat-one-rf-demo -- psql -U fraud_user -d frauddb -c "ALTER SYSTEM SET wal_level = logical;"

oc rollout restart deployment/postgres -n redhat-one-rf-demo


oc patch deployment postgres -n redhat-one-rf-demo --type='json' -p='[
  {
    "op": "add",
    "path": "/spec/template/spec/containers/0/command",
    "value": ["postgres"]
  },
  {
    "op": "add",
    "path": "/spec/template/spec/containers/0/args",
    "value": ["-c", "wal_level=logical", "-c", "max_replication_slots=4", "-c", "max_wal_senders=4"]
  }
]'



oc exec -it deployment/postgres -n redhat-one-rf-demo -- sh -c "echo \"wal_level = logical\" >> /tmp/pgdata/postgresql.conf"

oc rollout restart deployment/postgres -n redhat-one-rf-demo

oc get pods -n redhat-one-rf-demo
oc describe kafkaconnector postgres-fraud-connector -n kafka


oc describe kafkaconnector postgres-fraud-connector -n kafka


Probar crear un cliente en postgress



oc exec -it postgres-bb97ccbdb-dtf98 -n redhat-one-rf-demo -- psql -U fraud_user -d frauddb -c "INSERT INTO clients (client_id, risk_score, daily_limit) VALUES ('CLI-999999', 75, 50000.00) ON CONFLICT (client_id) DO UPDATE SET risk_score = 75, daily_limit = 50000.00;"
oc exec -it postgres-bb97ccbdb-dtf98 -n redhat-one-rf-demo -- psql -U fraud_user -d frauddb -c "INSERT INTO clients (client_id, full_name, risk_score, daily_limit) VALUES ('CLI-999999', 'Nombre de Ejemplo', 75, 50000.00) ON CONFLICT (client_id) DO UPDATE SET risk_score = 75, daily_limit = 50000.00;"


verificar que exxiste en postgress




validar lo mismo en redis



oc exec -it deployment/redis -n redhat-one-rf-demo -- redis-cli DBSIZE


// ***********************

Crear una aplicacion que enriquesca los datos de transacciones y clientes,tomando los datos de redis y postgress, y enviando la informacion enriquecida a kafka al topico de spei.enriched

Bash
mkdir spei-enricher
cd spei-enricher
go mod init spei-enricher


/* Codigo de main.go */
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
)

// Estructura de la transacción entrante (Inbound)
type TransactionMessage struct {
	TransactionID string  `json:"transaction_id"`
	ClientID      string  `json:"client_id"`
	Amount        float64 `json:"amount"`
	TargetAccount string  `json:"target_account"`
	Timestamp     string  `json:"timestamp"`
}

// Estructura del cliente enriquecido desde Redis
type ClientProfile struct {
	ClientID      string  `json:"client_id"`
	FullName      string  `json:"full_name"`
	Email         string  `json:"email"`
	AccountStatus string  `json:"account_status"`
	RiskScore     int     `json:"risk_score"`
	DailyLimit    float64 `json:"daily_limit"`
}

// Estructura final enviada al tópico enriquecido
type EnrichedTransaction struct {
	TransactionMessage
	Client        ClientProfile `json:"client_profile"`
	EnrichedAt    string        `json:"enriched_at"`
	PipelineStage string        `json:"pipeline_stage"`
}

var ctx = context.Background()

func main() {
	// Configuración de variables de entorno con valores por defecto para OpenShift
	kafkaBrokers := getEnv("KAFKA_BROKERS", "fraud-kafka-cluster-kafka-bootstrap.kafka.svc:9092")
	redisAddr := getEnv("REDIS_ADDR", "redis-service.redhat-one-rf-demo.svc:6379")
	inboundTopic := getEnv("INBOUND_TOPIC", "spei_raw_inbound")
	enrichedTopic := getEnv("ENRICHED_TOPIC", "spei_enriched")
	consumerGroup := getEnv("CONSUMER_GROUP", "spei-enricher-group")

	log.Println("Iniciando microservicio enriquecedor SPEI...")

	// 1. Conexión a Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatalf("Error al conectar a Redis: %v", err)
	}
	log.Println("Conectado exitosamente a Redis")

	// 2. Configuración del consumidor de Kafka
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    inboundTopic,
		GroupID:  consumerGroup,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	// 3. Configuración del productor de Kafka
	writer := &kafka.Writer{
		Addr:     kafka.KafkaGoConn(kafkaBrokers), // O kafka.TCP(kafkaBrokers)
		Topic:    enrichedTopic,
		Balancer: &kafka.LeastBytes{},
	}
	// Ajuste limpio para writer TCP
	writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    enrichedTopic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	// Manejo de señales para apagado graceful
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error leyendo mensaje de Kafka: %v", err)
				break
			}

			log.Printf("Mensaje recibido de %s [Part: %d, Offset: %d]", inboundTopic, msg.Partition, msg.Offset)

			// Parsear mensaje JSON de la transacción
			var tx TransactionMessage
			if err := json.Unmarshal(msg.Value, &tx); err != nil {
				log.Printf("Error deserializando JSON de transacción: %v | Data: %s", err, string(msg.Value))
				continue
			}

			// Consultar Redis para obtener el perfil del cliente (Asumiendo clave en Redis como "client:CLI-XXX" o el ID directo)
			redisKey := "client:" + tx.ClientID
			val, err := rdb.Get(ctx, redisKey).Result()
			
			var clientProfile ClientProfile
			if err == nil {
				// Si existe en Redis, lo parseamos
				_ = json.Unmarshal([]byte(val), &clientProfile)
			} else {
				log.Printf("Advertencia: No se encontró el cliente %s en Redis: %v", tx.ClientID, err)
				clientProfile = ClientProfile{
					ClientID:      tx.ClientID,
					FullName:      "UNKNOWN",
					AccountStatus: "UNKNOWN",
					RiskScore:     99, // Alto riesgo por defecto si no existe
					DailyLimit:    0.0,
				}
			}

			// Construir el mensaje enriquecido
			enriched := EnrichedTransaction{
				TransactionMessage: tx,
				Client:             clientProfile,
				EnrichedAt:         time.Now().UTC().Format(time.RFC3339),
				PipelineStage:      "ENRICHED_BY_REDIS",
			}

			enrichedPayload, err := json.Marshal(enriched)
			if err != nil {
				log.Printf("Error serializando mensaje enriquecido: %v", err)
				continue
			}

			// Publicar en el tópico spei_enriched
			err = writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(tx.TransactionID),
				Value: enrichedPayload,
			})
			if err != nil {
				log.Printf("Error publicando en Kafka (%s): %v", enrichedTopic, err)
			} else {
				log.Printf("Transacción %s enriquecida y publicada exitosamente en %s", tx.TransactionID, enrichedTopic)
			}
		}
	}()

	<-sigchan
	log.Println("Apagando microservicio ordenadamente...")
}

func getEnv(key, fallback string) string {
	if value, exists := os.Lookup(key); exists {
		return value
	}
	return fallback
}

/* Codigo de main.go */


Creamos el Dockerfile
cat << 'EOF' > Dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod download && CGO_ENABLED=0 GOOS=linux go build -o spei-enricher main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/spei-enricher .
CMD ["./spei-enricher"]
EOF



Empaquetar el spei-enricher en un contenedor y subirlo a OpenShift


oc extract secret/router-certs-default -n openshift-ingress --keys=tls.crt --to=- > /tmp/crc-ca.crt
sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain /tmp/crc-ca.crt


reinicar dockerdesktop
osascript -e 'quit app "Docker"'
open -a Docker

docker login





docker login -u $(oc whoami) -p $(oc whoami -t) default-route-openshift-image-registry.apps-crc.testing


go version - actualziazr DockerFile a golang:1.27-alpine


docker build -t default-route-openshift-image-registry.apps-crc.testing/redhat-one-rf-demo/spei-enricher:latest .


Sube la imagen a OpenShift:
oc create imagestream spei-enricher -n redhat-one-rf-demo

docker push default-route-openshift-image-registry.apps-crc.testing/redhat-one-rf-demo/spei-enricher:latest


cat << 'EOF' > enricher-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spei-enricher
  namespace: redhat-one-rf-demo
  labels:
    app: spei-enricher
spec:
  replicas: 1
  selector:
    matchLabels:
      app: spei-enricher
  template:
    metadata:
      labels:
        app: spei-enricher
    spec:
      containers:
      - name: spei-enricher
        image: default-route-openshift-image-registry.apps-crc.testing/redhat-one-rf-demo/spei-enricher:latest
        imagePullPolicy: Always
        env:
        - name: KAFKA_BROKERS
          value: "fraud-kafka-cluster-kafka-bootstrap.kafka.svc:9092"
        - name: REDIS_ADDR
          value: "redis-service.redhat-one-rf-demo.svc:6379"
        - name: POSTGRES_CONN
          value: "postgres://fraud_user:fraud_password@postgres-service.redhat-one-rf-demo.svc.cluster.local:5432/frauddb?sslmode=disable"
        - name: INBOUND_TOPIC
          value: "spei_raw_inbound"
        - name: ENRICHED_TOPIC
          value: "spei_enriched"
        - name: NOT_FOUND_TOPIC
          value: "spei_not_found"
        - name: CONSUMER_GROUP
          value: "spei-enricher-group"
        resources:
          limits:
            cpu: "500m"
            memory: "256Mi"
          requests:
            cpu: "100m"
            memory: "128Mi"
        securityContext:
          allowPrivilegeEscalation: false
          runAsNonRoot: true
          capabilities:
            drop:
            - ALL
EOF




oc adm policy add-scc-to-user privileged -z spei-enricher-sa -n redhat-one-rf-demo




consulatr topicos
oc run kafka-consumer-debug --rm -i --tty \
  --image=quay.io/strimzi/kafka:latest-kafka-3.5.0 \
  --restart=Never \
  --namespace=kafka \
  -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.217.5.92:9092 \
  --topic spei_raw_inbound \
  --from-beginning



  Contruyamos la imagen del generador de transacciones y subamos a OpenShift
  docker build -t default-route-openshift-image-registry.apps-crc.testing/redhat-one-rf-demo/spei-traffic-generator:latest .
  docker push default-route-openshift-image-registry.apps-crc.testing/redhat-one-rf-demo/spei-traffic-generator:latest




  oc logs deployment/spei-enricher -n redhat-one-rf-demo -f



  oc get kafka -A
oc get kafkatopic -A

oc get pods -n kafka

oc exec -it kafka-cluster-redhat-one-kafka-0 -n kafka -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic spei-raw-inbound \
  --from-beginning \
  --max-messages 10


  

-- CREACION DE DASHBOARD DE OPENSHIFT




Recrear instancia de Grafana en OpenShift
oc apply -n openshift-operators -f - <<EOF
apiVersion: grafana.integreatly.org/v1beta1
kind: Grafana
metadata:
  name: grafana-instance
  namespace: openshift-operators
  labels:
    dashboards: "grafana"
spec:
  dashboardLabelSelector:
    - matchLabels:
        dashboards: "grafana"
EOF