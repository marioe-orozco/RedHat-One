package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type TransactionMessage struct {
	TransactionID string  `json:"transaction_id"`
	ClientID      string  `json:"client_id"`
	Amount        float64 `json:"amount"`
	TargetAccount string  `json:"target_account"`
	Timestamp     string  `json:"timestamp"`
}

type ClientProfile struct {
	ClientID      string  `json:"client_id"`
	FullName      string  `json:"full_name"`
	Email         string  `json:"email"`
	AccountStatus string  `json:"account_status"`
	RiskScore     int     `json:"risk_score"`
	DailyLimit    float64 `json:"daily_limit"`
}

type EnrichedTransaction struct {
	TransactionMessage
	Client        ClientProfile `json:"client_profile"`
	EnrichedAt    string        `json:"enriched_at"`
	PipelineStage string        `json:"pipeline_stage"`
}

var ctx = context.Background()

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "10.217.5.92:9092")
	redisAddr := getEnv("REDIS_ADDR", "10.217.4.154:6379")
	postgresConnStr := getEnv("POSTGRES_CONN", "postgres://fraud_user:fraud_password@10.217.4.240:5432/frauddb?sslmode=disable")

	inboundTopic := getEnv("INBOUND_TOPIC", "spei_raw_inbound")
	enrichedTopic := getEnv("ENRICHED_TOPIC", "spei_enriched")
	notFoundTopic := getEnv("NOT_FOUND_TOPIC", "spei_not_found")
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

	// 2. Conexión a PostgreSQL (Fallback)
	db, err := sql.Open("postgres", postgresConnStr)
	if err != nil {
		log.Fatalf("Error al abrir conexión a PostgreSQL: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Error al hacer ping a PostgreSQL: %v", err)
	}
	log.Println("Conectado exitosamente a PostgreSQL (Fallback)")

	// 3. Configuración del consumidor de Kafka (Inbound) con tolerancias iniciales
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:       []string{kafkaBrokers},
		Topic:         inboundTopic,
		GroupID:       consumerGroup,
		MinBytes:      10e3, // 10KB
		MaxBytes:      10e6, // 10MB
		MaxWait:       3 * time.Second,
		QueueCapacity: 100,
	})
	defer reader.Close()

	// 4. Configuración de productores de Kafka
	writerEnriched := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    enrichedTopic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writerEnriched.Close()

	writerNotFound := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokers},
		Topic:    notFoundTopic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writerNotFound.Close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Aviso de Kafka (actualizando metadatos/reintentando): %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			log.Printf("Mensaje recibido de %s [Part: %d, Offset: %d]", inboundTopic, msg.Partition, msg.Offset)

			var tx TransactionMessage
			if err := json.Unmarshal(msg.Value, &tx); err != nil {
				log.Printf("Error deserializando JSON de transacción: %v | Data: %s", err, string(msg.Value))
				continue
			}

			var clientProfile ClientProfile
			found := false

			// A. Intentar buscar en Redis primero (Cache)
			redisKey := "client:" + tx.ClientID
			val, err := rdb.Get(ctx, redisKey).Result()
			if err == nil {
				if json.Unmarshal([]byte(val), &clientProfile) == nil {
					found = true
				}
			}

			// B. Si no está en Redis, hacer Fallback a PostgreSQL
			if !found {
				log.Printf("Cache miss en Redis para el cliente: %s. Consultando PostgreSQL...", tx.ClientID)
				query := "SELECT client_id, full_name, email, account_status, risk_score, daily_limit FROM clients WHERE client_id = $1"
				err = db.QueryRow(query, tx.ClientID).Scan(
					&clientProfile.ClientID,
					&clientProfile.FullName,
					&clientProfile.Email,
					&clientProfile.AccountStatus,
					&clientProfile.RiskScore,
					&clientProfile.DailyLimit,
				)
				if err == nil {
					found = true
					log.Printf("Cliente %s encontrado en PostgreSQL", tx.ClientID)
				}
			}

			// C. Decisión de enrutamiento final
			if !found {
				log.Printf("Cliente %s NO encontrado en Redis ni en Postgres. Enviando a %s", tx.ClientID, notFoundTopic)

				notFoundPayload, _ := json.Marshal(map[string]interface{}{
					"transaction": tx,
					"error":       "Client not found in cache or primary database",
					"timestamp":   time.Now().UTC().Format(time.RFC3339),
				})

				_ = writerNotFound.WriteMessages(ctx, kafka.Message{
					Key:   []byte(tx.TransactionID),
					Value: notFoundPayload,
				})
				continue
			}

			// D. Si se encontró -> Construir y publicar en spei_enriched
			enriched := EnrichedTransaction{
				TransactionMessage: tx,
				Client:             clientProfile,
				EnrichedAt:         time.Now().UTC().Format(time.RFC3339),
				PipelineStage:      "ENRICHED_SUCCESS",
			}

			enrichedPayload, err := json.Marshal(enriched)
			if err != nil {
				log.Printf("Error serializando mensaje enriquecido: %v", err)
				continue
			}

			err = writerEnriched.WriteMessages(ctx, kafka.Message{
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
	log.Println("Apagando microservicio ordenadamente :) ...")
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
