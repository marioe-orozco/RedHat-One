FROM golang:1.27-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod download && CGO_ENABLED=0 GOOS=linux go build -o spei-enricher main.go

FROM alpine:latest
WORKDIR /app

# Crear el grupo y el usuario con UID numérico 10001 y directorio /app
RUN addgroup -g 10001 appgroup && \
    adduser -u 10001 -S -G appgroup -h /app appuser

# Copiar el binario compilado a /app con permisos para el usuario 10001
COPY --from=builder --chown=10001:10001 /app/spei-enricher .

# Ejecutar como usuario numérico
USER 10001

CMD ["./spei-enricher"]