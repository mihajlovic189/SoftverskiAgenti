FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go.mod download || true

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o fl-node main.go

FROM alpine:latest
WORKDIR /app

COPY --from=builder /app/fl-node .
COPY --from=builder /app/data ./data

ENTRYPOINT ["./fl-node"]