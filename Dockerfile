# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /ghostmq ./main.go

# Runtime stage
FROM scratch

COPY --from=builder /ghostmq /ghostmq
COPY ghostmq.yaml /ghostmq.yaml

EXPOSE 8080

ENTRYPOINT ["/ghostmq"]
