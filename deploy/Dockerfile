FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/burrow-broker ./cmd/broker
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/burrow-cli ./cmd/cli

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /bin/burrow-broker /burrow-broker
COPY --from=builder /bin/burrow-cli    /burrow-cli
EXPOSE 9092 9093
ENTRYPOINT ["/burrow-broker"]
