# syntax=docker/dockerfile:1

FROM golang:1.26.1-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o /out/api-service ./cmd/api-service

# Run stage
FROM alpine:3.23.3

ENV USER=appuser
ENV GROUPNAME=appgroup
ENV UID=12345
ENV GID=23456

WORKDIR /home/$USER

RUN addgroup -g "$GID" "$GROUPNAME" && \
    adduser -D -h /home/$USER -G "$GROUPNAME" -u "$UID" "$USER"

COPY --from=builder /out/api-service /usr/local/bin/api-service

USER $USER

EXPOSE 50051

CMD ["api-service"]