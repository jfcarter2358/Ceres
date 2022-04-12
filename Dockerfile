FROM golang:1.18.0-alpine

WORKDIR /ceres-build
COPY ceres /ceres-build
RUN env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -v -o ceres

FROM alpine:latest  
WORKDIR /ceres
COPY --from=0 /ceres-build/ceres ./
COPY template /ceres

ENV CERES_CONFIG=/ceres/.ceres/config/config.json

CMD ["./ceres"]
