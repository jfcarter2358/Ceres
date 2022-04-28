FROM golang:1.18.0-alpine

WORKDIR /ceresdb-build
COPY ceresdb /ceresdb-build
RUN env GOOS=linux CGO_ENABLED=0 go build -v -o ceresdb

FROM alpine:latest  

RUN adduser --disabled-password ceresdb

WORKDIR /home/ceresdb

COPY --from=0 /ceresdb-build/ceresdb ./
COPY template /home/ceresdb

RUN chown -R ceresdb:ceresdb /home/ceresdb

USER ceresdb

ENV CERESDB_CONFIG=/home/ceresdb/.ceresdb/config/config.json

CMD ["./ceresdb"]
