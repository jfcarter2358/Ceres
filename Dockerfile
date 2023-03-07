FROM golang:1.18.0-alpine

WORKDIR /ceresdb-build
COPY ceresdb /ceresdb-build
RUN env GOOS=linux CGO_ENABLED=0 go build -v -o ceresdb

FROM alpine:latest  

RUN adduser --disabled-password ceresdb
RUN apk add curl jq bash netcat-openbsd

WORKDIR /home/ceresdb

COPY --from=0 /ceresdb-build/ceresdb ./
COPY template /home/ceresdb
ADD RUN.sh /home/ceresdb/RUN.sh

RUN chmod +x /home/ceresdb/RUN.sh

RUN chown -R ceresdb:ceresdb /home/ceresdb

USER ceresdb

ENV CERESDB_CONFIG=/home/ceresdb/.ceresdb/config/config.json

CMD ["./RUN.sh"]
