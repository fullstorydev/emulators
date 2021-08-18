## Build
FROM golang:1.13-buster AS build

WORKDIR /app

COPY go.mod ./
COPY cmd/ ./cmd
COPY gcsemu/ ./gcsemu
COPY gcsutil/ ./gcsutil

RUN go mod download
RUN cd cmd/gcsemulator/ && go build -o ../../gcsemulator

## Deploy
FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /app/gcsemulator /gcsemulator

EXPOSE 9000

ENTRYPOINT ["/gcsemulator", "-port", "9000", "-host", "0.0.0.0", "-dir", "var/storage"]
