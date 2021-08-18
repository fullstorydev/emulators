## Build
FROM golang:1.11-buster AS build

WORKDIR /app

COPY go.mod ./
COPY bttest/ ./bttest
COPY cmd/ ./cmd

RUN go mod download
RUN cd cmd/cbtemulator/ && go build -o ../../cbtemulator

## Deploy
FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /app/cbtemulator /cbtemulator

USER nonroot:nonroot
EXPOSE 9000

ENTRYPOINT ["/cbtemulator", "-port", "9000", "-host", "0.0.0.0", "-dir", "var/bigtable"]
