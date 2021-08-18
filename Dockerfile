## Build
FROM golang:1.13-buster AS build

COPY bigtable /src/bigtable
COPY storage /src/storage

WORKDIR /src/bigtable/
RUN go mod download
RUN go build -o /cbtemulator ./cmd/cbtemulator

WORKDIR /src/storage/
RUN go mod download
RUN go build -o /gcsemulator ./cmd/gcsemulator

## Deploy
FROM gcr.io/distroless/base-debian10 AS cbtemulator

WORKDIR /

COPY --from=build /cbtemulator /cbtemulator

EXPOSE 9000

ENTRYPOINT ["/cbtemulator", "-port", "9000", "-host", "0.0.0.0", "-dir", "var/bigtable"]


## Deploy
FROM gcr.io/distroless/base-debian10 AS gcsemulator

WORKDIR /

COPY --from=build /gcsemulator /gcsemulator

EXPOSE 9000

ENTRYPOINT ["/gcsemulator", "-port", "9000", "-host", "0.0.0.0", "-dir", "var/storage"]
