FROM golang:1.20-alpine As builder
MAINTAINER Fullstory Engineering

# create non-privileged group and user
RUN addgroup -S emulators && adduser -S emulators -G emulators
RUN mkdir -p /data

ENV CGO_ENABLED=0
ENV GO111MODULE=on

WORKDIR /tmp/fullstorydev/bigtable
COPY VERSION bigtable /tmp/fullstorydev/bigtable/
RUN go build -o /cbtemulator \
    -ldflags "-w -extldflags \"-static\" -X \"main.version=$(cat VERSION)\"" \
    ./cmd/cbtemulator

WORKDIR /tmp/fullstorydev/storage
COPY VERSION storage /tmp/fullstorydev/storage/
RUN go build -o /gcsemulator \
    -ldflags "-w -extldflags \"-static\" -X \"main.version=$(cat VERSION)\"" \
    ./cmd/gcsemulator


### Deploy
FROM scratch AS cbtemulator
WORKDIR /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /cbtemulator /bin/cbtemulator
COPY --from=builder --chown=emulators /data /data
EXPOSE 9000
USER emulators
ENTRYPOINT ["/bin/cbtemulator", "-port", "9000", "-host", "0.0.0.0", "-dir", "/data"]


### Deploy
FROM scratch AS gcsemulator
WORKDIR /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /gcsemulator /bin/gcsemulator
COPY --from=builder --chown=emulators /data /data
EXPOSE 9000
USER emulators
ENTRYPOINT ["/bin/gcsemulator", "-port", "9000", "-host", "0.0.0.0", "-dir", "/data"]
