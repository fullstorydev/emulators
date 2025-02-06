FROM golang:1.23-alpine As builder
MAINTAINER Fullstory Engineering

# create non-privileged group and user
RUN addgroup -S emulators && adduser -S emulators -G emulators
RUN mkdir -p /data
# copy just the files/sources we need to build the emulator
WORKDIR /tmp/fullstorydev/bigtable
COPY VERSION *.go go.* /tmp/fullstorydev/bigtable/
COPY bttest /tmp/fullstorydev/bigtable/bttest
COPY cmd /tmp/fullstorydev/bigtable/cmd
# and build a completely static binary (so we can use
# scratch as basis for the final image)
ENV CGO_ENABLED=0
ENV GO111MODULE=on
RUN go build -o /cbtemulator \
    -ldflags "-w -extldflags \"-static\" -X \"main.version=$(cat VERSION)\"" \
    ./cmd/cbtemulator

# New FROM so we have a nice'n'tiny image
FROM scratch AS cbtemulator
WORKDIR /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /cbtemulator /bin/cbtemulator
COPY --from=builder --chown=emulators /data /data
EXPOSE 9000
USER emulators
ENTRYPOINT ["/bin/cbtemulator", "-port", "9000", "-host", "0.0.0.0", "-dir", "/data"]
