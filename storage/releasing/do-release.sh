#!/bin/bash

# strict mode
set -euo pipefail
IFS=$'\n\t'

if [[ -z ${DRY_RUN:-} ]]; then
    PREFIX=""
else
    PREFIX="echo"
fi

# input validation
if [[ -z ${GITHUB_TOKEN:-} ]]; then
    echo "GITHUB_TOKEN environment variable must be set before running." >&2
    exit 1
fi
if [[ $# -ne 1 || $1 == "" ]]; then
    echo "This program requires one argument: the version number, in 'vM.N.P' format." >&2
    exit 1
fi
VERSION=$1

# Change to root of the repo
cd "$(dirname "$0")/.."

# Docker release

# make sure credentials are valid for later push steps; this might
# be interactive since this will prompt for username and password
# if there are no valid current credentials.
$PREFIX docker login
echo "$VERSION" > VERSION
# Docker Buildx support is included in Docker 19.03
# Below step installs emulators for different architectures on the host
# This enables running and building containers for below architectures mentioned using --platforms
$PREFIX docker run --privileged --rm tonistiigi/binfmt:qemu-v6.1.0 --install all
# Create a new builder instance
export DOCKER_CLI_EXPERIMENTAL=enabled
$PREFIX docker buildx create --use --name multiarch-builder --node multiarch-builder0
# push to docker hub, both the given version as a tag and for "latest" tag
$PREFIX docker buildx build --target=gcsemulator --platform linux/amd64,linux/s390x,linux/arm64,linux/ppc64le --tag fullstorydev/gcsemulator:${VERSION} --tag fullstorydev/gcsemulator:latest --push --progress plain --no-cache .
rm VERSION
