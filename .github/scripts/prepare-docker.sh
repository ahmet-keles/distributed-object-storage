#!/usr/bin/env bash
# Fail fast with a readable diagnosis when Docker is unusable, then pre-pull
# the images the test suite needs so a registry hiccup surfaces here — as a
# retried, clearly-attributed pull failure — instead of as a Testcontainers
# startup error buried in a test log.
#
# Usage: prepare-docker.sh IMAGE [IMAGE...]
set -euo pipefail

if [ "$#" -eq 0 ]; then
    echo "usage: $0 IMAGE [IMAGE...]" >&2
    exit 2
fi

if ! command -v docker > /dev/null 2>&1; then
    echo "::error::docker is not installed on this runner. Testcontainers needs a Docker engine; use a runner image that ships one (ubuntu-latest does)."
    exit 1
fi

if ! docker info > /dev/null 2>&1; then
    echo "::error::The Docker daemon is not reachable. Testcontainers needs a running engine; 'docker info' must succeed. On self-hosted runners start dockerd and expose /var/run/docker.sock to this job."
    docker info 2>&1 | tail -5 || true
    exit 1
fi

# Logged so version-skew failures (e.g. a daemon whose minimum API version
# rejects an old client) are diagnosable from the job log alone.
docker version --format 'Docker {{.Server.Version}} (server API {{.Server.APIVersion}}, min API {{.Server.MinAPIVersion}}); client API {{.Client.APIVersion}}'

for image in "$@"; do
    for attempt in 1 2 3; do
        if docker pull --quiet "$image"; then
            break
        fi
        if [ "$attempt" -eq 3 ]; then
            echo "::error::Failed to pull $image after 3 attempts. Registry outage or rate limit; re-run the job, or authenticate pulls to raise the anonymous Docker Hub limit."
            exit 1
        fi
        echo "pull of $image failed (attempt $attempt); retrying..."
        sleep $((attempt * 10))
    done
done
