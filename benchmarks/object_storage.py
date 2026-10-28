#!/usr/bin/env python3
"""Measure object upload/download, metadata reads, replica bytes, and repair time."""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter

DEFAULT_NODES = {
    "node-1": {"url": "http://localhost:9001", "container": "objectstore-node-1"},
    "node-2": {"url": "http://localhost:9002", "container": "objectstore-node-2"},
    "node-3": {"url": "http://localhost:9003", "container": "objectstore-node-3"},
}


def request(method: str, url: str, body: bytes | None = None, timeout: float = 10):
    req = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/octet-stream")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, response.read(), response.headers
    except urllib.error.HTTPError as error:
        return error.code, error.read(), error.headers


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def parse_mapping(values: list[str], field: str) -> dict[str, str]:
    parsed = {}
    for value in values:
        if "=" not in value:
            raise argparse.ArgumentTypeError(f"{field} must use NODE_ID=VALUE")
        key, item = value.split("=", 1)
        parsed[key] = item
    return parsed


def wait_for_node(base_url: str, timeout_seconds: int = 60) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            status, _, _ = request("GET", f"{base_url}/actuator/health", timeout=2)
            if status == 200:
                return
        except OSError:
            pass
        time.sleep(0.5)
    raise TimeoutError(f"storage node did not become healthy: {base_url}")


def measure_replica_bytes(metadata: dict, node_urls: dict[str, str]) -> int:
    stored_bytes = 0
    for chunk in metadata["chunks"]:
        for node_id in chunk["nodes"]:
            status, body, headers = request(
                "GET", f"{node_urls[node_id]}/chunks/{chunk['chunkId']}"
            )
            if status != 200:
                raise RuntimeError(
                    f"could not read replica {chunk['chunkId']} from {node_id}: HTTP {status}"
                )
            if headers.get("X-Chunk-Sha256", "").lower() != chunk["sha256"].lower():
                raise RuntimeError(f"checksum mismatch while counting replica bytes on {node_id}")
            stored_bytes += len(body)
    return stored_bytes


def has_full_redundancy(
    metadata: dict, node_urls: dict[str, str], unavailable_node: str, factor: int
) -> bool:
    for chunk in metadata["chunks"]:
        present = 0
        for node_id in chunk["nodes"]:
            if node_id == unavailable_node:
                continue
            try:
                status, _, _ = request(
                    "HEAD", f"{node_urls[node_id]}/chunks/{chunk['chunkId']}", timeout=2
                )
            except urllib.error.URLError:
                continue
            present += status == 200
        if present < factor:
            return False
    return True


def run() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--size-mib", type=int, default=8)
    parser.add_argument("--metadata-samples", type=int, default=100)
    parser.add_argument("--replication-factor", type=int, default=2)
    parser.add_argument("--repair-timeout-seconds", type=int, default=180)
    parser.add_argument("--node-url", action="append", default=[])
    parser.add_argument("--node-container", action="append", default=[])
    args = parser.parse_args()

    if args.size_mib < 1 or args.metadata_samples < 1:
        parser.error("size-mib and metadata-samples must be positive")
    if args.replication_factor < 2:
        parser.error("replication-factor must be at least 2")

    node_urls = {key: value["url"] for key, value in DEFAULT_NODES.items()}
    node_containers = {key: value["container"] for key, value in DEFAULT_NODES.items()}
    node_urls.update(parse_mapping(args.node_url, "--node-url"))
    node_containers.update(parse_mapping(args.node_container, "--node-container"))

    base_url = args.base_url.rstrip("/")
    object_key = f"benchmark-{uuid.uuid4().hex}"
    payload = bytes(
        (index * 31 + 17) % 251
        for index in range(args.size_mib * 1024 * 1024)
    )
    object_url = f"{base_url}/api/objects/{object_key}"
    created = False
    down_node = None
    down_container = None
    recovery_seconds = None
    survived_loss = False

    try:
        start = time.perf_counter()
        status, _, _ = request("PUT", object_url, payload, timeout=120)
        upload_seconds = time.perf_counter() - start
        if status != 201:
            raise RuntimeError(f"upload returned HTTP {status}")
        created = True

        start = time.perf_counter()
        status, downloaded, _ = request("GET", object_url, timeout=120)
        download_seconds = time.perf_counter() - start
        if status != 200 or downloaded != payload:
            raise RuntimeError(f"download verification failed (HTTP {status})")

        metadata_url = f"{object_url}/metadata"
        status, metadata_body, _ = request("GET", metadata_url)
        if status != 200:
            raise RuntimeError(f"metadata lookup returned HTTP {status}")
        import json
        metadata = json.loads(metadata_body)

        metadata_latencies = []
        for _ in range(args.metadata_samples):
            start = time.perf_counter()
            status, _, _ = request("GET", metadata_url)
            elapsed_ms = (time.perf_counter() - start) * 1000
            if status != 200:
                raise RuntimeError(f"metadata sample returned HTTP {status}")
            metadata_latencies.append(elapsed_ms)

        replica_bytes = measure_replica_bytes(metadata, node_urls)
        replicas_per_node = Counter(
            node_id for chunk in metadata["chunks"] for node_id in chunk["nodes"]
        )
        down_node = max(replicas_per_node, key=replicas_per_node.get)
        down_container = node_containers[down_node]
        print(
            f"Simulating loss of {down_node} ({down_container}); "
            f"it holds {replicas_per_node[down_node]} replica(s) for this object."
        )

        subprocess.run(["docker", "stop", down_container], check=True, capture_output=True)
        repair_started = time.perf_counter()
        status, recovered_bytes, _ = request("GET", object_url, timeout=120)
        survived_loss = status == 200 and recovered_bytes == payload
        if not survived_loss:
            raise RuntimeError(f"object was not readable during node loss (HTTP {status})")

        deadline = repair_started + args.repair_timeout_seconds
        while time.monotonic() < deadline:
            status, current_body, _ = request("GET", metadata_url)
            if status == 200:
                current_metadata = json.loads(current_body)
                if has_full_redundancy(
                    current_metadata, node_urls, down_node, args.replication_factor
                ):
                    recovery_seconds = time.perf_counter() - repair_started
                    break
            time.sleep(1)

        if recovery_seconds is None:
            raise TimeoutError(
                f"replicas did not recover within {args.repair_timeout_seconds}s"
            )

        print("\nObject-storage benchmark")
        print(f"object size:                 {len(payload) / (1024 * 1024):.2f} MiB")
        print(f"chunks:                      {metadata['chunkCount']}")
        print(f"replication factor:          {args.replication_factor}")
        print(f"upload throughput:           {len(payload) / upload_seconds / (1024 * 1024):.2f} MiB/s")
        print(f"download throughput:         {len(downloaded) / download_seconds / (1024 * 1024):.2f} MiB/s")
        print(f"replica bytes:               {replica_bytes}")
        print(f"storage overhead:             {replica_bytes / len(payload):.2f}x")
        print(f"metadata latency (p50):      {percentile(metadata_latencies, 0.50):.2f} ms")
        print(f"metadata latency (p95):      {percentile(metadata_latencies, 0.95):.2f} ms")
        print(f"recovery after node loss:    {recovery_seconds:.2f} s")
        print(f"download survived node loss: {survived_loss}")
    finally:
        if down_container is not None:
            subprocess.run(["docker", "start", down_container], check=False, capture_output=True)
            if down_node is not None:
                try:
                    wait_for_node(node_urls[down_node])
                except OSError as error:
                    print(
                        f"warning: node restart health check did not settle: {error}",
                        file=sys.stderr,
                    )
        if created:
            status, _, _ = request("DELETE", object_url, timeout=120)
            if status != 204:
                print(f"warning: cleanup returned HTTP {status} for {object_key}", file=sys.stderr)


if __name__ == "__main__":
    try:
        run()
    except (OSError, RuntimeError, TimeoutError, KeyError, ValueError) as error:
        print(f"benchmark failed: {error}", file=sys.stderr)
        sys.exit(1)
