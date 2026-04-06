#!/bin/bash
# One-shot cleanup: Docker image prune + k3s image prune + fstrim
set -e

echo "=== Docker status ==="
systemctl is-active docker || true

echo "=== Docker disk before ==="
docker system df 2>&1 || true

echo "=== Pruning all unused Docker images ==="
docker image prune -af 2>&1 || true

echo "=== Docker build cache prune ==="
docker builder prune -af 2>&1 || true

echo "=== Docker disk after ==="
docker system df 2>&1 || true

echo "=== k3s crictl rmi --prune ==="
k3s crictl rmi --prune 2>&1 | grep -v "^E" | head -20 || true

echo "=== Filesystem after ==="
df -h /

echo "=== Running fstrim ==="
fstrim -v /

echo "=== Filesystem final ==="
df -h /

echo "ALL DONE"
