#!/usr/bin/env bash
# pin-images.sh — Pin all GHCR images to a specific git SHA in
# kubernetes/kustomization.yaml images: block.
# Called by CI after a successful image build.
#
# Usage: bash scripts/pin-images.sh <git-sha>
set -euo pipefail

SHA="${1:?Usage: pin-images.sh <git-sha>}"
KUSTOMIZATION="kubernetes/kustomization.yaml"

images=(
  "ml-airflow-custom"
  "ml-ml-pipeline"
  "ml-frontend"
  "ml-ws-server"
)

for img in "${images[@]}"; do
  # Match the name: line for this image, advance to next line, replace newTag
  sed -i \
    "/name: ghcr\.io\/andrius-eng\/${img}/{n; s/newTag:.*/newTag: ${SHA}/}" \
    "$KUSTOMIZATION"
done

echo "Pinned all images to ${SHA} in ${KUSTOMIZATION}"
