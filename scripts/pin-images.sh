#!/usr/bin/env bash
# pin-images.sh — Update kubernetes/kustomization.yaml to pin all GHCR images to
# a specific git SHA tag. Called by CI after a successful image build.
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
  if command -v kustomize &>/dev/null; then
    (cd kubernetes && kustomize edit set image \
      "ghcr.io/andrius-eng/${img}=ghcr.io/andrius-eng/${img}:${SHA}")
  else
    sed -i \
      "/name: ghcr\.io\/andrius-eng\/${img}/{n; s/newTag:.*/newTag: ${SHA}/}" \
      "$KUSTOMIZATION"
  fi
done

echo "Pinned all images to ${SHA} in ${KUSTOMIZATION}"
