#!/usr/bin/env bash
# pin-images.sh — Update production kustomization to pin all GHCR images to a
# specific git SHA tag. Called by CI after a successful image build.
#
# Usage: bash scripts/pin-images.sh <git-sha>
#   e.g. bash scripts/pin-images.sh abc1234
set -euo pipefail

SHA="${1:?Usage: pin-images.sh <git-sha>}"
KUSTOMIZATION="kubernetes/overlays/production/kustomization.yaml"

images=(
  "ml-airflow-custom"
  "ml-ml-pipeline"
  "ml-frontend"
  "ml-ws-server"
)

for img in "${images[@]}"; do
  # Use kustomize edit if available, otherwise fall back to sed
  if command -v kustomize &>/dev/null; then
    (cd kubernetes/overlays/production && \
      kustomize edit set image "ghcr.io/andrius-eng/${img}=ghcr.io/andrius-eng/${img}:${SHA}")
  else
    sed -i \
      "/name: ghcr\.io\/andrius-eng\/${img}/{n; s/newTag:.*/newTag: ${SHA}/}" \
      "$KUSTOMIZATION"
  fi
done

echo "Pinned all images to ${SHA} in ${KUSTOMIZATION}"
