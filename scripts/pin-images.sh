#!/usr/bin/env bash
# pin-images.sh — Pin all GHCR images to a specific git SHA across all
# kustomization files that carry image pins (split-app layout).
# Called by CI after a successful image build.
#
# Usage: bash scripts/pin-images.sh <git-sha>
set -euo pipefail

SHA="${1:?Usage: pin-images.sh <git-sha>}"

# infra carries all 4 images (rollout-guard prepull DaemonSet uses them all)
# data carries ml-airflow-custom only
# ml carries ml-ml-pipeline, ml-frontend, ml-ws-server
declare -A GROUP_IMAGES
GROUP_IMAGES["kubernetes/infra/kustomization.yaml"]="ml-airflow-custom ml-ml-pipeline ml-frontend ml-ws-server"
GROUP_IMAGES["kubernetes/data/kustomization.yaml"]="ml-airflow-custom"
GROUP_IMAGES["kubernetes/ml/kustomization.yaml"]="ml-ml-pipeline ml-frontend ml-ws-server"

for kustomization in "${!GROUP_IMAGES[@]}"; do
  read -ra images <<< "${GROUP_IMAGES[$kustomization]}"
  for img in "${images[@]}"; do
    sed -i \
      "/name: ghcr\.io\/andrius-eng\/${img}/{n; s/newTag:.*/newTag: ${SHA}/}" \
      "$kustomization"
  done
  echo "Pinned ${GROUP_IMAGES[$kustomization]} to ${SHA} in ${kustomization}"
done
