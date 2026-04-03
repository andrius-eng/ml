#!/usr/bin/env bash
# setup-nfs-storage.sh — Run ONCE on the infra node (desktop-nnutaj7 / WSL2)
# Sets up NFS server and Helm provisioner so airflow-data and ml-output PVCs
# are backed by a shared NFS export accessible from all k3s nodes.
#
# Prerequisites:
#   - Helm installed: sudo snap install helm --classic
#   - k3s kubeconfig accessible (KUBECONFIG or default ~/.kube/config)
#   - Tailscale running (infra Tailscale IP used as NFS server address)
#
# Usage: bash kubernetes/scripts/setup-nfs-storage.sh

set -euo pipefail

INFRA_TS_IP="100.95.8.71"          # infra node Tailscale IP (NFS server)
NFS_EXPORT_DIR="/data/k8s-nfs"
POD_CIDR="10.42.0.0/16"
COMPUTE_TS_IP="100.66.184.9"       # worker node Tailscale IP
AIRFLOW_UID="50000"                # apache/airflow image runtime user
AIRFLOW_GID="500"                  # airflow group used in k8s manifests

echo "=== Step 1: Install NFS server packages ==="
sudo apt-get install -y nfs-kernel-server nfs-common

echo "=== Step 2: Create export directories ==="
sudo mkdir -p "${NFS_EXPORT_DIR}/airflow-data/dags"
sudo mkdir -p "${NFS_EXPORT_DIR}/airflow-data/logs/dag_processor"
sudo mkdir -p "${NFS_EXPORT_DIR}/airflow-data/logs/scheduler"
sudo mkdir -p "${NFS_EXPORT_DIR}/airflow-data/project/python/output"
sudo mkdir -p "${NFS_EXPORT_DIR}/ml-output"
sudo mkdir -p "${NFS_EXPORT_DIR}/mlflow-artifacts"
sudo chown -R "${AIRFLOW_UID}:${AIRFLOW_GID}" "${NFS_EXPORT_DIR}/airflow-data"
sudo chown -R "${AIRFLOW_UID}:${AIRFLOW_GID}" "${NFS_EXPORT_DIR}/ml-output"
sudo chmod -R 777 "${NFS_EXPORT_DIR}"
sudo find "${NFS_EXPORT_DIR}/airflow-data" -type d -exec chmod g+s {} +
sudo find "${NFS_EXPORT_DIR}/ml-output" -type d -exec chmod g+s {} +

echo "=== Step 3: Configure /etc/exports ==="
cat << EXPORTS | sudo tee /etc/exports
${NFS_EXPORT_DIR}/airflow-data${POD_CIDR}(rw,sync,no_subtree_check,no_root_squash)
${NFS_EXPORT_DIR}/airflow-data${COMPUTE_TS_IP}(rw,sync,no_subtree_check,no_root_squash)
${NFS_EXPORT_DIR}/airflow-data${INFRA_TS_IP}(rw,sync,no_subtree_check,no_root_squash)
${NFS_EXPORT_DIR}/ml-output${POD_CIDR}(rw,sync,no_subtree_check,no_root_squash)
${NFS_EXPORT_DIR}/ml-output${COMPUTE_TS_IP}(rw,sync,no_subtree_check,no_root_squash)
${NFS_EXPORT_DIR}/ml-output${INFRA_TS_IP}(rw,sync,no_subtree_check,no_root_squash)
EXPORTS

echo "=== Step 4: Load nfsd module and export ==="
sudo modprobe nfsd || true
sudo exportfs -rav
sudo systemctl enable --now nfs-kernel-server

echo "=== Step 5: Install nfs-subdir-external-provisioner via Helm ==="
helm repo add nfs-subdir-external-provisioner \
  https://kubernetes-sigs.github.io/nfs-subdir-external-provisioner/ 2>/dev/null || true
helm repo update

helm upgrade --install nfs-subdir-provisioner \
  nfs-subdir-external-provisioner/nfs-subdir-external-provisioner \
  --namespace kube-system \
  --set nfs.server="${INFRA_TS_IP}" \
  --set nfs.path="${NFS_EXPORT_DIR}" \
  --set storageClass.name=nfs-client \
  --set storageClass.defaultClass=false \
  --set storageClass.reclaimPolicy=Retain \
  --set storageClass.accessModes=ReadWriteMany \
  --set nodeSelector."kubernetes\\.io/hostname"=desktop-nnutaj7

kubectl -n kube-system wait pod \
  -l app=nfs-subdir-external-provisioner \
  --for=condition=Ready \
  --timeout=60s

echo "=== Step 6: Apply kubernetes manifests ==="
echo "Run: kubectl apply -k kubernetes/"

echo ""
echo "=== NFS setup complete ==="
echo "Static PVs pv-airflow-data-nfs and pv-ml-output-nfs will be created by kustomize."
echo "Data directories: ${NFS_EXPORT_DIR}/airflow-data  and  ${NFS_EXPORT_DIR}/ml-output"
echo ""
echo "NOTE: Pods using airflow-data/ml-output are pinned to infra (workload-role: infra)."
echo "      To enable cross-node NFS mounts later, install nfs-common on worker:"
echo "      ssh andrius@${COMPUTE_TS_IP} 'sudo apt-get install -y nfs-common'"
echo "      Then update nodeSelector in airflow.yaml, flink-beam.yaml to workload-role: compute."
