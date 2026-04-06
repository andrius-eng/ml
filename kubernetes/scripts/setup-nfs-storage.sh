#!/usr/bin/env bash
# setup-nfs-storage.sh — Run ONCE on the infra node (desktop-nnutaj7 / WSL2)
# Sets up NFS server and export directories for static PV-backed PVCs
# used by the k3s overlays.
#
# Prerequisites:
#   - k3s kubeconfig accessible (KUBECONFIG or default ~/.kube/config)
#   - Tailscale running (infra Tailscale IP used as NFS server address)
#
# Usage: bash kubernetes/scripts/setup-nfs-storage.sh

set -euo pipefail

INFRA_TS_IP="100.95.8.71"          # infra node Tailscale IP (NFS server)
NFS_EXPORT_DIR="/data/k8s-nfs"
POD_CIDR="10.42.0.0/16"
COMPUTE_TS_IP="100.66.184.9"       # k3s-worker-worker Tailscale IP
COMPUTE2_TS_IP="100.127.227.54"    # desktop-0qvhfr9 (Kali) Tailscale IP
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
sudo mkdir -p "${NFS_EXPORT_DIR}/dashboard-data"
sudo mkdir -p "${NFS_EXPORT_DIR}/mlflow-artifacts"
sudo mkdir -p "${NFS_EXPORT_DIR}/postgres-data"
sudo mkdir -p "${NFS_EXPORT_DIR}/beam-artifacts"
sudo mkdir -p "${NFS_EXPORT_DIR}/flink-job-archives"
sudo mkdir -p "${NFS_EXPORT_DIR}/ollama-data"
sudo mkdir -p "${NFS_EXPORT_DIR}/prometheus-data"
sudo mkdir -p "${NFS_EXPORT_DIR}/grafana-data"
sudo chown -R "${AIRFLOW_UID}:${AIRFLOW_GID}" "${NFS_EXPORT_DIR}/airflow-data"
sudo chown -R "${AIRFLOW_UID}:${AIRFLOW_GID}" "${NFS_EXPORT_DIR}/ml-output"
sudo chmod -R 777 "${NFS_EXPORT_DIR}"
sudo find "${NFS_EXPORT_DIR}/airflow-data" -type d -exec chmod g+s {} +
sudo find "${NFS_EXPORT_DIR}/ml-output" -type d -exec chmod g+s {} +

echo "=== Step 3: Configure /etc/exports ==="
cat << EXPORTS | sudo tee /etc/exports
/data/k8s-nfs   ${POD_CIDR}(rw,sync,no_subtree_check,no_root_squash)
/data/k8s-nfs   ${INFRA_TS_IP}(rw,sync,no_subtree_check,no_root_squash)
/data/k8s-nfs   ${COMPUTE_TS_IP}(rw,sync,no_subtree_check,no_root_squash)
/data/k8s-nfs   ${COMPUTE2_TS_IP}(rw,sync,no_subtree_check,no_root_squash)
EXPORTS

echo "=== Step 4: Load nfsd module and export ==="
sudo modprobe nfsd || true
sudo exportfs -rav
sudo systemctl enable --now nfs-kernel-server

echo "=== Step 5: Apply kubernetes manifests ==="
echo "Run: kubectl apply -k kubernetes/"

echo ""
echo "=== NFS setup complete ==="
echo "Static PVs are defined in kubernetes/overlays/k3s/infra/nfs-pvs.yaml."
echo "Data root: ${NFS_EXPORT_DIR}"
echo ""
echo "NOTE: Stateful workloads mount ${NFS_EXPORT_DIR} over NFS from all k3s nodes."
echo "      To enable cross-node NFS mounts later, install nfs-common on worker:"
echo "      ssh andrius@${COMPUTE_TS_IP} 'sudo apt-get install -y nfs-common'"
echo "      Then update nodeSelector in airflow.yaml, flink-beam.yaml to workload-role: compute."
