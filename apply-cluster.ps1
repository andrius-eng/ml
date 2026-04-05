# apply-cluster.ps1 — Run from Windows PowerShell (right-click -> Run with PowerShell)
# 1. Restarts WSL if crashed
# 2. Applies updated k8s manifests
# 3. Force-deletes stuck Unknown/dead-node pods
# 4. Commits and pushes

Write-Host "==> Restarting WSL..." -ForegroundColor Cyan
wsl --shutdown
Start-Sleep -Seconds 4

Write-Host "==> Testing WSL..." -ForegroundColor Cyan
$test = wsl -- echo "ok" 2>&1
if ($test -ne "ok") {
    Write-Host "WSL did not start cleanly: $test" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}
Write-Host "WSL is up." -ForegroundColor Green

Write-Host "`n==> Applying manifests..." -ForegroundColor Cyan
wsl -- bash -c @"
set -e
cd /mnt/c/Development/ml

kubectl apply -f kubernetes/data/airflow.yaml \
              -f kubernetes/data/flink-beam.yaml \
              -f kubernetes/ml/dashboard.yaml \
              -f kubernetes/monitoring/monitoring.yaml
echo 'Manifests applied.'

echo 'Force-deleting pods on dead node...'
kubectl get pods -A -o wide 2>/dev/null \
  | grep -E 'desktop-0qvhfr9|Unknown' \
  | grep -v Completed \
  | awk '{print \$1, \$2}' \
  | while read ns pod; do
      echo "Deleting \$ns/\$pod"
      kubectl delete pod -n \"\$ns\" \"\$pod\" --force --grace-period=0 2>/dev/null || true
    done

echo 'Committing...'
git add kubernetes/ README.md
git commit -m 'fix(scheduling): harden pod placement for single-node failure

Hard compute: airflow-scheduler, flink-taskmanager
Soft (float to infra on failure): airflow-webserver, dag-processor,
  ml-server, ws-server, frontend, beam-job-server
Hard infra: flink-jobmanager, flink-history-server, kube-state-metrics
kube-system: sealed-secrets, traefik, metrics-server pinned to infra
docs(readme): update stale k8s structure, nodes, ollama status' || echo 'Nothing new to commit.'

git push origin main
echo 'Done.'
"@

Write-Host "`n==> Cluster status:" -ForegroundColor Cyan
wsl -- kubectl get pods -n ml-stack -o wide

Read-Host "`nPress Enter to close"
