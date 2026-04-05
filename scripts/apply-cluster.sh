#!/usr/bin/env bash
# apply-cluster.sh — idempotent cluster reconcile
#
# Normal flow (ArgoCD available):
#   1. Trigger ArgoCD app-of-apps sync and wait for Healthy/Synced.
#
# Fallback (ArgoCD down / bootstrapping):
#   2. kubectl apply -k per kustomization group in dependency order.
#
# Usage:
#   bash scripts/apply-cluster.sh              # auto-detect
#   bash scripts/apply-cluster.sh --argocd     # force ArgoCD path
#   bash scripts/apply-cluster.sh --manual     # force manual path
#   bash scripts/apply-cluster.sh --purge-dead # also force-delete pods on dead nodes
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FORCE_MODE="${1:-}"
PURGE_DEAD=false
if [[ "${1:-}" == "--purge-dead" || "${2:-}" == "--purge-dead" ]]; then
  PURGE_DEAD=true
fi

# ── Colours ──────────────────────────────────────────────────────────────────
say()  { printf '\033[36m==> %s\033[0m\n' "$*"; }
ok()   { printf '\033[32m    ✓ %s\033[0m\n' "$*"; }
warn() { printf '\033[33m    ! %s\033[0m\n' "$*"; }
die()  { printf '\033[31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

# ── Preflight ────────────────────────────────────────────────────────────────
command -v kubectl >/dev/null 2>&1 || die "kubectl not found"
kubectl cluster-info --request-timeout=5s >/dev/null 2>&1 || die "Cluster unreachable"

# ── ArgoCD availability check ────────────────────────────────────────────────
argocd_available() {
  kubectl get deployment argocd-server -n argocd \
    --request-timeout=5s >/dev/null 2>&1 || return 1
  local ready
  ready=$(kubectl get deployment argocd-server -n argocd \
    -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
  [[ "${ready:-0}" -ge 1 ]]
}

# ── ArgoCD sync path ──────────────────────────────────────────────────────────
sync_argocd() {
  say "ArgoCD available — syncing app-of-apps"

  # Prefer argocd CLI if installed; fall back to kubectl patch
  if command -v argocd >/dev/null 2>&1; then
    # Ensure we're logged in (in-cluster service-account token)
    local argocd_server
    argocd_server=$(kubectl get svc argocd-server -n argocd \
      -o jsonpath='{.spec.clusterIP}' 2>/dev/null)
    argocd app sync ml-root \
      --server "${argocd_server}:443" \
      --insecure \
      --propagate-finalizer \
      --timeout 300 || true
  else
    warn "argocd CLI not found — triggering sync via annotation"
    kubectl annotate application ml-root -n argocd \
      argocd.argoproj.io/refresh=hard \
      --overwrite
  fi

  say "Waiting for all Applications to become Healthy + Synced (max 5 min)…"
  local apps=(ml-infra ml-data ml-serving ml-networking ml-monitoring)
  local deadline=$(( $(date +%s) + 300 ))
  local all_ok=false
  while [[ $(date +%s) -lt $deadline ]]; do
    all_ok=true
    for app in "${apps[@]}"; do
      local status
      status=$(kubectl get application "$app" -n argocd \
        -o jsonpath='{.status.sync.status}/{.status.health.status}' 2>/dev/null || echo "Unknown/Unknown")
      if [[ "$status" != "Synced/Healthy" ]]; then
        all_ok=false
        break
      fi
    done
    $all_ok && break
    sleep 10
  done

  if $all_ok; then
    ok "All ArgoCD Applications Synced + Healthy"
  else
    warn "Timeout waiting for ArgoCD — check 'kubectl get applications -n argocd'"
    for app in "${apps[@]}"; do
      local status
      status=$(kubectl get application "$app" -n argocd \
        -o jsonpath='{.status.sync.status}/{.status.health.status}' 2>/dev/null || echo "Unknown/Unknown")
      printf '    %-20s %s\n' "$app" "$status"
    done
  fi
}

# ── Manual kustomize fallback path ────────────────────────────────────────────
apply_manual() {
  say "Applying kustomize groups manually"

  # Order matters: infra (namespace, PVCs, secrets) before workloads
  local groups=(
    "kubernetes/overlays/k3s/infra"
    "kubernetes/overlays/k3s/data"
    "kubernetes/overlays/k3s/ml"
    "kubernetes/overlays/k3s/networking"
    "kubernetes/overlays/k3s/monitoring"
  )

  for group in "${groups[@]}"; do
    say "kubectl apply -k ${group}"
    kubectl apply -k "${REPO_ROOT}/${group}"
    ok "${group} applied"
  done

  say "Waiting for frontend rollout…"
  kubectl rollout status deployment/frontend -n ml-stack --timeout=120s
  ok "frontend rollout complete"
}

# ── Purge dead-node pods ──────────────────────────────────────────────────────
purge_dead_pods() {
  say "Force-deleting Unknown/Terminating pods on unreachable nodes"
  kubectl get pods -A -o wide 2>/dev/null \
    | awk 'NR>1 && ($4=="Unknown" || $4=="Terminating") {print $1, $2}' \
    | while read -r ns pod; do
        echo "  Deleting ${ns}/${pod}"
        kubectl delete pod -n "$ns" "$pod" --force --grace-period=0 2>/dev/null || true
      done
}

# ── Main ──────────────────────────────────────────────────────────────────────
say "Cluster reconcile starting (repo: ${REPO_ROOT})"

if [[ "$PURGE_DEAD" == true ]]; then
  purge_dead_pods
fi

case "$FORCE_MODE" in
  --argocd)  sync_argocd ;;
  --manual)  apply_manual ;;
  *)
    if argocd_available; then
      sync_argocd
    else
      warn "ArgoCD not available — using manual kustomize fallback"
      apply_manual
    fi
    ;;
esac

say "Cluster status"
kubectl get pods -n ml-stack -o wide

say "Done."
