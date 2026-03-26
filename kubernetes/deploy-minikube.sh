#!/usr/bin/env bash
# deploy-minikube.sh — stand up the full ml-stack on minikube
# Usage:
#   bash kubernetes/deploy-minikube.sh            # direct kubectl apply
#   bash kubernetes/deploy-minikube.sh --argocd   # via ArgoCD
set -euo pipefail

ARGOCD=false
for arg in "$@"; do [[ "$arg" == "--argocd" ]] && ARGOCD=true; done

echo "=== Checking minikube ==="
minikube status || minikube start --cpus=4 --memory=8192 --disk-size=40g

echo "=== Enabling ingress addon ==="
minikube addons enable ingress

if $ARGOCD; then
  echo "=== Installing ArgoCD ==="
  kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
  kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
  echo "Waiting for ArgoCD server..."
  kubectl wait --for=condition=available deployment/argocd-server -n argocd --timeout=300s
  kubectl apply -n argocd -f kubernetes/argocd/application-minikube.yaml
  echo ""
  echo "ArgoCD UI:  kubectl port-forward svc/argocd-server -n argocd 8443:443"
  echo "Password:   kubectl get secret argocd-initial-admin-secret -n argocd -o jsonpath='{.data.password}' | base64 -d"
else
  echo "=== Applying manifests via kustomize ==="
  kubectl apply -k kubernetes/overlays/minikube
fi

MINIKUBE_IP=$(minikube ip)
echo ""
echo "=== Done ==="
echo "Add to /etc/hosts:  $MINIKUBE_IP  ml-stack.local"
echo ""
echo "Endpoints (once pods are ready):"
echo "  Frontend:   http://ml-stack.local/"
echo "  Airflow:    http://ml-stack.local/airflow  (admin/admin)"
echo "  ML API:     http://ml-stack.local/api/docs"
echo "  Flink UI:   http://ml-stack.local/flink"
echo ""
echo "Watch pods:  kubectl get pods -n ml-stack -w"
