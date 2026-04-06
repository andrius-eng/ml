#!/usr/bin/env bash
# setup-gateway.sh — one-time setup for Gateway API + TLS on k3s/Traefik
# Run this once per cluster, then apply the kustomize overlays.
set -euo pipefail

CERTS_DIR="$(dirname "$0")/../certs"
NAMESPACE="ml-stack"

echo "==> Enabling Traefik Gateway API provider..."
kubectl apply -f - <<'EOF'
apiVersion: helm.cattle.io/v1
kind: HelmChartConfig
metadata:
  name: traefik
  namespace: kube-system
spec:
  valuesContent: |-
    providers:
      kubernetesGateway:
        enabled: true
EOF

echo "==> Waiting for Traefik to restart with Gateway provider..."
kubectl rollout restart deployment/traefik -n kube-system
kubectl rollout status deployment/traefik -n kube-system --timeout=60s

echo "==> Generating TLS certificates for ml-stack.local..."
mkdir -p "$CERTS_DIR"
cd "$CERTS_DIR"

if [[ ! -f ca.key ]]; then
  openssl genrsa -out ca.key 4096 2>/dev/null
  openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
    -subj "/O=ml-stack Local/CN=ml-stack Local CA" 2>/dev/null
  echo "    CA generated."
else
  echo "    CA already exists, skipping."
fi

if [[ ! -f tls.crt ]]; then
  openssl genrsa -out tls.key 2048 2>/dev/null
  openssl req -new -key tls.key -out tls.csr \
    -subj "/CN=ml-stack.local" 2>/dev/null
  openssl x509 -req -days 730 \
    -in tls.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out tls.crt \
    -extfile <(printf 'subjectAltName=DNS:ml-stack.local,DNS:*.ml-stack.local\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth') 2>/dev/null
  echo "    Server cert generated."
else
  echo "    Server cert already exists, skipping."
fi

echo "==> Creating ml-stack-tls secret in k8s..."
kubectl create secret tls ml-stack-tls -n "$NAMESPACE" \
  --cert=tls.crt --key=tls.key \
  --dry-run=client -o yaml | kubectl apply -f -

echo ""
echo "==> MANUAL STEP — Trust the CA on each machine that needs HTTPS:"
echo ""
echo "    WSL/Ubuntu:"
echo "      sudo cp $CERTS_DIR/ca.crt /usr/local/share/ca-certificates/ml-stack-local-ca.crt"
echo "      sudo update-ca-certificates"
echo ""
echo "    macOS (Mac k3s worker / browser):"
echo "      # Copy ca.crt to the Mac first (e.g. via scp or shared folder), then:"
echo "      sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain $CERTS_DIR/ca.crt"
echo ""
echo "    Windows (for browser on Windows host):"
echo "      # Copy ca.crt to Windows, then in PowerShell (admin):"
echo "      Import-Certificate -FilePath ca.crt -CertStoreLocation Cert:\\LocalMachine\\Root"
echo ""
echo "==> Add ml-stack.local to /etc/hosts (WSL) and C:\\Windows\\System32\\drivers\\etc\\hosts (Windows):"
echo "    <your-node-tailscale-ip>  ml-stack.local"
echo ""
echo "Done."
