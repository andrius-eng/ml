#!/usr/bin/env bash
# lint-images.sh — Enforce image best practices in Kubernetes manifests.
#
# Rules:
#   1. No local/unregistered image names (must be ghcr.io/*, docker.io/* or
#      another fully-qualified registry, OR well-known public images like
#      postgres:, alpine:, flink:, etc.)
#   2. Every ghcr.io/andrius-eng/* image must have imagePullPolicy: Always
#      (CI/CD always pushes :latest; IfNotPresent would silently run stale code)
#   3. No bare "image: my-*" or "image: local-*" tags
#
# Usage: bash scripts/lint-images.sh [manifest_glob]
#   Default glob: kubernetes/**/*.yaml
set -euo pipefail

GLOB="${1:-kubernetes/**/*.yaml}"
ERRORS=0

while IFS= read -r -d '' file; do
  # Rule 3: reject local/unregistered image names
  if grep -nP '^\s+image:\s+(my-|local-)' "$file"; then
    echo "ERROR [$file]: local image name found (must use fully-qualified registry)" >&2
    ERRORS=$((ERRORS + 1))
  fi

  # Rule 1: image must be fully qualified (contain a '/' or be a known short official image)
  while IFS= read -r line; do
    lineno=$(echo "$line" | cut -d: -f1)
    img=$(echo "$line" | sed 's/.*image: *//')
    # Allow known official short names
    if echo "$img" | grep -qP '^(postgres|alpine|flink|busybox|redis|nginx|python|node)[:@]'; then
      continue
    fi
    # Require at least one slash (registry/repo format)
    if ! echo "$img" | grep -q '/'; then
      echo "ERROR [$file:$lineno]: unqualified image '$img' — must use fully-qualified registry path" >&2
      ERRORS=$((ERRORS + 1))
    fi
  done < <(grep -nP '^\s+image: ' "$file")

  # Rule 2: ghcr.io/andrius-eng/* must have imagePullPolicy: Always on next line
  python3 - "$file" <<'PYEOF'
import sys, re, pathlib

path = pathlib.Path(sys.argv[1])
lines = path.read_text().splitlines()
errors = 0
for i, line in enumerate(lines):
    if re.search(r'image:\s+ghcr\.io/andrius-eng/', line):
        j = i + 1
        while j < len(lines) and lines[j].strip() == '':
            j += 1
        if j >= len(lines) or 'imagePullPolicy: Always' not in lines[j]:
            print(f"ERROR [{path}:{i+1}]: ghcr.io/andrius-eng image missing 'imagePullPolicy: Always' on next line", file=sys.stderr)
            errors += 1
sys.exit(errors)
PYEOF
  ERRORS=$((ERRORS + $?))
done < <(find kubernetes -name "*.yaml" -print0)

if [[ $ERRORS -gt 0 ]]; then
  echo ""
  echo "Image policy lint FAILED with $ERRORS error(s)." >&2
  echo "Fix: use ghcr.io/andrius-eng/<image>:latest with imagePullPolicy: Always" >&2
  exit 1
fi

echo "Image policy lint passed."
