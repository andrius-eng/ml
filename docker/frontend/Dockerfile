# ── Stage 1: build Vite app ──────────────────────────────────────────────────
FROM node:25-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci

COPY . .
RUN npm run build

# ── Stage 2: serve with nginx ─────────────────────────────────────────────────
FROM nginx:alpine AS runner

# Remove default nginx index page
RUN rm -rf /usr/share/nginx/html/*

# Copy built Vite output
COPY --from=builder /app/dist /usr/share/nginx/html

# Nginx config: serve static files, proxy /data writes from ws-server volume
COPY docker/frontend/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
