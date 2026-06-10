FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts

FROM node:22-alpine AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
ENV NEXT_TELEMETRY_DISABLED=1
# NEXT_PUBLIC_* values are baked into the client bundle at build time
# (src/config/platform.ts), so they must be present for `next build`.
ARG NEXT_PUBLIC_GOOGLE_CLIENT_ID
ARG NEXT_PUBLIC_GOOGLE_REDIRECT_URI
ENV NEXT_PUBLIC_GOOGLE_CLIENT_ID=$NEXT_PUBLIC_GOOGLE_CLIENT_ID \
    NEXT_PUBLIC_GOOGLE_REDIRECT_URI=$NEXT_PUBLIC_GOOGLE_REDIRECT_URI
RUN npm run build

FROM node:22-alpine AS runtime
RUN apk add --no-cache nginx gettext \
 && rm /etc/nginx/http.d/default.conf
WORKDIR /app
ENV NODE_ENV=production
COPY nginx/nginx.conf /etc/nginx/nginx.conf
# Only default.conf is an envsubst template; security_headers.inc goes to a
# non-templated path so the rendered config can include it verbatim.
COPY nginx/conf.d/default.conf /etc/nginx/templates/default.conf
COPY nginx/conf.d/security_headers.inc /etc/nginx/snippets/security_headers.inc
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static /usr/share/nginx/html/_next/static
COPY --from=builder /app/public /usr/share/nginx/html/
# /etc/nginx/conf.d is the envsubst render target (an emptyDir in k8s);
# /var/cache/nginx holds the redirected temp paths (see nginx.conf). The pid
# file is created by nginx itself under /tmp (world-writable everywhere).
RUN mkdir -p /etc/nginx/conf.d /var/cache/nginx \
 && chown -R nginx:nginx /usr/share/nginx/html /var/lib/nginx /var/log/nginx /etc/nginx/conf.d /var/cache/nginx
EXPOSE 8080
# HOSTNAME must be pinned: Next standalone binds to $HOSTNAME, which Docker/k8s
# set to the container hostname (the pod IP) — nginx proxies to 127.0.0.1:3000.
CMD ["sh", "-c", "envsubst '$$LEADERBOARD_API_HOST $$LEADERBOARD_API_PORT $$SUBMISSION_API_HOST $$SUBMISSION_API_PORT $$AUTH_API_HOST $$AUTH_API_PORT' < /etc/nginx/templates/default.conf > /etc/nginx/conf.d/default.conf && HOSTNAME=127.0.0.1 PORT=3000 node server.js & nginx -g 'daemon off;'"]
