FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts

FROM node:22-alpine AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

FROM node:22-alpine AS runtime
RUN apk add --no-cache nginx gettext \
 && rm /etc/nginx/http.d/default.conf
WORKDIR /app
ENV NODE_ENV=production
COPY nginx/nginx.conf /etc/nginx/nginx.conf
COPY nginx/conf.d/ /etc/nginx/templates/
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static /usr/share/nginx/html/_next/static
COPY --from=builder /app/public /usr/share/nginx/html/
RUN chown -R nginx:nginx /usr/share/nginx/html /var/lib/nginx /var/log/nginx \
 && touch /tmp/nginx.pid \
 && chown nginx:nginx /tmp/nginx.pid
EXPOSE 8080
CMD ["sh", "-c", "envsubst '$$LEADERBOARD_API_HOST $$LEADERBOARD_API_PORT $$SUBMISSION_API_HOST $$SUBMISSION_API_PORT $$AUTH_API_HOST $$AUTH_API_PORT' < /etc/nginx/templates/default.conf > /etc/nginx/conf.d/default.conf && node server.js & nginx -g 'daemon off;'"]
