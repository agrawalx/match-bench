# Build the static SPA, serve it from an unprivileged nginx (non-root by default,
# uid 101, port 8080) — matching the platform's non-root container requirement.
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
# Built with same-origin /api (nginx proxies it); no VITE_API_BASE baked in.
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 8080
