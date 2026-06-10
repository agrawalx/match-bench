add_header X-Content-Type-Options "nosniff" always;
add_header X-Frame-Options "DENY" always;
add_header X-XSS-Protection "0" always;
add_header Referrer-Policy "strict-origin-when-cross-origin" always;
add_header Permissions-Policy "geolocation=(), microphone=(), camera=()" always;
add_header Cross-Origin-Opener-Policy "same-origin" always;
add_header Cross-Origin-Resource-Policy "same-origin" always;
add_header Strict-Transport-Security "max-age=63072000; includeSubDomains; preload" always;
# script-src 'unsafe-inline' is an explicit interim trade-off: Next.js 14
# hydrates via inline bootstrap scripts, which a bare 'self' policy blocks.
# Proper nonce wiring (per-request nonce injected into both the CSP header and
# the Next-rendered script tags) is tracked for later.
add_header Content-Security-Policy "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self'; font-src 'self'; connect-src 'self'; img-src 'self' data: https://lh3.googleusercontent.com; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self';" always;
