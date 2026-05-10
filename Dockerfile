# Estonian Competition (ECA) MCP — multi-stage Dockerfile
# Build:  docker build -t estonian-competition-mcp .
# Run:    docker run --rm -p 3000:3000 estonian-competition-mcp
#
# The image expects a pre-built database at /app/data/eca.db.
# Override with ECA_DB_PATH for a custom location.

# --- Stage 1: Build TypeScript + native modules ---
FROM node:20-slim AS builder

WORKDIR /app

# Install build toolchain for better-sqlite3 native binding
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 make g++ \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json* ./
# Run full install (postinstall builds better-sqlite3 .node binding)
RUN npm ci

COPY tsconfig.json ./
COPY src/ src/
RUN npm run build

# Prune to production deps while preserving the built native binding
RUN npm prune --omit=dev

# --- Stage 2: Production ---
FROM node:20-slim AS production

WORKDIR /app
ENV NODE_ENV=production
ENV ECA_DB_PATH=/app/data/eca.db

COPY package.json package-lock.json* ./
# Bring node_modules (with pre-built native bindings) from builder
COPY --from=builder /app/node_modules/ node_modules/
COPY --from=builder /app/dist/ dist/

# Bring the database asset (provisioned by CI from GitHub Release)
COPY data/database.db data/eca.db

# Non-root user for security
RUN addgroup --system --gid 1001 mcp && \
    adduser --system --uid 1001 --ingroup mcp mcp && \
    chown -R mcp:mcp /app
USER mcp

# Health check: verify HTTP server responds
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health',r=>{process.exit(r.statusCode===200?0:1)}).on('error',()=>process.exit(1))"

CMD ["node", "dist/src/http-server.js"]
