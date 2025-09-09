# Development Dockerfile for LaykHaus UI
FROM node:20-alpine

# Install pnpm globally
RUN npm install -g pnpm@latest

WORKDIR /app

# Copy package files
COPY package.json pnpm-lock.yaml* ./

# Install ALL dependencies (including devDependencies)
RUN pnpm install

# We don't copy source files in dev mode - they're mounted as volumes
# This allows hot reloading

EXPOSE 3000

ENV PORT 3000
ENV HOSTNAME "0.0.0.0"
ENV NODE_ENV development
ENV NEXT_TELEMETRY_DISABLED 1

# Use the entrypoint script from root
ENTRYPOINT ["npx", "pnpm", "run", "dev"]