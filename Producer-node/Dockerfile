FROM node:18-slim

WORKDIR /app

# Install production dependencies
COPY package*.json ./
RUN npm ci --production

# Copy app sources (including app2.js and cert files if present)
COPY . .

# Recommended: provide Kafka certs via bind-mount or secrets at runtime:
#  - ./ca.pem, ./service.cert, ./service.key
ENV PM2_LOG_DIR=/root/.pm2/logs
ENV NODE_ENV=production

CMD ["node", "app2.js"]