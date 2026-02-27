# Stage 1: Build Next.js static export
FROM node:20-alpine AS frontend-builder
WORKDIR /build
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build
# Output: /build/out/

# Stage 2: Python runtime
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api/ ./api/
COPY db/ ./db/
COPY etl/ ./etl/
COPY services/ ./services/
COPY .env.example ./.env.example
COPY --from=frontend-builder /build/out/ ./frontend/
ENV DATABASE_PATH=/data/iowa_transparency.db
ENV ENVIRONMENT=production
EXPOSE 8000
COPY entrypoint.sh .
RUN sed -i 's/\r$//' entrypoint.sh && chmod +x entrypoint.sh
CMD ["./entrypoint.sh"]
