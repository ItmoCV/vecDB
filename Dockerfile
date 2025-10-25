# Dockerfile для VectorDB шарда
FROM rust:1.89-slim AS builder

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Создаем рабочую директорию
WORKDIR /app

# Копируем файлы проекта
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

# Собираем проект
RUN cargo build --release

# Финальный образ
FROM debian:bookworm-slim

# Устанавливаем runtime зависимости
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Создаем пользователя для безопасности с фиксированным UID
RUN useradd -r -s /bin/false -u 1000 vecdb

# Создаем директории
RUN mkdir -p /app/storage && chown -R vecdb:vecdb /app/storage

# Копируем бинарный файл
COPY --from=builder /app/target/release/vecDB /usr/local/bin/vecDB

# Устанавливаем права
RUN chmod +x /usr/local/bin/vecDB

# Переключаемся на пользователя vecdb
USER vecdb

# Рабочая директория
WORKDIR /app

# Открываем порт
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Команда по умолчанию
CMD ["vecDB", "/app/config.json"]
