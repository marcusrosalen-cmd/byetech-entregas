FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instala browsers do Playwright
RUN playwright install chromium --with-deps

COPY . .

# Cria diretório de dados persistentes
RUN mkdir -p /data

ENV DATABASE_URL=sqlite+aiosqlite:////data/byetech.db
ENV SESSION_FILE=/data/.byetech_session.json
ENV CPF_MAP_FILE=/data/.byetech_cpf_map.json
ENV PENDING_FILE=/data/.byetech_pending.json

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
