#!/bin/bash
set -e
echo "=== Iniciando Byetech Entregas ==="
echo "PORT=$PORT"
echo "DATABASE_URL=$DATABASE_URL"
echo "Python: $(python3 --version)"

echo "=== Testando imports ==="
python3 -c "
import sys
print('sys.path:', sys.path[:3])
try:
    import fastapi; print('fastapi ok')
    import uvicorn; print('uvicorn ok')
    import sqlalchemy; print('sqlalchemy ok')
    import aiosqlite; print('aiosqlite ok')
    import app.database; print('app.database ok')
    import app.main; print('app.main ok')
except Exception as e:
    import traceback
    traceback.print_exc()
    sys.exit(1)
"

echo "=== Iniciando uvicorn ==="
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
