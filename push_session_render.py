"""
push_session_render.py
======================
Lê a sessão Byetech salva em disco (.byetech_session.json) e envia ao Render.
Assim o Render pode atualizar contratos no Byetech CRM sem precisar de Playwright.

Uso:
    python push_session_render.py

Pré-requisito:
    - .byetech_session.json deve existir (gerado pelo sync local ou _refresh_byetech_session.py)
    - RENDER_SERVICE_URL no .env ou variável de ambiente
"""
import os
import sys
import json
import httpx
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

SESSION_FILE   = Path(__file__).parent / ".byetech_session.json"
RENDER_URL     = os.getenv("RENDER_SERVICE_URL", "https://byetech-entregas.onrender.com")
PUSH_SECRET    = os.getenv("SESSION_PUSH_SECRET", "byetech-local")
ENDPOINT       = f"{RENDER_URL}/api/byetech/push-session"


def main():
    if not SESSION_FILE.exists():
        print(f"[ERRO] {SESSION_FILE} não encontrado.")
        print("Execute primeiro: python _refresh_byetech_session.py")
        sys.exit(1)

    with open(SESSION_FILE, encoding="utf-8") as f:
        cookies = json.load(f)

    if not cookies:
        print("[ERRO] Arquivo de sessão vazio.")
        sys.exit(1)

    print(f"Enviando sessao para {ENDPOINT}...")
    try:
        resp = httpx.post(
            ENDPOINT,
            json={"cookies": cookies, "secret": PUSH_SECRET},
            timeout=60,
        )
        data = resp.json()
        if resp.status_code == 200 and data.get("ok"):
            print(f"OK - {data.get('message', 'Sessao enviada com sucesso!')}")
        else:
            print(f"ERRO {resp.status_code}: {data}")
            sys.exit(1)
    except Exception as e:
        print(f"Falha ao conectar: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
