"""
Faz login no api-production.byetech.pro usando email/senha (sem 2FA).
O api-production tem Sanctum proprio — independente do crm.byetech.pro.
Salva os cookies do api-production no .byetech_session.json junto com os do CRM.
"""
import asyncio, sys, os, json, httpx
from pathlib import Path
from urllib.parse import unquote

sys.path.insert(0, str(Path(__file__).parent))
from dotenv import load_dotenv
load_dotenv()

SESSION_FILE = Path(".byetech_session.json")
API_URL      = "https://api-production.byetech.pro"
EMAIL        = os.getenv("BYETECH_EMAIL", "")
SENHA        = os.getenv("BYETECH_PASSWORD", "")


async def login_api_production() -> dict:
    """Faz login no api-production via Sanctum e retorna os cookies."""
    print(f"Fazendo login no api-production como {EMAIL}...")

    jar = httpx.Cookies()

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://crm.byetech.pro",
            "Referer": "https://crm.byetech.pro/",
            "sec-fetch-site": "same-site",
            "sec-fetch-mode": "cors",
        }
    ) as client:

        # Passo 1: obter CSRF cookie
        print("  1. Obtendo CSRF cookie...")
        r = await client.get(f"{API_URL}/sanctum/csrf-cookie", timeout=20)
        print(f"     Status: {r.status_code}")

        xsrf = unquote(r.cookies.get("XSRF-TOKEN", ""))
        if not xsrf:
            # Tenta dos headers
            for h, v in r.headers.items():
                if "set-cookie" in h.lower() and "XSRF-TOKEN" in v:
                    import re
                    m = re.search(r"XSRF-TOKEN=([^;]+)", v)
                    if m:
                        xsrf = unquote(m.group(1))
            print(f"     XSRF (header): {xsrf[:30]}..." if xsrf else "     XSRF nao encontrado")
        else:
            print(f"     XSRF: {xsrf[:30]}...")

        # Passo 2: login
        print("  2. Fazendo login...")
        r2 = await client.post(
            f"{API_URL}/login",
            json={"email": EMAIL, "password": SENHA},
            headers={"X-XSRF-TOKEN": xsrf} if xsrf else {},
            timeout=20,
        )
        print(f"     Status: {r2.status_code}")
        print(f"     Body: {r2.text[:200]}")

        if r2.status_code in (200, 204):
            # Pega cookies da resposta
            cookies = {}
            for c in client.cookies.jar:
                cookies[c.name] = c.value
            # Tambem pega do response
            for name, value in r2.cookies.items():
                cookies[name] = value
            print(f"     Cookies obtidos: {list(cookies.keys())}")
            return cookies
        else:
            print(f"     Login falhou: {r2.text[:300]}")
            return {}


async def main():
    # Carrega sessao existente
    existing = {}
    if SESSION_FILE.exists():
        with open(SESSION_FILE) as f:
            existing = json.load(f)
        print(f"Sessao existente: {list(existing.keys())}")

    # Login no api-production
    api_cookies = await login_api_production()

    if not api_cookies:
        print("\nFalhou ao fazer login no api-production.")
        print("Execute _refresh_byetech_session.py com um novo codigo 2FA.")
        sys.exit(1)

    # Mescla com cookies existentes
    merged = {**existing, **api_cookies}
    print(f"\nCookies mesclados: {list(merged.keys())}")

    # Salva
    with open(SESSION_FILE, "w") as f:
        json.dump(merged, f, indent=2)
    print(f"Salvos em {SESSION_FILE}")

    # Testa
    print("\nTestando sessao...")
    from app.scrapers.byetech_crm import _test_session
    ok = await _test_session(merged)
    print(f"Sessao api-production valida: {'SIM' if ok else 'NAO'}")

    if ok:
        print("\nPronto! Execute agora:")
        print("  python push_session_render.py")
        print("  python processar_pendentes.py")
    else:
        print("\nSessao invalida. Tente _refresh_byetech_session.py com 2FA.")

asyncio.run(main())
