"""Testa login Byetech direto na API (api-production.byetech.pro) com JSON."""
import asyncio, logging, urllib.parse as up
import httpx
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(message)s')

from app.scrapers.byetech_crm import BYETECH_EMAIL, BYETECH_PASS, BYETECH_URL, API_URL

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"

async def test():
    print(f"Frontend: {BYETECH_URL}")
    print(f"API:      {API_URL}")
    print(f"Email:    {BYETECH_EMAIL}")
    print()

    async with httpx.AsyncClient(
        base_url=API_URL,
        follow_redirects=True,
        timeout=30,
        headers={
            "User-Agent": UA,
            "Origin":  BYETECH_URL,
            "Referer": BYETECH_URL + "/",
        },
    ) as c:
        # 1. CSRF
        r0 = await c.get("/sanctum/csrf-cookie",
                         headers={"Accept": "application/json, text/plain, */*"})
        print(f"CSRF cookie: HTTP {r0.status_code}")
        xsrf_raw = c.cookies.get("XSRF-TOKEN", "")
        xsrf = up.unquote(xsrf_raw)
        print(f"XSRF-TOKEN (decoded): {xsrf[:40] if xsrf else '(vazio)'}")
        print(f"Todos cookies: {list(c.cookies.keys())}")
        print()

        if not xsrf:
            print("❌ XSRF-TOKEN não recebido — não consegue prosseguir")
            return

        # 2. POST /login JSON
        hdrs = {
            "X-XSRF-TOKEN": xsrf,
            "Content-Type": "application/json",
            "Accept":       "application/json",
        }
        r = await c.post("/login",
                         json={"email": BYETECH_EMAIL, "password": BYETECH_PASS, "remember": False},
                         headers=hdrs)
        print(f"POST /login (JSON): HTTP {r.status_code}")
        print(f"Body: {r.text[:400]}")
        print(f"Cookies após login: {list(c.cookies.keys())}")
        print()

        try:
            body_json = r.json()
        except Exception:
            body_json = {}

        if body_json.get("two_factor") is True:
            print("ℹ️  2FA necessário.")
            code = input("Digite o código 2FA: ").strip()
            r2 = await c.post("/two-factor-challenge",
                              json={"code": code},
                              headers=hdrs)
            print(f"POST /two-factor-challenge: HTTP {r2.status_code}")
            print(f"Body: {r2.text[:300]}")
            print(f"Cookies após 2FA: {list(c.cookies.keys())}")
        elif r.status_code in (200, 201, 204):
            print("✅ Login OK (sem 2FA)")
        else:
            print(f"❌ Login falhou: HTTP {r.status_code}")

asyncio.run(test())
