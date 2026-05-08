"""
Restaura sessao api-production.byetech.pro via Playwright.
Usa os cookies existentes do crm.byetech.pro para tentar restaurar
a sessao sem precisar de novo 2FA.

Se a sessao do crm ainda for valida, o SPA autentica automaticamente
no api-production e capturamos todos os cookies.
"""
import asyncio, sys, os, json, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, '.')
from dotenv import load_dotenv
load_dotenv()

SESSION_FILE  = ".byetech_session.json"
BYETECH_URL   = os.getenv("BYETECH_URL", "https://crm.byetech.pro")
API_URL       = "https://api-production.byetech.pro"
EMAIL         = os.getenv("BYETECH_EMAIL", "")
SENHA         = os.getenv("BYETECH_PASSWORD", "")


async def main():
    from playwright.async_api import async_playwright

    # Carrega sessao atual
    existing = {}
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE) as f:
            existing = json.load(f)
    print(f"Sessao atual: {list(existing.keys())}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()

        # Injeta cookies existentes para ambos os dominios
        cookie_list = []
        for name, value in existing.items():
            for domain in [".byetech.pro", "crm.byetech.pro", "api-production.byetech.pro"]:
                cookie_list.append({
                    "name": name, "value": value,
                    "domain": domain, "path": "/",
                    "secure": True, "httpOnly": False,
                })
        if cookie_list:
            await ctx.add_cookies(cookie_list)
            print(f"Injetados {len(existing)} cookies em ambos os dominios")

        page = await ctx.new_page()

        # Monitora requests para api-production
        api_requests = []
        page.on("request", lambda req: api_requests.append(req.url) if "api-production" in req.url else None)

        # Tenta carregar pagina de contratos com sessao existente
        print("Navegando para contratos...")
        try:
            await page.goto(f"{BYETECH_URL}/contracts", wait_until="networkidle", timeout=20000)
        except Exception:
            pass
        await page.wait_for_timeout(2000)

        url_atual = page.url
        print(f"URL: {url_atual}")

        if "login" in url_atual.lower() or "verify-2fa" in url_atual.lower():
            # Sessao CRM expirada — tenta login sem 2FA primeiro
            print("Sessao CRM expirada. Tentando login...")
            try:
                await page.goto(f"{BYETECH_URL}/login", wait_until="networkidle", timeout=15000)
            except Exception:
                pass
            try:
                await page.fill("input[type='email']", EMAIL)
                await page.fill("input[type='password']", SENHA)
                await page.click("button[type='submit']")
                await page.wait_for_timeout(3000)
            except Exception as e:
                print(f"Erro no login: {e}")

            url_depois = page.url
            print(f"URL apos login: {url_depois}")

            if "verify-2fa" in url_depois:
                print("\n2FA necessario! Abra outro terminal e execute:")
                print("  python _refresh_byetech_session.py SEU_CODIGO_2FA")
                await browser.close()
                return

        # Aguarda SPA inicializar e chamar api-production
        print("Aguardando SPA inicializar api-production...")
        await page.wait_for_timeout(4000)

        # Requests feitos
        print(f"Requests para api-production: {len(api_requests)}")
        for r in api_requests[:8]:
            print(f"  {r[:80]}")

        # Captura cookies de ambos os dominios
        all_cookies = await ctx.cookies([BYETECH_URL, API_URL])
        await browser.close()

    cookies = {c["name"]: c["value"] for c in all_cookies}
    print(f"\nCookies capturados: {list(cookies.keys())}")

    if not cookies:
        print("Nenhum cookie capturado.")
        return

    # Verifica se tem cookies do api-production
    has_api = any(c["domain"] in ("api-production.byetech.pro", ".byetech.pro")
                  for c in all_cookies if c.get("domain"))
    print(f"Cookies api-production: {[c['name'] for c in all_cookies if 'api-production' in c.get('domain','')]}")

    # Salva
    with open(SESSION_FILE, "w") as f:
        json.dump(cookies, f, indent=2)
    print(f"Salvos em {SESSION_FILE}")

    # Testa
    print("\nTestando sessao...")
    from app.scrapers.byetech_crm import _test_session
    ok = await _test_session(cookies)
    print(f"Sessao valida: {'SIM' if ok else 'NAO'}")

    if ok:
        print("\nSessao OK! Execute:")
        print("  python push_session_render.py")
        print("  python processar_pendentes.py")
    else:
        print("\nSessao invalida para api-production.")
        print("Precisa de novo 2FA: python _refresh_byetech_session.py SEU_CODIGO")

asyncio.run(main())
