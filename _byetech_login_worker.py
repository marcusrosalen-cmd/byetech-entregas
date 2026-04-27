"""
Worker de login do Byetech CRM.
Executado como subprocesso separado para ter seu próprio ProactorEventLoop.
Recebe o código 2FA via stdin (ou vazio se não aplicável).
Retorna os cookies como JSON no stdout.
"""
import sys
import json
import asyncio
import os

# Força ProactorEventLoop antes de qualquer import de asyncio
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from playwright.async_api import async_playwright

BYETECH_URL   = os.getenv("BYETECH_URL", "https://crm.byetech.pro")
BYETECH_EMAIL = os.getenv("BYETECH_EMAIL")
BYETECH_PASS  = os.getenv("BYETECH_PASSWORD")


async def _login(twofa_code: str | None) -> dict:
    cookies = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            await page.goto(f"{BYETECH_URL}/login", wait_until="commit", timeout=30000)
        except Exception:
            pass
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        await page.wait_for_selector("input[type='email']", timeout=12000)
        await page.wait_for_timeout(500)

        await page.fill("input[type='email']", BYETECH_EMAIL or "")
        await page.fill("input[type='password']", BYETECH_PASS or "")
        await page.click("button[type='submit']")
        await page.wait_for_timeout(2500)

        if "verify-2fa" in page.url:
            if twofa_code is None:
                await browser.close()
                return {"__2fa_required__": True}
            await page.wait_for_selector("input[maxlength='6']", timeout=10000)
            await page.fill("input[maxlength='6']", twofa_code.strip())
            await page.click("button:has-text('Verificar')")
            await page.wait_for_timeout(3500)
            if "verify-2fa" in page.url:
                await browser.close()
                print(json.dumps({"error": "Código 2FA inválido ou expirado"}))
                return {}

        browser_cookies = await context.cookies()
        for c in browser_cookies:
            cookies[c["name"]] = c["value"]
        await browser.close()

    return cookies


if __name__ == "__main__":
    twofa_code = sys.argv[1] if len(sys.argv) > 1 else None
    result = asyncio.run(_login(twofa_code))
    if result.get("__2fa_required__"):
        print(json.dumps({"error": "2FA_REQUIRED"}))
    elif result:
        print(json.dumps(result))
    else:
        print(json.dumps({"error": "Falha no login Byetech — sem cookies de sessão"}))
