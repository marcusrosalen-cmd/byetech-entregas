"""
capturar_cookies_chrome.py
==========================
Captura cookies do Byetech CRM diretamente do Chrome em execucao via CDP.

Uso rapido:
    python capturar_cookies_chrome.py

Prerequisitos:
    1. Chrome aberto com depuracao remota habilitada:
       - Use o atalho "Chrome Byetech" na area de trabalho (criado por este script)
       - OU abra o Chrome assim:
         chrome.exe --remote-debugging-port=9222

    2. Estar logado em crm.byetech.pro no Chrome

Apos rodar:
    python push_session_render.py
    python processar_pendentes.py
"""
import asyncio
import json
import os
import sys
import subprocess
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

SESSION_FILE = Path(__file__).parent / ".byetech_session.json"
BYETECH_URL  = "https://crm.byetech.pro"
API_URL      = "https://api-production.byetech.pro"
CDP_PORT     = 9222

CHROME_PATHS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    Path(os.environ.get("LOCALAPPDATA", "")) / "Google" / "Chrome" / "Application" / "chrome.exe",
]

CHROME_USER_DATA = Path(os.environ.get("LOCALAPPDATA", "")) / "Google" / "Chrome" / "User Data"


def find_chrome() -> str:
    for path in CHROME_PATHS:
        if Path(path).exists():
            return str(path)
    return "chrome.exe"


def create_shortcut():
    """Cria atalho 'Chrome Byetech' na area de trabalho com --remote-debugging-port=9222."""
    import ctypes.wintypes
    # Pega a pasta Desktop correta (funciona com OneDrive)
    CSIDL_DESKTOP = 0
    buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
    ctypes.windll.shell32.SHGetFolderPathW(None, CSIDL_DESKTOP, None, 0, buf)
    desktop = buf.value or str(Path(os.environ.get("USERPROFILE", "~")) / "Desktop")

    shortcut_path = Path(desktop) / "Chrome Byetech.lnk"
    chrome_exe = find_chrome()

    # Usa PowerShell para criar o atalho
    ps_script = f"""
$WshShell = New-Object -comObject WScript.Shell
$Shortcut = $WshShell.CreateShortcut("{shortcut_path}")
$Shortcut.TargetPath = "{chrome_exe}"
$Shortcut.Arguments = '--remote-debugging-port={CDP_PORT} --profile-directory=Default'
$Shortcut.Description = "Chrome com debug remoto para Byetech"
$Shortcut.Save()
"""
    result = subprocess.run(
        ["powershell", "-Command", ps_script],
        capture_output=True, text=True, timeout=10
    )
    if shortcut_path.exists():
        print(f"Atalho criado: {shortcut_path}")
        return True
    else:
        print(f"Falha ao criar atalho: {result.stderr[:200]}")
        return False


async def capturar_via_cdp() -> dict:
    """Conecta ao Chrome via CDP e extrai cookies do Byetech."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp(
                f"http://localhost:{CDP_PORT}",
                timeout=5000,
            )
        except Exception as e:
            raise ConnectionError(
                f"Chrome nao acessivel na porta {CDP_PORT}.\n"
                f"  Abra o Chrome com o atalho 'Chrome Byetech' ou execute:\n"
                f"  \"{find_chrome()}\" --remote-debugging-port={CDP_PORT}\n"
                f"  Erro: {e}"
            )

        # Pega o contexto padrao
        ctx = browser.contexts[0] if browser.contexts else None
        if not ctx:
            await browser.close()
            raise RuntimeError("Nenhum contexto encontrado no Chrome")

        # Extrai cookies
        all_cookies = await ctx.cookies([BYETECH_URL, API_URL, "https://byetech.pro"])
        await browser.close()

    cookies = {c["name"]: c["value"] for c in all_cookies}
    return cookies


async def capturar_via_playwright_novo() -> dict:
    """Abre Chrome novo com o perfil existente e captura cookies (Chrome deve estar fechado)."""
    from playwright.async_api import async_playwright

    chrome_exe = find_chrome()
    if not Path(chrome_exe).exists():
        raise FileNotFoundError(f"Chrome nao encontrado: {chrome_exe}")

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            user_data_dir=str(CHROME_USER_DATA),
            channel="chrome",
            headless=True,
            args=["--no-first-run", "--no-default-browser-check"],
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        # Navega para o CRM para garantir que os cookies sejam carregados
        try:
            await page.goto(BYETECH_URL, wait_until="networkidle", timeout=15000)
        except Exception:
            pass
        await page.wait_for_timeout(2000)

        all_cookies = await ctx.cookies([BYETECH_URL, API_URL])
        await ctx.close()

    return {c["name"]: c["value"] for c in all_cookies}


async def main():
    print("=" * 60)
    print("  Capturador de Cookies Byetech")
    print("=" * 60)

    cookies = {}

    # Tenta metodo 1: CDP (Chrome ja aberto com --remote-debugging-port)
    print("\n[1/2] Tentando conectar ao Chrome via CDP (porta 9222)...")
    try:
        cookies = await capturar_via_cdp()
        print(f"     Conectado! {len(cookies)} cookies encontrados.")
    except ConnectionError as e:
        print(f"     Nao conectou: Chrome nao esta com debug remoto habilitado.")
        print()

        # Verifica se Chrome esta aberto
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq chrome.exe", "/NH"],
            capture_output=True, text=True, timeout=5
        )
        chrome_aberto = "chrome.exe" in result.stdout.lower()

        if chrome_aberto:
            print("     Chrome esta aberto mas SEM --remote-debugging-port.")
            print("     Criando atalho 'Chrome Byetech'...")
            create_shortcut()
            print()
            print("     PROXIMOS PASSOS:")
            print("     1. Feche o Chrome atual")
            print("     2. Abra o Chrome pelo atalho 'Chrome Byetech' na area de trabalho")
            print("     3. Logue no Byetech CRM normalmente (com 2FA se necessario)")
            print("     4. Execute este script novamente")
            return
        else:
            # Chrome fechado: tenta com perfil existente
            print("[2/2] Chrome fechado. Abrindo com perfil existente...")
            try:
                cookies = await capturar_via_playwright_novo()
                print(f"     {len(cookies)} cookies carregados do perfil.")
            except Exception as e2:
                print(f"     Falhou: {e2}")

    if not cookies:
        print("\nNenhum cookie capturado.")
        print("\nSolucao rapida agora (requer novo 2FA):")
        print("  python _refresh_byetech_session.py SEU_CODIGO_2FA")
        print()
        print("Solucao permanente (sem 2FA no futuro):")
        print("  1. Use o atalho 'Chrome Byetech' para abrir o Chrome")
        print("  2. Logue no Byetech normalmente")
        print("  3. Execute: python capturar_cookies_chrome.py")
        return

    # Verifica cookies essenciais
    print(f"\nCookies: {list(cookies.keys())}")
    essenciais = ["byetech_session", "XSRF-TOKEN"]
    faltando = [k for k in essenciais if k not in cookies]
    if faltando:
        print(f"AVISO: Faltam cookies: {faltando}")
        print("Certifique-se de estar logado no Byetech no Chrome.")

    # Salva
    with open(SESSION_FILE, "w", encoding="utf-8") as f:
        json.dump(cookies, f, indent=2)
    print(f"\nSalvos em {SESSION_FILE}")

    # Testa
    print("Testando sessao na API...")
    from app.scrapers.byetech_crm import _test_session
    ok = await _test_session(cookies)
    print(f"Sessao api-production valida: {'SIM' if ok else 'NAO'}")

    if ok:
        print("\nPROXIMO PASSO: execute o seguinte e pronto!")
        print("  python push_session_render.py && python processar_pendentes.py")
    else:
        print("\nSessao invalida. Possivelmente expirada no Chrome.")
        print("Refaca o login no Byetech no Chrome e execute este script novamente.")


if __name__ == "__main__":
    asyncio.run(main())
