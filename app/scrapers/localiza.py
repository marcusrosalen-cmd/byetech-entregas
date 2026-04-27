"""
Scraper Localiza (localiza.my.site.com/meoorevendas)

Processo:
1. Login no portal
2. Para cada cliente: vai em Account > filtra CPF/CNPJ
3. Acessa perfil > Histórico
4. Encontra contratos ativos — usa sempre a ÚLTIMA atualização
"""
import re
import os
import asyncio
from datetime import datetime
from typing import Optional
from playwright.async_api import async_playwright, Page
from dotenv import load_dotenv

load_dotenv()

LOCALIZA_URL = os.getenv("LOCALIZA_URL", "https://localiza.my.site.com/meoorevendas/s/")
LOCALIZA_EMAIL = os.getenv("LOCALIZA_EMAIL", "")
LOCALIZA_PASSWORD = os.getenv("LOCALIZA_PASSWORD", "")


def _parse_date(text: str) -> Optional[datetime]:
    if not text:
        return None
    for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]:
        try:
            return datetime.strptime(text.strip(), fmt)
        except ValueError:
            continue
    return None


async def _login(page: Page) -> bool:
    """Login no portal Localiza."""
    login_url = "https://localiza.my.site.com/meoorevendas/login"
    try:
        await page.goto(login_url, wait_until="networkidle", timeout=20000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)

    # Salesforce-based login
    email_selectors = [
        'input[name="username"]', 'input[type="email"]',
        'input[id="username"]', 'input[placeholder*="email"]',
        'input[placeholder*="usuário"]',
    ]
    pass_selectors = [
        'input[name="password"]', 'input[type="password"]',
        'input[id="password"]',
    ]

    for sel in email_selectors:
        el = await page.query_selector(sel)
        if el:
            await el.fill(LOCALIZA_EMAIL)
            break

    for sel in pass_selectors:
        el = await page.query_selector(sel)
        if el:
            await el.fill(LOCALIZA_PASSWORD)
            break

    await page.keyboard.press("Enter")
    try:
        await page.click('input[type="submit"], button[type="submit"]', timeout=3000)
    except Exception:
        pass

    try:
        await page.wait_for_load_state("networkidle", timeout=20000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)

    return "login" not in page.url.lower()


async def _buscar_conta_por_cpf(page: Page, cpf_cnpj: str) -> Optional[str]:
    """Navega para Accounts, filtra por CPF/CNPJ e retorna URL do perfil."""
    cpf_limpo = re.sub(r"[^\d]", "", cpf_cnpj)

    # Navega para lista de Accounts
    accounts_url = "https://localiza.my.site.com/meoorevendas/s/account"
    try:
        await page.goto(accounts_url, wait_until="networkidle", timeout=20000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)

    # Busca campo de filtro
    search_selectors = [
        'input[placeholder*="Pesquisar"]', 'input[placeholder*="Search"]',
        'input[placeholder*="CPF"]', 'input[placeholder*="CNPJ"]',
        '.searchInput input', 'lightning-input input', 'input[type="search"]',
    ]
    for sel in search_selectors:
        el = await page.query_selector(sel)
        if el:
            await el.fill(cpf_limpo)
            await page.keyboard.press("Enter")
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            await page.wait_for_timeout(1500)
            break

    # Pega primeiro resultado
    result_selectors = [
        "a[href*='/account/']", ".slds-truncate a", "table tbody tr:first-child a",
        ".accountName a", "th a",
    ]
    for sel in result_selectors:
        el = await page.query_selector(sel)
        if el:
            href = await el.get_attribute("href")
            if href:
                return href
    return None


async def _get_historico_ativacao(page: Page, account_href: str) -> list[dict]:
    """
    Acessa perfil da conta > aba Histórico.
    Retorna contratos com status de ativação (última atualização).
    """
    contratos = []

    base = "https://localiza.my.site.com"
    url = account_href if account_href.startswith("http") else base + account_href
    try:
        await page.goto(url, wait_until="networkidle", timeout=20000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)

    # Busca aba "Histórico" ou "Atividade"
    tab_selectors = [
        'a[title="Histórico"]', 'a[title="History"]',
        'a[title="Atividade"]', 'lightning-tab[label*="Hist"]',
        'a[data-label*="Hist"]',
    ]
    for sel in tab_selectors:
        el = await page.query_selector(sel)
        if el:
            await el.click()
            await page.wait_for_timeout(1500)
            break

    # Extrai itens do histórico
    history_selectors = [
        ".timeline-item", ".slds-timeline__item", "li[data-type]",
        ".activityItem", ".historyItem",
    ]
    items = []
    for sel in history_selectors:
        items = await page.query_selector_all(sel)
        if items:
            break

    if not items:
        # Fallback: tabela de histórico
        items = await page.query_selector_all("table tbody tr")

    most_recent = None
    most_recent_date = None

    for item in items:
        try:
            text = await item.inner_text()
            if not text.strip():
                continue

            # Busca menção a contrato ativo
            if "ativ" not in text.lower() and "contrat" not in text.lower():
                continue

            date_match = re.search(r"\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2}", text)
            item_date = _parse_date(date_match.group()) if date_match else None

            # Número do contrato
            contrato_match = re.search(r"\b([A-Z0-9]{6,20})\b", text)
            contrato_num = contrato_match.group(1) if contrato_match else ""

            record = {
                "status_atual": "Contrato Ativo",
                "data_ativacao": item_date,
                "contrato_localiza": contrato_num,
                "raw": text.strip()[:200],
            }

            # Mantém a mais recente
            if most_recent_date is None or (item_date and item_date > most_recent_date):
                most_recent = record
                most_recent_date = item_date

        except Exception:
            continue

    if most_recent:
        contratos.append(most_recent)

    return contratos


async def scrape_localiza(clientes: list[dict]) -> list[dict]:
    """
    clientes: lista de dicts com {cpf_cnpj, nome, byetech_contrato_id}
    Retorna status de ativação para cada cliente.
    """
    resultados = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        logged = await _login(page)
        if not logged:
            await browser.close()
            raise Exception("Falha no login da Localiza")

        for cliente in clientes:
            # Aceita tanto "cliente_cpf_cnpj" (sync_service) quanto "cpf_cnpj" (legado)
            cpf_raw = cliente.get("cliente_cpf_cnpj") or cliente.get("cpf_cnpj", "")
            cpf = re.sub(r"[^\d]", "", cpf_raw)
            # Aceita tanto "cliente_nome" quanto "nome"
            nome = cliente.get("cliente_nome") or cliente.get("nome", "")
            if not cpf:
                continue

            try:
                account_href = await asyncio.wait_for(
                    _buscar_conta_por_cpf(page, cpf), timeout=30
                )

                if not account_href:
                    resultados.append({
                        "fonte": "LOCALIZA",
                        "cliente_cpf_cnpj": cpf,
                        "cliente_nome": nome,
                        "byetech_contrato_id": cliente.get("byetech_contrato_id", ""),
                        "status_atual": "Não encontrado no portal",
                        "erro": "CPF/CNPJ não localizado",
                    })
                    continue

                historico = await _get_historico_ativacao(page, account_href)

                if historico:
                    ultimo = historico[0]
                    resultados.append({
                        "fonte": "LOCALIZA",
                        "id_externo": ultimo.get("contrato_localiza", ""),
                        "cliente_cpf_cnpj": cpf,
                        "cliente_nome": nome,
                        "veiculo": cliente.get("veiculo", ""),
                        "byetech_contrato_id": cliente.get("byetech_contrato_id", ""),
                        "status_atual": ultimo.get("status_atual", "Contrato Ativo"),
                        "data_ultima_atualizacao": ultimo.get("data_ativacao"),
                        "entregue": False,  # Localiza só informa quando ativa
                    })
                else:
                    resultados.append({
                        "fonte": "LOCALIZA",
                        "cliente_cpf_cnpj": cpf,
                        "cliente_nome": nome,
                        "veiculo": cliente.get("veiculo", ""),
                        "byetech_contrato_id": cliente.get("byetech_contrato_id", ""),
                        "status_atual": "Aguardando ativação",
                    })

            except Exception as e:
                resultados.append({
                    "fonte": "LOCALIZA",
                    "cliente_cpf_cnpj": cpf,
                    "byetech_contrato_id": cliente.get("byetech_contrato_id", ""),
                    "erro": str(e),
                })

        await browser.close()

    return resultados
