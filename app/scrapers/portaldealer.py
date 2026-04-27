"""
Scraper do Portal Dealer (lmmobilidade.com.br)
Usado para: GWM, Sign, Drive (login 1) e LM (login 2)

Processo:
1. Login
2. Consulta por CPF/CNPJ de cada cliente dos contratos ativos
3. Acha o pedido "concluído"
4. Expande e coleta etapas de entrega do veículo
5. Detecta variações de etapa desde a última consulta
"""
import re
from datetime import datetime
from typing import Optional
from playwright.async_api import async_playwright, Page
from dotenv import load_dotenv
import os

load_dotenv()

PORTAL_URL = os.getenv("PORTALDEALER_URL", "https://portaldealer.lmmobilidade.com.br")

ACCOUNTS = {
    "GWM": {
        "login": os.getenv("PORTALDEALER_LOGIN_GWM", ""),
        "password": os.getenv("PORTALDEALER_PASS_GWM", ""),
        "fontes": ["GWM", "SIGN", "DRIVE"],
    },
    "LM": {
        "login": os.getenv("PORTALDEALER_LOGIN_LM", ""),
        "password": os.getenv("PORTALDEALER_PASS_LM", ""),
        "fontes": ["LM"],
    },
}

ETAPAS_ENTREGUE = [
    "entregue",
    "entrega realizada",
    "veículo entregue",
    "definitivo",
]


def _is_etapa_final(etapa: str) -> bool:
    return any(e in etapa.lower() for e in ETAPAS_ENTREGUE)


def _parse_date(text: str) -> Optional[datetime]:
    if not text:
        return None
    text = text.strip()
    for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d/%m/%Y %H:%M"]:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


async def _login(page: Page, login: str, password: str) -> bool:
    """Faz login no portal."""
    try:
        await page.goto(PORTAL_URL, wait_until="networkidle", timeout=20000)
    except Exception:
        pass

    # Campo de usuário/CPF — o portal usa placeholder "CPF"
    login_selectors = [
        'input[placeholder*="CPF"]', 'input[placeholder*="cpf"]',
        'input[placeholder*="usuario"]', 'input[placeholder*="login"]',
        'input[name="login"]', 'input[name="username"]',
        'input[name="cpf"]', 'input[name="email"]',
        'input[type="email"]', 'input[id*="login"]',
    ]
    pass_selectors = [
        'input[type="password"]', 'input[name="password"]',
        'input[placeholder*="senha"]', 'input[placeholder*="Senha"]',
        'input[id*="senha"]', 'input[id*="password"]',
    ]

    for sel in login_selectors:
        el = await page.query_selector(sel)
        if el:
            await el.fill(login)
            break

    for sel in pass_selectors:
        el = await page.query_selector(sel)
        if el:
            await el.fill(password)
            break

    # Tenta clicar no botão de submit
    for sel in ['button[type="submit"]', 'input[type="submit"]',
                'button:has-text("Entrar")', 'button:has-text("Login")',
                '.btn-login', '#btn-login']:
        try:
            await page.click(sel, timeout=2000)
            break
        except Exception:
            continue
    else:
        await page.keyboard.press("Enter")

    try:
        await page.wait_for_load_state("networkidle", timeout=20000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)

    # Verifica se logou — portal redireciona para dashboard/pedidos após login OK
    url = page.url.lower()
    content = (await page.inner_text("body")).lower()
    if "erro na autenticação" in content or "erro na autenticacao" in content:
        return False
    if "sign_in" in url or ("login" in url and "dashboard" not in url):
        return False
    return True


async def _buscar_cliente(page: Page, cpf_cnpj: str) -> list[dict]:
    """Busca pedidos de um cliente pelo CPF/CNPJ."""
    pedidos = []
    cpf_limpo = re.sub(r"[^\d]", "", cpf_cnpj)

    # Busca campo de pesquisa
    search_selectors = [
        'input[placeholder*="CPF"]', 'input[placeholder*="CNPJ"]',
        'input[placeholder*="cpf"]', 'input[placeholder*="documento"]',
        'input[name*="cpf"]', 'input[name*="search"]', 'input[name*="busca"]',
        '#search', '.search-input', 'input[type="search"]',
    ]

    search_field = None
    for sel in search_selectors:
        el = await page.query_selector(sel)
        if el:
            search_field = el
            break

    if not search_field:
        # Tenta navegar diretamente para URL de busca
        try:
            await page.goto(f"{PORTAL_URL}/search?q={cpf_limpo}", wait_until="networkidle", timeout=15000)
        except Exception:
            pass
    else:
        await search_field.fill(cpf_limpo)
        await page.keyboard.press("Enter")
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

    await page.wait_for_timeout(1500)

    # Coleta resultados/pedidos
    pedidos = await _extrair_pedidos_pagina(page, cpf_limpo)
    return pedidos


async def _extrair_pedidos_pagina(page: Page, cpf_cnpj: str) -> list[dict]:
    """Extrai pedidos da página de resultado de busca."""
    pedidos = []

    # Seletores genéricos para pedidos/contratos
    item_selectors = [
        ".order-item", ".pedido-item", ".contract-item",
        "tr.pedido", "tr.order", ".card-pedido",
        "[data-type='order']", "[data-order-id]",
    ]

    items = []
    for sel in item_selectors:
        items = await page.query_selector_all(sel)
        if items:
            break

    if not items:
        # Fallback: tenta linhas de tabela
        items = await page.query_selector_all("tbody tr")

    for item in items:
        try:
            text = await item.inner_text()
            if not text.strip():
                continue

            # Verifica se é pedido concluído
            status_text = text.lower()
            is_concluido = any(s in status_text for s in ["concluído", "concluido", "aprovado", "ativo"])

            pedido_id_match = re.search(r"\b(\d{6,})\b", text)
            pedido_id = pedido_id_match.group(1) if pedido_id_match else ""

            pedido = {
                "id_externo": pedido_id,
                "cliente_cpf_cnpj": cpf_cnpj,
                "status_pedido": "concluido" if is_concluido else "pendente",
                "etapas": [],
                "raw_text": text,
            }

            # Expande o pedido para ver etapas de entrega
            expand_btn = await item.query_selector(
                "button.expand, .toggle-details, [data-toggle], .btn-expand, "
                "button[aria-expanded], .accordion-toggle"
            )
            if expand_btn:
                await expand_btn.click()
                await page.wait_for_timeout(800)

            # Tenta link de detalhe
            link = await item.query_selector("a[href*='pedido'], a[href*='order'], a[href*='contrato']")
            if link:
                href = await link.get_attribute("href")
                if href:
                    etapas = await _get_etapas_entrega(page, href)
                    pedido["etapas"] = etapas
                    if etapas:
                        pedido["status_atual"] = etapas[-1].get("nome", "")
                        pedido["data_ultima_etapa"] = etapas[-1].get("data")
                        pedido["entregue"] = _is_etapa_final(pedido["status_atual"])

            pedidos.append(pedido)

        except Exception:
            continue

    return pedidos


async def _get_etapas_entrega(page: Page, href: str) -> list[dict]:
    """Acessa detalhe do pedido e coleta etapas de entrega."""
    etapas = []
    try:
        detail_page = await page.context.new_page()
        base = PORTAL_URL
        url = href if href.startswith("http") else base + href

        try:
            await detail_page.goto(url, wait_until="networkidle", timeout=15000)
        except Exception:
            pass
        await detail_page.wait_for_timeout(1000)

        # Busca seção de entrega/etapas
        etapa_selectors = [
            ".etapa-entrega", ".delivery-step", ".step-item",
            ".timeline-item", ".delivery-stage", "[data-step]",
            ".entrega-etapa", "li.etapa",
        ]

        items = []
        for sel in etapa_selectors:
            items = await detail_page.query_selector_all(sel)
            if items:
                break

        for item in items:
            text = await item.inner_text()
            date_match = re.search(r"\d{2}/\d{2}/\d{4}", text)
            etapa = {
                "nome": text.strip().split("\n")[0],
                "data": _parse_date(date_match.group()) if date_match else None,
                "concluida": any(c in text.lower() for c in ["✓", "✔", "concluída", "ok", "feito"]),
            }
            etapas.append(etapa)

        # Busca também informações do veículo
        placa_match = re.search(r"[A-Z]{3}[-\s]?\d[A-Z0-9]\d{2}", await detail_page.content())
        if placa_match:
            # Retorna placa junto com etapas como metadado especial
            etapas.insert(0, {"_placa": placa_match.group(), "_meta": True})

        await detail_page.close()
    except Exception:
        pass

    return etapas


async def scrape_portaldealer(clientes: list[dict], account_key: str = "GWM") -> list[dict]:
    """
    Executa scraping do portal para uma conta específica.
    clientes: lista de dicts com {cpf_cnpj, nome, byetech_contrato_id, fonte}
    account_key: "GWM" ou "LM"
    """
    account = ACCOUNTS.get(account_key)
    if not account:
        raise ValueError(f"Conta '{account_key}' não encontrada")

    resultados = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        logged = await _login(page, account["login"], account["password"])
        if not logged:
            await browser.close()
            raise Exception(f"Falha no login do portal ({account_key})")

        import asyncio as _asyncio
        for cliente in clientes:
            try:
                # Aceita tanto "cliente_cpf_cnpj" (sync_service) quanto "cpf_cnpj" (legado)
                cpf_raw = cliente.get("cliente_cpf_cnpj") or cliente.get("cpf_cnpj", "")
                cpf = re.sub(r"[^\d]", "", cpf_raw)
                if not cpf:
                    continue

                # Aceita tanto "cliente_nome" quanto "nome"
                nome = cliente.get("cliente_nome") or cliente.get("nome", "")

                pedidos = await _asyncio.wait_for(_buscar_cliente(page, cpf), timeout=30)

                for pedido in pedidos:
                    # Filtra etapas meta
                    etapas_reais = [e for e in pedido.get("etapas", []) if not e.get("_meta")]
                    placa = next(
                        (e.get("_placa") for e in pedido.get("etapas", []) if e.get("_meta")),
                        None,
                    )

                    resultado = {
                        "fonte": account["fontes"][0],
                        "id_externo": pedido.get("id_externo", ""),
                        "byetech_contrato_id": cliente.get("byetech_contrato_id", ""),
                        "cliente_nome": nome,
                        "cliente_cpf_cnpj": cpf,
                        "placa": placa or cliente.get("placa", ""),
                        "veiculo": cliente.get("veiculo", ""),
                        "status_atual": pedido.get("status_atual", ""),
                        "etapas": etapas_reais,
                        "entregue": pedido.get("entregue", False),
                        "data_ultima_etapa": pedido.get("data_ultima_etapa"),
                    }
                    resultados.append(resultado)

            except Exception as e:
                resultados.append({
                    "fonte": account["fontes"][0],
                    "cliente_cpf_cnpj": cpf_raw if "cpf_raw" in dir() else "",
                    "byetech_contrato_id": cliente.get("byetech_contrato_id", ""),
                    "erro": str(e),
                })

        await browser.close()

    return resultados
