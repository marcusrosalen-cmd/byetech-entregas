"""
Scraper do Portal Dealer (portaldealer.lmmobilidade.com.br)
Usado para: GWM, Sign & Drive (conta GWM) e LM

Abordagem: pagina toda a listagem de pedidos do portal e extrai CPF + status.
Nao busca por CPF individualmente (portal nao tem essa funcionalidade estavel).
Detecta contratos em estagio final ("Contrato" concluido) como indicador de entrega iminente.
"""
import re
import logging
from datetime import datetime
from typing import Optional
from playwright.async_api import async_playwright, Page
from dotenv import load_dotenv
import os

load_dotenv()
logger = logging.getLogger("portaldealer")

PORTAL_URL = os.getenv("PORTALDEALER_URL", "https://portaldealer.lmmobilidade.com.br")

ACCOUNTS = {
    "GWM": {
        "login": os.getenv("PORTALDEALER_LOGIN_GWM", ""),
        "password": os.getenv("PORTALDEALER_PASS_GWM", ""),
        "fontes": ["GWM", "SIGN", "DRIVE", "SIGN & DRIVE", "VW"],
    },
    "LM": {
        "login": os.getenv("PORTALDEALER_LOGIN_LM", ""),
        "password": os.getenv("PORTALDEALER_PASS_LM", ""),
        "fontes": ["LM"],
    },
}

# Etapas que indicam contrato concluido / iminente de entrega
ETAPAS_CONTRATO_CONCLUIDO = [
    "contrato", "ativo", "entregue", "entrega",
    "concluido", "concluída", "aprovado",
]

# Etapas de cancelamento / reprovação — não vira entrega
ETAPAS_CANCELADO = [
    "cancelado", "reprovado", "recusado", "expirado",
]


def _normalize(text: str) -> str:
    """Remove acentos para comparação robusta."""
    import unicodedata
    return unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode("ascii").lower()


def _is_etapa_contrato(etapa: str) -> bool:
    s = _normalize(etapa)
    return any(e in s for e in ETAPAS_CONTRATO_CONCLUIDO)


def _is_cancelado(etapa: str) -> bool:
    s = _normalize(etapa)
    return any(e in s for e in ETAPAS_CANCELADO)


def _clean_cpf(text: str) -> str:
    return re.sub(r"[^\d]", "", text or "")


def _parse_date(text: str) -> Optional[datetime]:
    if not text:
        return None
    text = text.strip()
    for fmt in ["%d/%m/%Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


async def _login(page: Page, login: str, password: str) -> bool:
    """Faz login no portal."""
    try:
        await page.goto(PORTAL_URL, wait_until="networkidle", timeout=25000)
    except Exception:
        pass

    await page.wait_for_timeout(1500)

    # Preenche login
    login_selectors = [
        'input[placeholder*="CPF"]', 'input[placeholder*="cpf"]',
        'input[placeholder*="usuario"]', 'input[placeholder*="login"]',
        'input[name="login"]', 'input[name="username"]',
        'input[name="cpf"]', 'input[name="email"]',
        'input[type="email"]', 'input[id*="login"]',
    ]
    for sel in login_selectors:
        try:
            el = await page.wait_for_selector(sel, state="visible", timeout=3000)
            if el:
                await el.click()
                await el.fill(login)
                break
        except Exception:
            continue

    # Preenche senha
    for sel in ['input[type="password"]', 'input[name="password"]',
                 'input[placeholder*="senha"]', 'input[id*="senha"]']:
        try:
            el = await page.query_selector(sel)
            if el:
                await el.click()
                await el.fill(password)
                break
        except Exception:
            continue

    # Submit
    for sel in ['button[type="submit"]', 'button:has-text("Entrar")',
                 'button:has-text("Login")', '.btn-login']:
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

    url = page.url.lower()
    content = (await page.inner_text("body")).lower()
    if "erro na autenticação" in content or "erro na autenticacao" in content:
        return False
    if "sign_in" in url or ("login" in url and "pedidos" not in url and "orders" not in url):
        return False
    return True


async def _clear_segment_filter(page: Page):
    """
    Limpa o filtro de segmento para que o portal mostre TODOS os pedidos
    (Sign & Drive, Assine Car GWM, Volkswagen, etc.).
    O portal Ant Design tem um select de segmento que por padrão pode estar
    filtrado para apenas um segmento.
    """
    try:
        await page.wait_for_timeout(1000)
        # Tenta clicar em botão de limpar filtros
        for sel in [
            'button:has-text("Limpar")',
            'button:has-text("Limpar filtros")',
            'button:has-text("Todos")',
            '.ant-btn:has-text("Limpar")',
        ]:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    await page.wait_for_timeout(800)
                    logger.info("[portaldealer] Filtro limpo via botão")
                    break
            except Exception:
                continue

        # Tenta selects de segmento — busca por placeholder "Segmento" ou similar
        for placeholder in ["Segmento", "segmento", "Segment"]:
            try:
                sel_el = page.locator(f".ant-select:has(.ant-select-selection-placeholder:has-text('{placeholder}'))")
                if await sel_el.count() == 0:
                    # Tenta pelo label próximo
                    sel_el = page.locator(f"[placeholder*='{placeholder}']").first
                if await sel_el.count() > 0:
                    # Já está sem seleção (placeholder visível = sem filtro), ok
                    pass
            except Exception:
                continue

        # Se há um select com valor selecionado (não placeholder), limpa clicando no X
        try:
            clear_icons = await page.query_selector_all(".ant-select-clear")
            for icon in clear_icons:
                await icon.click()
                await page.wait_for_timeout(500)
            if clear_icons:
                logger.info(f"[portaldealer] {len(clear_icons)} filtro(s) de select limpos")
        except Exception:
            pass

        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        await page.wait_for_timeout(500)
    except Exception as e:
        logger.warning(f"[portaldealer] _clear_segment_filter: {e}")


async def _search_by_document(page: Page, cpf_formatted: str) -> list[dict]:
    """
    Usa a busca por coluna 'Documento' do portal Ant Design para encontrar
    pedidos de um CPF específico, independente do filtro de data ativo.
    cpf_formatted: CPF no formato "XXX.XXX.XXX-XX"
    """
    try:
        # Clica no ícone de busca da coluna Documento (Q icon no header)
        doc_search_icons = [
            'th:has-text("Documento") .ant-table-filter-trigger',
            'th:has-text("Documento") button',
            '.ant-table-filter-column:has-text("Documento") .ant-table-filter-trigger',
        ]
        clicked = False
        for sel in doc_search_icons:
            try:
                icon = await page.query_selector(sel)
                if icon:
                    await icon.click()
                    await page.wait_for_timeout(600)
                    clicked = True
                    break
            except Exception:
                continue

        if not clicked:
            return []

        # Preenche o campo de busca que apareceu
        search_input = await page.query_selector(
            ".ant-table-filter-dropdown input, "
            ".ant-dropdown input[type='text'], "
            "input[placeholder*='Buscar'], input[placeholder*='Search'], "
            "input[placeholder*='Pesquisar']"
        )
        if not search_input:
            # Fecha o dropdown sem fazer nada
            await page.keyboard.press("Escape")
            return []

        await search_input.click(click_count=3)
        await search_input.fill(cpf_formatted)
        await page.wait_for_timeout(300)

        # Clica em "Buscar" / "OK" / pressiona Enter
        for sel in [
            'button:has-text("Buscar")',
            'button:has-text("OK")',
            'button[type="submit"]',
            '.ant-btn-primary',
        ]:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    break
            except Exception:
                continue
        else:
            await page.keyboard.press("Enter")

        await page.wait_for_timeout(1500)
        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass

        # Extrai os resultados
        rows = await _extract_table_rows(page)

        # Limpa o filtro clicando no reset / "Limpar"
        for sel in [
            'button:has-text("Limpar")',
            'button:has-text("Redefinir")',
            'button:has-text("Reset")',
        ]:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    await page.wait_for_timeout(500)
                    break
            except Exception:
                continue
        else:
            # Fecha o dropdown
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)

        try:
            await page.wait_for_load_state("networkidle", timeout=6000)
        except Exception:
            pass

        return rows

    except Exception as e:
        logger.warning(f"[portaldealer] _search_by_document({cpf_formatted}): {e}")
        return []


async def _format_cpf(cpf: str) -> str:
    """Formata CPF de 11 dígitos para XXX.XXX.XXX-XX."""
    c = re.sub(r"[^\d]", "", cpf)
    if len(c) == 11:
        return f"{c[:3]}.{c[3:6]}.{c[6:9]}-{c[9:]}"
    return cpf


async def _set_page_size(page: Page, size: int = 100):
    """Tenta aumentar itens por página."""
    try:
        # Ant Design pagination select
        paginator = await page.query_selector(".ant-select-selector")
        if paginator:
            await paginator.click()
            await page.wait_for_timeout(500)
            opt = await page.query_selector(f"[title='{size} / página'], [title='{size}/página']")
            if opt:
                await opt.click()
                await page.wait_for_timeout(1000)
    except Exception:
        pass


async def _set_date_range(page: Page, days_back: int = 180):
    """Define o range de datas para cobrir os ultimos N dias."""
    try:
        from datetime import timedelta
        hoje = datetime.now()
        inicio = hoje - timedelta(days=days_back)
        inicio_str = inicio.strftime("%d/%m/%Y")
        hoje_str = hoje.strftime("%d/%m/%Y")

        # Usa locators (auto-retry, nao desancora do DOM)
        ini_loc = page.locator("input[placeholder='Data inicial']")
        fim_loc = page.locator("input[placeholder='Data final']")

        if await ini_loc.count() >= 1:
            await ini_loc.first.click(click_count=3)
            await ini_loc.first.fill(inicio_str)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(300)

        if await fim_loc.count() >= 1:
            await fim_loc.first.click(click_count=3)
            await fim_loc.first.fill(hoje_str)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(500)

        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        await page.wait_for_timeout(1000)
    except Exception as e:
        logger.warning(f"[portaldealer] Erro ao definir datas: {e}")


async def _extract_table_rows(page: Page) -> list[dict]:
    """Extrai linhas da tabela de pedidos da pagina atual."""
    rows = []
    try:
        # Aguarda linhas reais (exclui ant-table-measure-row que e hidden)
        await page.wait_for_selector(
            ".ant-table-row:not(.ant-table-measure-row), table tbody tr:not([aria-hidden='true'])",
            timeout=10000,
        )
        await page.wait_for_timeout(500)

        # Pega apenas linhas visiveis (nao hidden, nao measure-row)
        tr_list = await page.query_selector_all(
            ".ant-table-row:not(.ant-table-measure-row):not([aria-hidden='true'])"
        )
        if not tr_list:
            tr_list = await page.query_selector_all(
                "tbody tr:not([aria-hidden='true'])"
            )

        for tr in tr_list:
            try:
                cells = await tr.query_selector_all("td")
                if not cells or len(cells) < 4:
                    continue

                texts = []
                for cell in cells:
                    texts.append((await cell.inner_text()).strip())

                # Estrutura esperada da tabela:
                # Pedido | Segmento | Tipo | Nome | Documento (CPF) | Data Inclusão | Data Status | Valor | Status | Ações
                if len(texts) < 5:
                    continue

                # Extrai CPF (coluna Documento — geralmente a 5a coluna, índice 4)
                cpf_raw = ""
                for t in texts:
                    if re.search(r"\d{3}\.\d{3}\.\d{3}-\d{2}|\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}", t):
                        cpf_raw = t
                        break

                cpf = _clean_cpf(cpf_raw)
                if not cpf:
                    continue

                # Pedido ID — padrões conhecidos: SDI/SDB (Sign & Drive), ACF (Assine Car GWM/VW), GWM, LM, CI
                pedido_id = ""
                nome = ""
                status = ""
                data_status = None

                for t in texts:
                    if re.match(r"^(SDI|SDB|ACF|GWM|LM|CI)\d+", t) and not pedido_id:
                        pedido_id = t
                    elif re.match(r"\d{2}/\d{2}/\d{4}", t) and not data_status:
                        data_status = _parse_date(t)

                # Nome é tipicamente o campo mais longo antes do CPF
                for i, t in enumerate(texts):
                    if len(t) > 10 and not re.search(r"\d{3}\.\d{3}\.\d{3}|\d{2}/\d{2}/\d{4}|R\$|\d+%", t):
                        if not re.match(r"^(SDI|SDB|ACF|GWM|LM|CI)\d+", t) and "Drive" not in t and "Sign" not in t:
                            nome = t
                            break

                # Status: último campo relevante antes de Ações
                for t in reversed(texts[:-1]):
                    if len(t) > 5 and not re.search(r"R\$|\d{2}/\d{2}/\d{4}|\d{3}\.\d{3}\.\d{3}", t):
                        if not re.match(r"^(SDI|SDB|GWM|LM)\d+", t):
                            status = t
                            break

                rows.append({
                    "pedido_id": pedido_id,
                    "nome": nome,
                    "cpf": cpf,
                    "status": status,
                    "data_status": data_status,
                    "texts": texts,
                })
            except Exception:
                continue

    except Exception as e:
        logger.warning(f"[portaldealer] Erro ao extrair tabela: {e}")

    return rows


async def _get_all_orders(page: Page) -> list[dict]:
    """Coleta todos os pedidos paginando a tabela do portal."""
    all_rows = []

    # Aumenta itens por página para 100
    await _set_page_size(page, 100)

    page_num = 1
    max_pages = 200  # Aumentado para cobrir janelas de 2 anos
    while page_num <= max_pages:
        rows = await _extract_table_rows(page)
        if not rows:
            break

        all_rows.extend(rows)
        logger.info(f"[portaldealer] Página {page_num}: {len(rows)} pedidos extraídos")

        # Tenta ir para próxima página
        try:
            next_btn = await page.query_selector(
                ".ant-pagination-next:not(.ant-pagination-disabled) button, "
                "button[aria-label='Next Page']:not([disabled]), "
                "li.ant-pagination-next:not(.ant-pagination-disabled)"
            )
            if not next_btn:
                break
            await next_btn.click()
            await page.wait_for_timeout(1500)
            try:
                await page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass
            page_num += 1
        except Exception:
            break

    logger.info(f"[portaldealer] Total extraído: {len(all_rows)} pedidos em {page_num} páginas")
    return all_rows


async def scrape_portaldealer(clientes: list[dict], account_key: str = "GWM") -> list[dict]:
    """
    Executa scraping do portal para uma conta especifica.
    Pagina toda a listagem e cruza por CPF com os clientes fornecidos.

    clientes: lista de dicts com {cliente_cpf_cnpj, cliente_nome, byetech_contrato_id, ...}
    account_key: "GWM" ou "LM"

    Retorna: lista de resultados com status atual do portal por contrato.
    """
    account = ACCOUNTS.get(account_key)
    if not account:
        raise ValueError(f"Conta '{account_key}' nao encontrada")

    resultados = []

    # Monta indice CPF -> cliente
    cpf_index: dict[str, dict] = {}
    for cli in clientes:
        cpf_raw = cli.get("cliente_cpf_cnpj") or cli.get("cpf_cnpj", "")
        cpf = _clean_cpf(cpf_raw)
        if cpf:
            cpf_index[cpf] = cli
            # Variantes de CPF
            if len(cpf) == 11:
                cpf_index[cpf.zfill(11)] = cli
            elif len(cpf) == 12:
                cpf_index[cpf[:-1]] = cli

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        logged = await _login(page, account["login"], account["password"])
        if not logged:
            await browser.close()
            raise Exception(f"Falha no login do portal ({account_key})")

        logger.info(f"[portaldealer] Login OK — URL: {page.url}")

        # Limpa filtros de segmento para exibir TODOS os tipos de pedido
        # (Sign & Drive SDI, Assine Car GWM ACF, Volkswagen CI, etc.)
        await _clear_segment_filter(page)

        # Define range de data amplo (6 meses) para pegar mais contratos
        await _set_date_range(page, days_back=180)

        # Coleta todos os pedidos da listagem
        portal_orders = await _get_all_orders(page)
        await browser.close()

    logger.info(f"[portaldealer] {len(portal_orders)} pedidos lidos do portal")

    # Cruza com contratos do banco por CPF
    matched = set()
    for order in portal_orders:
        cpf = order.get("cpf", "")
        cli = cpf_index.get(cpf)
        if not cli:
            # Tenta variantes
            for v in [cpf.zfill(11), cpf[:-1] if len(cpf) == 12 else cpf]:
                cli = cpf_index.get(v)
                if cli:
                    break

        if not cli:
            continue

        cpf_key = _clean_cpf(cli.get("cliente_cpf_cnpj") or cli.get("cpf_cnpj", ""))
        if cpf_key in matched:
            continue
        matched.add(cpf_key)

        status_portal = order.get("status", "")
        data_status = order.get("data_status")

        # Detecta entrega: "Contrato" concluído = veículo deve ser entregue em breve
        # (entrega fisica confirmada so pelo Byetech CRM)
        em_contrato = _is_etapa_contrato(status_portal)
        cancelado = _is_cancelado(status_portal)

        resultados.append({
            "fonte": account["fontes"][0],
            "id_externo": cli.get("byetech_contrato_id", ""),
            "byetech_contrato_id": cli.get("byetech_contrato_id", ""),
            "cliente_nome": cli.get("cliente_nome") or cli.get("nome", ""),
            "cliente_cpf_cnpj": cli.get("cliente_cpf_cnpj") or cli.get("cpf_cnpj", ""),
            "placa": cli.get("placa", ""),
            "veiculo": cli.get("veiculo", ""),
            "status_atual": status_portal,
            "data_status_portal": data_status,
            "entregue": False,   # Portal nao confirma entrega fisica — usa Byetech
            "em_contrato": em_contrato,
            "cancelado": cancelado,
            "pedido_id_portal": order.get("pedido_id", ""),
        })

    # Contratos nao encontrados no portal
    for cli in clientes:
        cpf_key = _clean_cpf(cli.get("cliente_cpf_cnpj") or cli.get("cpf_cnpj", ""))
        if cpf_key not in matched:
            resultados.append({
                "fonte": account["fontes"][0],
                "cliente_cpf_cnpj": cli.get("cliente_cpf_cnpj", ""),
                "byetech_contrato_id": cli.get("byetech_contrato_id", ""),
                "erro": "CPF nao encontrado no portal",
            })

    return resultados


async def scrape_portaldealer_gwm(clientes: list[dict]) -> list[dict]:
    """
    Busca contratos GWM (Assine Car GWM / ACF) no portal dealer.
    Usa busca individual por CPF via filtro de coluna, ignorando o filtro
    de data — necessário porque os contratos GWM podem ser antigos.

    clientes: lista de dicts com {cliente_cpf_cnpj, cliente_nome, byetech_contrato_id, ...}
    Retorna: lista de resultados com status atual do portal por contrato.
    """
    account = ACCOUNTS.get("GWM")
    if not account:
        raise ValueError("Conta GWM nao encontrada")

    resultados = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        logged = await _login(page, account["login"], account["password"])
        if not logged:
            await browser.close()
            raise Exception("Falha no login do portal (GWM)")

        logger.info(f"[portaldealer] GWM login OK — URL: {page.url}")

        # Limpa filtros iniciais
        await _clear_segment_filter(page)
        # Sem filtro de data para capturar contratos antigos
        # Aguarda tabela carregar
        await page.wait_for_timeout(2000)

        for cli in clientes:
            cpf_raw = cli.get("cliente_cpf_cnpj") or cli.get("cpf_cnpj", "")
            cpf = _clean_cpf(cpf_raw)
            if not cpf:
                resultados.append({
                    "fonte": "GWM",
                    "byetech_contrato_id": cli.get("byetech_contrato_id", ""),
                    "cliente_nome": cli.get("cliente_nome", ""),
                    "cliente_cpf_cnpj": cpf_raw,
                    "erro": "CPF vazio",
                })
                continue

            cpf_fmt = await _format_cpf(cpf)
            logger.info(f"[portaldealer] Buscando GWM CPF={cpf_fmt} ({cli.get('cliente_nome', '')})")
            rows = await _search_by_document(page, cpf_fmt)

            if not rows:
                resultados.append({
                    "fonte": "GWM",
                    "byetech_contrato_id": cli.get("byetech_contrato_id", ""),
                    "cliente_nome": cli.get("cliente_nome", ""),
                    "cliente_cpf_cnpj": cpf_raw,
                    "erro": "CPF nao encontrado no portal",
                })
                continue

            # Pega o pedido mais recente (primeiro da lista)
            order = rows[0]
            status_portal = order.get("status", "")
            em_contrato = _is_etapa_contrato(status_portal)
            cancelado   = _is_cancelado(status_portal)

            resultados.append({
                "fonte": "GWM",
                "id_externo": cli.get("byetech_contrato_id", ""),
                "byetech_contrato_id": cli.get("byetech_contrato_id", ""),
                "cliente_nome": cli.get("cliente_nome") or order.get("nome", ""),
                "cliente_cpf_cnpj": cpf_raw,
                "placa": cli.get("placa", ""),
                "veiculo": cli.get("veiculo", ""),
                "status_atual": status_portal,
                "data_status_portal": order.get("data_status"),
                "entregue": False,
                "em_contrato": em_contrato,
                "cancelado": cancelado,
                "pedido_id_portal": order.get("pedido_id", ""),
                "todos_pedidos": [r.get("pedido_id") for r in rows],
            })
            logger.info(f"[portaldealer] GWM {cli.get('cliente_nome','')} -> {order.get('pedido_id','')} | {status_portal}")
            await page.wait_for_timeout(500)  # pausa entre buscas

        await browser.close()

    return resultados
