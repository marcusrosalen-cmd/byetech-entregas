"""
Scraper da API Sign & Drive (VW leasing).
Autenticacao via JWT - nao usa Playwright.
"""
import re
import json
import urllib.request
import asyncio
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("signanddrive")

API_BASE  = "https://backend.vwsignanddrive.com.br/api"
API_LOGIN = f"{API_BASE}/signin/dealership"
API_MGMT  = (
    f"{API_BASE}/dealership-management"
    f"?dealershipId=395&dealershipGroupId=401"
    f"&role=Admin+da+Concession%C3%A1ria"
    f"&userDnId=0&Page=true&QuantityPerPage=100"
    f"&dateStart={{ds}}&dateEnd={{de}}&status=&CurrentPage={{pg}}"
)
API_ITEMS = f"{API_BASE}/orderitems?orderId={{}}&noCache=true"

PORTAL_LOGIN = "44850372821"
PORTAL_PASS  = "Toriba@2024"

STATUS_MAP = {
    1: "Pedido Criado",
    2: "Aguardando Preparacao",
    3: "Aguardando Faturamento",
    4: "Aguardando Transporte",
    5: "Veiculo Entregue",
    6: "Veiculo Disponivel",
    7: "Cancelado",
}


def _date_windows():
    """Janelas de data para paginacao: ano corrente + historico desde 2023."""
    today = datetime.now().strftime("%Y-%m-%d")
    year  = datetime.now().year
    wins  = [(f"{year}-01-01", today)]
    for y in range(year - 1, 2022, -1):
        wins += [(f"{y}-07-01", f"{y}-12-31"), (f"{y}-01-01", f"{y}-06-30")]
    return wins


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _normalizar_cpf(raw: str) -> str:
    d = _digits(raw)
    if len(d) == 12:
        d = d[:-1]
    if len(d) == 11 and d.endswith("0"):
        c = "0" + d[:-1]
        if len(c) == 11:
            return c
    return d.zfill(11)


def _cpf_variants(raw: str) -> set:
    d = _digits(raw)
    vs = {d, d.zfill(11)}
    if len(d) == 11 and d.endswith("0"):
        vs.add("0" + d[:-1])
    elif len(d) == 12:
        vs.add(d[:-1])
        vs.add(d[:-1].zfill(11))
    elif len(d) == 10:
        vs.add(d.zfill(11))
        vs.add(d + "0")
    return {v for v in vs if v}


# ── Auth ─────────────────────────────────────────────────────────────────────

def _sd_login() -> str:
    payload = json.dumps({"login": PORTAL_LOGIN, "password": PORTAL_PASS}).encode()
    req = urllib.request.Request(
        API_LOGIN, data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())["token"]


# ── CPF Index ─────────────────────────────────────────────────────────────────

def _fetch_page(token: str, ds: str, de: str, page: int):
    url = API_MGMT.format(ds=ds, de=de, pg=page)
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}", "User-Agent": "Mozilla/5.0",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
            items = d.get("items", {})
            return items.get("itens", []), items.get("hasNextPage", False)
    except Exception:
        return [], False


def _build_cpf_index(token: str) -> dict:
    """Retorna {cpf_norm: orderId} para todos os pedidos PF da concessionaria."""
    idx = {}
    for ds, de in _date_windows():
        page = 1
        while True:
            itens, has_next = _fetch_page(token, ds, de, page)
            if not itens:
                break
            for it in itens:
                cpf_raw = it.get("cpfCnpj", "")
                if not cpf_raw or len(_digits(cpf_raw)) > 11:
                    continue
                cpf_norm = _normalizar_cpf(cpf_raw)
                oid = it.get("orderId")
                if cpf_norm not in idx:
                    idx[cpf_norm] = oid
                    for v in _cpf_variants(cpf_raw):
                        idx.setdefault(v, oid)
            if not has_next:
                break
            page += 1
    return idx


# ── Order Detail ──────────────────────────────────────────────────────────────

def _fetch_order(order_id) -> list:
    req = urllib.request.Request(
        API_ITEMS.format(order_id),
        headers={"User-Agent": "Mozilla/5.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception:
        return []


def _parse_order(data) -> dict:
    if not data or not isinstance(data, list):
        return {}
    item     = data[0]
    modelo   = item.get("model", "")
    placa    = item.get("finalPlate", "") or ""
    statuses = item.get("orderItemStatus", [])

    if not statuses:
        num = item.get("status", 0)
        return {
            "modelo": modelo, "placa": placa,
            "status_desc": STATUS_MAP.get(num, "Pedido Criado"),
            "data_entrega": None, "entregue": num == 5,
        }

    sorted_s = sorted(statuses, key=lambda s: s.get("dateCreated", ""), reverse=True)
    latest = sorted_s[0]
    num    = latest.get("status")
    desc   = latest.get("statusDescription") or STATUS_MAP.get(num, str(num))
    placa  = latest.get("deliveryPlate") or placa
    modelo = latest.get("deliveryModel") or modelo

    data_entrega = None
    for s in statuses:
        if s.get("status") == 5 or "Entregue" in (s.get("statusDescription") or ""):
            dc = s.get("dateCreated", "")
            if dc:
                try:
                    data_entrega = datetime.fromisoformat(dc[:19])
                except Exception:
                    pass
            placa  = s.get("deliveryPlate") or placa
            modelo = s.get("deliveryModel") or modelo
            break

    return {
        "modelo":       modelo,
        "placa":        placa,
        "status_desc":  desc,
        "data_entrega": data_entrega,
        "entregue":     num == 5 or data_entrega is not None,
    }


# ── Public async API ──────────────────────────────────────────────────────────

async def scrape_signanddrive(clientes: list[dict]) -> list[dict]:
    """
    Consulta a API Sign & Drive para cada cliente.

    clientes: [{cliente_cpf_cnpj, cliente_nome, byetech_contrato_id, veiculo, placa, ...}]

    Retorna: [{fonte, id_externo, cliente_cpf_cnpj, cliente_nome,
               veiculo, placa, status_atual, data_entrega_definitiva, entregue}]
    """
    def _run_sync():
        logger.info(f"Sign & Drive: autenticando ({len(clientes)} clientes)...")
        token = _sd_login()

        logger.info("Sign & Drive: construindo indice CPF->orderId...")
        idx = _build_cpf_index(token)
        logger.info(f"Sign & Drive: {len(idx)} entradas no indice")

        order_map: dict[int, list] = {}
        sem_pedido: list = []

        for cli in clientes:
            cpf_raw  = cli.get("cliente_cpf_cnpj", "")
            cpf_norm = _normalizar_cpf(cpf_raw)
            oid = idx.get(cpf_norm)
            if not oid:
                for v in _cpf_variants(cpf_raw):
                    oid = idx.get(v)
                    if oid:
                        break
            if oid:
                order_map.setdefault(oid, []).append(cli)
            else:
                sem_pedido.append(cli)

        logger.info(
            f"Sign & Drive: {len(order_map)} orderIds unicos, "
            f"{len(sem_pedido)} sem pedido"
        )

        order_details: dict[int, dict] = {}
        with ThreadPoolExecutor(max_workers=15) as ex:
            futs = {ex.submit(_fetch_order, oid): oid for oid in order_map}
            for fut in as_completed(futs):
                oid = futs[fut]
                order_details[oid] = _parse_order(fut.result())

        resultados = []
        for oid, clis in order_map.items():
            detail = order_details.get(oid, {})
            for cli in clis:
                resultados.append({
                    "fonte":                   "SIGN & DRIVE",
                    "id_externo":              str(oid),
                    "cliente_cpf_cnpj":        cli.get("cliente_cpf_cnpj", ""),
                    "cliente_nome":            cli.get("cliente_nome", ""),
                    "byetech_contrato_id":     cli.get("byetech_contrato_id", ""),
                    "veiculo":                 detail.get("modelo") or cli.get("veiculo", ""),
                    "placa":                   detail.get("placa") or cli.get("placa", ""),
                    "status_atual":            detail.get("status_desc", ""),
                    "data_entrega_definitiva": detail.get("data_entrega"),
                    "entregue":                detail.get("entregue", False),
                })

        for cli in sem_pedido:
            resultados.append({
                "fonte":               "SIGN & DRIVE",
                "cliente_cpf_cnpj":    cli.get("cliente_cpf_cnpj", ""),
                "cliente_nome":        cli.get("cliente_nome", ""),
                "byetech_contrato_id": cli.get("byetech_contrato_id", ""),
                "erro":                "CPF nao encontrado no portal Sign & Drive",
            })

        return resultados

    return await asyncio.to_thread(_run_sync)
