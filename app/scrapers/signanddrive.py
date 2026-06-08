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
# {st} = filtro de status (&status= vazio = ativos; &status=11 = concluidos; etc.)
API_MGMT  = (
    f"{API_BASE}/dealership-management"
    f"?dealershipId=395&dealershipGroupId=401"
    f"&role=Admin+da+Concession%C3%A1ria"
    f"&userDnId=0&Page=true&QuantityPerPage=100"
    f"&dateStart={{ds}}&dateEnd={{de}}&status={{st}}&CurrentPage={{pg}}"
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
    """
    Janelas TRIMESTRAIS para evitar HTTP 400 da management API.

    A API retorna 400 para janelas de ~6 meses dependendo do volume de pedidos
    (ex: 2025-07-01 a 2025-12-31 falha). Trimestres de ~3 meses sao seguros.
    Retorna janelas da mais recente para a mais antiga (prioriza dados recentes).
    """
    today = datetime.now()
    year  = today.year
    today_str = today.strftime("%Y-%m-%d")

    # Trimestres: (inicio, fim)
    Q = [
        ("01-01", "03-31"),
        ("04-01", "06-30"),
        ("07-01", "09-30"),
        ("10-01", "12-31"),
    ]
    cur_q = (today.month - 1) // 3  # 0=Q1 .. 3=Q4

    wins = []
    # Ano corrente: trimestres do mais recente ate o mais antigo
    for q in range(cur_q, -1, -1):
        ds = f"{year}-{Q[q][0]}"
        de = today_str if q == cur_q else f"{year}-{Q[q][1]}"
        wins.append((ds, de))

    # Anos anteriores (2025, 2024, 2023): Q4 → Q1
    for y in range(year - 1, 2022, -1):
        for q in range(3, -1, -1):
            wins.append((f"{y}-{Q[q][0]}", f"{y}-{Q[q][1]}"))

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

def _fetch_page(token: str, ds: str, de: str, page: int, status: str = ""):
    url = API_MGMT.format(ds=ds, de=de, pg=page, st=status)
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


def _build_doc_index(token: str) -> dict:
    """
    Retorna {doc_norm: orderId} para todos os pedidos da concessionaria — PF e PJ.

    Indexa tanto CPF (11 dígitos) quanto CNPJ (14 dígitos), cobrindo:
    - Contratos pessoa física (CPF): Sign & Drive, UNIDAS, MOVIDA, LOCALIZA, LM…
    - Contratos pessoa jurídica (CNPJ): VW Empresas, Localiza PJ, LM PJ, GWM PJ…

    Consulta a management API com diferentes filtros de status:
    - status="" (vazio)  → pedidos ativos/em andamento
    - status="11"        → Pedido concluido (assinado, nao necessariamente entregue)
    - status="5"         → Veiculo Entregue (escala antiga; pode retornar entregues)
    """
    idx = {}
    status_filters = ["", "11", "5"]
    for ds, de in _date_windows():
        for st in status_filters:
            page = 1
            while True:
                itens, has_next = _fetch_page(token, ds, de, page, st)
                if not itens:
                    break
                for it in itens:
                    doc_raw = it.get("cpfCnpj", "")
                    if not doc_raw:
                        continue
                    digits = _digits(doc_raw)
                    oid = it.get("orderId")
                    if len(digits) <= 11:
                        # CPF — normaliza e indexa variantes
                        cpf_norm = _normalizar_cpf(doc_raw)
                        if cpf_norm not in idx:
                            idx[cpf_norm] = oid
                            for v in _cpf_variants(doc_raw):
                                idx.setdefault(v, oid)
                    else:
                        # CNPJ — indexa com e sem zeros à esquerda
                        cnpj_norm = digits.zfill(14)
                        idx.setdefault(cnpj_norm, oid)
                        idx.setdefault(digits, oid)
                if not has_next:
                    break
                page += 1
    return idx


# Mantém alias para retrocompatibilidade
_build_cpf_index = _build_doc_index


# ── Order Detail ──────────────────────────────────────────────────────────────

def _fetch_order(order_id, token: str = "") -> list:
    url = API_ITEMS.format(order_id)
    headers = {"User-Agent": "Mozilla/5.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    import time
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)  # 1s, 2s backoff
            else:
                logger.warning(f"_fetch_order({order_id}) falhou após 3 tentativas: {e}")
                return []
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

    clientes: [{cliente_cpf_cnpj, cliente_nome, byetech_contrato_id, veiculo, placa,
                pedido_portal_id (opcional — orderId ja salvo no banco), ...}]

    Estrategia:
    - Clientes com pedido_portal_id ja salvo no banco -> _fetch_order direto (bypassa
      management API, que so mostra pedidos ATIVOS — entregues somem dela).
    - Clientes sem pedido_portal_id -> usa _build_cpf_index (management API) para
      descobrir o orderId, depois _fetch_order.

    Retorna: [{fonte, id_externo, cliente_cpf_cnpj, cliente_nome,
               veiculo, placa, status_atual, data_entrega_definitiva, entregue}]
    """
    def _run_sync():
        logger.info(f"Sign & Drive: autenticando ({len(clientes)} clientes)...")
        token = _sd_login()

        # ── Separa clientes com orderId ja conhecido vs novos ────────────────
        clientes_com_oid = [c for c in clientes if c.get("pedido_portal_id")]
        clientes_sem_oid = [c for c in clientes if not c.get("pedido_portal_id")]

        logger.info(
            f"Sign & Drive: {len(clientes_com_oid)} com orderId salvo, "
            f"{len(clientes_sem_oid)} precisam de lookup CPF->orderId"
        )

        order_map: dict[int, list] = {}
        sem_pedido: list = []

        # Clientes com orderId salvo -> usa diretamente (funciona mesmo pos-entrega)
        for cli in clientes_com_oid:
            try:
                oid = int(cli["pedido_portal_id"])
                order_map.setdefault(oid, []).append(cli)
            except (ValueError, TypeError):
                clientes_sem_oid.append(cli)  # fallback para lookup CPF

        # Clientes sem orderId -> management API (so mostra pedidos ativos)
        if clientes_sem_oid:
            logger.info("Sign & Drive: construindo indice DOC->orderId (CPF+CNPJ)...")
            idx = _build_doc_index(token)
            logger.info(f"Sign & Drive: {len(idx)} entradas no indice")

            for cli in clientes_sem_oid:
                doc_raw = cli.get("cliente_cpf_cnpj", "")
                digits  = _digits(doc_raw)
                oid = None

                if len(digits) > 11:
                    # CNPJ (PJ) — VW Empresas, Localiza PJ, LM PJ, GWM PJ…
                    oid = idx.get(digits.zfill(14)) or idx.get(digits)
                else:
                    # CPF (PF)
                    cpf_norm = _normalizar_cpf(doc_raw)
                    oid = idx.get(cpf_norm)
                    if not oid:
                        for v in _cpf_variants(doc_raw):
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
        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = {ex.submit(_fetch_order, oid, token): oid for oid in order_map}
            for fut in as_completed(futs):
                oid = futs[fut]
                order_details[oid] = _parse_order(fut.result())

        n_entregues = sum(1 for d in order_details.values() if d.get("entregue"))
        logger.info(f"Sign & Drive: {len(order_details)} ordens consultadas, {n_entregues} entregues detectadas")

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
