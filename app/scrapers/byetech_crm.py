"""
Scraper do CRM Byetech (crm.byetech.pro / api-production.byetech.pro)

Fluxo:
1. Playwright abre o browser, faz login + 2FA
2. Extrai os cookies de sessão Laravel Sanctum
3. httpx usa esses cookies para chamar a API diretamente (rápido, sem browser)
4. Busca todos os contratos com status != Definitivo Entregue
5. Ordena do mais antigo ao mais novo

A sessão é cacheada em memória — login só ocorre uma vez por execução.
"""
import asyncio
import logging
import os
import re
import json
import httpx
from datetime import datetime
from typing import Optional
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

logger = logging.getLogger("byetech_crm")

load_dotenv()

BYETECH_URL   = os.getenv("BYETECH_URL", "https://crm.byetech.pro")
API_URL       = "https://api-production.byetech.pro"
BYETECH_EMAIL = os.getenv("BYETECH_EMAIL")
BYETECH_PASS  = os.getenv("BYETECH_PASSWORD")

# IDs de fase do Byetech CRM
PHASE_ID_ENTREGUE = "9f727a78-6cfb-456b-a4b2-2189edd8ebdb"  # "Definitivo entregue"
PHASE_NAMES = {
    "9f727a78-625a-4bd5-bccf-15006a679fc0": "Venda concluída",
    "9f727a78-6afd-4e40-b21c-aadf3e6fe64d": "Onboarding em andamento",
    "9f727a78-6bca-4e06-85f2-ff991bf21996": "Onboarding concluído",
    "9f727a78-6c68-4e6b-ba4c-f11d8d3fda31": "Provisório retirado",
    "9f727a78-6cfb-456b-a4b2-2189edd8ebdb": "Definitivo entregue",
}

# Cache da sessão — em memória e em disco para sobreviver reinicializações
_session_cookies: Optional[dict] = None
_session_lock = asyncio.Lock()

SESSION_FILE = os.getenv(
    "SESSION_FILE",
    os.path.join(os.path.dirname(__file__), "..", "..", ".byetech_session.json")
)

# Evento para fornecer código 2FA via portal
_twofa_event = asyncio.Event()
_twofa_code: Optional[str] = None
_twofa_lock = asyncio.Lock()


# ── Persistência de sessão ────────────────────────────────
def _save_session(cookies: dict):
    """Salva cookies em disco para reutilizar entre reinicializações."""
    try:
        with open(SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(cookies, f)
        logger.info("Sessão Byetech salva em disco")
    except Exception as e:
        logger.warning(f"Não foi possível salvar sessão: {e}")


def _load_session_from_disk() -> Optional[dict]:
    """Carrega cookies do disco se existirem."""
    try:
        if os.path.exists(SESSION_FILE):
            with open(SESSION_FILE, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


async def _test_session(cookies: dict) -> bool:
    """Verifica se a sessão ainda é válida com uma chamada leve à API."""
    try:
        headers = _make_headers(cookies)
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(
                f"{API_URL}/api/contracts",
                params={"page": 1},
                headers=headers,
                cookies=cookies,
                timeout=10,
            )
            return resp.status_code == 200
    except Exception:
        return False


# ── 2FA helpers ───────────────────────────────────────────
async def provide_twofa_code(code: str):
    global _twofa_code
    async with _twofa_lock:
        _twofa_code = code
        _twofa_event.set()


async def _wait_for_twofa_code() -> str:
    global _twofa_code
    async with _twofa_lock:
        if _twofa_code:
            code = _twofa_code
            _twofa_code = None
            return code
    _twofa_event.clear()
    await asyncio.wait_for(_twofa_event.wait(), timeout=300)
    async with _twofa_lock:
        code = _twofa_code
        _twofa_code = None
    return code


def clear_session():
    """Limpa sessão da memória e do disco — só chamado explicitamente."""
    global _session_cookies
    _session_cookies = None
    try:
        if os.path.exists(SESSION_FILE):
            os.remove(SESSION_FILE)
            logger.info("Sessão Byetech removida do disco")
    except Exception:
        pass


# ── Login via browser ─────────────────────────────────────
def _login_via_subprocess(twofa_code: str | None) -> dict:
    """
    Executa o login em um subprocesso Python separado (próprio ProactorEventLoop).
    Evita conflito com o SelectorEventLoop do uvicorn no Windows (Python 3.14+).
    """
    import subprocess as _sp
    import sys as _sys
    worker = os.path.join(os.path.dirname(__file__), "..", "..", "_byetech_login_worker.py")
    cmd = [_sys.executable, worker]
    if twofa_code:
        cmd.append(twofa_code.strip())
    env = {**os.environ}
    result = _sp.run(cmd, capture_output=True, text=True, timeout=90, env=env)
    # Última linha do stdout é o JSON
    output = (result.stdout or "").strip().splitlines()
    json_line = next((l for l in reversed(output) if l.startswith("{")), None)
    if not json_line:
        raise Exception(f"Login worker sem saída. stderr: {result.stderr[:300]}")
    data = json.loads(json_line)
    if "error" in data:
        raise Exception(data["error"])
    if not data:
        raise Exception("Falha no login Byetech — sem cookies de sessão")
    return data


async def _login_and_get_cookies(twofa_callback=None) -> dict:
    """
    Faz login completo via Playwright e retorna os cookies de sessão.
    Usa subprocesso separado para ter ProactorEventLoop próprio (Windows/Python 3.14).
    """
    # Primeira tentativa: sem código 2FA
    try:
        return await asyncio.to_thread(_login_via_subprocess, None)
    except Exception as e:
        if "2FA_REQUIRED" not in str(e):
            raise

    # 2FA necessário — obtém o código via callback e tenta novamente
    if twofa_callback is None:
        raise Exception(
            "2FA_REQUIRED: Login Byetech requer código 2FA. "
            "Execute _refresh_byetech_session.py para renovar a sessão."
        )

    code = await twofa_callback()
    return await asyncio.to_thread(_login_via_subprocess, code)


# ── Sessão com cache (memória + disco) ───────────────────
async def get_session(twofa_callback=None) -> dict:
    """
    Retorna sessão válida. Ordem de prioridade:
    1. Cache em memória (rápido)
    2. Cookies salvos em disco — testa se ainda são válidos (evita 2FA desnecessário)
    3. Login completo + 2FA — só quando a sessão realmente expirou
    """
    global _session_cookies
    async with _session_lock:
        # 1. Memória
        if _session_cookies:
            return _session_cookies

        # 2. Disco
        saved = _load_session_from_disk()
        if saved:
            logger.info("Testando sessão salva em disco...")
            if await _test_session(saved):
                logger.info("Sessão do disco ainda é válida — 2FA não necessário")
                _session_cookies = saved
                return _session_cookies
            else:
                logger.info("Sessão expirada — fazendo novo login + 2FA")

        # 3. Login + 2FA
        _session_cookies = await _login_and_get_cookies(twofa_callback)
        _save_session(_session_cookies)
        return _session_cookies


# ── Chamadas de API via httpx ─────────────────────────────
def _make_headers(cookies: dict) -> dict:
    xsrf = cookies.get("XSRF-TOKEN", "")
    # O token vem URL-encoded — decodifica
    import urllib.parse
    xsrf_decoded = urllib.parse.unquote(xsrf)
    return {
        "X-XSRF-TOKEN": xsrf_decoded,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": BYETECH_URL,
        "Referer": f"{BYETECH_URL}/contracts",
    }


def _parse_date(text) -> Optional[datetime]:
    if not text:
        return None
    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%S"]:
        try:
            return datetime.strptime(str(text)[:19], fmt[:len(str(text)[:19])])
        except ValueError:
            continue
    return None


def _is_entregue(c: dict) -> bool:
    """Retorna True se o contrato já foi definitivamente entregue."""
    # Filtra pelo phase_id exato (mais confiável)
    if c.get("phase_id") == PHASE_ID_ENTREGUE:
        return True
    # Fallback por nome do status
    status = (c.get("status_atual") or "").lower()
    return "definitivo entregue" in status or bool(c.get("entrega_definitivo"))


async def _fetch_contracts_page(
    client: httpx.AsyncClient,
    cookies: dict,
    page: int = 1,
    phase_ids: list = None,
) -> dict:
    """
    Busca uma página de contratos.
    phase_ids: lista de UUIDs de fases para filtrar (ex: apenas não-entregues).
    """
    headers = _make_headers(cookies)
    params = {"page": page}

    # Filtro por fases — exclui "Definitivo entregue" na origem para não buscar ~6k registros
    if phase_ids:
        for pid in phase_ids:
            params.setdefault("phase_id[]", [])
            if isinstance(params.get("phase_id[]"), list):
                params["phase_id[]"].append(pid)
            else:
                params["phase_id[]"] = [pid]

    resp = await client.get(
        f"{API_URL}/api/contracts",
        params=params,
        headers=headers,
        cookies=cookies,
        timeout=30,
    )

    if resp.status_code == 401:
        clear_session()
        raise Exception("Sessão expirada — precisa de novo login")

    resp.raise_for_status()
    return resp.json()


def _contract_to_dict(c: dict) -> dict:
    """
    Normaliza um contrato da API Byetech para o formato interno.
    Estrutura real: contrato → order → {client, vehicle, rental_company, opportunity}
    """
    order = c.get("order") or {}

    # Cliente
    client      = order.get("client") or {}
    opportunity = order.get("opportunity") or {}
    cliente_nome = client.get("nome_completo") or opportunity.get("name") or ""
    cpf  = re.sub(r"[^\d]", "", str(client.get("num_cpf")  or ""))
    cnpj = re.sub(r"[^\d]", "", str(client.get("num_cnpj") or ""))
    cliente_cpf  = cpf or cnpj
    cliente_email = opportunity.get("email") or client.get("email") or ""

    # Veículo
    vehicle  = order.get("vehicle") or {}
    veiculo  = vehicle.get("nome") or ""

    # Locadora
    rental   = order.get("rental_company") or {}
    locadora_nome = rental.get("nome") or rental.get("name") or ""

    # Status via phase_id
    phase_id   = c.get("phase_id") or ""
    status_nome = PHASE_NAMES.get(phase_id, phase_id)

    # Datas
    data_prevista  = _parse_date(c.get("retirada_provisorio"))
    data_definitiva = _parse_date(c.get("entrega_definitivo"))

    return {
        "byetech_contrato_id": str(c.get("uuid") or c.get("id") or ""),
        "id_externo": str(c.get("id") or ""),
        "fonte": _map_locadora(locadora_nome),
        "cliente_nome": cliente_nome,
        "cliente_cpf_cnpj": cliente_cpf,
        "cliente_email": cliente_email,
        "veiculo": veiculo,
        "placa": c.get("placa_carro") or "",
        "status_atual": status_nome,
        "data_prevista_entrega": data_prevista,
        "data_entrega_definitiva": data_definitiva,
        "locadora_nome": locadora_nome,
        "_raw": c,
    }


def _map_locadora(nome: str) -> str:
    """Mapeia nome da locadora para o código interno."""
    n = (nome or "").upper()
    if "SIGN" in n or "DRIVE" in n:
        return "SIGN & DRIVE"
    if "GWM" in n:
        return "GWM"
    if "LOCALIZA" in n:
        return "LOCALIZA"
    if "MOVIDA" in n:
        return "MOVIDA"
    if "UNIDAS" in n:
        return "UNIDAS"
    if "ASSINECAR" in n or " LM" in n or n == "LM" or "LM " in n:
        return "LM"
    if "VOLKSWAGEN" in n:
        return "VW"
    if "FLUA" in n:
        return "FLUA"
    if "NISSAN" in n:
        return "NISSAN"
    return nome or "OUTRO"


# Fases ativas (não entregues) — usadas para filtrar na API
PHASE_IDS_ATIVOS = [
    "9f727a78-625a-4bd5-bccf-15006a679fc0",  # Venda concluída
    "9f727a78-6afd-4e40-b21c-aadf3e6fe64d",  # Onboarding em andamento
    "9f727a78-6bca-4e06-85f2-ff991bf21996",  # Onboarding concluído
    "9f727a78-6c68-4e6b-ba4c-f11d8d3fda31",  # Provisório retirado
]


# ── Scrape principal ──────────────────────────────────────
async def scrape_contratos(twofa_callback=None) -> list[dict]:
    """
    Retorna todos os contratos pendentes de entrega definitiva.
    Filtra na API pelas fases ativas (exclui Definitivo entregue).
    Ordena do mais antigo ao mais novo por data prevista.
    """
    cookies = await get_session(twofa_callback)

    contratos = []
    pagina = 1

    async with httpx.AsyncClient(follow_redirects=True) as client:
        while True:
            # Rate limit: aguarda entre páginas para não receber 429
            if pagina > 1:
                await asyncio.sleep(0.5)

            # Tenta a requisição com retry automático em 429
            for tentativa in range(4):
                try:
                    data = await _fetch_contracts_page(
                        client, cookies, page=pagina, phase_ids=PHASE_IDS_ATIVOS
                    )
                    break
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        wait = 5 * (tentativa + 1)
                        logger.warning(f"429 Rate limit — aguardando {wait}s (tentativa {tentativa+1})")
                        await asyncio.sleep(wait)
                    elif e.response.status_code == 401:
                        clear_session()
                        raise Exception("Sessão expirada — precisa de novo login")
                    else:
                        raise
                except Exception as e:
                    if "expirada" in str(e).lower():
                        cookies = await get_session(twofa_callback)
                    else:
                        raise
            else:
                logger.error("Máximo de tentativas atingido na paginação — encerrando")
                break

            # Estrutura real: response.data.contracts.{data, last_page, ...}
            contracts_obj = data.get("data", {}).get("contracts", {})
            if not isinstance(contracts_obj, dict):
                break

            items = contracts_obj.get("data", [])
            total_pages = contracts_obj.get("last_page", 1)

            if not items:
                break

            for item in items:
                c = _contract_to_dict(item)
                if not _is_entregue(item):
                    contratos.append(c)

            if pagina % 10 == 0 or pagina == total_pages:
                logger.info(f"  página {pagina}/{total_pages} — {len(contratos)} contratos coletados")

            if pagina >= total_pages:
                break
            pagina += 1

    # Ordena do mais antigo ao mais novo
    contratos.sort(key=lambda x: x.get("data_prevista_entrega") or datetime.max)
    return contratos


# ── Mapeamento portal → fase Byetech ─────────────────────
# Ordem importa: mais específico primeiro
PORTAL_STATUS_PHASE_MAP = [
    (["definitivo entregue", "entregue", "entrega realizada",
      "veículo entregue", "definitivo"],               PHASE_ID_ENTREGUE),
    (["provisório retirado", "provisorio retirado",
      "provisório", "provisorio", "retirada provisória",
      "retirada provisoria"],                          "9f727a78-6c68-4e6b-ba4c-f11d8d3fda31"),
    (["onboarding concluído", "onboarding concluido",
      "onboarding completo"],                          "9f727a78-6bca-4e06-85f2-ff991bf21996"),
    (["onboarding"],                                   "9f727a78-6afd-4e40-b21c-aadf3e6fe64d"),
    (["venda concluída", "venda concluida",
      "aprovado", "concluído", "concluido"],           "9f727a78-625a-4bd5-bccf-15006a679fc0"),
]


def _map_status_to_phase(status: str) -> Optional[str]:
    """Mapeia um nome de status/etapa do portal para o phase_id do Byetech."""
    s = (status or "").lower().strip()
    for keywords, phase_id in PORTAL_STATUS_PHASE_MAP:
        if any(k in s for k in keywords):
            return phase_id
    return None


async def update_phase_by_cpf(
    cpf_raw: str,
    novo_status: str,
    twofa_callback=None,
) -> tuple[bool, str]:
    """
    Move um contrato no Byetech para a fase correspondente ao novo_status.
    Retorna (sucesso: bool, mensagem: str).
    Retorna (True, "sem_mapeamento") se o status não tem fase correspondente.
    """
    from pathlib import Path as _Path

    phase_id = _map_status_to_phase(novo_status)
    if not phase_id:
        return True, "sem_mapeamento"

    # Normaliza CPF
    digits = re.sub(r"[^\d]", "", cpf_raw or "")
    if len(digits) == 12 and digits.endswith("0"):
        cpf_norm = digits[:-1]
    elif len(digits) > 11:
        cpf_norm = digits[-11:]
    else:
        cpf_norm = digits.zfill(11)

    # Carrega mapa CPF
    cpf_map_file = os.getenv("CPF_MAP_FILE", str(_Path(__file__).parent.parent.parent / ".byetech_cpf_map.json"))
    if not os.path.exists(cpf_map_file):
        return False, "mapa_cpf_ausente"

    with open(cpf_map_file, encoding="utf-8") as f:
        cpf_map = json.load(f)

    entry = (cpf_map.get(cpf_norm)
             or cpf_map.get(digits)
             or cpf_map.get(cpf_norm + "0"))
    if not entry:
        return False, f"cpf_nao_encontrado:{cpf_norm}"

    contract_id = entry.get("id")
    if not contract_id:
        return False, f"id_ausente_no_mapa:{cpf_norm}"

    cookies = await get_session(twofa_callback)
    headers = _make_headers(cookies)

    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.patch(
            f"{API_URL}/api/contracts/{contract_id}/move",
            json={"phaseId": phase_id},
            headers=headers, cookies=cookies, timeout=30,
        )
        if resp.status_code not in (200, 201, 204):
            body = resp.text
            if resp.status_code == 500 and "atualizar" in body:
                return True, "ja_na_fase"
            return False, f"http_{resp.status_code}:{body[:150]}"

    phase_name = PHASE_NAMES.get(phase_id, phase_id)
    logger.info(f"✅ [Byetech] Contrato {contract_id} → {phase_name} (status='{novo_status}')")
    return True, f"ok:{phase_name}"


# ── Atualizar data de entrega via API ─────────────────────
async def update_delivery_by_cpf(
    cpf_raw: str,
    data_entrega: datetime,
    placa: str = None,
    cpf_map_file: str = None,
    twofa_callback=None,
) -> bool:
    """
    Atualiza entrega_definitivo no Byetech CRM para um contrato identificado pelo CPF.
    Usa o mapa CPF em disco para obter o ID inteiro e os campos existentes,
    depois faz PATCH completo + /move para a fase 'Definitivo Entregue'.
    Retorna True se sucesso.
    """
    import re as _re
    from pathlib import Path

    # Normaliza CPF
    digits = _re.sub(r"[^\d]", "", cpf_raw or "")
    if len(digits) == 12 and digits.endswith("0"):
        cpf_norm = digits[:-1]
    elif len(digits) > 11:
        cpf_norm = digits[-11:]
    else:
        cpf_norm = digits.zfill(11)

    # Carrega mapa CPF
    if cpf_map_file is None:
        cpf_map_file = str(Path(__file__).parent.parent.parent / ".byetech_cpf_map.json")
    if not os.path.exists(cpf_map_file):
        logger.error("[Byetech] Mapa CPF não encontrado — execute scraper completo primeiro")
        return False

    with open(cpf_map_file, encoding="utf-8") as f:
        cpf_map = json.load(f)

    entry = (cpf_map.get(cpf_norm)
             or cpf_map.get(digits)
             or cpf_map.get(cpf_norm + "0"))
    if not entry:
        logger.error(f"[Byetech] CPF {cpf_norm!r} não encontrado no mapa CPF")
        return False

    contract_id = entry.get("id")
    if not contract_id:
        logger.error(f"[Byetech] ID inteiro ausente no mapa para CPF {cpf_norm!r}")
        return False

    # Sessão válida
    cookies = await get_session(twofa_callback)
    headers = _make_headers(cookies)

    # Payload completo (evita erros de campos null no Laravel)
    data_str = data_entrega.strftime("%Y-%m-%d")
    effective_placa = placa or entry.get("placa_carro")
    payload = {
        "kmExcedenteValue":        entry.get("km_excedente_value") or "0.00",
        "entregaDefinitivo":       data_str,
        "retiradaProvisorio":      entry.get("retirada_provisorio"),
        "placaCarro":              effective_placa,
        "frequencyOfUse":          entry.get("frequency_of_use"),
        "usageType":               entry.get("usage_type"),
        "isReversal":              entry.get("is_reversal", 0),
        "reversalValue":           entry.get("reversal_value"),
        "franquiaCoparticipacao":  entry.get("franquia_coparticipacao") or "0",
        "coberturaDanosMateriais": entry.get("cobertura_danos_materiais") or "--",
        "coberturaDanosCorporais": entry.get("cobertura_danos_corporais") or "--",
        "isActive":                entry.get("is_active", 1),
        "isExtended":              entry.get("is_extended", 0),
        "extensionMonths":         entry.get("extension_months"),
        "automaticSendLink":       entry.get("automatic_send_link", 1),
    }

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # PATCH com todos os campos
        resp = await client.patch(
            f"{API_URL}/api/contracts/{contract_id}",
            json=payload, headers=headers, cookies=cookies, timeout=30,
        )
        if resp.status_code not in (200, 201, 204):
            logger.error(f"[Byetech] PATCH {contract_id}: {resp.status_code} {resp.text[:300]}")
            return False

        # /move para fase Definitivo Entregue
        resp_move = await client.patch(
            f"{API_URL}/api/contracts/{contract_id}/move",
            json={"phaseId": PHASE_ID_ENTREGUE},
            headers=headers, cookies=cookies, timeout=30,
        )
        if resp_move.status_code not in (200, 201, 204):
            body_txt = resp_move.text
            if resp_move.status_code == 500 and "atualizar" in body_txt:
                logger.info(f"[Byetech] Contrato {contract_id} já estava na fase Definitivo Entregue")
            else:
                logger.error(f"[Byetech] MOVE {contract_id}: {resp_move.status_code} {body_txt[:200]}")
                return False

    logger.info(f"✅ [Byetech] Contrato {contract_id} → Definitivo Entregue em {data_str}"
                f" (placa={effective_placa!r})")
    return True


async def update_delivery_date(
    contrato_uuid: str,
    data_entrega: datetime,
    twofa_callback=None,
) -> bool:
    """
    Mantido por compatibilidade. Prefira update_delivery_by_cpf.
    Tenta PATCH pelo ID inteiro se contrato_uuid for numérico, caso contrário falha.
    """
    logger.warning("[Byetech] update_delivery_date() está obsoleto — use update_delivery_by_cpf()")
    # Se for um ID numérico, tenta diretamente
    if str(contrato_uuid).isdigit():
        cookies = await get_session(twofa_callback)
        headers = _make_headers(cookies)
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.patch(
                f"{API_URL}/api/contracts/{contrato_uuid}",
                json={"entregaDefinitivo": data_entrega.strftime("%Y-%m-%d")},
                headers=headers, cookies=cookies, timeout=30,
            )
            return resp.status_code in (200, 201, 204)
    logger.error(f"[Byetech] update_delivery_date: ID '{contrato_uuid}' não é numérico — use update_delivery_by_cpf()")
    return False
