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
# Playwright importado de forma LAZY (dentro de _login_via_subprocess)
# para que o módulo possa ser importado no Render (sem Playwright instalado).
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

# Sessão recebida remotamente (enviada pelo servidor local via /api/byetech/push-session)
_remote_session: Optional[dict] = None


def set_remote_session(cookies: dict):
    """Injeta cookies de sessão recebidos via API (enviados pela máquina local)."""
    global _session_cookies, _remote_session
    _remote_session = cookies
    _session_cookies = cookies
    # Persiste em disco para sobreviver reinícios do processo
    _save_session(cookies)
    logger.info("[Byetech] Sessão remota recebida e armazenada")

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
                params={"page": 1, "per_page": 1},
                headers=headers,
                cookies=cookies,
                timeout=45,
            )
        logger.info(f"[_test_session] status={resp.status_code} url={resp.url}")
        # 200/429 = sessao valida (429 = rate limited mas autenticado)
        if resp.status_code in (200, 429):
            return True
        if resp.status_code in (401, 403):
            return False
        if "login" in str(resp.url).lower():
            return False
        return False
    except Exception as e:
        logger.warning(f"[_test_session] excecao: {type(e).__name__}: {e}")
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


# ── Login direto via API (sem browser) ───────────────────
async def _login_via_api(
    twofa_code: str = None,
    email: str = None,
    senha: str = None,
) -> Optional[dict]:
    """
    Faz login no Byetech CRM usando httpx puro, sem Playwright.
    Fluxo Laravel Fortify + Sanctum:
      1. GET  /sanctum/csrf-cookie   → obtém XSRF-TOKEN
      2. POST /login                 → autentica (pode retornar two_factor=true)
      3. POST /two-factor-challenge  → resolve 2FA se necessário
    Retorna dict de cookies ou None se o fluxo não for suportado.
    Lança "2FA_REQUIRED" quando 2FA é obrigatório e twofa_code não foi fornecido.
    email/senha opcionais: se não fornecidos, usa variáveis de ambiente.
    """
    _email = email or BYETECH_EMAIL
    _senha = senha or BYETECH_PASS
    if not _email or not _senha:
        return None

    import urllib.parse as _up

    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    try:
        async with httpx.AsyncClient(
            base_url=BYETECH_URL,
            follow_redirects=True,
            timeout=30,
        ) as client:
            # 1. CSRF — busca cookie XSRF-TOKEN
            await client.get("/sanctum/csrf-cookie", headers={"User-Agent": UA})
            # Envia o valor RAW (URL-encoded) — Laravel decodifica no servidor
            xsrf_raw = client.cookies.get("XSRF-TOKEN", "")
            xsrf     = xsrf_raw  # mantém encoded; Laravel aceita ambos

            hdrs = {
                "X-XSRF-TOKEN": xsrf,
                "Content-Type": "application/json",
                "Accept":       "application/json",
                "Referer":      BYETECH_URL + "/",
                "Origin":       BYETECH_URL,
                "User-Agent":   UA,
            }

            # 2. Login — sem follow_redirect para ver status real
            r = await client.post("/login", json={
                "email":    _email,
                "password": _senha,
            }, headers=hdrs)

            logger.info(f"[Byetech] POST /login → HTTP {r.status_code}")

            if r.status_code == 419:
                # CSRF inválido — tenta sem o header XSRF
                logger.warning("[Byetech] 419 CSRF mismatch — tentando sem X-XSRF-TOKEN")
                hdrs2 = {k: v for k, v in hdrs.items() if k != "X-XSRF-TOKEN"}
                r = await client.post("/login", json={
                    "email":    _email,
                    "password": _senha,
                }, headers=hdrs2)
                logger.info(f"[Byetech] POST /login (sem XSRF) → HTTP {r.status_code}")

            if r.status_code == 422:
                try:
                    detail = r.json()
                except Exception:
                    detail = r.text[:200]
                raise Exception(f"CREDENCIAIS_INVALIDAS: {detail}")

            if r.status_code == 419:
                raise Exception("CSRF_INVALIDO: token XSRF rejeitado (419). Tente novamente.")

            if r.status_code == 404:
                raise Exception("ENDPOINT_NAO_ENCONTRADO: /login retornou 404")

            if r.status_code not in (200, 201, 204):
                raise Exception(f"HTTP_{r.status_code}: resposta inesperada do servidor Byetech")

            # Atualiza XSRF após login bem-sucedido
            xsrf = client.cookies.get("XSRF-TOKEN", xsrf)
            hdrs["X-XSRF-TOKEN"] = xsrf

            try:
                body = r.json()
            except Exception:
                body = {}

            if body.get("two_factor"):
                if not twofa_code:
                    raise Exception("2FA_REQUIRED")
                r2 = await client.post("/two-factor-challenge", json={
                    "code": twofa_code,
                }, headers=hdrs)
                if r2.status_code not in (200, 201, 204):
                    raise Exception(f"2FA_FALHOU: HTTP {r2.status_code}")
                xsrf = client.cookies.get("XSRF-TOKEN", xsrf)

            cookies_dict = dict(client.cookies)
            if not cookies_dict:
                raise Exception("SEM_COOKIES: login pareceu OK mas nenhum cookie de sessão foi retornado")

            logger.info(f"[Byetech] Login via API OK — {len(cookies_dict)} cookies")

            # Garante XSRF atualizado no dict
            if xsrf:
                cookies_dict["XSRF-TOKEN"] = _up.quote(xsrf)

            ok = await _test_session(cookies_dict)
            if not ok:
                logger.warning("[Byetech] Login via API: cookies obtidos mas sessão inválida")
                return None

            logger.info("[Byetech] ✅ Login via API bem-sucedido (sem Playwright)")
            return cookies_dict

    except Exception as e:
        if "2FA_REQUIRED" in str(e):
            raise
        logger.warning(f"[Byetech] Login via API falhou: {type(e).__name__}: {e}")
        return None


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
    Faz login completo. Ordem de tentativas:
      1. Login via API httpx (sem browser) — funciona no Render se o endpoint existir
      2. Login via Playwright (subprocesso) — fallback local se a API não funcionar
    """
    # ── 1. Tentativa via API (funciona no Render, sem Playwright) ──
    try:
        cookies = await _login_via_api(twofa_code=None)
        if cookies:
            return cookies
    except Exception as e:
        if "2FA_REQUIRED" in str(e):
            # API confirmou que 2FA é necessário
            if twofa_callback is None:
                raise Exception(
                    "2FA_REQUIRED: Login Byetech requer código 2FA via API."
                )
            code = await twofa_callback()
            cookies = await _login_via_api(twofa_code=code)
            if cookies:
                return cookies

    # ── 2. Fallback: Playwright (apenas ambientes com browser) ────
    try:
        return await asyncio.to_thread(_login_via_subprocess, None)
    except Exception as e:
        if "2FA_REQUIRED" not in str(e):
            raise

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
    3. Login completo + 2FA via Playwright — só quando a sessão realmente expirou
       (no Render, Playwright não existe → lança RuntimeError com instrução clara)
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

        # 3. Login + 2FA (requer Playwright — só funciona localmente)
        try:
            _session_cookies = await _login_and_get_cookies(twofa_callback)
        except Exception as e:
            if "playwright" in str(e).lower() or "No module named" in str(e):
                raise RuntimeError(
                    "SESSAO_BYETECH_EXPIRADA: Playwright não disponível neste ambiente. "
                    "Execute localmente: python _refresh_byetech_session.py "
                    "e depois POST /api/byetech/push-session para sincronizar com o Render."
                ) from e
            raise
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
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": BYETECH_URL,
        "Referer": f"{BYETECH_URL}/contracts",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
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
    # SIGN & DRIVE antes de GWM: evita que "GWM Sign & Drive" vire GWM
    if "SIGN" in n or "DRIVE" in n:
        return "SIGN & DRIVE"
    if "GWM" in n or "HAVAL" in n or "ORA" in n or "TANK" in n:
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

    # Carrega mapa CPF (local) ou busca na API como fallback
    cpf_map_file = os.getenv("CPF_MAP_FILE", str(_Path(__file__).parent.parent.parent / ".byetech_cpf_map.json"))
    entry = None
    if os.path.exists(cpf_map_file):
        with open(cpf_map_file, encoding="utf-8") as f:
            cpf_map = json.load(f)
        entry = (cpf_map.get(cpf_norm)
                 or cpf_map.get(digits)
                 or cpf_map.get(cpf_norm + "0"))

    # Fallback: busca o contrato diretamente na API Byetech pelo CPF
    if not entry:
        logger.info(f"[Byetech] Mapa CPF indisponível — buscando na API para CPF {cpf_norm[:6]}...")
        entry = await _lookup_contrato_por_cpf(cpf_norm, digits, twofa_callback)

    if not entry:
        return False, f"cpf_nao_encontrado:{cpf_norm}"

    contract_id = entry.get("id")
    if not contract_id:
        return False, f"id_ausente_no_mapa:{cpf_norm}"

    # Sessão já foi obtida em _lookup_contrato_por_cpf; get_session retorna do cache
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


async def _lookup_contrato_por_cpf(cpf_norm: str, digits: str, twofa_callback=None) -> Optional[dict]:
    """
    Busca um contrato na API Byetech pelo CPF quando o mapa local não existe.
    Retorna um dict compatível com as entradas do cpf_map ou None.

    Estrutura real da resposta: data → data → contracts → data (lista)
    CPF real está em: item → order → client → num_cpf
    """
    try:
        cookies = await get_session(twofa_callback)
        headers = _make_headers(cookies)
        async with httpx.AsyncClient(follow_redirects=True, timeout=20) as client:
            for cpf_try in [cpf_norm, digits, cpf_norm.lstrip("0")]:
                resp = await client.get(
                    f"{API_URL}/api/contracts",
                    params={"cpfCnpj": cpf_try, "per_page": 5},
                    headers=headers,
                    cookies=cookies,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()

                # Estrutura real: {"data": {"contracts": {"data": [...], "last_page": N}}}
                nested = (data.get("data") or {}) if isinstance(data, dict) else {}
                if isinstance(nested, dict):
                    contracts_obj = nested.get("contracts") or {}
                    items = contracts_obj.get("data", []) if isinstance(contracts_obj, dict) else []
                elif isinstance(nested, list):
                    items = nested
                else:
                    items = []

                for item in items:
                    # CPF está em item.order.client.num_cpf
                    order      = item.get("order") or {}
                    client_obj = order.get("client") or {}
                    cpf_api    = re.sub(r"\D", "", str(client_obj.get("num_cpf") or ""))
                    if not cpf_api:
                        # fallback: campos camelCase direto no item (versões antigas)
                        cpf_api = re.sub(r"\D", "", str(
                            item.get("cpfCnpj") or item.get("cpf_cnpj") or ""
                        ))

                    if cpf_api.lstrip("0") == cpf_norm.lstrip("0") or cpf_api == digits:
                        cid = item.get("id") or item.get("contract_id")
                        if cid:
                            logger.info(f"[Byetech] CPF {cpf_norm[:6]}... → contrato {cid} via API")
                            return {
                                "id": str(cid),
                                "placa_carro":              item.get("placa_carro")               or item.get("placaCarro"),
                                "retirada_provisorio":      item.get("retirada_provisorio")       or item.get("retiradaProvisorio"),
                                "km_excedente_value":       item.get("km_excedente_value")        or item.get("kmExcedenteValue")        or "0.00",
                                "frequency_of_use":         item.get("frequency_of_use")          or item.get("frequencyOfUse"),
                                "usage_type":               item.get("usage_type")                or item.get("usageType"),
                                "is_reversal":              item.get("is_reversal")               or item.get("isReversal")              or 0,
                                "reversal_value":           item.get("reversal_value")            or item.get("reversalValue"),
                                "franquia_coparticipacao":  item.get("franquia_coparticipacao")   or item.get("franquiaCoparticipacao")  or "0",
                                "cobertura_danos_materiais":item.get("cobertura_danos_materiais") or item.get("coberturaDanosMateriais") or "--",
                                "cobertura_danos_corporais":item.get("cobertura_danos_corporais") or item.get("coberturaDanosCorporais") or "--",
                                "is_active":                item.get("is_active")                 or item.get("isActive")                or 1,
                                "is_extended":              item.get("is_extended")               or item.get("isExtended")              or 0,
                                "extension_months":         item.get("extension_months")          or item.get("extensionMonths"),
                                "automatic_send_link":      item.get("automatic_send_link")       or item.get("automaticSendLink")       or 1,
                            }
    except Exception as e:
        logger.error(f"[Byetech] Erro ao buscar contrato por CPF via API: {e}")
    return None


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

    # Carrega mapa CPF (arquivo local) ou busca na API como fallback
    if cpf_map_file is None:
        cpf_map_file = str(Path(__file__).parent.parent.parent / ".byetech_cpf_map.json")

    entry = None
    if os.path.exists(cpf_map_file):
        with open(cpf_map_file, encoding="utf-8") as f:
            cpf_map = json.load(f)
        entry = (cpf_map.get(cpf_norm)
                 or cpf_map.get(digits)
                 or cpf_map.get(cpf_norm + "0"))

    # Fallback: busca o contrato diretamente na API Byetech pelo CPF
    if not entry:
        logger.info(f"[Byetech] Mapa CPF indisponível — buscando na API para CPF {cpf_norm[:6]}...")
        entry = await _lookup_contrato_por_cpf(cpf_norm, digits, twofa_callback)

    if not entry:
        logger.error(f"[Byetech] CPF {cpf_norm!r} não encontrado no mapa CPF nem na API")
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
