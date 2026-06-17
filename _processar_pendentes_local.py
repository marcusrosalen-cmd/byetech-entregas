"""
Processa a fila de pendentes do Byetech localmente.

Usa o .byetech_cpf_map.json local (que inclui todos os contratos,
inclusive já entregues) e a sessão Byetech local para atualizar
diretamente sem depender do Render.

Fluxo:
1. Busca os pendentes em Render
2. Para cada um: localiza no mapa CPF ou busca na API Byetech
3. Faz PATCH (entrega_definitivo) + MOVE (Definitivo Entregue)
4. Marca como processado no Render via /pendentes/{id}/done

PRÉ-REQUISITO:
    python _refresh_byetech_session.py   # gera .byetech_session.json
    python _processar_pendentes_local.py # processa tudo
"""
import asyncio, json, os, re, sys, io, time
import urllib.request, urllib.parse
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

RENDER_BASE    = "https://byetech-entregas.onrender.com"
API_URL        = "https://api-production.byetech.pro"
BYETECH_URL    = "https://crm.byetech.pro"
SESSION_FILE   = Path(__file__).parent / ".byetech_session.json"
CPF_MAP_FILE   = Path(__file__).parent / ".byetech_cpf_map.json"
PHASE_ENTREGUE = "9f727a78-6cfb-456b-a4b2-2189edd8ebdb"

# Segredo para auth de scripts locais no Render (SESSION_PUSH_SECRET no .env/Render)
_SYNC_SECRET = os.environ.get("SESSION_PUSH_SECRET", "byetech-local")
PHASE_NAMES = {
    "9f727a78-625a-4bd5-bccf-15006a679fc0": "Venda concluida",
    "9f727a78-6afd-4e40-b21c-aadf3e6fe64d": "Onboarding em andamento",
    "9f727a78-6bca-4e06-85f2-ff991bf21996": "Onboarding concluido",
    "9f727a78-6c68-4e6b-ba4c-f11d8d3fda31": "Provisorio retirado",
    "9f727a78-6cfb-456b-a4b2-2189edd8ebdb": "Definitivo entregue",
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def _d(s): return re.sub(r'\D', '', s or '')

def _norm(cpf):
    d = _d(cpf)
    if len(d) == 12 and d.endswith('0'): d = d[:-1]
    if len(d) > 11: d = d[-11:]
    return d.zfill(11)

def _variants(cpf):
    d = _d(cpf)
    vs = {d, d.zfill(11)}
    if len(d) == 11 and d.endswith('0'): vs.add('0' + d[:-1])
    elif len(d) == 12: vs.add(d[:-1]); vs.add(d[:-1].zfill(11))
    return {v for v in vs if v}

def _render_get(path):
    req = urllib.request.Request(
        f"{RENDER_BASE}{path}",
        headers={"User-Agent": "Mozilla/5.0", "X-Sync-Secret": _SYNC_SECRET},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def _render_post(path, payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{RENDER_BASE}{path}", data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
            "X-Sync-Secret": _SYNC_SECRET,
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

# ── Session ───────────────────────────────────────────────────────────────────
def load_session():
    if not SESSION_FILE.exists():
        print("ERRO: .byetech_session.json nao encontrado!")
        print("Execute primeiro: python _refresh_byetech_session.py")
        sys.exit(1)
    with open(SESSION_FILE, encoding="utf-8") as f:
        return json.load(f)

def make_headers(cookies):
    xsrf = urllib.parse.unquote(cookies.get("XSRF-TOKEN", ""))
    return {
        "X-XSRF-TOKEN": xsrf,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": BYETECH_URL,
        "Referer": f"{BYETECH_URL}/contracts",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
    }

# ── Byetech API ───────────────────────────────────────────────────────────────
async def test_session(client, cookies, headers):
    resp = await client.get(
        f"{API_URL}/api/contracts",
        params={"page": 1, "per_page": 1},
        headers=headers, cookies=cookies, timeout=30,
    )
    return resp.status_code in (200, 429)

async def search_cpf_all_phases(client, cookies, headers, cpf_norm, digits):
    """Busca contrato por CPF em TODAS as fases (incluindo Definitivo Entregue)."""
    try:
        import httpx
        for cpf_try in [cpf_norm, digits, cpf_norm.lstrip("0")]:
            if not cpf_try:
                continue
            resp = await client.get(
                f"{API_URL}/api/contracts",
                params={"cpfCnpj": cpf_try, "per_page": 10},
                headers=headers, cookies=cookies, timeout=30,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            contracts_obj = ((data.get("data") or {}).get("contracts") or {})
            items = contracts_obj.get("data", []) if isinstance(contracts_obj, dict) else []
            for item in items:
                order = item.get("order") or {}
                client_obj = order.get("client") or {}
                cpf_api = re.sub(r"\D", "", str(client_obj.get("num_cpf") or ""))
                if cpf_api.lstrip("0") == cpf_norm.lstrip("0") or cpf_api == digits:
                    cid = item.get("id")
                    if cid:
                        return {
                            "id": str(cid),
                            "phase_id": item.get("phase_id"),
                            "entrega_definitivo": item.get("entrega_definitivo"),
                            "placa_carro": item.get("placa_carro"),
                            "retirada_provisorio": item.get("retirada_provisorio"),
                            "km_excedente_value": str(item.get("km_excedente_value") or "0.00"),
                            "franquia_coparticipacao": str(item.get("franquia_coparticipacao") or "0"),
                            "cobertura_danos_materiais": item.get("cobertura_danos_materiais") or "--",
                            "cobertura_danos_corporais": item.get("cobertura_danos_corporais") or "--",
                            "is_reversal": item.get("is_reversal") or 0,
                            "is_active": item.get("is_active") if item.get("is_active") is not None else 1,
                            "is_extended": item.get("is_extended") or 0,
                            "automatic_send_link": item.get("automatic_send_link") if item.get("automatic_send_link") is not None else 1,
                            "frequency_of_use": item.get("frequency_of_use"),
                            "usage_type": item.get("usage_type"),
                            "reversal_value": item.get("reversal_value"),
                            "extension_months": item.get("extension_months"),
                        }
    except Exception as e:
        print(f"  [busca API] erro: {e}")
    return None

async def patch_and_move(client, cookies, headers, entry, data_entrega_str, placa=""):
    """Faz PATCH da data de entrega + MOVE para Definitivo Entregue."""
    contract_id = entry.get("id")
    effective_placa = placa or entry.get("placa_carro")

    payload_raw = {
        "entregaDefinitivo":       data_entrega_str,
        "kmExcedenteValue":        entry.get("km_excedente_value") or "0.00",
        "franquiaCoparticipacao":  entry.get("franquia_coparticipacao") or "0",
        "coberturaDanosMateriais": entry.get("cobertura_danos_materiais") or "--",
        "coberturaDanosCorporais": entry.get("cobertura_danos_corporais") or "--",
        "isReversal":              entry.get("is_reversal") if entry.get("is_reversal") is not None else 0,
        "isActive":                entry.get("is_active")  if entry.get("is_active")  is not None else 1,
        "isExtended":              entry.get("is_extended") if entry.get("is_extended") is not None else 0,
        "automaticSendLink":       entry.get("automatic_send_link") if entry.get("automatic_send_link") is not None else 1,
        "retiradaProvisorio":      entry.get("retirada_provisorio"),
        "placaCarro":              effective_placa,
        "frequencyOfUse":          entry.get("frequency_of_use"),
        "usageType":               entry.get("usage_type"),
        "reversalValue":           entry.get("reversal_value"),
        "extensionMonths":         entry.get("extension_months"),
    }
    payload = {k: v for k, v in payload_raw.items() if v is not None}

    # PATCH
    resp_patch = await client.patch(
        f"{API_URL}/api/contracts/{contract_id}",
        json=payload, headers=headers, cookies=cookies, timeout=30,
    )
    if resp_patch.status_code not in (200, 201, 204):
        return False, f"PATCH HTTP {resp_patch.status_code}: {resp_patch.text[:150]}"

    # MOVE para Definitivo Entregue
    resp_move = await client.patch(
        f"{API_URL}/api/contracts/{contract_id}/move",
        json={"phaseId": PHASE_ENTREGUE},
        headers=headers, cookies=cookies, timeout=30,
    )
    if resp_move.status_code not in (200, 201, 204):
        body_txt = resp_move.text
        if resp_move.status_code == 500 and "atualizar" in body_txt:
            return True, "ja_na_fase_definitivo_entregue"
        return False, f"MOVE HTTP {resp_move.status_code}: {body_txt[:150]}"

    return True, "ok"

async def mark_done_on_render(pendente_id: int, sucesso: bool, mensagem: str = "", erro: str = ""):
    """Marca item como processado no Render."""
    try:
        payload = {"sucesso": sucesso}
        if not sucesso and (erro or mensagem):
            payload["erro"] = erro or mensagem
        _render_post(
            f"/api/byetech/pendentes/{pendente_id}/done",
            payload
        )
    except Exception as e:
        print(f"  [render] erro ao marcar {pendente_id}: {e}")

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    try:
        import httpx
    except ImportError:
        print("ERRO: httpx nao instalado. Execute: pip install httpx")
        return

    print("=" * 70)
    print("PROCESSAMENTO LOCAL DE PENDENTES BYETECH")
    print(f"Hora: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 70)

    # 1. Carrega sessão local
    cookies = load_session()
    headers = make_headers(cookies)
    print(f"\n[1] Sessao carregada: {list(cookies.keys())}")

    # 2. Carrega mapa CPF local
    if not CPF_MAP_FILE.exists():
        print("AVISO: .byetech_cpf_map.json nao encontrado — usando apenas busca API")
        cpf_map = {}
    else:
        with open(CPF_MAP_FILE, encoding="utf-8") as f:
            cpf_map = json.load(f)
    print(f"[2] Mapa CPF local: {len(cpf_map)} entradas")

    # 3. Verifica sessão
    async with httpx.AsyncClient(follow_redirects=True) as client:
        ok = await test_session(client, cookies, headers)
        if not ok:
            print("\nERRO: Sessao invalida! Execute: python _refresh_byetech_session.py")
            return
        print("[3] Sessao valida")

    # 4. Busca pendentes no Render
    try:
        fila = _render_get("/api/byetech/pendentes")
        pendentes = fila.get("pendentes", [])
    except Exception as e:
        print(f"ERRO ao buscar fila: {e}")
        return
    print(f"[4] {len(pendentes)} pendentes na fila do Render")

    if not pendentes:
        print("\nNenhum item na fila. Nada a processar.")
        return

    # 5. Processa cada pendente
    print(f"\n[5] Processando {len(pendentes)} itens...")
    print("-" * 70)
    ok_count = 0
    err_count = 0
    skip_count = 0
    nao_encontrado = []

    async with httpx.AsyncClient(follow_redirects=True) as client:
        for p in pendentes:
            pid       = p.get("id")
            cpf_raw   = p.get("cliente_cpf") or ""
            nome      = p.get("cliente_nome") or "?"
            placa     = p.get("placa") or ""
            de_str_raw = p.get("data_entrega") or ""

            # Parse data de entrega
            try:
                if "T" in de_str_raw:
                    data_dt = datetime.fromisoformat(de_str_raw[:19])
                else:
                    data_dt = datetime.strptime(de_str_raw[:10], "%Y-%m-%d")
                de_str = data_dt.strftime("%Y-%m-%d")
            except Exception:
                de_str = datetime.utcnow().strftime("%Y-%m-%d")

            cpf_norm = _norm(cpf_raw)

            # Busca entry no mapa local
            entry = cpf_map.get(cpf_norm)
            if not entry:
                for v in _variants(cpf_raw):
                    entry = cpf_map.get(v)
                    if entry: break

            # Se não está no mapa local, busca na API
            if not entry:
                digits = _d(cpf_raw)
                entry = await search_cpf_all_phases(client, cookies, headers, cpf_norm, digits)

            if not entry:
                print(f"  NAO ENCONTRADO  {nome[:45]:<46} CPF={cpf_norm}")
                nao_encontrado.append({"id": pid, "nome": nome, "cpf": cpf_norm})
                err_count += 1
                continue

            # Verifica se já está entregue
            phase_atual = entry.get("phase_id") or ""
            byt_id = entry.get("id")

            if phase_atual == PHASE_ENTREGUE and entry.get("entrega_definitivo"):
                # Já está em Definitivo Entregue - só marca como done
                print(f"  JA ENTREGUE     {nome[:45]:<46} Byetech={byt_id} | entrega={entry.get('entrega_definitivo', '')[:10]}")
                await mark_done_on_render(pid, True)
                ok_count += 1
                await asyncio.sleep(0.2)
                continue

            # Aplica PATCH + MOVE
            ok_patch, msg = await patch_and_move(client, cookies, headers, entry, de_str, placa)

            if ok_patch:
                fase_msg = PHASE_NAMES.get(phase_atual, phase_atual) if phase_atual else "?"
                print(f"  OK              {nome[:45]:<46} Byetech={byt_id} | {de_str} | {msg}")
                await mark_done_on_render(pid, True)
                ok_count += 1
            else:
                print(f"  ERRO PATCH      {nome[:45]:<46} Byetech={byt_id} | {msg}")
                await mark_done_on_render(pid, False, erro=msg)
                err_count += 1

            await asyncio.sleep(0.3)  # Rate limit

    print("\n" + "=" * 70)
    print(f"CONCLUIDO: {ok_count} OK | {err_count} erros | {skip_count} ignorados")

    if nao_encontrado:
        print(f"\nNAO ENCONTRADOS NO BYETECH ({len(nao_encontrado)}):")
        print("  Estes contratos podem nao existir no Byetech CRM.")
        print("  Verifique manualmente ou ignore se sao contratos sem vinculo:")
        for item in nao_encontrado:
            print(f"    - {item['nome'][:50]} | CPF: {item['cpf']}")

    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
