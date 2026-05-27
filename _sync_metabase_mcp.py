"""
Sync Metabase → Render via MCP (script local, roda todo dia de manhã).

Consulta o analytic_db (Metabase db_id=2) diretamente e envia os contratos
para o endpoint POST /api/sync/push-contratos no Render.

Vantagens em relação ao card público:
  • Inclui num_cnpj (essencial para contratos VW Empresas no portal S&D)
  • data_prevista_entrega calculada diretamente pelo MySQL via DATE_ADD
  • Permite filtros mais precisos (locadora, cancelado, contrato_ativo)
  • Detecta contratos entregues diretamente no Byetech (data_entrega_definitivo preenchida)

Uso:
  python _sync_metabase_mcp.py           # sync padrão (ativos + entregues 30 dias)
  python _sync_metabase_mcp.py --full    # todos os contratos S&D/VW/GWM
  python _sync_metabase_mcp.py --dry-run # apenas mostra o que seria enviado

Pré-requisito:
  O Metabase MCP deve estar configurado no Claude Code (analytics.byetech.pro).
  Este script usa a API REST do Metabase com autenticação via API Key.
  Configure METABASE_API_KEY no .env ou como variável de ambiente.
  Para criar uma API Key: Metabase → Admin → People → API Keys → Create an API key.
"""

import sys
import io
import json
import re
import os
import time
import argparse
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta, date

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ── Configuração ──────────────────────────────────────────────────────────────
# Carrega .env se disponível
_env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_path):
    with open(_env_path, encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

METABASE_URL   = os.getenv("METABASE_URL",   "https://analytics.byetech.pro")
METABASE_DB_ID = int(os.getenv("METABASE_DB_ID", "2"))
RENDER_URL     = os.getenv("RENDER_SERVICE_URL", "https://byetech-entregas.onrender.com")

# Autenticação Metabase: API Key (preferido) ou usuário/senha
METABASE_API_KEY  = os.getenv("METABASE_API_KEY",  "")
METABASE_USER     = os.getenv("METABASE_USER",     "")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "")

# Locadoras a sincronizar (do portal Sign & Drive / VW)
LOCADORAS_SD = ("Sign & Drive", "Volkswagen Empresas", "GWM")

# ── SQL principal ──────────────────────────────────────────────────────────────
SQL_ATIVOS = """
SELECT
  p.id,
  p.nome_completo,
  p.num_cpf,
  p.num_cnpj,
  p.locadora,
  p.nome_veiculo,
  p.data_venda,
  p.previsao_entrega,
  DATE_ADD(p.data_venda, INTERVAL p.previsao_entrega DAY) AS data_prevista_entrega,
  c.contrato_fase,
  c.placa_carro,
  c.data_entrega_definitivo,
  c.contrato_ativo,
  c.estorno
FROM pedidos p
LEFT JOIN contratos c ON c.pedido_id = p.id
WHERE
  p.cancelado = 0
  AND (c.contrato_ativo = 1 OR c.contrato_ativo IS NULL)
  AND (c.estorno = 0 OR c.estorno IS NULL)
  AND p.locadora IN ('Sign & Drive', 'Volkswagen Empresas', 'GWM')
  AND (
    c.contrato_fase NOT IN ('Definitivo entregue', 'Cancelado')
    OR c.data_entrega_definitivo >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
  )
ORDER BY p.data_venda DESC
"""

SQL_FULL = """
SELECT
  p.id,
  p.nome_completo,
  p.num_cpf,
  p.num_cnpj,
  p.locadora,
  p.nome_veiculo,
  p.data_venda,
  p.previsao_entrega,
  DATE_ADD(p.data_venda, INTERVAL p.previsao_entrega DAY) AS data_prevista_entrega,
  c.contrato_fase,
  c.placa_carro,
  c.data_entrega_definitivo,
  c.contrato_ativo,
  c.estorno
FROM pedidos p
LEFT JOIN contratos c ON c.pedido_id = p.id
WHERE
  p.cancelado = 0
  AND p.locadora IN ('Sign & Drive', 'Volkswagen Empresas', 'GWM')
  AND p.data_venda >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
ORDER BY p.data_venda DESC
"""


# ── Metabase API ───────────────────────────────────────────────────────────────

def _mb_session_login() -> str:
    """Autentica via usuário/senha e retorna o token de sessão."""
    url = f"{METABASE_URL}/api/session"
    payload = json.dumps({"username": METABASE_USER, "password": METABASE_PASSWORD}).encode()
    req = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "byetech-sync/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read())
    token = data.get("id")
    if not token:
        raise RuntimeError(f"Login Metabase falhou: {data}")
    return token


def _mb_headers(session_token: str = "") -> dict:
    """Retorna headers de autenticação para a API do Metabase."""
    if METABASE_API_KEY:
        return {
            "Content-Type": "application/json",
            "User-Agent":   "byetech-sync/1.0",
            "x-api-key":    METABASE_API_KEY,
        }
    elif session_token:
        return {
            "Content-Type":       "application/json",
            "User-Agent":         "byetech-sync/1.0",
            "x-metabase-session": session_token,
        }
    else:
        raise RuntimeError(
            "Sem credenciais Metabase. Configure METABASE_API_KEY ou "
            "METABASE_USER + METABASE_PASSWORD no .env"
        )


def metabase_query(sql: str, session_token: str = "") -> list[dict]:
    """Executa SQL no Metabase e retorna lista de dicts."""
    url = f"{METABASE_URL}/api/dataset"
    payload = json.dumps({
        "database": METABASE_DB_ID,
        "type":     "native",
        "native":   {"query": sql},
    }).encode()
    req = urllib.request.Request(
        url, data=payload,
        headers=_mb_headers(session_token),
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        resp = json.loads(r.read())

    data_obj = resp.get("data", {})
    cols  = [c.get("name") for c in data_obj.get("cols", [])]
    rows  = data_obj.get("rows", [])
    return [dict(zip(cols, row)) for row in rows]


# ── Mapeamento de locadora ─────────────────────────────────────────────────────

LOCADORA_MAP = [
    (["MOVIDA"],                        "MOVIDA"),
    (["UNIDAS"],                        "UNIDAS"),
    (["SIGN & DRIVE", "SIGNANDDRIVE"],  "SIGN & DRIVE"),
    (["GWM"],                           "GWM"),
    (["VOLKSWAGEN EMPRESAS"],           "VW"),
    (["VOLKSWAGEN"],                    "VW"),
    (["LOCALIZA"],                      "LOCALIZA"),
    (["ASSINECAR", " LM"],              "LM"),
    (["FLUA"],                          "FLUA"),
    (["NISSAN"],                        "NISSAN"),
]

def map_locadora(nome: str) -> str:
    n = (nome or "").upper()
    for palavras, fonte in LOCADORA_MAP:
        if any(p in n for p in palavras):
            return fonte
    return (nome or "OUTRO").upper()


def _parse_date_str(val) -> str | None:
    """Retorna string ISO YYYY-MM-DD ou None."""
    if not val or str(val).strip() in ("", "NaT", "None", "nan", "null"):
        return None
    s = str(val).strip()
    # Remove fuso horário se presente (ex: "2025-08-06T00:00:00-03:00")
    s = re.sub(r"[T ](\d{2}:\d{2}:\d{2}).*", "", s)
    try:
        datetime.strptime(s[:10], "%Y-%m-%d")
        return s[:10]
    except ValueError:
        pass
    for fmt in ["%d/%m/%Y"]:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _row_to_contrato(row: dict) -> dict:
    locadora = str(row.get("locadora") or "").strip()
    fonte = map_locadora(locadora)

    cpf_raw  = re.sub(r"[^\d]", "", str(row.get("num_cpf")  or ""))
    cnpj_raw = re.sub(r"[^\d]", "", str(row.get("num_cnpj") or ""))

    # VW Empresas: portal S&D registra pedidos pelo CNPJ
    if fonte == "VW" and cnpj_raw:
        cpf_cnpj = cnpj_raw
    elif cpf_raw:
        cpf_cnpj = cpf_raw
    elif cnpj_raw:
        cpf_cnpj = cnpj_raw
    else:
        cpf_cnpj = ""

    # data_prevista_entrega — pode ser coluna calculada do MySQL
    data_prevista = _parse_date_str(row.get("data_prevista_entrega"))
    if not data_prevista:
        data_venda   = _parse_date_str(row.get("data_venda"))
        previsao_int = row.get("previsao_entrega")
        if data_venda and previsao_int is not None:
            try:
                dv  = datetime.strptime(data_venda, "%Y-%m-%d")
                dp  = dv + timedelta(days=int(previsao_int))
                data_prevista = dp.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                data_prevista = None

    return {
        "id_externo":              str(row.get("id") or ""),
        "byetech_contrato_id":     str(row.get("id") or ""),
        "fonte":                   fonte,
        "locadora_nome":           locadora,
        "cliente_nome":            str(row.get("nome_completo") or "").strip(),
        "cliente_cpf_cnpj":        cpf_cnpj,
        "cliente_email":           "",
        "veiculo":                 str(row.get("nome_veiculo") or "").strip(),
        "placa":                   str(row.get("placa_carro") or "").strip(),
        "status_atual":            str(row.get("contrato_fase") or ""),
        "data_prevista_entrega":   data_prevista,
        "data_entrega_definitiva": _parse_date_str(row.get("data_entrega_definitivo")),
        "data_venda":              _parse_date_str(row.get("data_venda")),
        "pedido_id_locadora":      str(row.get("id") or ""),
    }


# ── Render push ────────────────────────────────────────────────────────────────

def render_push(contratos: list[dict], fonte: str = "metabase_mcp") -> dict:
    """Envia lote de contratos para o endpoint /api/sync/push-contratos do Render."""
    url = f"{RENDER_URL}/api/sync/push-contratos"
    payload = json.dumps({"contratos": contratos, "fonte": fonte}).encode()
    req = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "byetech-sync/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Render HTTP {e.code}: {body[:400]}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sync Metabase MCP → Render")
    parser.add_argument("--full",    action="store_true", help="Busca todos os contratos (365 dias)")
    parser.add_argument("--dry-run", action="store_true", help="Apenas mostra o que seria enviado")
    args = parser.parse_args()

    print("=" * 70)
    print("SYNC METABASE MCP → RENDER")
    print(f"Hora: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"Modo: {'FULL' if args.full else 'PADRÃO'}{' [DRY-RUN]' if args.dry_run else ''}")
    print("=" * 70)

    # ── Autenticação ──────────────────────────────────────────────────────────
    session_token = ""
    if not METABASE_API_KEY:
        if not METABASE_USER or not METABASE_PASSWORD:
            print("\n❌ ERRO: Configure METABASE_API_KEY ou METABASE_USER + METABASE_PASSWORD no .env")
            print("\nComo obter a API Key:")
            print("  1. Acesse https://analytics.byetech.pro")
            print("  2. Vá em Admin → People → API Keys → Create an API key")
            print("  3. Adicione METABASE_API_KEY=mb_... no .env")
            sys.exit(1)
        print("\n[auth] Fazendo login no Metabase via usuário/senha...")
        try:
            session_token = _mb_session_login()
            print("  ✅ Login OK")
        except Exception as e:
            print(f"  ❌ Falha no login: {e}")
            sys.exit(1)
    else:
        print(f"\n[auth] Usando API Key: {METABASE_API_KEY[:12]}...")

    # ── Query ─────────────────────────────────────────────────────────────────
    sql = SQL_FULL if args.full else SQL_ATIVOS
    print(f"\n[query] Consultando Metabase (db_id={METABASE_DB_ID})...")
    t0 = time.time()
    try:
        rows = metabase_query(sql, session_token)
    except Exception as e:
        print(f"  ❌ Erro na query: {e}")
        sys.exit(1)
    print(f"  ✅ {len(rows)} linhas em {time.time()-t0:.1f}s")

    # ── Transformação ─────────────────────────────────────────────────────────
    contratos = []
    por_fonte: dict[str, int] = {}
    for r in rows:
        c = _row_to_contrato(r)
        if not c["id_externo"]:
            continue
        contratos.append(c)
        por_fonte[c["fonte"]] = por_fonte.get(c["fonte"], 0) + 1

    print(f"\n[transform] {len(contratos)} contratos válidos:")
    for fonte, cnt in sorted(por_fonte.items()):
        print(f"  {fonte}: {cnt}")

    # ── VW CNPJ check ─────────────────────────────────────────────────────────
    vw_com_cnpj    = sum(1 for c in contratos if c["fonte"] == "VW" and len(c.get("cliente_cpf_cnpj","")) == 14)
    vw_sem_cnpj    = sum(1 for c in contratos if c["fonte"] == "VW" and len(c.get("cliente_cpf_cnpj","")) != 14)
    if vw_com_cnpj or vw_sem_cnpj:
        print(f"\n  VW com CNPJ (14 dígitos): {vw_com_cnpj}")
        print(f"  VW sem CNPJ (CPF/vazio):  {vw_sem_cnpj}")

    if args.dry_run:
        print("\n[DRY-RUN] Primeiros 5 contratos:")
        for c in contratos[:5]:
            print(f"  {c['fonte']:<12} {c['cliente_nome'][:40]:<42} "
                  f"cpf/cnpj={c['cliente_cpf_cnpj']:<16} "
                  f"prev={c['data_prevista_entrega'] or '—'}")
        print("\n[DRY-RUN] Nada foi enviado ao Render.")
        return

    # ── Push ao Render ────────────────────────────────────────────────────────
    print(f"\n[push] Enviando {len(contratos)} contratos para {RENDER_URL}...")
    t1 = time.time()
    try:
        result = render_push(contratos)
    except Exception as e:
        print(f"  ❌ Erro no push: {e}")
        sys.exit(1)

    print(f"  ✅ {result.get('importados', 0)} importados | "
          f"{result.get('novas_entregas', 0)} novas entregas detectadas | "
          f"{len(result.get('erros', []))} erros "
          f"({time.time()-t1:.1f}s)")

    if result.get("erros"):
        print("\n  Primeiros erros:")
        for err in result["erros"][:5]:
            print(f"    - {err}")

    print("\n" + "=" * 70)
    print(f"CONCLUÍDO: {result.get('importados', 0)} contratos sincronizados")
    print("=" * 70)


if __name__ == "__main__":
    main()
