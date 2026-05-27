"""
Scraper do Metabase (analytics.byetech.pro)
Busca todos os contratos ativos e novos contratos por data de venda.
"""
import httpx
import json
import logging
import re
import urllib.parse
from datetime import datetime, date, timedelta
from typing import Optional

logger = logging.getLogger("metabase")

METABASE_URL = "https://analytics.byetech.pro"
CARD_ID = "d62003b2-fb64-4072-92a2-23edab2caf69"

LOCADORA_MAP = [
    (["MOVIDA"],                            "MOVIDA"),
    (["UNIDAS"],                            "UNIDAS"),
    (["SIGN & DRIVE", "SIGNANDDRIVE"],      "SIGN & DRIVE"),
    (["GWM"],                               "GWM"),
    (["VOLKSWAGEN EMPRESAS"],               "VW"),
    (["VOLKSWAGEN"],                        "VW"),
    (["LOCALIZA"],                          "LOCALIZA"),
    (["ASSINECAR", " LM"],                  "LM"),
    (["FLUA"],                              "FLUA"),
    (["NISSAN"],                            "NISSAN"),
]

def map_locadora(nome: str) -> str:
    n = (nome or "").upper()
    for palavras, fonte in LOCADORA_MAP:
        if any(p in n for p in palavras):
            return fonte
    return (nome or "OUTRO").upper()


def _parse_date(val) -> Optional[datetime]:
    if not val or str(val).strip() in ("", "NaT", "None", "nan"):
        return None
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"]:
        try:
            return datetime.strptime(str(val)[:19], fmt[:len(str(val)[:19])])
        except ValueError:
            continue
    return None


def _row_to_contrato(row: dict) -> dict:
    """
    Converte linha do Metabase em dicionário de contrato.

    Suporta dois formatos de coluna:
    - Formato card público:  previsao_entrega (INT), date(pedidos.data_venda), num_cpf
    - Formato MCP/direto:    data_prevista_entrega (DATE), data_venda (DATE),
                             num_cpf, num_cnpj (VW Empresas = CNPJ)
    """
    locadora = str(row.get("locadora") or "").strip()
    fonte = map_locadora(locadora)

    # ── CPF / CNPJ ────────────────────────────────────────────────────────────
    cpf_raw  = re.sub(r"[^\d]", "", str(row.get("num_cpf")  or ""))
    cnpj_raw = re.sub(r"[^\d]", "", str(row.get("num_cnpj") or ""))

    # VW Empresas: o portal Sign & Drive registra os pedidos pelo CNPJ da empresa.
    # Usar num_cnpj garante que a busca no portal encontre o pedido correto.
    if fonte == "VW" and cnpj_raw:
        cpf_cnpj = cnpj_raw
    elif cpf_raw:
        cpf_cnpj = cpf_raw
    elif cnpj_raw:
        cpf_cnpj = cnpj_raw   # fallback genérico
    else:
        cpf_cnpj = ""

    # ── data_prevista_entrega ─────────────────────────────────────────────────
    # Tenta ler como coluna calculada (formato MCP: data_prevista_entrega ou data_previsao_entrega)
    data_prevista = (
        _parse_date(row.get("data_prevista_entrega"))
        or _parse_date(row.get("data_previsao_entrega"))
    )

    # Fallback: calcula a partir de data_venda + previsao_entrega (INT dias)
    # Necessário quando o card público retorna previsao_entrega como INT
    if not data_prevista:
        # data_venda pode vir com nome literal "date(pedidos.data_venda)" do card público
        data_venda = (
            _parse_date(row.get("data_venda"))
            or _parse_date(row.get("date(pedidos.data_venda)"))
        )
        previsao_dias = row.get("previsao_entrega")
        if data_venda and previsao_dias is not None:
            try:
                data_prevista = data_venda + timedelta(days=int(previsao_dias))
            except (ValueError, TypeError):
                data_prevista = None

    # ── data_venda ────────────────────────────────────────────────────────────
    data_venda_dt = (
        _parse_date(row.get("data_venda"))
        or _parse_date(row.get("date(pedidos.data_venda)"))
    )

    return {
        "id_externo":               str(row.get("id") or ""),
        "byetech_contrato_id":      str(row.get("id") or ""),
        "fonte":                    fonte,
        "locadora_nome":            locadora,
        "cliente_nome":             str(row.get("nome_completo") or "").strip(),
        "cliente_cpf_cnpj":         cpf_cnpj,
        "cliente_email":            str(row.get("email") or "").strip(),
        "veiculo":                  str(row.get("nome_veiculo") or "").strip(),
        "placa":                    str(row.get("placa_carro") or "").strip(),
        "status_atual":             str(row.get("contrato_fase") or ""),
        "data_prevista_entrega":    data_prevista,
        "data_entrega_definitiva":  _parse_date(row.get("data_entrega_definitivo")),
        "data_venda":               data_venda_dt,
        "pedido_id_locadora":       row.get("pedido_id") or row.get("id"),
    }


async def _fetch_metabase(params_json: list = None) -> list[dict]:
    url = f"{METABASE_URL}/api/public/card/{CARD_ID}/query/json"
    query = ""
    if params_json:
        query = "?parameters=" + urllib.parse.quote(json.dumps(params_json))

    async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
        resp = await client.get(url + query)
        resp.raise_for_status()
        data = resp.json()

    if isinstance(data, list):
        return data
    return []


async def fetch_all_active(include_recent_delivered_days: int = 30) -> list[dict]:
    """
    Busca todos os contratos ativos + contratos entregues nos últimos N dias.

    Inclui recém-entregues para que run_metabase_sync consiga detectar a transição
    "pendente → entregue" e notificar o Byetech CRM automaticamente.
    """
    rows = await _fetch_metabase()
    logger.info(f"Metabase: {len(rows)} registros totais")
    cutoff = datetime.utcnow() - timedelta(days=include_recent_delivered_days)

    result = []
    n_entregues = 0
    for r in rows:
        if r.get("contrato_ativo") is False or r.get("estorno"):
            continue
        fase = str(r.get("contrato_fase") or "")
        if fase.lower() in ("definitivo entregue", "definitivo_entregue", "pedido concluído", "pedido concluido"):
            data_ent = _parse_date(r.get("data_entrega_definitivo"))
            if data_ent and data_ent >= cutoff:
                result.append(_row_to_contrato(r))
                n_entregues += 1
        else:
            result.append(_row_to_contrato(r))

    logger.info(f"Metabase: {len(result) - n_entregues} ativos + {n_entregues} entregues recentes")
    return result


async def fetch_contracts_by_date(dt: date) -> list[dict]:
    """Busca contratos com data de venda igual a dt (inclui entregues recentes)."""
    dt_str = dt.strftime("%Y-%m-%d")
    params = [{
        "type": "date/range",
        "value": f"{dt_str}~{dt_str}",
        "target": ["variable", ["template-tag", "data_venda"]],
    }]
    rows = await _fetch_metabase(params)
    logger.info(f"Metabase ({dt_str}): {len(rows)} registros")
    # Não filtra entregues — permite detectar a transição de status
    return [
        _row_to_contrato(r) for r in rows
        if r.get("contrato_ativo") is not False
        and not r.get("estorno")
    ]


def rows_to_contratos(rows: list[dict]) -> list[dict]:
    """
    Converte linhas brutas do Metabase MCP (formato da query direta ao analytic_db)
    em dicionários de contrato prontos para _upsert_contrato.

    Usado pelo script local _sync_metabase_mcp.py para processar os resultados
    do MCP antes de enviar ao Render.

    A query MCP deve selecionar:
      p.id, p.nome_completo, p.num_cpf, p.num_cnpj,
      p.locadora, p.nome_veiculo, p.data_venda, p.previsao_entrega,
      DATE_ADD(p.data_venda, INTERVAL p.previsao_entrega DAY) as data_prevista_entrega,
      c.contrato_fase, c.placa_carro, c.data_entrega_definitivo,
      c.contrato_ativo, c.estorno
    """
    result = []
    for r in rows:
        ativo = r.get("contrato_ativo")
        estorno = r.get("estorno")
        # Aceita tanto bool quanto int (1/0) — MySQL retorna int via MCP
        if ativo is False or ativo == 0 or ativo == "0":
            continue
        if estorno is True or estorno == 1 or estorno == "1":
            continue
        result.append(_row_to_contrato(r))
    return result
