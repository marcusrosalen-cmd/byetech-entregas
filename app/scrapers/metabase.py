"""
Scraper do Metabase (analytics.byetech.pro)
Busca todos os contratos ativos e novos contratos por data de venda.
"""
import httpx
import json
import logging
import re
import urllib.parse
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger("metabase")

METABASE_URL = "https://analytics.byetech.pro"
CARD_ID = "d62003b2-fb64-4072-92a2-23edab2caf69"

LOCADORA_MAP = [
    (["MOVIDA"],                  "MOVIDA"),
    (["UNIDAS"],                  "UNIDAS"),
    (["SIGN & DRIVE", "SIGN", "DRIVE"], "SIGN & DRIVE"),
    (["GWM"],                     "GWM"),
    (["VOLKSWAGEN"],              "VW"),
    (["LOCALIZA"],                "LOCALIZA"),
    (["ASSINECAR", " LM"],        "LM"),
    (["FLUA"],                    "FLUA"),
    (["NISSAN"],                  "NISSAN"),
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
    cpf = re.sub(r"[^\d]", "", str(row.get("num_cpf") or ""))
    fase = str(row.get("contrato_fase") or "")
    locadora = str(row.get("locadora") or "")
    return {
        "id_externo":               str(row.get("id") or ""),
        "byetech_contrato_id":      str(row.get("id") or ""),
        "fonte":                    map_locadora(locadora),
        "locadora_nome":            locadora,
        "cliente_nome":             str(row.get("nome_completo") or "").strip(),
        "cliente_cpf_cnpj":         cpf,
        "cliente_email":            str(row.get("email") or "").strip(),
        "veiculo":                  str(row.get("nome_veiculo") or "").strip(),
        "placa":                    str(row.get("placa_carro") or "").strip(),
        "status_atual":             fase,
        "data_prevista_entrega":    _parse_date(row.get("previsao_entrega")),
        "data_entrega_definitiva":  _parse_date(row.get("data_entrega_definitivo")),
        "data_venda":               _parse_date(row.get("date(pedidos.data_venda)")),
        "pedido_id_locadora":       row.get("pedido_id"),
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
    from datetime import timedelta
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
