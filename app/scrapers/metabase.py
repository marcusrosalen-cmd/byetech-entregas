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

# Prazo padrão (em dias) por locadora quando previsao_entrega não está cadastrado.
# Baseado na média histórica dos contratos ativos de cada locadora.
PRAZO_PADRAO_DIAS: dict[str, int] = {
    "MOVIDA":      83,
    "UNIDAS":      81,
    "LOCALIZA":    95,
    "SIGN & DRIVE": 48,
    "VW":          47,
    "FLUA":        66,
    "LM":          41,
    "GWM":         71,
    "NISSAN":      90,
}

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

    # Fallback 1: calcula a partir de data_venda + previsao_entrega (INT dias)
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

    # Fallback 2: previsao_entrega não cadastrado — usa prazo médio da locadora
    # Garante que todos os contratos ativos tenham data_prevista_entrega
    if not data_prevista:
        data_venda_fb = (
            _parse_date(row.get("data_venda"))
            or _parse_date(row.get("date(pedidos.data_venda)"))
        )
        padrao_dias = PRAZO_PADRAO_DIAS.get(fonte)
        if data_venda_fb and padrao_dias:
            data_prevista = data_venda_fb + timedelta(days=padrao_dias)
            logger.debug(
                f"[metabase] prazo padrão {padrao_dias}d aplicado para {fonte} "
                f"(id={row.get('id')}, previsao_entrega ausente)"
            )

    # ── data_venda ────────────────────────────────────────────────────────────
    data_venda_dt = (
        _parse_date(row.get("data_venda"))
        or _parse_date(row.get("date(pedidos.data_venda)"))
    )

    # O card público retorna id=c.id (contrato) e pedido_id=p.id (pedido).
    # O MCP retorna apenas p.id como "id".
    # Usar pedido_id preferentemente garante que ambas as fontes gerem o
    # mesmo id_externo, evitando registros duplicados no banco.
    pedido_id_externo = str(row.get("pedido_id") or row.get("id") or "")

    return {
        "id_externo":               pedido_id_externo,
        "byetech_contrato_id":      pedido_id_externo,
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
        "vendedor":                 str(row.get("usuario_atribuido") or "").strip(),
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
    Busca todos os contratos ativos + TODOS os contratos entregues do Metabase.

    O parâmetro include_recent_delivered_days é mantido por compatibilidade,
    mas não limita mais o período de entregues: todos são incluídos para garantir
    que os históricos sejam restaurados automaticamente após restart do servidor
    (Render free tier tem filesystem efêmero — o sync diário reconstrói o banco).

    Regra de inclusão:
    - Ativo (contrato_ativo != false/0): sempre inclui
    - Inativo COM data_entrega_definitivo: inclui (entregue)
    - Inativo SEM data_entrega_definitivo: pula (cancelado / erro de cadastro)
    - estorno=true: pula sempre
    """
    rows = await _fetch_metabase()
    logger.info(f"Metabase: {len(rows)} registros totais")

    result = []
    n_entregues = 0
    for r in rows:
        if r.get("estorno"):
            continue

        ativo = r.get("contrato_ativo")
        is_inactive = (ativo is False or ativo == 0 or ativo == "0")
        tem_entrega = bool(_parse_date(r.get("data_entrega_definitivo")))

        if is_inactive and not tem_entrega:
            continue  # cancelado ou inativo sem entrega — ignora

        c = _row_to_contrato(r)
        if tem_entrega:
            n_entregues += 1
        result.append(c)

    logger.info(f"Metabase: {len(result) - n_entregues} ativos + {n_entregues} entregues (histórico completo)")
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


async def fetch_historico_entregues() -> list[dict]:
    """
    Busca TODOS os contratos com data_entrega_definitivo cadastrada no Metabase,
    independente de contrato_ativo. Usado para importação histórica de entregas.

    Idempotente: pode ser chamado várias vezes — o upsert apenas atualiza
    data_entrega_definitiva nos registros já existentes ou cria novos.
    """
    rows = await _fetch_metabase()
    result = []
    for r in rows:
        if r.get("estorno"):
            continue
        data_ent = _parse_date(r.get("data_entrega_definitivo"))
        if not data_ent:
            continue  # sem data = ainda não entregue
        result.append(_row_to_contrato(r))
    logger.info(
        f"[metabase] histórico: {len(result)} contratos entregues "
        f"(de {len(rows)} registros totais no card)"
    )
    return result


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
