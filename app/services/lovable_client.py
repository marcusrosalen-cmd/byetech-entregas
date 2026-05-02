"""
Integração com o Lovable Cloud (banco Supabase gerenciado pelo Lovable).

Fluxo:
    Python scraper → SQLite local (scrapers leem daqui)
                   → Lovable Cloud  (time visualiza pelo Lovable)

O SQLite continua como banco operacional dos scrapers.
O Lovable Cloud é a "vitrine" para o time — recebe espelho dos dados.

Configuração necessária no .env:
    LOVABLE_URL=https://xxxx.supabase.co
    LOVABLE_SERVICE_KEY=eyJhbGc...   (service_role key — bypassa RLS)

IMPORTANTE: no Lovable, adicione UNIQUE constraint na coluna
    contratos.id_externo  →  ALTER TABLE contratos ADD UNIQUE (id_externo);
    (pode fazer pelo Supabase SQL editor que o Lovable expõe)
"""

import logging
import os
from datetime import datetime, date
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("lovable")

LOVABLE_URL = os.getenv("LOVABLE_URL", "")
LOVABLE_SERVICE_KEY = os.getenv("LOVABLE_SERVICE_KEY", "")

# Cliente lazy — criado na primeira chamada
_client = None


def _get_client():
    """Retorna o cliente Supabase (cria se necessário)."""
    global _client
    if _client is not None:
        return _client

    if not LOVABLE_URL or not LOVABLE_SERVICE_KEY:
        raise RuntimeError(
            "LOVABLE_URL e LOVABLE_SERVICE_KEY não configurados no .env\n"
            "Pegue essas credenciais em: Lovable → Settings → Database"
        )

    try:
        from supabase import create_client, Client
        _client = create_client(LOVABLE_URL, LOVABLE_SERVICE_KEY)
        logger.info("✅ Lovable Cloud conectado")
        return _client
    except ImportError:
        raise RuntimeError(
            "Biblioteca 'supabase' não instalada.\n"
            "Execute: pip install supabase>=2.3.0"
        )


def is_configured() -> bool:
    """Retorna True se as credenciais do Lovable estão definidas no .env."""
    return bool(LOVABLE_URL and LOVABLE_SERVICE_KEY)


# ─────────────────────────────────────────────
# Mapeamento de campos
# ─────────────────────────────────────────────

def _etapa_kanban(status: str) -> str:
    """
    Converte o status_atual do contrato para a etapa do Kanban no Lovable.
    Mantém a etapa atual se o status não for reconhecido.
    """
    if not status:
        return "faturamento"

    s = status.lower()

    if any(x in s for x in ["faturad", "fatura"]):
        return "faturamento"
    if any(x in s for x in ["saiu", "saída", "saida", "fabric", "produc"]):
        return "saida_fabrica"
    if any(x in s for x in ["transport", "trânsito", "transito", "rota", "caminho"]):
        return "transporte"
    if any(x in s for x in ["disponível", "disponivel", "loja", "estoque", "chegou"]):
        return "disponivel_loja"
    if any(x in s for x in ["aguardando", "aguard", "retirada", "cliente"]):
        return "aguardando_cliente"
    if any(x in s for x in ["entregue", "entrega definitiv", "definitivo entregue"]):
        return "entregue"

    return "faturamento"


def _dt_iso(dt) -> Optional[str]:
    """Converte datetime/date para string ISO8601 ou None."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt.isoformat()
    if isinstance(dt, date):
        return dt.isoformat()
    return str(dt)


def _contrato_para_lovable(data: dict, contrato_id_local: str) -> dict:
    """
    Converte um dict de contrato (formato interno do sync_service)
    para o formato da tabela 'contratos' do Lovable Cloud.

    contrato_id_local: o ID string local (ex: 'GWM_123456') — usado como
    id_externo no Lovable para garantir upsert correto.
    """
    status = data.get("status_atual", "")
    dp = data.get("data_prevista_entrega")

    # Calcula dias
    dias = None
    atrasado = False
    if dp:
        dp_date = dp.date() if isinstance(dp, datetime) else dp
        delta = (dp_date - date.today()).days
        dias = delta
        atrasado = delta < 0

    return {
        # id_externo armazena nosso ID local → garante upsert único
        "id_externo": contrato_id_local,

        "fonte":              data.get("fonte", ""),
        "byetech_contrato_id": data.get("byetech_contrato_id", ""),

        "cliente_nome":      data.get("cliente_nome", ""),
        "cliente_cpf_cnpj":  data.get("cliente_cpf_cnpj", ""),
        "cliente_email":     data.get("cliente_email", ""),
        "veiculo":           data.get("veiculo", ""),
        "placa":             data.get("placa", ""),

        "status_atual":      status,
        "kanban_etapa":      _etapa_kanban(status),

        "data_prevista_entrega":   _dt_iso(dp),
        "data_entrega_definitiva": _dt_iso(data.get("data_entrega_definitiva")),
        "data_venda":              _dt_iso(data.get("data_venda")),

        "origem_dados":     data.get("origem_dados", "SYNC"),
        "ultima_sync":      datetime.utcnow().isoformat(),
        "ultima_atualizacao": datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────────
# Operações principais
# ─────────────────────────────────────────────

def upsert_contrato(data: dict, contrato_id_local: str) -> bool:
    """
    Insere ou atualiza um contrato no Lovable Cloud.

    data: dict no formato interno do sync_service
    contrato_id_local: ID string local (ex: 'GWM_CPF123')

    Retorna True se sucesso, False se erro (erros são logados, nunca propagados).
    """
    if not is_configured():
        return False

    try:
        client = _get_client()
        payload = _contrato_para_lovable(data, contrato_id_local)

        client.table("contratos").upsert(
            payload,
            on_conflict="id_externo"
        ).execute()

        return True

    except Exception as e:
        logger.warning(f"[Lovable] upsert_contrato falhou para {contrato_id_local}: {e}")
        return False


def add_historico(
    contrato_id_externo: str,
    status_anterior: Optional[str],
    status_novo: Optional[str],
    fonte: str,
    usuario_email: str = "sistema",
    kanban_anterior: Optional[str] = None,
    kanban_nova: Optional[str] = None,
    observacao: str = "",
) -> bool:
    """
    Registra uma linha no histórico de status no Lovable Cloud.
    Primeiro busca o UUID real do contrato pelo id_externo.
    """
    if not is_configured():
        return False

    try:
        client = _get_client()

        # Busca UUID real pelo id_externo
        res = client.table("contratos") \
            .select("id") \
            .eq("id_externo", contrato_id_externo) \
            .maybe_single() \
            .execute()

        if not res.data:
            logger.warning(f"[Lovable] Contrato não encontrado para histórico: {contrato_id_externo}")
            return False

        contrato_uuid = res.data["id"]

        hist = {
            "contrato_id":           contrato_uuid,
            "status_anterior":       status_anterior,
            "status_novo":           status_novo,
            "kanban_etapa_anterior": kanban_anterior,
            "kanban_etapa_nova":     kanban_nova,
            "fonte":                 fonte,
            "usuario_email":         usuario_email,
            "observacao":            observacao,
        }

        client.table("historico_status").insert(hist).execute()
        return True

    except Exception as e:
        logger.warning(f"[Lovable] add_historico falhou para {contrato_id_externo}: {e}")
        return False


def log_sync_start(fonte: str) -> Optional[str]:
    """
    Registra início de sincronização no Lovable Cloud.
    Retorna o ID do log (uuid string) para atualizar depois com log_sync_end.
    """
    if not is_configured():
        return None

    try:
        client = _get_client()
        res = client.table("sync_log").insert({
            "fonte":   fonte,
            "status":  "em_andamento",
            "iniciado_em": datetime.utcnow().isoformat(),
        }).execute()

        log_id = res.data[0]["id"] if res.data else None
        logger.info(f"[Lovable] sync_log iniciado: {fonte} (id={log_id})")
        return log_id

    except Exception as e:
        logger.warning(f"[Lovable] log_sync_start falhou: {e}")
        return None


def log_sync_end(
    log_id: Optional[str],
    status: str,              # "sucesso" | "erro"
    importados: int = 0,
    atualizados: int = 0,
    erro: Optional[str] = None,
) -> bool:
    """Atualiza o registro de sync_log com o resultado final."""
    if not is_configured() or not log_id:
        return False

    try:
        client = _get_client()
        client.table("sync_log").update({
            "status":               status,
            "finalizado_em":        datetime.utcnow().isoformat(),
            "contratos_importados": importados,
            "contratos_atualizados": atualizados,
            "erro_mensagem":        erro,
        }).eq("id", log_id).execute()

        return True

    except Exception as e:
        logger.warning(f"[Lovable] log_sync_end falhou: {e}")
        return False


def marcar_entregue(
    contrato_id_externo: str,
    data_entrega: datetime,
    usuario_email: str = "sistema",
) -> bool:
    """
    Marca um contrato como entregue no Lovable Cloud.
    Atualiza data_entrega_definitiva, status e kanban_etapa.
    """
    if not is_configured():
        return False

    try:
        client = _get_client()

        # Busca etapa atual para histórico
        res = client.table("contratos") \
            .select("id, kanban_etapa, status_atual") \
            .eq("id_externo", contrato_id_externo) \
            .maybe_single() \
            .execute()

        if not res.data:
            return False

        contrato_uuid   = res.data["id"]
        etapa_anterior  = res.data.get("kanban_etapa", "aguardando_cliente")
        status_anterior = res.data.get("status_atual", "")

        # Atualiza contrato
        client.table("contratos").update({
            "data_entrega_definitiva": data_entrega.isoformat(),
            "status_atual":   "Definitivo Entregue",
            "kanban_etapa":   "entregue",
            "ultima_atualizacao": datetime.utcnow().isoformat(),
        }).eq("id", contrato_uuid).execute()

        # Registra histórico
        client.table("historico_status").insert({
            "contrato_id":           contrato_uuid,
            "status_anterior":       status_anterior,
            "status_novo":           "Definitivo Entregue",
            "kanban_etapa_anterior": etapa_anterior,
            "kanban_etapa_nova":     "entregue",
            "fonte":                 "MANUAL",
            "usuario_email":         usuario_email,
        }).execute()

        logger.info(f"[Lovable] ✅ Contrato entregue: {contrato_id_externo}")
        return True

    except Exception as e:
        logger.warning(f"[Lovable] marcar_entregue falhou para {contrato_id_externo}: {e}")
        return False


def get_contratos_pendentes(fontes: Optional[list] = None) -> list:
    """
    Retorna contratos pendentes do Lovable Cloud.
    Útil para rodar validação de portais a partir dos dados do Lovable.

    fontes: lista de fontes para filtrar (ex: ['GWM', 'LM'])
            None = todas as fontes
    """
    if not is_configured():
        return []

    try:
        client = _get_client()

        query = client.table("contratos") \
            .select("*") \
            .is_("data_entrega_definitiva", "null")

        if fontes:
            query = query.in_("fonte", fontes)

        res = query.order("data_prevista_entrega", desc=False).execute()
        return res.data or []

    except Exception as e:
        logger.warning(f"[Lovable] get_contratos_pendentes falhou: {e}")
        return []


# ─────────────────────────────────────────────
# Push em lote (útil para sync inicial)
# ─────────────────────────────────────────────

def push_contratos_em_lote(contratos: list[tuple[dict, str]], batch_size: int = 50) -> tuple[int, int]:
    """
    Envia múltiplos contratos de uma vez para o Lovable Cloud.

    contratos: lista de tuplas (data_dict, contrato_id_local)
    batch_size: quantos enviar por chamada (máx recomendado: 100)

    Retorna: (total_sucesso, total_erro)
    """
    if not is_configured() or not contratos:
        return 0, 0

    try:
        client = _get_client()
    except Exception as e:
        logger.error(f"[Lovable] push_em_lote: cliente não disponível: {e}")
        return 0, len(contratos)

    ok_count = 0
    err_count = 0

    # Divide em batches
    for i in range(0, len(contratos), batch_size):
        batch = contratos[i:i + batch_size]
        payloads = []
        for data, cid in batch:
            try:
                payloads.append(_contrato_para_lovable(data, cid))
            except Exception as e:
                logger.warning(f"[Lovable] Erro ao converter {cid}: {e}")
                err_count += 1

        if not payloads:
            continue

        try:
            client.table("contratos").upsert(
                payloads,
                on_conflict="id_externo"
            ).execute()
            ok_count += len(payloads)
            logger.info(f"[Lovable] Lote {i // batch_size + 1}: {len(payloads)} contratos enviados")
        except Exception as e:
            logger.error(f"[Lovable] Erro no lote {i // batch_size + 1}: {e}")
            err_count += len(payloads)

    return ok_count, err_count
