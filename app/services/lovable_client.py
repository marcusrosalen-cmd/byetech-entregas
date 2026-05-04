"""
╔══════════════════════════════════════════════════════════════╗
║          LOVABLE CLOUD — CONECTOR PYTHON                     ║
║                                                              ║
║  Espelha todos os dados do SQLite local para o Lovable Cloud ║
║  (banco Supabase gerenciado pelo Lovable) em tempo real.     ║
║                                                              ║
║  Configure no .env:                                          ║
║    LOVABLE_URL=https://xxxx.supabase.co                      ║
║    LOVABLE_SERVICE_KEY=eyJhbGc...  (service_role key)        ║
║                                                              ║
║  IMPORTANTE — execute UMA VEZ no SQL Editor do Lovable:      ║
║    ALTER TABLE contratos                                     ║
║      ADD CONSTRAINT contratos_id_externo_key                 ║
║      UNIQUE (id_externo);                                    ║
╚══════════════════════════════════════════════════════════════╝
"""

import logging
import os
from datetime import datetime, date
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("lovable")

LOVABLE_URL         = os.getenv("LOVABLE_URL", "")
LOVABLE_SERVICE_KEY = os.getenv("LOVABLE_SERVICE_KEY", "")

_client = None  # singleton lazy


# ─── Conexão ────────────────────────────────────────────────

def is_configured() -> bool:
    """True se as credenciais estão no .env."""
    return bool(LOVABLE_URL and LOVABLE_SERVICE_KEY)


def get_client():
    """Retorna cliente Supabase, criando na primeira chamada."""
    global _client
    if _client:
        return _client
    if not is_configured():
        raise RuntimeError(
            "LOVABLE_URL e LOVABLE_SERVICE_KEY não configurados no .env\n"
            "Pegue em: Lovable → Settings → Database"
        )
    try:
        from supabase import create_client
        _client = create_client(LOVABLE_URL, LOVABLE_SERVICE_KEY)
        logger.info("✅ Lovable Cloud conectado")
        return _client
    except ImportError:
        raise RuntimeError("Execute: pip install supabase==2.10.0")


# ─── Mapeamento de campos ────────────────────────────────────

def _etapa_kanban(status: str) -> str:
    """Converte status_atual para etapa do Kanban no Lovable."""
    if not status:
        return "faturamento"
    s = status.lower()
    if any(x in s for x in ["faturad", "fatura"]):
        return "faturamento"
    if any(x in s for x in ["saiu", "saída", "saida", "fabric"]):
        return "saida_fabrica"
    if any(x in s for x in ["transport", "trânsito", "transito", "rota"]):
        return "transporte"
    if any(x in s for x in ["disponível", "disponivel", "loja", "estoque", "chegou"]):
        return "disponivel_loja"
    if any(x in s for x in ["aguardando", "retirada"]):
        return "aguardando_cliente"
    if any(x in s for x in ["entregue", "definitivo"]):
        return "entregue"
    return "faturamento"


def _iso(dt) -> Optional[str]:
    """datetime/date → string ISO8601."""
    if dt is None:
        return None
    if isinstance(dt, (datetime, date)):
        return dt.isoformat()
    return str(dt)


def _montar_payload(data: dict, contrato_id_local: str) -> dict:
    """
    Converte dict interno do sync_service para o formato
    da tabela 'contratos' do Lovable Cloud.

    contrato_id_local = ID string local (ex: 'GWM_123456')
    → salvo em id_externo para garantir upsert único
    """
    status = data.get("status_atual") or ""
    dp     = data.get("data_prevista_entrega")

    return {
        "id_externo":            contrato_id_local,
        "fonte":                 data.get("fonte", ""),
        "byetech_contrato_id":   data.get("byetech_contrato_id") or "",
        "cliente_nome":          data.get("cliente_nome") or "",
        "cliente_cpf_cnpj":      data.get("cliente_cpf_cnpj") or "",
        "cliente_email":         data.get("cliente_email") or "",
        "veiculo":               data.get("veiculo") or "",
        "placa":                 data.get("placa") or "",
        "status_atual":          status,
        "kanban_etapa":          _etapa_kanban(status),
        "data_prevista_entrega":   _iso(dp),
        "data_entrega_definitiva": _iso(data.get("data_entrega_definitiva")),
        "data_venda":              _iso(data.get("data_venda")),
        "origem_dados":          data.get("origem_dados") or "SYNC",
        "ultima_sync":           datetime.utcnow().isoformat(),
        "ultima_atualizacao":    datetime.utcnow().isoformat(),
    }


# ─── Operações individuais ───────────────────────────────────

def upsert_contrato(data: dict, contrato_id_local: str) -> bool:
    """
    Insere ou atualiza um contrato no Lovable Cloud.
    Chamado automaticamente pelo sync_service após salvar no SQLite.
    Erros são logados e nunca propagados — SQLite é o banco primário.
    """
    if not is_configured():
        return False
    try:
        payload = _montar_payload(data, contrato_id_local)
        get_client().table("contratos").upsert(
            payload, on_conflict="id_externo"
        ).execute()
        return True
    except Exception as e:
        logger.warning(f"[Lovable] upsert_contrato({contrato_id_local}): {e}")
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
    """Registra mudança de status no histórico do Lovable."""
    if not is_configured():
        return False
    try:
        client = get_client()
        # Busca UUID real pelo id_externo
        res = client.table("contratos") \
            .select("id") \
            .eq("id_externo", contrato_id_externo) \
            .maybe_single() \
            .execute()
        if not res.data:
            return False

        client.table("historico_status").insert({
            "contrato_id":           res.data["id"],
            "status_anterior":       status_anterior,
            "status_novo":           status_novo,
            "kanban_etapa_anterior": kanban_anterior,
            "kanban_etapa_nova":     kanban_nova,
            "fonte":                 fonte,
            "usuario_email":         usuario_email,
            "observacao":            observacao,
        }).execute()
        return True
    except Exception as e:
        logger.warning(f"[Lovable] add_historico({contrato_id_externo}): {e}")
        return False


def marcar_entregue(
    contrato_id_externo: str,
    data_entrega: datetime,
    usuario_email: str = "sistema",
) -> bool:
    """Marca contrato como entregue no Lovable Cloud."""
    if not is_configured():
        return False
    try:
        client = get_client()
        res = client.table("contratos") \
            .select("id, kanban_etapa, status_atual") \
            .eq("id_externo", contrato_id_externo) \
            .maybe_single() \
            .execute()
        if not res.data:
            return False

        uuid           = res.data["id"]
        etapa_anterior = res.data.get("kanban_etapa", "aguardando_cliente")
        status_ant     = res.data.get("status_atual", "")

        client.table("contratos").update({
            "data_entrega_definitiva": data_entrega.isoformat(),
            "status_atual":            "Definitivo Entregue",
            "kanban_etapa":            "entregue",
            "ultima_atualizacao":      datetime.utcnow().isoformat(),
        }).eq("id", uuid).execute()

        client.table("historico_status").insert({
            "contrato_id":           uuid,
            "status_anterior":       status_ant,
            "status_novo":           "Definitivo Entregue",
            "kanban_etapa_anterior": etapa_anterior,
            "kanban_etapa_nova":     "entregue",
            "fonte":                 "MANUAL",
            "usuario_email":         usuario_email,
        }).execute()

        logger.info(f"[Lovable] ✅ Entregue: {contrato_id_externo}")
        return True
    except Exception as e:
        logger.warning(f"[Lovable] marcar_entregue({contrato_id_externo}): {e}")
        return False


# ─── Log de sincronização ────────────────────────────────────

def log_sync_start(fonte: str) -> Optional[str]:
    """Registra início de sync. Retorna ID do log para atualizar depois."""
    if not is_configured():
        return None
    try:
        res = get_client().table("sync_log").insert({
            "fonte":       fonte,
            "status":      "em_andamento",
            "iniciado_em": datetime.utcnow().isoformat(),
        }).execute()
        return res.data[0]["id"] if res.data else None
    except Exception as e:
        logger.warning(f"[Lovable] log_sync_start: {e}")
        return None


def log_sync_end(
    log_id: Optional[str],
    status: str,          # "sucesso" | "erro"
    importados: int = 0,
    atualizados: int = 0,
    erro: Optional[str] = None,
) -> bool:
    """Atualiza o log de sync com resultado final."""
    if not is_configured() or not log_id:
        return False
    try:
        get_client().table("sync_log").update({
            "status":                status,
            "finalizado_em":         datetime.utcnow().isoformat(),
            "contratos_importados":  importados,
            "contratos_atualizados": atualizados,
            "erro_mensagem":         erro,
        }).eq("id", log_id).execute()
        return True
    except Exception as e:
        logger.warning(f"[Lovable] log_sync_end: {e}")
        return False


# ─── Push em lote ────────────────────────────────────────────

def push_em_lote(contratos: list[tuple[dict, str]], batch: int = 100) -> tuple[int, int]:
    """
    Envia múltiplos contratos de uma vez.
    contratos: lista de (data_dict, contrato_id_local)
    Retorna: (ok, erros)
    """
    if not is_configured() or not contratos:
        return 0, 0
    try:
        client = get_client()
    except Exception as e:
        logger.error(f"[Lovable] push_em_lote: cliente indisponível: {e}")
        return 0, len(contratos)

    ok_total = err_total = 0
    for i in range(0, len(contratos), batch):
        grupo = contratos[i:i + batch]
        payloads = []
        for data, cid in grupo:
            try:
                payloads.append(_montar_payload(data, cid))
            except Exception as e:
                logger.warning(f"[Lovable] conversão {cid}: {e}")
                err_total += 1
        if not payloads:
            continue
        try:
            client.table("contratos").upsert(
                payloads, on_conflict="id_externo"
            ).execute()
            ok_total += len(payloads)
            logger.info(f"[Lovable] lote {i//batch+1}: {len(payloads)} enviados")
        except Exception as e:
            logger.error(f"[Lovable] lote {i//batch+1} falhou: {e}")
            err_total += len(payloads)

    return ok_total, err_total


# ─── Leitura (usada pelo sync_lovable.py) ───────────────────

def get_contratos_pendentes(fontes: Optional[list] = None) -> list:
    """Retorna contratos pendentes do Lovable Cloud."""
    if not is_configured():
        return []
    try:
        q = get_client().table("contratos") \
            .select("*") \
            .is_("data_entrega_definitiva", "null")
        if fontes:
            q = q.in_("fonte", fontes)
        return q.order("data_prevista_entrega", desc=False).execute().data or []
    except Exception as e:
        logger.warning(f"[Lovable] get_contratos_pendentes: {e}")
        return []


def testar_conexao() -> dict:
    """
    Testa a conexão com o Lovable Cloud.
    Retorna dict com resultado de cada verificação.
    """
    resultado = {
        "configurado":  is_configured(),
        "conectado":    False,
        "tabela_ok":    False,
        "upsert_ok":    False,
        "sync_log_ok":  False,
        "erro":         None,
    }
    if not resultado["configurado"]:
        resultado["erro"] = "LOVABLE_URL ou LOVABLE_SERVICE_KEY ausente no .env"
        return resultado
    try:
        client = get_client()
        resultado["conectado"] = True

        # Testa tabela contratos
        client.table("contratos").select("id").limit(1).execute()
        resultado["tabela_ok"] = True

        # Testa upsert (constraint única)
        client.table("contratos").upsert(
            {"id_externo": "__teste_conexao__", "fonte": "TEST", "cliente_nome": "TESTE"},
            on_conflict="id_externo"
        ).execute()
        client.table("contratos").delete() \
            .eq("id_externo", "__teste_conexao__").execute()
        resultado["upsert_ok"] = True

        # Testa sync_log
        client.table("sync_log").select("id").limit(1).execute()
        resultado["sync_log_ok"] = True

    except Exception as e:
        resultado["erro"] = str(e)

    return resultado
