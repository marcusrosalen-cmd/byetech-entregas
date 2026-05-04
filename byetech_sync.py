"""
╔══════════════════════════════════════════════════════════════════╗
║          BYETECH ENTREGAS — SYNC PARA LOVABLE CLOUD              ║
║                                                                  ║
║  Arquivo único. Roda no seu computador Windows.                  ║
║  Puxa dados do Metabase + Byetech CRM e envia para o Lovable.   ║
║                                                                  ║
║  CONFIGURAÇÃO (só fazer uma vez):                                ║
║    1. pip install supabase==2.10.0 httpx python-dotenv           ║
║    2. Edite as variáveis na seção CONFIG abaixo                  ║
║    3. python byetech_sync.py --teste                             ║
║    4. python byetech_sync.py --inicial   (1ª vez)               ║
║    5. python byetech_sync.py             (sync diário)           ║
╚══════════════════════════════════════════════════════════════════╝
"""

# ═══════════════════════════════════════════════════════════════════
#  CONFIG — preencha com seus dados do Lovable
#  (ou coloque num arquivo .env na mesma pasta)
# ═══════════════════════════════════════════════════════════════════

import os
from dotenv import load_dotenv
load_dotenv()

# Lovable Cloud → Settings → Database
LOVABLE_URL         = os.getenv("LOVABLE_URL", "")          # https://xxxx.supabase.co
LOVABLE_SERVICE_KEY = os.getenv("LOVABLE_SERVICE_KEY", "")  # eyJhbGc... (service_role)

# Metabase (sua instância)
METABASE_URL  = os.getenv("METABASE_URL",  "https://metabase.byecar.com.br")
METABASE_CARD = os.getenv("METABASE_CARD", "")   # ID do card público (número)

# Byetech CRM
BYETECH_URL      = os.getenv("BYETECH_URL",      "https://crm.byetech.pro")
BYETECH_EMAIL    = os.getenv("BYETECH_EMAIL",    "")
BYETECH_PASSWORD = os.getenv("BYETECH_PASSWORD", "")

# ═══════════════════════════════════════════════════════════════════
#  IMPORTS
# ═══════════════════════════════════════════════════════════════════

import sys
import asyncio
import argparse
import logging
import json
import re
from datetime import datetime, date, timedelta
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("byetech_sync.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("byetech")


# ═══════════════════════════════════════════════════════════════════
#  LOVABLE CLOUD — funções de escrita no banco
# ═══════════════════════════════════════════════════════════════════

_supabase_client = None

def _cliente_lovable():
    """Retorna o cliente Supabase (cria uma única vez)."""
    global _supabase_client
    if _supabase_client:
        return _supabase_client
    if not LOVABLE_URL or not LOVABLE_SERVICE_KEY:
        raise RuntimeError(
            "\n❌ LOVABLE_URL e LOVABLE_SERVICE_KEY não configurados!\n"
            "   Edite o arquivo .env ou a seção CONFIG deste script.\n"
            "   Pegue as credenciais em: Lovable → Settings → Database\n"
        )
    from supabase import create_client
    _supabase_client = create_client(LOVABLE_URL, LOVABLE_SERVICE_KEY)
    log.info("✅ Lovable Cloud conectado")
    return _supabase_client


def _etapa_kanban(status: str) -> str:
    """Converte status_atual para etapa do Kanban."""
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
    if dt is None:
        return None
    if isinstance(dt, (datetime, date)):
        return dt.isoformat()
    return str(dt)


def _id_contrato(fonte: str, id_externo: str, cpf: str) -> str:
    """ID único local: FONTE_CHAVE."""
    chave = id_externo or cpf or "unknown"
    return f"{fonte}_{chave}".upper()


def _montar_payload(c: dict, id_local: str) -> dict:
    """Monta o dict para inserir/atualizar no Lovable."""
    status = c.get("status_atual") or ""
    return {
        "id_externo":              id_local,
        "fonte":                   c.get("fonte") or "",
        "byetech_contrato_id":     c.get("byetech_contrato_id") or "",
        "cliente_nome":            c.get("cliente_nome") or "",
        "cliente_cpf_cnpj":        c.get("cliente_cpf_cnpj") or "",
        "cliente_email":           c.get("cliente_email") or "",
        "veiculo":                 c.get("veiculo") or "",
        "placa":                   c.get("placa") or "",
        "status_atual":            status,
        "kanban_etapa":            _etapa_kanban(status),
        "data_prevista_entrega":   _iso(c.get("data_prevista_entrega")),
        "data_entrega_definitiva": _iso(c.get("data_entrega_definitiva")),
        "data_venda":              _iso(c.get("data_venda")),
        "origem_dados":            c.get("origem_dados") or "SYNC",
        "ultima_sync":             datetime.utcnow().isoformat(),
        "ultima_atualizacao":      datetime.utcnow().isoformat(),
    }


def lovable_upsert(contrato: dict, id_local: str) -> bool:
    """Insere ou atualiza um contrato no Lovable Cloud."""
    try:
        payload = _montar_payload(contrato, id_local)
        _cliente_lovable().table("contratos").upsert(
            payload, on_conflict="id_externo"
        ).execute()
        return True
    except Exception as e:
        log.warning(f"[Lovable] upsert falhou ({id_local}): {e}")
        return False


def lovable_upsert_lote(contratos: list[tuple[dict, str]], batch: int = 100) -> tuple[int, int]:
    """Envia muitos contratos de uma vez. Retorna (ok, erros)."""
    ok_total = err_total = 0
    for i in range(0, len(contratos), batch):
        grupo    = contratos[i:i + batch]
        payloads = [_montar_payload(c, cid) for c, cid in grupo]
        try:
            _cliente_lovable().table("contratos").upsert(
                payloads, on_conflict="id_externo"
            ).execute()
            ok_total += len(payloads)
            log.info(f"[Lovable] lote {i//batch+1}: {len(payloads)} enviados")
        except Exception as e:
            log.error(f"[Lovable] lote {i//batch+1} falhou: {e}")
            err_total += len(payloads)
    return ok_total, err_total


def lovable_add_historico(id_local: str, status_ant: str, status_novo: str, fonte: str):
    """Registra mudança de status no histórico."""
    try:
        cli = _cliente_lovable()
        res = cli.table("contratos").select("id, kanban_etapa") \
            .eq("id_externo", id_local).maybe_single().execute()
        if not res.data:
            return
        cli.table("historico_status").insert({
            "contrato_id":           res.data["id"],
            "status_anterior":       status_ant,
            "status_novo":           status_novo,
            "kanban_etapa_anterior": res.data.get("kanban_etapa"),
            "kanban_etapa_nova":     _etapa_kanban(status_novo),
            "fonte":                 fonte,
            "usuario_email":         "sistema",
        }).execute()
    except Exception as e:
        log.debug(f"[Lovable] historico ignorado: {e}")


def lovable_log_sync(fonte: str) -> Optional[str]:
    """Registra início de sync. Retorna ID para atualizar depois."""
    try:
        res = _cliente_lovable().table("sync_log").insert({
            "fonte": fonte,
            "status": "em_andamento",
            "iniciado_em": datetime.utcnow().isoformat(),
        }).execute()
        return res.data[0]["id"] if res.data else None
    except Exception as e:
        log.debug(f"[Lovable] log_sync_start: {e}")
        return None


def lovable_log_fim(log_id: Optional[str], status: str,
                    importados: int = 0, atualizados: int = 0,
                    erro: Optional[str] = None):
    """Atualiza log de sync com resultado."""
    if not log_id:
        return
    try:
        _cliente_lovable().table("sync_log").update({
            "status":                status,
            "finalizado_em":         datetime.utcnow().isoformat(),
            "contratos_importados":  importados,
            "contratos_atualizados": atualizados,
            "erro_mensagem":         erro,
        }).eq("id", log_id).execute()
    except Exception as e:
        log.debug(f"[Lovable] log_sync_end: {e}")


def lovable_marcar_entregue(id_local: str, data_entrega: datetime,
                             usuario: str = "sistema") -> bool:
    """Marca contrato como entregue no Lovable."""
    try:
        cli = _cliente_lovable()
        res = cli.table("contratos").select("id, kanban_etapa, status_atual") \
            .eq("id_externo", id_local).maybe_single().execute()
        if not res.data:
            return False
        uuid = res.data["id"]
        cli.table("contratos").update({
            "data_entrega_definitiva": data_entrega.isoformat(),
            "status_atual":            "Definitivo Entregue",
            "kanban_etapa":            "entregue",
            "ultima_atualizacao":      datetime.utcnow().isoformat(),
        }).eq("id", uuid).execute()
        cli.table("historico_status").insert({
            "contrato_id":           uuid,
            "status_anterior":       res.data.get("status_atual", ""),
            "status_novo":           "Definitivo Entregue",
            "kanban_etapa_anterior": res.data.get("kanban_etapa", ""),
            "kanban_etapa_nova":     "entregue",
            "fonte":                 "MANUAL",
            "usuario_email":         usuario,
        }).execute()
        return True
    except Exception as e:
        log.warning(f"[Lovable] marcar_entregue({id_local}): {e}")
        return False


# ═══════════════════════════════════════════════════════════════════
#  METABASE — busca contratos
# ═══════════════════════════════════════════════════════════════════

async def metabase_buscar(data_ref: date) -> list[dict]:
    """
    Busca contratos no Metabase para uma data específica.
    Usa a API pública do card configurado em METABASE_CARD.
    """
    import httpx

    if not METABASE_CARD:
        log.warning("[Metabase] METABASE_CARD não configurado — pulando")
        return []

    url = f"{METABASE_URL}/api/public/card/{METABASE_CARD}/query/json"
    params = {}

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(url, params=params)

        if resp.status_code != 200:
            log.error(f"[Metabase] HTTP {resp.status_code}: {resp.text[:200]}")
            return []

        rows = resp.json()
        if not isinstance(rows, list):
            log.error(f"[Metabase] Resposta inesperada: {str(rows)[:200]}")
            return []

        contratos = []
        for row in rows:
            # Adapte os nomes das colunas conforme seu card do Metabase
            fonte_raw = str(row.get("Locadora", row.get("locadora", ""))).upper()
            fonte = _mapear_fonte(fonte_raw)

            id_ext = str(row.get("ID", row.get("id", row.get("Contrato", ""))))
            cpf    = _limpar_cpf(str(row.get("CPF", row.get("cpf", ""))))

            dp = _parse_data(
                row.get("Data Prevista", row.get("data_prevista", row.get("Prazo", "")))
            )
            dv = _parse_data(
                row.get("Data Venda", row.get("data_venda", row.get("Venda", "")))
            )

            contratos.append({
                "fonte":                  fonte,
                "id_externo":             id_ext,
                "cliente_nome":           str(row.get("Cliente", row.get("cliente", ""))),
                "cliente_cpf_cnpj":       cpf,
                "cliente_email":          str(row.get("Email", row.get("email", ""))),
                "veiculo":                str(row.get("Veículo", row.get("veiculo", row.get("Modelo", "")))),
                "placa":                  str(row.get("Placa", row.get("placa", ""))),
                "status_atual":           str(row.get("Status", row.get("status", ""))),
                "data_prevista_entrega":  dp,
                "data_venda":             dv,
                "origem_dados":           "METABASE",
            })

        log.info(f"[Metabase] {len(contratos)} contratos recebidos")
        return contratos

    except Exception as e:
        log.error(f"[Metabase] Erro: {e}")
        return []


def _mapear_fonte(locadora_raw: str) -> str:
    """Normaliza nome da locadora para o padrão do sistema."""
    l = locadora_raw.upper()
    if "UNIDAS"  in l: return "UNIDAS"
    if "GWM"     in l: return "GWM"
    if "SIGN"    in l: return "SIGN & DRIVE"
    if "DRIVE"   in l: return "SIGN & DRIVE"
    if "LM"      in l: return "LM"
    if "LOCALIZA"in l: return "LOCALIZA"
    if "MOVIDA"  in l: return "MOVIDA"
    if "ASSINE"  in l: return "LM"
    return locadora_raw or "OUTRO"


def _limpar_cpf(cpf: str) -> str:
    return re.sub(r"\D", "", cpf or "")


def _parse_data(valor) -> Optional[date]:
    """Tenta converter string para date."""
    if not valor:
        return None
    if isinstance(valor, (date, datetime)):
        return valor if isinstance(valor, date) else valor.date()
    s = str(valor).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return None


# ═══════════════════════════════════════════════════════════════════
#  BYETECH CRM — usa o scraper existente do projeto
# ═══════════════════════════════════════════════════════════════════

async def byetech_buscar() -> list[dict]:
    """
    Chama o scraper do Byetech CRM (já existente no projeto).
    Solicita 2FA no terminal quando necessário.
    """
    try:
        from app.scrapers.byetech_crm import scrape_contratos

        async def twofa_cb():
            print("\n🔐 O Byetech CRM está pedindo verificação em dois fatores.")
            code = input("   Digite o código 2FA: ").strip()
            return code

        contratos = await scrape_contratos(twofa_callback=twofa_cb)
        log.info(f"[Byetech] {len(contratos)} contratos recebidos")
        return contratos

    except ImportError:
        log.warning("[Byetech] Scraper não encontrado — certifique que está na pasta do projeto")
        return []
    except Exception as e:
        log.error(f"[Byetech] Erro no scrape: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════
#  TESTE DE CONEXÃO
# ═══════════════════════════════════════════════════════════════════

def cmd_teste():
    print("\n" + "═" * 55)
    print("  TESTE DE CONEXÃO — Lovable Cloud")
    print("═" * 55)

    checks = {
        "Configurado": bool(LOVABLE_URL and LOVABLE_SERVICE_KEY),
        "Conectado":   False,
        "Tabela OK":   False,
        "Upsert OK":   False,
        "Sync Log OK": False,
    }
    erro = None

    if not checks["Configurado"]:
        print("\n  ❌ LOVABLE_URL ou LOVABLE_SERVICE_KEY vazios!")
        print("\n  Adicione ao .env (na mesma pasta deste script):")
        print("    LOVABLE_URL=https://xxxx.supabase.co")
        print("    LOVABLE_SERVICE_KEY=eyJhbGc...")
        print("\n  Ou edite a seção CONFIG no topo deste arquivo.")
        sys.exit(1)

    try:
        cli = _cliente_lovable()
        checks["Conectado"] = True

        cli.table("contratos").select("id").limit(1).execute()
        checks["Tabela OK"] = True

        # Testa upsert com registro temporário
        cli.table("contratos").upsert(
            {"id_externo": "__teste__", "fonte": "TEST", "cliente_nome": "TESTE"},
            on_conflict="id_externo"
        ).execute()
        cli.table("contratos").delete().eq("id_externo", "__teste__").execute()
        checks["Upsert OK"] = True

        cli.table("sync_log").select("id").limit(1).execute()
        checks["Sync Log OK"] = True

    except Exception as e:
        erro = str(e)

    print()
    for nome, ok in checks.items():
        print(f"  {'✅' if ok else '❌'} {nome}")

    if erro:
        print(f"\n  Erro: {erro}")

    if not checks["Upsert OK"]:
        print("\n  ⚠️  Execute no SQL Editor do Lovable/Supabase:")
        print("     ALTER TABLE contratos")
        print("       ADD CONSTRAINT contratos_id_externo_key UNIQUE (id_externo);")

    if all(checks.values()):
        print(f"\n  ✅ Tudo OK! Próximo passo:")
        print("     python byetech_sync.py --inicial   (1ª vez)")
        print("     python byetech_sync.py             (diário)\n")
    else:
        print("\n  Corrija os erros antes de sincronizar.\n")
        sys.exit(1)


# ═══════════════════════════════════════════════════════════════════
#  SYNC INICIAL — exporta SQLite → Lovable
# ═══════════════════════════════════════════════════════════════════

async def cmd_inicial():
    """Exporta todos os contratos do SQLite local para o Lovable Cloud."""
    print("\n⚠️  Sync inicial: vai enviar TODOS os contratos existentes para o Lovable.")
    ok = input("Confirmar? (s/n): ").strip().lower()
    if ok != "s":
        print("Cancelado.")
        return

    log_id = lovable_log_sync("INICIAL")

    try:
        # Tenta usar SQLite local do projeto
        sys.path.insert(0, os.path.dirname(__file__))
        from app.database import init_db, SessionLocal, Contrato
        from sqlalchemy import select

        await init_db()
        async with SessionLocal() as session:
            result = await session.execute(select(Contrato))
            todos  = result.scalars().all()

        print(f"\n📦 {len(todos)} contratos encontrados. Enviando...\n")

        lote = []
        for c in todos:
            data = {
                "fonte":                   c.fonte,
                "id_externo":              c.id_externo,
                "byetech_contrato_id":     c.byetech_contrato_id,
                "cliente_nome":            c.cliente_nome,
                "cliente_cpf_cnpj":        c.cliente_cpf_cnpj,
                "cliente_email":           c.cliente_email,
                "veiculo":                 c.veiculo,
                "placa":                   c.placa,
                "status_atual":            c.status_atual,
                "data_prevista_entrega":   c.data_prevista_entrega,
                "data_entrega_definitiva": c.data_entrega_definitiva,
                "data_venda":              c.data_venda,
                "origem_dados":            "METABASE",
            }
            lote.append((data, c.id))

        enviados, erros = lovable_upsert_lote(lote, batch=100)
        lovable_log_fim(log_id, "sucesso" if erros == 0 else "erro",
                        importados=enviados)

        print(f"\n{'✅' if erros == 0 else '⚠️ '} Concluído!")
        print(f"   Enviados : {enviados}")
        print(f"   Erros    : {erros}")
        print("\n▶ Próximo: python byetech_sync.py\n")

    except ImportError:
        print("\n⚠️  SQLite local não encontrado.")
        print("   Rode em: python byetech_sync.py --fonte metabase\n")
        lovable_log_fim(log_id, "erro", erro="SQLite não encontrado")


# ═══════════════════════════════════════════════════════════════════
#  SYNC PRINCIPAL
# ═══════════════════════════════════════════════════════════════════

async def sync_metabase(full: bool = False) -> dict:
    """Busca contratos no Metabase e envia para o Lovable."""
    log_id = lovable_log_sync("METABASE")
    log.info(f"[Metabase] Buscando contratos {'(completo)' if full else '(hoje + ontem)'}...")

    # Tenta usar o scraper existente do projeto primeiro
    try:
        from app.services.sync_service import run_metabase_sync
        resultado = await run_metabase_sync(full=full)
        importados = resultado.get("importados", 0)

        # Após salvar no SQLite, espelha para o Lovable
        from app.database import SessionLocal, Contrato
        from sqlalchemy import select
        async with SessionLocal() as session:
            res = await session.execute(select(Contrato))
            todos = res.scalars().all()
        lote = [
            ({"fonte": c.fonte, "id_externo": c.id_externo,
              "byetech_contrato_id": c.byetech_contrato_id,
              "cliente_nome": c.cliente_nome, "cliente_cpf_cnpj": c.cliente_cpf_cnpj,
              "cliente_email": c.cliente_email, "veiculo": c.veiculo, "placa": c.placa,
              "status_atual": c.status_atual,
              "data_prevista_entrega": c.data_prevista_entrega,
              "data_entrega_definitiva": c.data_entrega_definitiva,
              "data_venda": c.data_venda, "origem_dados": "METABASE"}, c.id)
            for c in todos
        ]
        ok, err = lovable_upsert_lote(lote)
        lovable_log_fim(log_id, "sucesso", importados=ok)
        log.info(f"[Metabase] ✅ {ok} contratos no Lovable")
        return {"ok": True, "importados": ok}

    except ImportError:
        pass  # scraper não disponível, usa API direta

    # Fallback: API pública do Metabase
    hoje    = date.today()
    ontem   = hoje - timedelta(days=1)
    datas   = [ontem, hoje] if not full else [hoje - timedelta(days=i) for i in range(30)]

    todos: dict[str, dict] = {}
    for dt in datas:
        contratos = await metabase_buscar(dt)
        for c in contratos:
            key = c.get("id_externo") or c.get("cliente_cpf_cnpj") or id(c)
            todos[key] = c

    lote = []
    for c in todos.values():
        cid = _id_contrato(c.get("fonte", ""), c.get("id_externo", ""), c.get("cliente_cpf_cnpj", ""))
        lote.append((c, cid))

    ok, err = lovable_upsert_lote(lote)
    lovable_log_fim(log_id, "sucesso" if err == 0 else "erro", importados=ok, erro=None)
    log.info(f"[Metabase] ✅ {ok} enviados | ❌ {err} erros")
    return {"ok": True, "importados": ok, "erros": err}


async def sync_byetech() -> dict:
    """Busca contratos no Byetech CRM e envia para o Lovable."""
    log_id  = lovable_log_sync("BYETECH")
    contratos = await byetech_buscar()

    if not contratos:
        lovable_log_fim(log_id, "sucesso", importados=0)
        return {"ok": True, "importados": 0}

    lote = []
    for c in contratos:
        c["origem_dados"] = "BYETECH_CRM"
        id_ext = c.get("id_externo") or c.get("byetech_contrato_id", "")
        cid    = _id_contrato(c.get("fonte", "BYETECH"), id_ext, c.get("cliente_cpf_cnpj", ""))
        lote.append((c, cid))

    ok, err = lovable_upsert_lote(lote)
    lovable_log_fim(log_id, "sucesso" if err == 0 else "erro",
                    importados=ok, atualizados=ok)
    log.info(f"[Byetech] ✅ {ok} enviados | ❌ {err} erros")
    return {"ok": True, "importados": ok, "erros": err}


async def sync_completo(full_metabase: bool = False) -> dict:
    """Roda Metabase + Byetech em sequência."""
    print("\n" + "═" * 45)
    print(f"  SYNC COMPLETO — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("═" * 45)

    inicio = datetime.now()

    print("\n📊 [1/2] Metabase...")
    r_meta = await sync_metabase(full=full_metabase)
    _imprimir("Metabase", r_meta)

    print("\n🏢 [2/2] Byetech CRM...")
    r_byte = await sync_byetech()
    _imprimir("Byetech", r_byte)

    duracao = int((datetime.now() - inicio).total_seconds())
    print(f"\n{'='*45}")
    print(f"✅ Concluído em {duracao}s")
    print(f"{'='*45}\n")

    return {"metabase": r_meta, "byetech": r_byte}


def _imprimir(nome: str, res: dict):
    if res.get("ok"):
        imp = res.get("importados", 0)
        err = res.get("erros", 0)
        print(f"   ✅ {nome}: {imp} importados" + (f" | {err} erros" if err else ""))
    else:
        print(f"   ❌ {nome}: {res.get('erro', 'erro')}")


# ═══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Byetech Entregas — Sync para Lovable Cloud",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python byetech_sync.py                  → sync completo (Metabase + Byetech)
  python byetech_sync.py --teste          → verifica conexão com Lovable
  python byetech_sync.py --inicial        → 1ª vez: exporta SQLite → Lovable
  python byetech_sync.py --fonte metabase → só Metabase
  python byetech_sync.py --fonte byetech  → só Byetech CRM
  python byetech_sync.py --full           → Metabase completo (todos os ativos)
        """
    )
    parser.add_argument("--teste",   action="store_true", help="Testa conexão com Lovable")
    parser.add_argument("--inicial", action="store_true", help="Exporta SQLite → Lovable (1ª vez)")
    parser.add_argument("--full",    action="store_true", help="Metabase completo")
    parser.add_argument("--fonte",
        choices=["metabase", "byetech", "completo"],
        default="completo",
        help="Qual fonte sincronizar (padrão: completo)"
    )
    args = parser.parse_args()

    if args.teste:
        cmd_teste()

    elif args.inicial:
        asyncio.run(cmd_inicial())

    elif args.fonte == "metabase":
        asyncio.run(sync_metabase(full=args.full))

    elif args.fonte == "byetech":
        asyncio.run(sync_byetech())

    else:
        asyncio.run(sync_completo(full_metabase=args.full))


if __name__ == "__main__":
    main()
