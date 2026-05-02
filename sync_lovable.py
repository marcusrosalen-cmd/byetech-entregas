"""
sync_lovable.py — Script principal de sincronização para o Lovable Cloud.

Roda de forma standalone (sem precisar do servidor FastAPI).
Ideal para agendar no Windows Task Scheduler ou rodar manualmente.

Uso:
    python sync_lovable.py                  # sync completo (Metabase + Byetech)
    python sync_lovable.py --fonte metabase # só Metabase
    python sync_lovable.py --fonte byetech  # só Byetech CRM
    python sync_lovable.py --inicial        # importa TODOS os contratos ativos (1ª vez)
    python sync_lovable.py --teste          # verifica conexão sem sincronizar

Fluxo:
    1. Busca contratos no Metabase (fonte de verdade para dados novos)
    2. Enriquece com dados do Byetech CRM (nome, CPF, e-mail)
    3. Faz upsert no Lovable Cloud via Supabase API
    4. Registra log de sync no Lovable (tabela sync_log)
    5. Loga resultado no console e em sync_lovable.log
"""

import sys
import os
import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Garante que o pacote 'app' seja encontrado
sys.path.insert(0, str(Path(__file__).parent))

# Carrega .env
from dotenv import load_dotenv
load_dotenv()

# Configura logging para console + arquivo
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("sync_lovable.log", encoding="utf-8"),
    ]
)
logger = logging.getLogger("sync_lovable")


# ─────────────────────────────────────────────
# Verificação de conexão
# ─────────────────────────────────────────────

def cmd_teste():
    """Verifica se o Lovable Cloud está acessível."""
    from app.services.lovable_client import is_configured, _get_client, LOVABLE_URL

    print("\n=== Teste de Conexão Lovable Cloud ===\n")

    if not is_configured():
        print("❌ LOVABLE_URL e LOVABLE_SERVICE_KEY não configurados no .env")
        print("\nAdicione ao seu .env:")
        print("  LOVABLE_URL=https://xxxx.supabase.co")
        print("  LOVABLE_SERVICE_KEY=eyJhbGc...")
        print("\nPegue essas credenciais em: Lovable → Settings → Database")
        sys.exit(1)

    print(f"🔗 URL: {LOVABLE_URL}")

    try:
        client = _get_client()
        res = client.table("contratos").select("id").limit(1).execute()
        n = len(res.data)
        print(f"✅ Conexão OK — tabela 'contratos' acessível ({n} linha(s) de teste)")
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        print("\nVerifique:")
        print("  1. A URL e a chave service_role no .env")
        print("  2. Se o projeto Lovable está publicado")
        print("  3. Se a tabela 'contratos' foi criada (rode o Lovable primeiro)")
        sys.exit(1)

    # Testa sync_log
    try:
        client.table("sync_log").select("id").limit(1).execute()
        print("✅ Tabela 'sync_log' acessível")
    except Exception:
        print("⚠️  Tabela 'sync_log' não encontrada — crie pelo Lovable primeiro")

    # Testa constraint única (necessária para upsert)
    try:
        client.table("contratos").upsert(
            {"id_externo": "__test_connection__", "fonte": "TEST", "cliente_nome": "TESTE"},
            on_conflict="id_externo"
        ).execute()
        # Limpa o registro de teste
        client.table("contratos").delete().eq("id_externo", "__test_connection__").execute()
        print("✅ Constraint UNIQUE em id_externo configurada corretamente")
    except Exception as e:
        print(f"⚠️  Constraint UNIQUE em id_externo pode não estar configurada: {e}")
        print("   Execute no SQL Editor do Lovable/Supabase:")
        print("   ALTER TABLE contratos ADD CONSTRAINT contratos_id_externo_unique UNIQUE (id_externo);")

    print("\n✅ Tudo pronto! Rode 'python sync_lovable.py' para sincronizar.\n")


# ─────────────────────────────────────────────
# Sync via Metabase
# ─────────────────────────────────────────────

async def sync_metabase(full: bool = False) -> dict:
    """
    Busca contratos no Metabase e envia para o Lovable Cloud.

    full=True  → busca todos os ativos (bootstrap inicial)
    full=False → apenas contratos dos últimos 2 dias (sync diário)
    """
    from app.services.lovable_client import (
        log_sync_start, log_sync_end, push_contratos_em_lote, upsert_contrato
    )
    from app.database import init_db, SessionLocal
    from app.services.sync_service import _upsert_contrato, _contrato_id
    from sqlalchemy import select
    from app.database import Contrato

    log_id = log_sync_start("METABASE")
    logger.info(f"[Metabase] Iniciando sync {'COMPLETO' if full else 'diário'}...")

    try:
        from app.scrapers.metabase import fetch_contratos_metabase
        contratos_raw = await fetch_contratos_metabase(full=full)
        logger.info(f"[Metabase] {len(contratos_raw)} contratos recebidos do Metabase")
    except Exception as e:
        logger.error(f"[Metabase] Erro ao buscar dados: {e}")
        log_sync_end(log_id, "erro", erro=str(e))
        return {"ok": False, "erro": str(e)}

    if not contratos_raw:
        log_sync_end(log_id, "sucesso", importados=0, atualizados=0)
        return {"ok": True, "importados": 0, "atualizados": 0}

    # Salva no SQLite local primeiro (necessário para outros scrapers)
    await init_db()
    importados_sqlite = 0
    async with SessionLocal() as session:
        for data in contratos_raw:
            try:
                await _upsert_contrato(session, data)
                importados_sqlite += 1
            except Exception as e:
                logger.warning(f"[Metabase→SQLite] Erro: {e}")
        await session.commit()

    logger.info(f"[Metabase→SQLite] {importados_sqlite} contratos salvos localmente")

    # Prepara lote para Lovable Cloud
    lote = []
    async with SessionLocal() as session:
        for data in contratos_raw:
            fonte = data.get("fonte", "")
            id_externo = data.get("id_externo", "")
            cpf = data.get("cliente_cpf_cnpj", "")
            cid = _contrato_id(fonte, id_externo, cpf)
            lote.append((data, cid))

    ok_count, err_count = push_contratos_em_lote(lote)
    logger.info(f"[Metabase→Lovable] {ok_count} enviados, {err_count} erros")

    log_sync_end(log_id, "sucesso", importados=ok_count, atualizados=ok_count)
    return {"ok": True, "importados": ok_count, "erros": err_count}


# ─────────────────────────────────────────────
# Sync via Byetech CRM
# ─────────────────────────────────────────────

async def sync_byetech() -> dict:
    """
    Busca contratos pendentes no Byetech CRM e envia para o Lovable Cloud.
    Enriquece os contratos já existentes com dados do CRM (nome, CPF, e-mail).
    """
    from app.services.lovable_client import (
        log_sync_start, log_sync_end, push_contratos_em_lote
    )
    from app.database import init_db, SessionLocal
    from app.services.sync_service import _upsert_contrato, _contrato_id

    log_id = log_sync_start("BYETECH")
    logger.info("[Byetech] Iniciando sync...")

    try:
        from app.scrapers.byetech_crm import fetch_contratos_byetech
        contratos_raw = await fetch_contratos_byetech()
        logger.info(f"[Byetech] {len(contratos_raw)} contratos recebidos")
    except Exception as e:
        logger.error(f"[Byetech] Erro ao buscar dados: {e}")
        log_sync_end(log_id, "erro", erro=str(e))
        return {"ok": False, "erro": str(e)}

    if not contratos_raw:
        log_sync_end(log_id, "sucesso", importados=0)
        return {"ok": True, "importados": 0}

    # Salva no SQLite local
    await init_db()
    async with SessionLocal() as session:
        for data in contratos_raw:
            try:
                await _upsert_contrato(session, data)
            except Exception as e:
                logger.warning(f"[Byetech→SQLite] Erro: {e}")
        await session.commit()

    # Envia para Lovable
    lote = []
    for data in contratos_raw:
        fonte = data.get("fonte", "BYETECH")
        id_externo = data.get("id_externo", "")
        cpf = data.get("cliente_cpf_cnpj", "")
        cid = _contrato_id(fonte, id_externo, cpf)
        data["origem_dados"] = "BYETECH_CRM"
        lote.append((data, cid))

    ok_count, err_count = push_contratos_em_lote(lote)
    logger.info(f"[Byetech→Lovable] {ok_count} enviados, {err_count} erros")

    log_sync_end(log_id, "sucesso", importados=ok_count, atualizados=ok_count)
    return {"ok": True, "importados": ok_count, "erros": err_count}


# ─────────────────────────────────────────────
# Sync inicial (bootstrap)
# ─────────────────────────────────────────────

async def sync_inicial():
    """
    Importa TODOS os contratos ativos do SQLite local para o Lovable Cloud.
    Use apenas na primeira vez, para popular o Lovable com dados existentes.
    """
    from app.services.lovable_client import push_contratos_em_lote, log_sync_start, log_sync_end
    from app.database import init_db, SessionLocal, Contrato
    from sqlalchemy import select

    log_id = log_sync_start("INICIAL")
    logger.info("[Inicial] Exportando contratos do SQLite local para o Lovable...")

    await init_db()

    async with SessionLocal() as session:
        result = await session.execute(select(Contrato))
        todos = result.scalars().all()

    logger.info(f"[Inicial] {len(todos)} contratos encontrados no SQLite local")

    lote = []
    for c in todos:
        data = {
            "fonte":              c.fonte,
            "id_externo":         c.id_externo,
            "byetech_contrato_id": c.byetech_contrato_id,
            "cliente_nome":       c.cliente_nome,
            "cliente_cpf_cnpj":   c.cliente_cpf_cnpj,
            "cliente_email":      c.cliente_email,
            "veiculo":            c.veiculo,
            "placa":              c.placa,
            "status_atual":       c.status_atual,
            "data_prevista_entrega": c.data_prevista_entrega,
            "data_entrega_definitiva": c.data_entrega_definitiva,
            "data_venda":         c.data_venda,
            "origem_dados":       "METABASE",
        }
        lote.append((data, c.id))

    ok_count, err_count = push_contratos_em_lote(lote, batch_size=100)
    logger.info(f"[Inicial] ✅ {ok_count} enviados | ❌ {err_count} erros")

    log_sync_end(log_id, "sucesso", importados=ok_count, atualizados=ok_count)
    print(f"\n✅ Sync inicial concluído: {ok_count} contratos enviados para o Lovable!\n")


# ─────────────────────────────────────────────
# Sync completo (Metabase + Byetech)
# ─────────────────────────────────────────────

async def sync_completo():
    """Roda Metabase + Byetech em sequência."""
    logger.info("═══ SYNC COMPLETO INICIADO ═══")
    inicio = datetime.now()

    # Metabase primeiro (dados base)
    res_meta = await sync_metabase(full=False)
    logger.info(f"[Metabase] Resultado: {res_meta}")

    # Byetech enriquece com dados do cliente
    res_byte = await sync_byetech()
    logger.info(f"[Byetech] Resultado: {res_byte}")

    duracao = (datetime.now() - inicio).seconds
    logger.info(f"═══ SYNC COMPLETO FINALIZADO em {duracao}s ═══")

    return {
        "metabase": res_meta,
        "byetech":  res_byte,
        "duracao_s": duracao,
    }


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sincroniza dados para o Lovable Cloud (Byetech Entregas)"
    )
    parser.add_argument(
        "--fonte",
        choices=["metabase", "byetech", "completo"],
        default="completo",
        help="Qual fonte sincronizar (padrão: completo)",
    )
    parser.add_argument(
        "--inicial",
        action="store_true",
        help="Importa TODOS os contratos do SQLite local (use apenas na 1ª vez)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Busca todos os contratos ativos no Metabase (não só os recentes)",
    )
    parser.add_argument(
        "--teste",
        action="store_true",
        help="Testa a conexão com o Lovable Cloud sem sincronizar",
    )
    args = parser.parse_args()

    # Teste de conexão
    if args.teste:
        cmd_teste()
        return

    # Sync inicial (bootstrap)
    if args.inicial:
        print("\n⚠️  Sync inicial: isso vai enviar TODOS os contratos do SQLite para o Lovable.")
        confirma = input("Confirmar? (s/n): ").strip().lower()
        if confirma != "s":
            print("Cancelado.")
            return
        asyncio.run(sync_inicial())
        return

    # Sync normal
    print(f"\n🔄 Iniciando sync [{args.fonte}] às {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")

    if args.fonte == "metabase":
        resultado = asyncio.run(sync_metabase(full=args.full))
    elif args.fonte == "byetech":
        resultado = asyncio.run(sync_byetech())
    else:
        resultado = asyncio.run(sync_completo())

    print(f"\n✅ Sync concluído: {resultado}\n")


if __name__ == "__main__":
    main()
