"""
╔══════════════════════════════════════════════════════════════════╗
║   sync_lovable.py — Script de Sincronização Byetech → Lovable   ║
║                                                                  ║
║  Roda standalone (sem FastAPI). Agende no Windows Task Scheduler.║
║                                                                  ║
║  USO:                                                            ║
║    python sync_lovable.py                  → sync completo       ║
║    python sync_lovable.py --inicial        → 1ª vez (tudo)       ║
║    python sync_lovable.py --teste          → verifica conexão    ║
║    python sync_lovable.py --fonte metabase → só Metabase         ║
║    python sync_lovable.py --fonte byetech  → só Byetech CRM      ║
║                                                                  ║
║  O QUE FAZ:                                                      ║
║    1. Byetech CRM  → SQLite local + Lovable Cloud                ║
║    2. Metabase     → SQLite local + Lovable Cloud                ║
║    3. Portal GWM   → atualiza status no Lovable                  ║
║    4. Portal LM    → atualiza status no Lovable                  ║
║    5. Loga tudo em sync_lovable.log e na tabela sync_log         ║
╚══════════════════════════════════════════════════════════════════╝
"""

import sys
import os
import asyncio
import argparse
import logging
from datetime import datetime, timedelta, date
from pathlib import Path

# Garante import do pacote app
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("sync_lovable.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("sync_lovable")


# ════════════════════════════════════════════════════════════════════════════
#  TESTE DE CONEXÃO
# ════════════════════════════════════════════════════════════════════════════

def cmd_teste():
    from app.services.lovable_client import testar_conexao, LOVABLE_URL

    print("\n" + "═" * 55)
    print("  TESTE DE CONEXÃO — Lovable Cloud")
    print("═" * 55)

    r = testar_conexao()

    print(f"\n  Configurado   : {'✅' if r['configurado']  else '❌'}")
    print(f"  Conectado     : {'✅' if r['conectado']    else '❌'}")
    print(f"  Tabela OK     : {'✅' if r['tabela_ok']    else '❌'}")
    print(f"  Upsert OK     : {'✅' if r['upsert_ok']    else '❌'}")
    print(f"  Sync Log OK   : {'✅' if r['sync_log_ok']  else '❌'}")

    if r["erro"]:
        print(f"\n  ❌ Erro: {r['erro']}")

    if not r["configurado"]:
        print("\n  Adicione ao .env:")
        print("    LOVABLE_URL=https://xxxx.supabase.co")
        print("    LOVABLE_SERVICE_KEY=eyJhbGc...")
        print("\n  Pegue em: Lovable → Settings → Database\n")
        sys.exit(1)

    if not r["upsert_ok"]:
        print("\n  ⚠️  Execute no SQL Editor do Lovable/Supabase:")
        print("    ALTER TABLE contratos")
        print("      ADD CONSTRAINT contratos_id_externo_key UNIQUE (id_externo);")

    if all([r["conectado"], r["tabela_ok"], r["upsert_ok"]]):
        print(f"\n  ✅ Tudo OK! URL: {LOVABLE_URL}")
        print("  Rode: python sync_lovable.py --inicial\n")
    else:
        print("\n  Corrija os erros acima antes de sincronizar.\n")
        sys.exit(1)


# ════════════════════════════════════════════════════════════════════════════
#  SYNC INICIAL — exporta SQLite → Lovable (use uma única vez)
# ════════════════════════════════════════════════════════════════════════════

async def cmd_inicial():
    from app.services.lovable_client import push_em_lote, log_sync_start, log_sync_end
    from app.database import init_db, SessionLocal, Contrato
    from sqlalchemy import select

    print("\n⚠️  Sync inicial: exporta TODOS os contratos do SQLite para o Lovable.")
    confirma = input("Confirmar? (s/n): ").strip().lower()
    if confirma != "s":
        print("Cancelado.")
        return

    logger.info("═══ SYNC INICIAL ═══")
    log_id = log_sync_start("INICIAL")

    await init_db()
    async with SessionLocal() as session:
        result = await session.execute(select(Contrato))
        todos  = result.scalars().all()

    logger.info(f"[Inicial] {len(todos)} contratos no SQLite local")
    print(f"\n📦 {len(todos)} contratos encontrados. Enviando para o Lovable...\n")

    lote = []
    for c in todos:
        data = {
            "fonte":                  c.fonte,
            "id_externo":             c.id_externo,
            "byetech_contrato_id":    c.byetech_contrato_id,
            "cliente_nome":           c.cliente_nome,
            "cliente_cpf_cnpj":       c.cliente_cpf_cnpj,
            "cliente_email":          c.cliente_email,
            "veiculo":                c.veiculo,
            "placa":                  c.placa,
            "status_atual":           c.status_atual,
            "data_prevista_entrega":  c.data_prevista_entrega,
            "data_entrega_definitiva":c.data_entrega_definitiva,
            "data_venda":             c.data_venda,
            "origem_dados":           "METABASE",
        }
        lote.append((data, c.id))

    ok, erros = push_em_lote(lote, batch=100)
    log_sync_end(log_id, "sucesso" if erros == 0 else "erro", importados=ok)

    print(f"\n{'✅' if erros == 0 else '⚠️ '} Sync inicial concluído!")
    print(f"   Enviados : {ok}")
    print(f"   Erros    : {erros}")
    print("\nAgora rode: python sync_lovable.py  (para o sync diário)\n")


# ════════════════════════════════════════════════════════════════════════════
#  SYNC METABASE
# ════════════════════════════════════════════════════════════════════════════

async def sync_metabase(full: bool = False) -> dict:
    """
    Busca contratos no Metabase e salva no SQLite + Lovable Cloud.
    O hook no sync_service.py empurra para o Lovable automaticamente
    a cada _upsert_contrato.
    """
    from app.services.lovable_client import log_sync_start, log_sync_end
    from app.services.sync_service import run_metabase_sync

    log_id = log_sync_start("METABASE")
    logger.info(f"[Metabase] Iniciando sync {'COMPLETO' if full else 'diário'}...")

    try:
        resultado = await run_metabase_sync(full=full)
        importados = resultado.get("importados", 0)
        log_sync_end(log_id, "sucesso", importados=importados, atualizados=importados)
        logger.info(f"[Metabase] ✅ {importados} contratos processados")
        return {"ok": True, "importados": importados}
    except Exception as e:
        logger.error(f"[Metabase] ❌ {e}")
        log_sync_end(log_id, "erro", erro=str(e)[:500])
        return {"ok": False, "erro": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  SYNC BYETECH CRM
# ════════════════════════════════════════════════════════════════════════════

async def sync_byetech() -> dict:
    """
    Scrapa contratos do Byetech CRM e salva no SQLite + Lovable Cloud.
    Inclui 2FA interativo quando necessário.
    """
    from app.services.lovable_client import log_sync_start, log_sync_end
    from app.database import init_db, SessionLocal
    from app.services.sync_service import _upsert_contrato, _contrato_id

    log_id = log_sync_start("BYETECH")
    logger.info("[Byetech] Iniciando scrape...")

    try:
        from app.scrapers.byetech_crm import scrape_contratos

        # 2FA interativo no terminal
        async def twofa_cb():
            code = input("\n🔐 Digite o código 2FA do Byetech CRM: ").strip()
            return code

        contratos = await scrape_contratos(twofa_callback=twofa_cb)
        logger.info(f"[Byetech] {len(contratos)} contratos recebidos")

    except Exception as e:
        logger.error(f"[Byetech] ❌ Erro no scrape: {e}")
        log_sync_end(log_id, "erro", erro=str(e)[:500])
        return {"ok": False, "erro": str(e)}

    if not contratos:
        log_sync_end(log_id, "sucesso", importados=0)
        return {"ok": True, "importados": 0}

    await init_db()
    importados = 0
    async with SessionLocal() as session:
        for c in contratos:
            try:
                # _upsert_contrato já chama lovable_client.upsert_contrato internamente
                await _upsert_contrato(session, {
                    **c,
                    "origem_dados": "BYETECH_CRM",
                    "id_externo": c.get("id_externo") or c.get("byetech_contrato_id", ""),
                })
                importados += 1
            except Exception as e:
                logger.warning(f"[Byetech] upsert erro: {e}")
        await session.commit()

    logger.info(f"[Byetech] ✅ {importados} contratos salvos")
    log_sync_end(log_id, "sucesso", importados=importados, atualizados=importados)
    return {"ok": True, "importados": importados}


# ════════════════════════════════════════════════════════════════════════════
#  SYNC PORTAIS (GWM / LM) — atualiza status
# ════════════════════════════════════════════════════════════════════════════

async def sync_portais() -> dict:
    """
    Consulta portal GWM e LM para atualizar status dos contratos pendentes.
    Detecta entregas e atualiza Byetech CRM + Lovable.
    """
    from app.services.lovable_client import log_sync_start, log_sync_end
    from app.services.sync_service import run_gwm_lm_validation

    log_id = log_sync_start("PORTAIS_GWM_LM")
    logger.info("[Portais] Iniciando validação GWM / LM...")

    try:
        resultado = await run_gwm_lm_validation(days_back=1)
        n_ent = len(resultado.get("entregues", []))
        n_mud = len(resultado.get("mudancas_status", []))
        n_err = len(resultado.get("erros", []))

        logger.info(f"[Portais] ✅ {n_ent} entregas | {n_mud} mudanças | {n_err} erros")
        log_sync_end(log_id, "sucesso", importados=n_ent + n_mud, atualizados=n_ent)
        return {"ok": True, "entregas": n_ent, "mudancas": n_mud, "erros": n_err}

    except Exception as e:
        logger.error(f"[Portais] ❌ {e}")
        log_sync_end(log_id, "erro", erro=str(e)[:500])
        return {"ok": False, "erro": str(e)}


# ════════════════════════════════════════════════════════════════════════════
#  SYNC COMPLETO (orquestrador principal)
# ════════════════════════════════════════════════════════════════════════════

async def sync_completo(full_metabase: bool = False) -> dict:
    """
    Roda tudo em sequência:
      1. Metabase → contratos novos / atualizados
      2. Byetech CRM → enriquece com dados do cliente
      3. Portais GWM/LM → atualiza status

    Cada etapa salva no SQLite E no Lovable automaticamente.
    """
    from app.database import init_db
    await init_db()

    logger.info("══════════════════════════════════════")
    logger.info("  SYNC COMPLETO INICIADO")
    logger.info(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    logger.info("══════════════════════════════════════")

    inicio = datetime.now()
    resultados = {}

    # 1. Metabase
    print("\n📊 [1/3] Sincronizando Metabase...")
    res_meta = await sync_metabase(full=full_metabase)
    resultados["metabase"] = res_meta
    _print_resultado("Metabase", res_meta)

    # 2. Byetech CRM
    print("\n🏢 [2/3] Sincronizando Byetech CRM...")
    res_byte = await sync_byetech()
    resultados["byetech"] = res_byte
    _print_resultado("Byetech", res_byte)

    # 3. Portais GWM / LM
    print("\n🔍 [3/3] Validando portais GWM / LM...")
    res_port = await sync_portais()
    resultados["portais"] = res_port
    _print_resultado("Portais", res_port)

    duracao = int((datetime.now() - inicio).total_seconds())

    logger.info("══════════════════════════════════════")
    logger.info(f"  SYNC COMPLETO FINALIZADO em {duracao}s")
    logger.info("══════════════════════════════════════")

    print(f"\n{'='*45}")
    print(f"✅ Sync completo em {duracao}s")
    print(f"{'='*45}\n")

    return resultados


def _print_resultado(nome: str, res: dict):
    if res.get("ok"):
        detalhes = []
        if "importados" in res:
            detalhes.append(f"{res['importados']} importados")
        if "entregas" in res:
            detalhes.append(f"{res['entregas']} entregas")
        if "mudancas" in res:
            detalhes.append(f"{res['mudancas']} mudanças")
        print(f"   ✅ {nome}: {' | '.join(detalhes) or 'ok'}")
    else:
        print(f"   ❌ {nome}: {res.get('erro', 'erro desconhecido')}")


# ════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Byetech Entregas — Sync para Lovable Cloud",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python sync_lovable.py                   Sync completo (Metabase + Byetech + Portais)
  python sync_lovable.py --inicial         1ª vez: exporta tudo do SQLite para o Lovable
  python sync_lovable.py --teste           Verifica conexão com o Lovable Cloud
  python sync_lovable.py --fonte metabase  Só Metabase
  python sync_lovable.py --fonte byetech   Só Byetech CRM
  python sync_lovable.py --fonte portais   Só portais GWM/LM
  python sync_lovable.py --full            Metabase completo (todos os contratos ativos)
        """
    )
    parser.add_argument(
        "--fonte",
        choices=["metabase", "byetech", "portais", "completo"],
        default="completo",
        help="Qual fonte sincronizar (padrão: completo)",
    )
    parser.add_argument(
        "--inicial",
        action="store_true",
        help="Exporta TODOS os contratos do SQLite → Lovable (use apenas na 1ª vez)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Metabase completo — todos os ativos (não só os recentes)",
    )
    parser.add_argument(
        "--teste",
        action="store_true",
        help="Testa conexão com o Lovable Cloud sem sincronizar",
    )
    args = parser.parse_args()

    # ── Teste de conexão ──
    if args.teste:
        cmd_teste()
        return

    # ── Sync inicial (bootstrap) ──
    if args.inicial:
        asyncio.run(cmd_inicial())
        return

    # ── Sync normal ──
    print(f"\n🔄 Sync [{args.fonte}] iniciado — {datetime.now().strftime('%d/%m/%Y %H:%M')}")

    if args.fonte == "metabase":
        resultado = asyncio.run(sync_metabase(full=args.full))
    elif args.fonte == "byetech":
        resultado = asyncio.run(sync_byetech())
    elif args.fonte == "portais":
        resultado = asyncio.run(sync_portais())
    else:
        resultado = asyncio.run(sync_completo(full_metabase=args.full))

    logger.info(f"Resultado final: {resultado}")


if __name__ == "__main__":
    main()
