"""
Scheduler — APScheduler
Jobs:
  1. sync_all     → toda noite às 08:00 — scraping completo
  2. check_alerts → diariamente após sync — verifica prazos e envia Slack
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.services.slack_service import send_daily_alert, send_prazo_alert
from app.services.sync_service import run_full_sync

logger = logging.getLogger("scheduler")

_scheduler: Optional[AsyncIOScheduler] = None

ALERT_DAYS = [20, 15, 10, 5, 2, 1]


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if not _scheduler:
        _scheduler = AsyncIOScheduler(timezone="America/Sao_Paulo")
    return _scheduler


async def _push_session_to_render():
    """Empurra a sessão Byetech para o Render após sync local bem-sucedido."""
    import os, json, httpx
    render_url    = os.getenv("RENDER_SERVICE_URL", "https://byetech-entregas.onrender.com")
    push_secret   = os.getenv("SESSION_PUSH_SECRET", "byetech-local")
    session_file  = os.path.join(os.path.dirname(__file__), "..", "..", ".byetech_session.json")
    if not os.path.exists(session_file):
        logger.warning("[Scheduler] .byetech_session.json não encontrado — push de sessão ignorado")
        return
    try:
        with open(session_file, encoding="utf-8") as f:
            cookies = json.load(f)
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{render_url}/api/byetech/push-session",
                json={"cookies": cookies, "secret": push_secret},
            )
            if resp.status_code == 200:
                data = resp.json()
                logger.info(f"[Scheduler] Sessão enviada ao Render: {data.get('message','ok')}")
            else:
                logger.warning(f"[Scheduler] Push sessão falhou {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"[Scheduler] Push sessão erro: {e}")


async def job_sync_all():
    """
    Job diário: scraping completo de todos os portais.
    Roda independente do estado da sessao Byetech — se expirada, usa fallback do banco
    e continua verificando Sign & Drive, Localiza e demais portais normalmente.
    """
    logger.info("⏰ [scheduler] Iniciando sync diario completo...")

    # Verifica sessao Byetech APENAS para log informativo — nao cancela o sync
    try:
        from app.scrapers.byetech_crm import _load_session_from_disk, _test_session
        cookies = _load_session_from_disk()
        if not cookies:
            logger.warning(
                "[scheduler] Sessao Byetech: sem arquivo de sessao — "
                "contratos serao lidos do banco local."
            )
        else:
            _sessao_ok = await _test_session(cookies)
            if not _sessao_ok:
                logger.warning(
                    "[scheduler] Sessao Byetech EXPIRADA — sync continua usando banco local. "
                    "Execute push_session_render.py para renovar."
                )
                try:
                    from app.services.slack_service import get_client, get_or_create_channel
                    channel = await get_or_create_channel()
                    client = get_client()
                    await client.chat_postMessage(
                        channel=channel,
                        text=(
                            ":warning: *Sessao Byetech expirada* — sync continua usando contratos do banco local.\n"
                            "Execute `push_session_render.py` para renovar a sessao e habilitar atualizacoes no Byetech CRM."
                        ),
                    )
                except Exception as _slack_e:
                    logger.warning(f"[scheduler] Slack aviso sessao: {_slack_e}")
            else:
                logger.info("[scheduler] Sessao Byetech OK.")
    except Exception as _check_e:
        logger.warning(f"[scheduler] Verificacao de sessao falhou (nao critico): {_check_e}")

    try:
        result = await run_full_sync()
        logger.info(f"✅ [scheduler] Sync concluida: {result}")
        # Apos sync bem-sucedido, empurra sessao para o Render (apenas localmente)
        import os as _os
        if not _os.getenv("RENDER"):
            await _push_session_to_render()
    except Exception as e:
        logger.error(f"❌ [scheduler] Erro no sync: {e}")


async def job_check_alerts():
    """
    Verifica prazos, envia alertas Slack e dispara e-mails automáticos Unidas.
    Roda às 09:00, após os syncs do dia.
    """
    from app.database import SessionLocal
    from app.database import Contrato, AlertaEnviado
    from sqlalchemy import select, and_

    logger.info("⏰ [scheduler] Verificando alertas de prazo...")

    async with SessionLocal() as session:
        # Pega contratos ativos sem entrega definitiva
        result = await session.execute(
            select(Contrato).where(Contrato.data_entrega_definitiva.is_(None))
        )
        contratos = result.scalars().all()

        hoje = datetime.now().date()
        alertas_enviados = 0

        for c in contratos:
            if not c.data_prevista_entrega:
                continue

            data_prev = c.data_prevista_entrega.date() if isinstance(
                c.data_prevista_entrega, datetime
            ) else c.data_prevista_entrega

            dias_restantes = (data_prev - hoje).days
            c.dias_para_entrega = dias_restantes
            c.atrasado = dias_restantes < 0

            # Verifica se deve enviar alerta de prazo
            for dias_antes in ALERT_DAYS:
                if dias_restantes == dias_antes:
                    # Verifica se já enviou hoje
                    alerta_check = await session.execute(
                        select(AlertaEnviado).where(
                            and_(
                                AlertaEnviado.contrato_id == c.id,
                                AlertaEnviado.tipo == "slack",
                                AlertaEnviado.dias_antes == dias_antes,
                            )
                        ).order_by(AlertaEnviado.enviado_em.desc()).limit(1)
                    )
                    ultimo = alerta_check.scalar_one_or_none()

                    # Só envia uma vez por dia para o mesmo contrato+dias_antes
                    ja_enviou_hoje = (
                        ultimo and ultimo.enviado_em.date() == hoje
                    ) if ultimo else False

                    if not ja_enviou_hoje:
                        try:
                            contrato_dict = {
                                "id": c.id,
                                "fonte": c.fonte,
                                "cliente_nome": c.cliente_nome,
                                "veiculo": c.veiculo,
                                "placa": c.placa,
                                "status_atual": c.status_atual,
                                "data_prevista_entrega": c.data_prevista_entrega,
                                "dias_para_entrega": dias_restantes,
                                "atrasado": c.atrasado,
                            }
                            await send_prazo_alert(contrato_dict, dias_antes)

                            # Registra alerta enviado
                            alerta = AlertaEnviado(
                                contrato_id=c.id,
                                tipo="slack",
                                dias_antes=dias_antes,
                            )
                            session.add(alerta)
                            alertas_enviados += 1
                        except Exception as e:
                            logger.error(f"Erro ao enviar alerta para {c.id}: {e}")

        await session.commit()

        # Envia resumo diário (sempre, mesmo sem alertas pontuais)
        contratos_dict = [
            {
                "id": c.id,
                "fonte": c.fonte,
                "cliente_nome": c.cliente_nome,
                "veiculo": c.veiculo,
                "status_atual": c.status_atual,
                "data_prevista_entrega": c.data_prevista_entrega,
                "dias_para_entrega": c.dias_para_entrega,
                "atrasado": c.atrasado,
            }
            for c in contratos
        ]
        try:
            from app.services.slack_service import send_relatorio_completo
            await send_relatorio_completo(dias_vendas=5, dias_entregas=7)
        except Exception as e:
            logger.error(f"Erro ao enviar relatório completo Slack: {e}")

    logger.info(f"✅ [scheduler] Alertas enviados: {alertas_enviados}")


async def job_metabase_daily():
    """Job diário: busca novos contratos do Metabase (data de hoje)."""
    from app.services.sync_service import run_metabase_sync
    logger.info("⏰ [scheduler] Metabase sync diário...")
    try:
        result = await run_metabase_sync(full=False)
        logger.info(f"✅ [scheduler] Metabase: {result['importados']} contratos processados")
    except Exception as e:
        logger.error(f"❌ [scheduler] Metabase erro: {e}")


async def job_signanddrive_daily():
    """Job diário (dias úteis): atualiza status dos pedidos Sign & Drive via API."""
    from app.services.sync_service import run_signanddrive_sync
    logger.info("⏰ [scheduler] Sign & Drive sync diário...")
    try:
        result = await run_signanddrive_sync()
        logger.info(f"✅ [scheduler] Sign & Drive: {result}")
    except Exception as e:
        logger.error(f"❌ [scheduler] Sign & Drive erro: {e}")


def start_scheduler():
    """
    Inicia o scheduler com os jobs configurados.
    Todos os jobs rodam apenas em dias úteis (seg-sex), exceto Metabase
    que também roda aos sábados para capturar contratos do fim de semana.
    """
    scheduler = get_scheduler()
    BRA = "America/Sao_Paulo"

    # Sync completo Byetech CRM — 10:00 dias úteis (seg-sex)
    scheduler.add_job(
        job_sync_all,
        CronTrigger(hour=10, minute=0, day_of_week="mon-fri", timezone=BRA),
        id="sync_all",
        replace_existing=True,
        name="Sync completo (dias úteis)",
    )

    # Sign & Drive sync — 10:20 dias úteis (após sync Byetech)
    scheduler.add_job(
        job_signanddrive_daily,
        CronTrigger(hour=10, minute=20, day_of_week="mon-fri", timezone=BRA),
        id="signanddrive_daily",
        replace_existing=True,
        name="Sign & Drive sync diário",
    )

    # Metabase sync — 10:45 seg-sab (captura contratos criados no fim de semana)
    scheduler.add_job(
        job_metabase_daily,
        CronTrigger(hour=10, minute=45, day_of_week="mon-sat", timezone=BRA),
        id="metabase_daily",
        replace_existing=True,
        name="Metabase sync diário",
    )

    # Alertas Slack + relatório — 11:00 dias úteis
    scheduler.add_job(
        job_check_alerts,
        CronTrigger(hour=11, minute=0, day_of_week="mon-fri", timezone=BRA),
        id="check_alerts",
        replace_existing=True,
        name="Alertas Slack + e-mails Unidas (dias úteis)",
    )

    scheduler.start()
    logger.info(
        "✅ Scheduler iniciado — "
        "Byetech 10:00 (seg-sex) | S&D 10:20 (seg-sex) | "
        "Metabase 10:45 (seg-sab) | Alertas 11:00 (seg-sex)"
    )
    return scheduler
