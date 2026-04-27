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


async def job_sync_all():
    """Job diário: scraping completo. Só roda se sessão Byetech estiver válida."""
    logger.info("⏰ [scheduler] Verificando sessão Byetech antes do sync...")

    from app.scrapers.byetech_crm import _load_session_from_disk, _test_session
    cookies = _load_session_from_disk()
    if not cookies or not await _test_session(cookies):
        logger.warning(
            "⏰ [scheduler] Sessão Byetech EXPIRADA — sync automático cancelado. "
            "Acesse o portal e clique em '🔑 Renovar sessão' para reativar."
        )
        # Notifica Slack
        try:
            from app.services.slack_service import get_client, get_or_create_channel
            channel = await get_or_create_channel()
            client = get_client()
            await client.chat_postMessage(
                channel=channel,
                text=(
                    "⚠️ *Sync automático cancelado — sessão Byetech expirada*\n"
                    "Acesse o portal em *http://localhost:8001* e clique em "
                    "*🔑 Renovar sessão* para reativar e processar as entregas pendentes."
                ),
            )
        except Exception as e:
            logger.warning(f"Aviso Slack sessão expirada: {e}")
        return

    logger.info("⏰ [scheduler] Sessão válida — iniciando sync completo...")
    try:
        result = await run_full_sync()
        logger.info(f"✅ [scheduler] Sync concluída: {result}")
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


def start_scheduler():
    """Inicia o scheduler com os jobs configurados."""
    scheduler = get_scheduler()

    # Sync completo Byetech CRM — 08:00 todos os dias
    scheduler.add_job(
        job_sync_all,
        CronTrigger(hour=8, minute=0, timezone="America/Sao_Paulo"),
        id="sync_all",
        replace_existing=True,
        name="Sync completo diário",
    )

    # Metabase sync diário — 08:45 (novos contratos do dia, antes dos alertas)
    scheduler.add_job(
        job_metabase_daily,
        CronTrigger(hour=8, minute=45, timezone="America/Sao_Paulo"),
        id="metabase_daily",
        replace_existing=True,
        name="Metabase sync diário",
    )

    # Alertas Slack + e-mails Unidas — 09:00 (após sync Metabase)
    scheduler.add_job(
        job_check_alerts,
        CronTrigger(hour=9, minute=0, timezone="America/Sao_Paulo"),
        id="check_alerts",
        replace_existing=True,
        name="Alertas Slack + e-mails Unidas",
    )

    scheduler.start()
    logger.info("✅ Scheduler iniciado — Byetech 08:00 | Metabase 08:45 | Alertas+Unidas 09:00 (Brasília)")
    return scheduler
