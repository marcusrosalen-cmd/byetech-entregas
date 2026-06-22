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


async def _get_pending_count() -> int:
    """Retorna quantos itens estão na fila byetech_pendentes sem processamento."""
    try:
        from app.database import SessionLocal, ByetechPendente
        from sqlalchemy import select, func
        async with SessionLocal() as s:
            res = await s.execute(
                select(func.count()).select_from(ByetechPendente)
                .where(ByetechPendente.processado_em.is_(None))
            )
            return res.scalar() or 0
    except Exception:
        return 0


async def _slack_alerta_sessao_byetech(motivo: str):
    """
    Envia alerta Slack acionável pedindo login manual no Byetech CRM.
    Inclui contagem de pendentes e URL do portal.
    """
    import os
    portal_url = os.getenv("RENDER_SERVICE_URL", "https://byetech-entregas.onrender.com")
    pendentes  = await _get_pending_count()

    linhas = [
        ":rotating_light: *Login Byetech CRM necessário*",
        f"> {motivo}",
        "",
        f"*Acesse o portal:* {portal_url}",
        "1. Clique em *⚙ Mais ▾* (menu lateral)",
        "2. Clique em *🔑 Login Byetech*",
        "3. Informe e-mail, senha e código 2FA",
        "",
        "*Impacto nos syncs de hoje:*",
        "• 10:00 — Sync completo Byetech CRM",
        "• 10:20 — Sign & Drive / VW",
        "• 10:40 — GWM / LM",
        "Todos dependem da sessão para atualizar o CRM quando houver entrega.",
    ]
    if pendentes > 0:
        linhas += [
            "",
            f":inbox_tray: *{pendentes} atualização(ões) na fila* aguardando sessão válida.",
            "Serão processadas automaticamente após o login.",
        ]

    try:
        from app.services.slack_service import get_or_create_channel, _post
        channel = await get_or_create_channel()
        await _post(channel=channel, text="\n".join(linhas))
    except Exception as slack_e:
        logger.warning(f"[scheduler] Slack alerta sessão: {slack_e}")


async def job_renew_byetech_session():
    """
    Renova automaticamente a sessão Byetech CRM via API (sem Playwright).
    Roda às 09:45, antes de todos os syncs do dia.
    Se a sessão já está válida, não faz nada.
    Se está expirada, tenta login via API pura (httpx).
    Se precisar de 2FA, envia alerta Slack acionável com URL e contagem de pendentes.
    """
    logger.info("⏰ [scheduler] Verificando sessão Byetech...")
    from app.scrapers.byetech_crm import (
        _load_session_from_disk, _test_session, _login_via_api,
        _save_session, set_remote_session,
    )

    # 1. Testa sessão atual
    cookies = _load_session_from_disk()
    if cookies:
        try:
            ok = await _test_session(cookies)
            if ok:
                logger.info("[scheduler] Sessão Byetech válida — nenhuma ação necessária.")
                return
            logger.warning("[scheduler] Sessão Byetech expirada — tentando renovar via API...")
        except Exception as e:
            logger.warning(f"[scheduler] Erro ao testar sessão: {e}")
    else:
        logger.warning("[scheduler] Sem sessão em disco — tentando login via API...")

    # 2. Tenta renovar via API (não requer Playwright, funciona no Render)
    try:
        new_cookies = await _login_via_api(twofa_code=None)
        if new_cookies:
            set_remote_session(new_cookies)
            logger.info("[scheduler] Sessão Byetech renovada com sucesso via API.")
            return
    except Exception as e:
        err_msg = str(e)
        if "2FA_REQUIRED" in err_msg:
            logger.warning(
                "[scheduler] Renovação Byetech requer 2FA — aguardando login manual."
            )
            await _slack_alerta_sessao_byetech(
                "Sessão expirada e renovação automática bloqueada por 2FA."
            )
            return
        else:
            logger.error(f"[scheduler] Falha ao renovar sessão Byetech: {e}")
            await _slack_alerta_sessao_byetech(
                f"Falha técnica ao renovar sessão: `{err_msg[:120]}`"
            )
            return

    # 3. Falhou sem exceção (retornou None) — notifica Slack
    await _slack_alerta_sessao_byetech("Renovação automática não retornou sessão válida.")


async def job_sync_all():
    """
    Job diário: scraping completo de todos os portais.
    Roda independente do estado da sessao Byetech — se expirada, usa fallback do banco
    e continua verificando Sign & Drive, Localiza e demais portais normalmente.
    """
    logger.info("⏰ [scheduler] Iniciando sync diario completo...")

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
    """Job diário (dias úteis): atualiza status S&D + LM Assinecar via API."""
    from app.services.sync_service import run_signanddrive_sync, run_lm_portal_sync
    logger.info("⏰ [scheduler] Sign & Drive + LM sync diário...")
    try:
        result = await run_signanddrive_sync()
        n_ent = len(result.get("entregues", []))
        n_mud = len(result.get("mudancas_status", []))
        logger.info(f"✅ [scheduler] Sign & Drive: {n_ent} entregues | {n_mud} mudanças de status")
    except Exception as e:
        logger.error(f"❌ [scheduler] Sign & Drive erro: {e}")

    try:
        result_lm = await run_lm_portal_sync()
        n_ent_lm = len(result_lm.get("entregues", []))
        n_mud_lm = len(result_lm.get("mudancas_status", []))
        logger.info(f"✅ [scheduler] LM Assinecar: {n_ent_lm} entregues | {n_mud_lm} mudanças")
    except Exception as e:
        logger.error(f"❌ [scheduler] LM Assinecar erro: {e}")


async def job_gwm_lm_daily():
    """
    Job diário (dias úteis): consulta os portais GWM e LM para detectar entregas
    e mudanças de status. Roda após o Sign & Drive (10:40), antes do Metabase (10:45).
    Usa contratos do banco local — não requer sessão Byetech ativa.
    """
    from app.services.sync_service import run_gwm_lm_validation
    logger.info("⏰ [scheduler] GWM/LM portal sync diário...")
    try:
        result = await run_gwm_lm_validation(days_back=1)
        n_ent = len(result.get("entregues", []))
        n_mud = len(result.get("mudancas_status", []))
        n_err = len(result.get("erros", []))
        logger.info(
            f"✅ [scheduler] GWM/LM: {n_ent} entregues | "
            f"{n_mud} mudanças de status | {n_err} erros"
        )
        if result.get("entregues"):
            for e in result["entregues"]:
                logger.info(
                    f"   📦 {e.get('cliente_nome','?')} — {e.get('veiculo','?')} "
                    f"[{e.get('fonte','?')}]"
                )
    except Exception as e:
        logger.error(f"❌ [scheduler] GWM/LM erro: {e}")


async def job_reconcile_stale():
    """
    Job semanal (segunda 08:30): remove contratos 'órfãos' — ativos no banco
    mas não retornados por nenhum sync nos últimos 7 dias.
    Evita acúmulo de registros que foram cancelados ou entregues sem registro formal.
    """
    from app.database import SessionLocal, Contrato
    from sqlalchemy import delete, select, func

    cutoff = datetime.utcnow() - timedelta(days=7)
    status_excluidos = [
        "Definitivo entregue", "Definitivo Entregue",
        "definitivo entregue", "definitivo_entregue",
        "Cancelado", "cancelado",
    ]

    async with SessionLocal() as session:
        res = await session.execute(
            select(func.count()).select_from(Contrato).where(
                Contrato.ultima_atualizacao < cutoff,
                Contrato.data_entrega_definitiva.is_(None),
                Contrato.status_atual.not_in(status_excluidos),
            )
        )
        n_orphans = res.scalar() or 0

        if n_orphans == 0:
            logger.info("[scheduler] Reconciliação semanal: nenhum órfão encontrado.")
            return

        await session.execute(
            delete(Contrato).where(
                Contrato.ultima_atualizacao < cutoff,
                Contrato.data_entrega_definitiva.is_(None),
                Contrato.status_atual.not_in(status_excluidos),
            )
        )
        await session.commit()

    logger.info(f"[scheduler] Reconciliação semanal: {n_orphans} contratos órfãos removidos.")

    try:
        from app.services.slack_service import get_or_create_channel, _post
        channel = await get_or_create_channel()
        await _post(
            channel=channel,
            text=(
                f":broom: *Reconciliação semanal*: {n_orphans} contrato(s) removido(s) "
                "por não aparecerem nos syncs dos últimos 7 dias "
                "(cancelados ou entregues sem registro formal)."
            ),
        )
    except Exception as e:
        logger.warning(f"[scheduler] Slack reconciliação: {e}")


def start_scheduler():
    """
    Inicia o scheduler com os jobs configurados.
    Todos os jobs rodam apenas em dias úteis (seg-sex), exceto Metabase
    que também roda aos sábados para capturar contratos do fim de semana.
    """
    scheduler = get_scheduler()
    BRA = "America/Sao_Paulo"

    # Renovação automática da sessão Byetech — 09:45 dias úteis (antes de todos os syncs)
    scheduler.add_job(
        job_renew_byetech_session,
        CronTrigger(hour=9, minute=45, day_of_week="mon-fri", timezone=BRA),
        id="renew_byetech_session",
        replace_existing=True,
        name="Renovação sessão Byetech",
    )

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

    # GWM + LM portal sync — 10:40 dias úteis (após Sign & Drive)
    scheduler.add_job(
        job_gwm_lm_daily,
        CronTrigger(hour=10, minute=40, day_of_week="mon-fri", timezone=BRA),
        id="gwm_lm_daily",
        replace_existing=True,
        name="GWM + LM portal sync diário",
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

    # Reconciliação semanal de contratos órfãos — segunda 08:30 (antes de todos os syncs)
    scheduler.add_job(
        job_reconcile_stale,
        CronTrigger(hour=8, minute=30, day_of_week="mon", timezone=BRA),
        id="reconcile_stale",
        replace_existing=True,
        name="Reconciliação semanal de contratos órfãos",
    )

    scheduler.start()
    logger.info(
        "✅ Scheduler iniciado — "
        "Reconciliação seg 08:30 | Sessão 09:45 | Byetech 10:00 | S&D 10:20 | "
        "GWM/LM 10:40 | Metabase 10:45 | Alertas 11:00 (seg-sex)"
    )
    return scheduler
