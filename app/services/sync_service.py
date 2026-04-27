"""
Serviço central de sincronização.
Orquestra todos os scrapers e atualiza o banco de dados.
"""
import asyncio
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import SessionLocal, Contrato, HistoricoStatus

logger = logging.getLogger("sync")

# Estado global do sync (para polling da UI)
_sync_state = {
    "status": "idle",      # idle | running | done | error | needs_2fa
    "message": "",
    "atualizados": 0,
    "iniciado_em": None,
    "system": "",          # qual sistema está pedindo 2FA
    "entregas_hoje": [],   # lista de entregas registradas no dia
}

_twofa_event = asyncio.Event()
_twofa_code: Optional[str] = None


def get_sync_state() -> dict:
    return dict(_sync_state)


def set_sync_state(**kwargs):
    _sync_state.update(kwargs)


async def provide_twofa(code: str):
    global _twofa_code
    _twofa_code = code
    _twofa_event.set()


async def _wait_twofa() -> str:
    global _twofa_code
    _twofa_event.clear()
    await asyncio.wait_for(_twofa_event.wait(), timeout=300)
    code = _twofa_code
    _twofa_code = None
    return code


def _contrato_id(fonte: str, id_externo: str, cpf: str) -> str:
    """Gera ID único para o contrato."""
    key = id_externo or cpf or "unknown"
    return f"{fonte}_{key}".upper()


async def _upsert_contrato(session: AsyncSession, data: dict, portal_update: bool = False) -> bool:
    """
    Insere ou atualiza um contrato.
    Retorna True se houve mudança de status.

    portal_update=True → atualiza apenas status e datas (portal é fonte de status,
    nunca de dados do cliente). Byetech CRM é sempre a fonte de verdade para
    nome, locadora, veículo, placa e e-mail.
    """
    fonte = data.get("fonte", "")
    id_externo = data.get("id_externo", "")
    cpf = data.get("cliente_cpf_cnpj", "")
    contrato_id = _contrato_id(fonte, id_externo, cpf)

    result = await session.execute(select(Contrato).where(Contrato.id == contrato_id))
    existing = result.scalar_one_or_none()

    status_novo = data.get("status_atual", "")
    status_anterior = existing.status_atual if existing else None
    mudou_status = existing and status_anterior != status_novo and status_novo

    if existing:
        # Sempre atualiza status e datas
        existing.status_anterior = status_anterior
        existing.status_atual = status_novo or existing.status_atual
        existing.ultima_atualizacao = datetime.utcnow()

        dp = data.get("data_prevista_entrega")
        if dp:
            existing.data_prevista_entrega = dp

        if data.get("data_entrega_definitiva"):
            existing.data_entrega_definitiva = data["data_entrega_definitiva"]

        if data.get("data_venda"):
            existing.data_venda = data["data_venda"]

        if data.get("pedido_id_locadora"):
            existing.pedido_id_locadora = data["pedido_id_locadora"]

        if not portal_update:
            # Só Byetech CRM pode atualizar dados do cliente
            existing.cliente_nome = data.get("cliente_nome") or existing.cliente_nome
            existing.cliente_email = data.get("cliente_email") or existing.cliente_email
            existing.veiculo = data.get("veiculo") or existing.veiculo
            existing.placa = data.get("placa") or existing.placa
            existing.byetech_contrato_id = data.get("byetech_contrato_id") or existing.byetech_contrato_id
        else:
            # Portal: só atualiza placa se ainda não temos (campo operacional)
            if data.get("placa") and not existing.placa:
                existing.placa = data["placa"]

        # Calcula dias
        if existing.data_prevista_entrega:
            delta = (existing.data_prevista_entrega.date() - datetime.now().date())
            existing.dias_para_entrega = delta.days
            existing.atrasado = delta.days < 0

    else:
        dp = data.get("data_prevista_entrega")
        dias = None
        atrasado = False
        if dp:
            delta = (dp.date() if isinstance(dp, datetime) else dp) - datetime.now().date()
            dias = delta.days
            atrasado = delta.days < 0

        novo = Contrato(
            id=contrato_id,
            fonte=fonte,
            id_externo=id_externo,
            cliente_nome=data.get("cliente_nome", ""),
            cliente_cpf_cnpj=cpf,
            cliente_email=data.get("cliente_email", ""),
            veiculo=data.get("veiculo", ""),
            placa=data.get("placa", ""),
            status_atual=status_novo,
            byetech_contrato_id=data.get("byetech_contrato_id", ""),
            data_prevista_entrega=dp,
            data_entrega_definitiva=data.get("data_entrega_definitiva"),
            data_venda=data.get("data_venda"),
            pedido_id_locadora=data.get("pedido_id_locadora"),
            dias_para_entrega=dias,
            atrasado=atrasado,
        )
        session.add(novo)

    # Registra mudança de status no histórico
    if mudou_status:
        hist = HistoricoStatus(
            contrato_id=contrato_id,
            status_anterior=status_anterior,
            status_novo=status_novo,
            fonte=fonte,
        )
        session.add(hist)

    return mudou_status


async def _aplicar_entrega_portal(r: dict):
    """
    Se o portal reportou entrega (r['entregue'] == True), propaga:
    - data_entrega_definitiva → data da última etapa ou hoje
    - status_atual → 'Definitivo entregue'
    - Chama Byetech CRM para registrar a data
    """
    if not r.get("entregue"):
        return

    if r.get("data_entrega_definitiva"):
        data_entrega = r["data_entrega_definitiva"]
    elif r.get("data_ultima_etapa"):
        data_entrega = r["data_ultima_etapa"]
    else:
        data_entrega = datetime.utcnow()

    r["data_entrega_definitiva"] = data_entrega
    r["status_atual"] = "Definitivo entregue"
    logger.info(f"Entrega detectada no portal: {r.get('cliente_nome','?')} ({r.get('fonte','?')}) em {data_entrega}")

    # Atualiza no Byetech CRM em background (usa CPF para lookup)
    cpf = r.get("cliente_cpf_cnpj")
    if cpf:
        import asyncio as _asyncio
        _asyncio.create_task(_update_byetech_crm_entrega(cpf, r.get("placa", ""), data_entrega))


async def _update_byetech_crm_entrega(cpf: str, placa: str, data: datetime):
    try:
        from app.scrapers.byetech_crm import update_delivery_by_cpf
        ok = await update_delivery_by_cpf(cpf_raw=cpf, data_entrega=data, placa=placa or None)
        if ok:
            logger.info(f"[Byetech] CPF {cpf[:6]}... → Definitivo Entregue em {data.date()}")
        else:
            logger.error(f"[Byetech] Falha ao atualizar CPF {cpf[:6]}... — veja logs do scraper")
    except Exception as e:
        logger.error(f"Erro ao atualizar Byetech CRM entrega: {e}")


async def _resumo_entregas_hoje() -> list[dict]:
    """Retorna contratos marcados como entregues hoje."""
    from datetime import date
    hoje_inicio = datetime.combine(date.today(), datetime.min.time())
    hoje_fim    = datetime.combine(date.today(), datetime.max.time())
    from sqlalchemy import and_
    from app.database import Contrato as _Contrato
    async with SessionLocal() as s:
        res = await s.execute(
            select(_Contrato).where(
                and_(
                    _Contrato.data_entrega_definitiva >= hoje_inicio,
                    _Contrato.data_entrega_definitiva <= hoje_fim,
                )
            )
        )
        rows = res.scalars().all()
    return [
        {
            "id": r.id,
            "cliente_nome": r.cliente_nome or "—",
            "veiculo": r.veiculo or "—",
            "placa": r.placa or "",
            "fonte": r.fonte or "—",
            "data_entrega": r.data_entrega_definitiva.strftime("%d/%m/%Y %H:%M")
            if r.data_entrega_definitiva else "—",
        }
        for r in rows
    ]


async def _sync_byetech_fase(r: dict, erros: list):
    """
    Quando uma mudança de status vem do portal, tenta mover a fase no Byetech.
    Adiciona ao lista de erros se falhar.
    """
    cpf        = r.get("cliente_cpf_cnpj", "")
    novo_status = r.get("status_atual", "")
    if not cpf or not novo_status:
        return
    try:
        from app.scrapers.byetech_crm import update_phase_by_cpf
        ok, msg = await update_phase_by_cpf(cpf_raw=cpf, novo_status=novo_status)
        if not ok:
            nome = r.get("cliente_nome", "?")
            erros.append(f"Byetech fase {nome}: {msg}")
            logger.warning(f"[Byetech] Fase não sincronizada para {cpf[:6]}…: {msg}")
    except Exception as e:
        erros.append(f"Byetech fase {r.get('cliente_nome','?')}: {e}")


async def run_full_sync(twofa_event_fn=None) -> dict:
    """
    Executa sincronização completa de todos os portais.
    twofa_event_fn: se fornecido, é chamado quando 2FA é necessário (retorna código).
    A sessão do Byetech é cacheada em disco — 2FA só solicitado quando a sessão expirar.
    """
    from app.scrapers.byetech_crm import scrape_contratos as scrape_byetech
    from app.scrapers.portaldealer import scrape_portaldealer
    from app.scrapers.localiza import scrape_localiza

    import traceback as _tb
    def _dbg(msg):
        try:
            with open("sync_debug.log", "a", encoding="utf-8") as _f:
                _f.write(f"{datetime.now().isoformat()} {msg}\n")
        except Exception:
            pass

    set_sync_state(
        status="running",
        message="Iniciando...",
        atualizados=0,
        iniciado_em=datetime.utcnow().isoformat(),
    )
    _dbg("run_full_sync iniciado")

    total_atualizados = 0
    erros = []

    try:
        # 1. Byetech CRM — fonte de verdade dos contratos
        set_sync_state(message="Lendo contratos do Byetech CRM...")
        logger.info("📥 Buscando contratos Byetech...")
        _dbg("antes de scrape_byetech")

        needs_2fa_flag = False

        async def _2fa_cb():
            set_sync_state(status="needs_2fa", message="Aguardando código 2FA...", system="Byetech CRM")
            if twofa_event_fn:
                return await twofa_event_fn()
            return await _wait_twofa()

        byetech_contratos = await scrape_byetech(twofa_callback=_2fa_cb)
        set_sync_state(status="running")

        logger.info(f"  → {len(byetech_contratos)} contratos encontrados no Byetech")

        # Agrupa clientes por locadora
        clientes_gwm      = []
        clientes_lm       = []
        clientes_localiza = []
        clientes_unidas   = []

        async with SessionLocal() as session:
            for c in byetech_contratos:
                # Usa a fonte já mapeada pelo byetech_crm (GWM, SIGN & DRIVE, LM, UNIDAS, etc.)
                fonte = c.get("fonte", "OUTRO")
                locadora_nome = c.get("locadora_nome", "").upper()

                cliente_base = {
                    "cliente_cpf_cnpj": c.get("cliente_cpf_cnpj", ""),
                    "cliente_nome": c.get("cliente_nome", ""),
                    "cliente_email": c.get("cliente_email", ""),
                    "byetech_contrato_id": c.get("byetech_contrato_id", ""),
                    "veiculo": c.get("veiculo", ""),
                    "placa": c.get("placa", ""),
                    "data_prevista_entrega": c.get("data_prevista_entrega"),
                }

                # Agrupa por locadora para scraping secundário
                # Usa a fonte já mapeada + nome original para fallback
                fonte_upper = (fonte + " " + locadora_nome).upper()
                for palavras, lista in [
                    (["GWM", "SIGN & DRIVE", "SIGN", "DRIVE"], clientes_gwm),
                    (["LM", "ASSINECAR"], clientes_lm),
                    (["LOCALIZA"], clientes_localiza),
                    (["UNIDAS"], clientes_unidas),
                    # MOVIDA é via planilha — não vai no scraping automático
                ]:
                    if any(p in fonte_upper for p in palavras):
                        lista.append(cliente_base)
                        break

                # Salva contrato usando id_externo (ID inteiro) — consistente com import do Excel/Metabase
                await _upsert_contrato(session, {
                    **cliente_base,
                    "fonte": fonte,
                    "status_atual": c.get("status_atual", ""),
                    "id_externo": c.get("id_externo", "") or c.get("byetech_contrato_id", ""),
                })

            await session.commit()

        # 2. GWM / Sign / Drive
        if clientes_gwm:
            set_sync_state(message=f"Consultando portal GWM/Sign/Drive ({len(clientes_gwm)} clientes)...")
            logger.info(f"🔍 Portal GWM — {len(clientes_gwm)} clientes")
            try:
                gwm_resultados = await scrape_portaldealer(clientes_gwm, "GWM")
                async with SessionLocal() as session:
                    for r in gwm_resultados:
                        if not r.get("erro"):
                            await _aplicar_entrega_portal(r)
                            changed = await _upsert_contrato(session, r, portal_update=True)
                            if changed:
                                total_atualizados += 1
                                if not r.get("entregue"):
                                    await _sync_byetech_fase(r, erros)
                    await session.commit()
            except Exception as e:
                erros.append(f"GWM: {e}")
                logger.error(f"Erro GWM: {e}")

        # 3. LM
        if clientes_lm:
            set_sync_state(message=f"Consultando portal LM ({len(clientes_lm)} clientes)...")
            logger.info(f"🔍 Portal LM — {len(clientes_lm)} clientes")
            try:
                lm_resultados = await scrape_portaldealer(clientes_lm, "LM")
                async with SessionLocal() as session:
                    for r in lm_resultados:
                        if not r.get("erro"):
                            await _aplicar_entrega_portal(r)
                            changed = await _upsert_contrato(session, r, portal_update=True)
                            if changed:
                                total_atualizados += 1
                                if not r.get("entregue"):
                                    await _sync_byetech_fase(r, erros)
                    await session.commit()
            except Exception as e:
                erros.append(f"LM: {e}")
                logger.error(f"Erro LM: {e}")

        # 4. Localiza
        if clientes_localiza:
            set_sync_state(message=f"Consultando portal Localiza ({len(clientes_localiza)} clientes)...")
            logger.info(f"🔍 Localiza — {len(clientes_localiza)} clientes")
            try:
                loc_resultados = await scrape_localiza(clientes_localiza)
                async with SessionLocal() as session:
                    for r in loc_resultados:
                        if not r.get("erro"):
                            await _aplicar_entrega_portal(r)
                            changed = await _upsert_contrato(session, r, portal_update=True)
                            if changed:
                                total_atualizados += 1
                    await session.commit()
            except Exception as e:
                erros.append(f"Localiza: {e}")
                logger.error(f"Erro Localiza: {e}")

        # 5. Unidas — apenas registra no banco (scraping via email)
        if clientes_unidas:
            async with SessionLocal() as session:
                for c in clientes_unidas:
                    await _upsert_contrato(session, {
                        **c,
                        "fonte": "UNIDAS",
                        "status_atual": "Aguardando confirmação",
                        "id_externo": c.get("byetech_contrato_id", ""),
                    })
                await session.commit()

        # Resumo de entregas registradas hoje
        entregas_hoje = await _resumo_entregas_hoje()

        entregues_msg = ""
        if entregas_hoje:
            entregues_msg = f" | {len(entregas_hoje)} entregue(s) hoje"
            logger.info(f"📦 Entregas registradas hoje ({len(entregas_hoje)}):")
            for e in entregas_hoje:
                logger.info(
                    f"   ✓ {e['cliente_nome']} — {e['veiculo']} [{e['fonte']}] "
                    f"em {e['data_entrega']}"
                )
            # Notifica Slack com o resumo
            try:
                from app.services.slack_service import send_entregas_resumo
                asyncio.create_task(send_entregas_resumo(
                    entregas_hoje=entregas_hoje,
                    atualizados=total_atualizados,
                    erros=erros,
                ))
            except Exception as slack_err:
                logger.warning(f"Slack resumo não enviado: {slack_err}")

        set_sync_state(
            status="done",
            message=f"Sync concluída. {total_atualizados} contratos atualizados{entregues_msg}.",
            atualizados=total_atualizados,
            entregas_hoje=entregas_hoje,
        )
        logger.info(f"✅ Sync completa — {total_atualizados} atualizados, {len(erros)} erros")

        # Notifica Slack com resumo executivo do sync
        try:
            from app.services.slack_service import send_sync_concluido
            import time as _time
            _inicio_str = _sync_state.get("iniciado_em")
            _inicio_dt = datetime.fromisoformat(_inicio_str) if _inicio_str else None
            _duracao = (datetime.utcnow() - _inicio_dt).total_seconds() if _inicio_dt else 0
            asyncio.create_task(send_sync_concluido(
                atualizados=total_atualizados,
                entregas_hoje=entregas_hoje,
                erros=erros,
                duracao_seg=_duracao,
            ))
        except Exception as _se:
            logger.warning(f"Slack sync_concluido não enviado: {_se}")

        return {
            "atualizados": total_atualizados,
            "erros": erros,
            "total_contratos": len(byetech_contratos),
            "entregas_hoje": entregas_hoje,
        }

    except TimeoutError:
        msg = "Tempo esgotado aguardando o código 2FA (5 min). Clique em Sync novamente e insira o código quando o popup aparecer."
        set_sync_state(status="error", message=msg)
        logger.error("❌ Timeout aguardando 2FA")
        _dbg("TimeoutError capturado")
    except Exception as e:
        msg_e = str(e) or f"({type(e).__name__})"
        set_sync_state(status="error", message=msg_e)
        logger.error(f"❌ Erro crítico no sync: {type(e).__name__}: {e}")
        _dbg(f"Exception: {type(e).__name__}: {e}\n{_tb.format_exc()}")
        raise


async def run_gwm_lm_validation(days_back: int = 4, max_por_fonte: int = None) -> dict:
    """
    Teste de validação GWM / LM:
    1. Consulta portal Sign&Drive/LM para os contratos mais urgentes (atrasados e próximos do prazo).
    2. Detecta entregas confirmadas e mudanças de status.
    3. Para entregas: atualiza Byetech CRM via CPF.
    4. Sincroniza Metabase para os últimos `days_back` dias, rastreando novas vendas por dia.

    max_por_fonte: limita o scraping por fonte (padrão 80) — prioriza contratos mais urgentes.
    Retorna dict com entregues, mudancas_status, novas_vendas_por_dia, erros.
    """
    from app.scrapers.portaldealer import scrape_portaldealer
    from app.scrapers.metabase import fetch_contracts_by_date
    from datetime import date, timedelta
    from sqlalchemy import and_

    resultado: dict = {
        "entregues": [],
        "mudancas_status": [],
        "novas_vendas_por_dia": {},
        "erros": [],
    }

    # ── 1. Lê contratos GWM e LM do banco local ───────────
    # Ordena por urgência: atrasados primeiro, depois mais próximos do prazo
    hoje = datetime.now().date()

    async with SessionLocal() as session:
        res = await session.execute(
            select(Contrato).where(
                and_(
                    Contrato.fonte.in_(["GWM", "SIGN & DRIVE", "LM"]),
                    Contrato.data_entrega_definitiva.is_(None),
                )
            )
        )
        contratos_db = res.scalars().all()

    def _urgencia_key(c):
        if not c.data_prevista_entrega:
            return 9999
        dp = c.data_prevista_entrega.date() if isinstance(c.data_prevista_entrega, datetime) else c.data_prevista_entrega
        return (dp - hoje).days  # negativo = atrasado (prioridade máxima)

    contratos_db = sorted(contratos_db, key=_urgencia_key)

    clientes_gwm, clientes_lm = [], []
    for c in contratos_db:
        base = {
            "cliente_cpf_cnpj":      c.cliente_cpf_cnpj,
            "cliente_nome":          c.cliente_nome,
            "cliente_email":         c.cliente_email,
            "byetech_contrato_id":   c.byetech_contrato_id,
            "veiculo":               c.veiculo,
            "placa":                 c.placa,
            "data_prevista_entrega": c.data_prevista_entrega,
        }
        if c.fonte in ("GWM", "SIGN & DRIVE"):
            clientes_gwm.append(base)
        else:
            clientes_lm.append(base)

    # Aplica limite por fonte se definido
    total_gwm = len(clientes_gwm)
    total_lm  = len(clientes_lm)
    if max_por_fonte:
        clientes_gwm = clientes_gwm[:max_por_fonte]
        clientes_lm  = clientes_lm[:max_por_fonte]

    logger.info(
        f"🔍 Validação GWM/LM — GWM: {len(clientes_gwm)}/{total_gwm} "
        f"| LM: {len(clientes_lm)}/{total_lm}"
        + (f" (limite={max_por_fonte})" if max_por_fonte else " (sem limite)")
    )

    # ── 2. Scrape portal para cada fonte ──────────────────
    for fonte, clientes in [("GWM", clientes_gwm), ("LM", clientes_lm)]:
        if not clientes:
            continue
        try:
            logger.info(f"🔍 Scraping {fonte} — {len(clientes)} clientes...")
            set_sync_state(message=f"Validação GWM/LM: scraping {fonte} ({len(clientes)} contratos urgentes)...")
            resultados = await scrape_portaldealer(clientes, fonte)

            async with SessionLocal() as session:
                for r in resultados:
                    if r.get("erro"):
                        continue
                    if fonte == "LM":
                        r["fonte"] = "LM"

                    # Calcula o ID do contrato para buscar status anterior
                    fonte_r = r.get("fonte", fonte)
                    cid = _contrato_id(fonte_r, r.get("id_externo", ""), r.get("cliente_cpf_cnpj", ""))
                    res_q = await session.execute(select(Contrato).where(Contrato.id == cid))
                    c_db   = res_q.scalar_one_or_none()
                    status_before = c_db.status_atual if c_db else None

                    # Detecta e processa entrega
                    if r.get("entregue"):
                        await _aplicar_entrega_portal(r)
                        resultado["entregues"].append({
                            "fonte":        fonte,
                            "cliente_nome": r.get("cliente_nome", "—"),
                            "veiculo":      r.get("veiculo", "—"),
                            "placa":        r.get("placa", ""),
                            "data_entrega": r.get("data_entrega_definitiva"),
                        })

                    # Upsert — portal só atualiza status, Byetech é fonte de verdade p/ dados do cliente
                    changed = await _upsert_contrato(session, r, portal_update=True)
                    if changed and not r.get("entregue"):
                        novo_status = r.get("status_atual", "")
                        mudanca = {
                            "fonte":           fonte,
                            "cliente_nome":    r.get("cliente_nome", "—"),
                            "veiculo":         r.get("veiculo", "—"),
                            "status_anterior": status_before or "—",
                            "status_novo":     novo_status,
                            "byetech_ok":      None,
                        }
                        # Sincroniza fase no Byetech
                        cpf = r.get("cliente_cpf_cnpj", "")
                        if cpf and novo_status:
                            try:
                                from app.scrapers.byetech_crm import update_phase_by_cpf
                                ok, msg = await update_phase_by_cpf(cpf_raw=cpf, novo_status=novo_status)
                                mudanca["byetech_ok"] = ok
                                mudanca["byetech_msg"] = msg
                                if not ok:
                                    resultado["erros"].append(
                                        f"Byetech {r.get('cliente_nome','?')} ({fonte}): {msg}"
                                    )
                                    logger.warning(f"[Byetech] Fase não atualizada para {cpf[:6]}…: {msg}")
                            except Exception as e:
                                mudanca["byetech_ok"] = False
                                mudanca["byetech_msg"] = str(e)[:80]
                                resultado["erros"].append(
                                    f"Byetech {r.get('cliente_nome','?')}: {e}"
                                )
                        resultado["mudancas_status"].append(mudanca)

                await session.commit()
            logger.info(
                f"✅ {fonte}: {len(resultado['entregues'])} entregas | "
                f"{len(resultado['mudancas_status'])} mudanças"
            )
        except Exception as e:
            resultado["erros"].append(f"{fonte}: {e}")
            logger.error(f"❌ Erro {fonte}: {e}")

    # ── 3. Metabase — últimos N dias ──────────────────────
    set_sync_state(message=f"Validação GWM/LM: sincronizando Metabase (últimos {days_back} dias)...")
    today = date.today()
    for i in range(days_back - 1, -1, -1):
        dt     = today - timedelta(days=i)
        dt_str = str(dt)
        try:
            contratos_dia = await fetch_contracts_by_date(dt)
            novos = 0
            async with SessionLocal() as session:
                for c in contratos_dia:
                    cid = _contrato_id(
                        c.get("fonte", ""),
                        c.get("id_externo", ""),
                        c.get("cliente_cpf_cnpj", ""),
                    )
                    existe = await session.execute(
                        select(Contrato.id).where(Contrato.id == cid)
                    )
                    if not existe.scalar_one_or_none():
                        novos += 1
                    # Metabase: cria contrato novo com todos os campos; existente só atualiza status/datas
                    await _upsert_contrato(session, c, portal_update=True)
                await session.commit()

            resultado["novas_vendas_por_dia"][dt_str] = {
                "total": len(contratos_dia),
                "novos": novos,
            }
            logger.info(f"📊 Metabase {dt_str}: {len(contratos_dia)} contratos, {novos} novos")
        except Exception as e:
            resultado["erros"].append(f"Metabase {dt_str}: {e}")
            logger.error(f"❌ Metabase {dt_str}: {e}")

    logger.info(
        f"✅ Validação concluída — {len(resultado['entregues'])} entregas | "
        f"{len(resultado['mudancas_status'])} mudanças de status"
    )
    return resultado


async def run_signanddrive_sync(
    fontes: list[str] | None = None,
) -> dict:
    """
    Valida contratos no portal Sign & Drive (vwsignanddrive.com.br).
    fontes: lista de valores do campo 'fonte' no banco a incluir.
            Padrao: ['SIGN & DRIVE', 'VW', 'GWM']
    - Para entregues: atualiza data + status no banco e no Byetech CRM.
    - Grava observacoes com status atual do portal em todos os contratos.
    Retorna {entregues, mudancas_status, sem_pedido, erros}.
    """
    from app.scrapers.signanddrive import scrape_signanddrive
    from sqlalchemy import and_

    if fontes is None:
        fontes = ["SIGN & DRIVE", "VW", "GWM"]

    resultado: dict = {
        "entregues":       [],
        "mudancas_status": [],
        "sem_pedido":      [],
        "erros":           [],
    }

    async with SessionLocal() as session:
        res = await session.execute(
            select(Contrato).where(
                and_(
                    Contrato.fonte.in_(fontes),
                    Contrato.data_entrega_definitiva.is_(None),
                )
            )
        )
        contratos_db = res.scalars().all()

    if not contratos_db:
        logger.info("Sign & Drive: nenhum contrato pendente no banco")
        return resultado

    import re as _re

    def _d(s):
        return _re.sub(r"\D", "", s or "")

    # Indice CPF -> contrato existente (lookup por CPF, nao por ID gerado)
    cpf_to_contrato: dict[str, Contrato] = {}
    for c in contratos_db:
        d = _d(c.cliente_cpf_cnpj)
        for v in {d, d.zfill(11), d[:-1] if len(d)==12 else d,
                  ("0"+d[:-1]) if (len(d)==11 and d.endswith("0")) else d}:
            cpf_to_contrato.setdefault(v, c)

    clientes = [
        {
            "cliente_cpf_cnpj":      c.cliente_cpf_cnpj,
            "cliente_nome":          c.cliente_nome,
            "byetech_contrato_id":   c.byetech_contrato_id,
            "veiculo":               c.veiculo,
            "placa":                 c.placa,
            "data_prevista_entrega": c.data_prevista_entrega,
            "_contrato_id_db":       c.id,          # preserva ID original do banco
            "_id_externo_db":        c.id_externo,  # preserva id_externo original
        }
        for c in contratos_db
    ]

    logger.info(f"Sign & Drive: consultando {len(clientes)} contratos pendentes...")
    set_sync_state(message=f"Sign & Drive: consultando {len(clientes)} contratos no portal...")

    try:
        resultados = await scrape_signanddrive(clientes)
    except Exception as e:
        resultado["erros"].append(str(e))
        logger.error(f"Sign & Drive scrape erro: {e}")
        return resultado

    async with SessionLocal() as session:
        for r in resultados:
            if r.get("erro"):
                resultado["sem_pedido"].append(r.get("cliente_nome", "?"))
                continue

            # Lookup por CPF para encontrar o contrato original do banco
            cpf_raw = r.get("cliente_cpf_cnpj", "")
            c_orig = cpf_to_contrato.get(_d(cpf_raw))
            if not c_orig:
                for v in {_d(cpf_raw), _d(cpf_raw).zfill(11)}:
                    c_orig = cpf_to_contrato.get(v)
                    if c_orig:
                        break

            # Usa o ID/id_externo/fonte original para que _upsert_contrato atualize o registro certo
            if c_orig:
                r["id_externo"] = c_orig.id_externo
                r["byetech_contrato_id"] = c_orig.byetech_contrato_id
                r["fonte"] = c_orig.fonte  # GWM/VW/SIGN & DRIVE — deve bater com o banco

            fonte_r = r.get("fonte", "SIGN & DRIVE")
            cid = _contrato_id(fonte_r, r.get("id_externo", ""), cpf_raw)
            res_q = await session.execute(select(Contrato).where(Contrato.id == cid))
            c_db  = res_q.scalar_one_or_none()
            status_before = c_db.status_atual if c_db else None

            if r.get("entregue"):
                await _aplicar_entrega_portal(r)
                resultado["entregues"].append({
                    "cliente_nome": r.get("cliente_nome", "—"),
                    "placa":        r.get("placa", ""),
                    "veiculo":      r.get("veiculo", "—"),
                    "data_entrega": r.get("data_entrega_definitiva"),
                })

            changed = await _upsert_contrato(session, r, portal_update=True)

            # Grava status do portal em observacoes (no contrato original do banco)
            hoje_str = datetime.now().strftime("%d/%m/%Y")
            status_portal = r.get("status_atual", "")
            de = r.get("data_entrega_definitiva")
            if r.get("entregue") and de:
                de_fmt = de.strftime("%d/%m/%Y") if hasattr(de, "strftime") else str(de)[:10]
                obs = f"Portal {hoje_str}: Entregue em {de_fmt}"
            elif status_portal:
                obs = f"Portal {hoje_str}: {status_portal}"
            else:
                obs = None

            if obs and c_db:
                c_db.observacoes = obs
            elif obs and c_orig:
                # Fallback: atualiza direto via SQL pelo ID original
                await session.execute(
                    select(Contrato).where(Contrato.id == c_orig.id)
                )
                res_orig = await session.execute(
                    select(Contrato).where(Contrato.id == c_orig.id)
                )
                c_orig_db = res_orig.scalar_one_or_none()
                if c_orig_db:
                    c_orig_db.observacoes = obs

            if changed and not r.get("entregue"):
                novo_status = r.get("status_atual", "")
                mudanca = {
                    "cliente_nome":    r.get("cliente_nome", "—"),
                    "status_anterior": status_before or "—",
                    "status_novo":     novo_status,
                }
                cpf = cpf_raw
                if cpf and novo_status:
                    try:
                        from app.scrapers.byetech_crm import update_phase_by_cpf
                        ok, msg = await update_phase_by_cpf(cpf_raw=cpf, novo_status=novo_status)
                        mudanca["byetech_ok"]  = ok
                        mudanca["byetech_msg"] = msg
                        if not ok:
                            resultado["erros"].append(
                                f"Byetech fase {r.get('cliente_nome','?')}: {msg}"
                            )
                    except Exception as e:
                        mudanca["byetech_ok"]  = False
                        mudanca["byetech_msg"] = str(e)[:80]
                resultado["mudancas_status"].append(mudanca)

        await session.commit()

    logger.info(
        f"Sign & Drive sync: {len(resultado['entregues'])} entregas | "
        f"{len(resultado['mudancas_status'])} mudancas | "
        f"{len(resultado['sem_pedido'])} sem pedido"
    )
    return resultado


async def run_metabase_sync(full: bool = False) -> dict:
    """
    Sincroniza contratos do Metabase.
    full=True → busca todos os contratos ativos (bootstrap).
    full=False → busca contratos de hoje + ontem (novas vendas do dia anterior incluídas).
    """
    from app.scrapers.metabase import fetch_all_active, fetch_contracts_by_date
    from datetime import date, timedelta

    logger.info(f"📊 Metabase sync ({'completo' if full else 'diário'})...")

    if full:
        contratos = await fetch_all_active()
    else:
        today = date.today()
        yesterday = today - timedelta(days=1)
        logger.info(f"  → Buscando vendas de hoje ({today}) e ontem ({yesterday})...")
        contratos_hoje  = await fetch_contracts_by_date(today)
        contratos_ontem = await fetch_contracts_by_date(yesterday)
        # Merge, deduplica por id_externo (o de hoje prevalece sobre o de ontem)
        seen = {}
        for c in contratos_ontem + contratos_hoje:
            key = c.get("id_externo") or c.get("cliente_cpf_cnpj") or id(c)
            seen[key] = c
        contratos = list(seen.values())
        logger.info(f"  → {len(contratos_hoje)} hoje | {len(contratos_ontem)} ontem | {len(contratos)} únicos")

    importados = 0
    async with SessionLocal() as session:
        for c in contratos:
            # Metabase: cria contrato novo com todos os campos; existente só atualiza status/datas
            await _upsert_contrato(session, c, portal_update=True)
            importados += 1
        await session.commit()

    logger.info(f"✅ Metabase sync: {importados} contratos processados")
    return {"importados": importados}
