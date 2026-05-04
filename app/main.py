import sys
import asyncio

# Playwright precisa do ProactorEventLoop no Windows para abrir subprocessos
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, date
from typing import Optional

from fastapi import FastAPI, HTTPException, UploadFile, File, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from pydantic import BaseModel
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import init_db, get_db, SessionLocal, Contrato, HistoricoStatus, AlertaEnviado
from app.services.scheduler import start_scheduler
from app.services.sync_service import (
    get_sync_state, run_full_sync, provide_twofa, set_sync_state,
    run_metabase_sync, run_gwm_lm_validation, run_signanddrive_sync,
)
from app.scrapers.movida import parse_movida_spreadsheet, get_unmapped_columns
from app.services.email_service import send_unidas_confirmation

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    start_scheduler()
    # Sync Metabase no startup (pega contratos de hoje e ontem caso o job das 09:00 tenha sido perdido)
    asyncio.create_task(_startup_metabase_sync())
    logger.info("🚀 Byetech Entregas iniciado")
    yield
    logger.info("Encerrando...")


async def _startup_metabase_sync():
    """Roda Metabase sync no startup para garantir dados atualizados."""
    import asyncio as _asyncio
    await _asyncio.sleep(3)  # aguarda o servidor inicializar
    try:
        result = await run_metabase_sync(full=False)
        logger.info(f"[startup] Metabase sync: {result['importados']} contratos")
    except Exception as e:
        logger.error(f"[startup] Metabase sync erro: {e}")


app = FastAPI(title="Byetech Entregas", lifespan=lifespan)

# Static files e templates
BASE_DIR = os.path.dirname(__file__)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


# ── Frontend ─────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request, "index.html")


# ── API: Contratos ────────────────────────────────────────
@app.get("/api/contratos")
async def get_contratos(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Contrato)
        .where(Contrato.data_entrega_definitiva.is_(None))
        .order_by(Contrato.data_prevista_entrega.asc().nullslast())
    )
    contratos = result.scalars().all()

    # Recalcula dias
    hoje = datetime.now().date()
    total = len(contratos)
    atrasados = 0
    criticos = 0
    unidas = 0

    contratos_list = []
    for c in contratos:
        dias = None
        if c.data_prevista_entrega:
            dp = c.data_prevista_entrega
            if isinstance(dp, datetime):
                dp = dp.date()
            dias = (dp - hoje).days
            c.dias_para_entrega = dias
            c.atrasado = dias < 0

        if c.atrasado:
            atrasados += 1
        elif dias is not None and dias <= 5:
            criticos += 1
        if c.fonte == "UNIDAS":
            unidas += 1

        contratos_list.append(_contrato_to_dict(c))

    # Última sync
    sync_state = get_sync_state()

    return {
        "contratos": contratos_list,
        "stats": {
            "total": total,
            "atrasados": atrasados,
            "criticos": criticos,
            "unidas": unidas,
        },
        "ultima_sync": sync_state.get("iniciado_em"),
    }


@app.get("/api/contratos/{contrato_id}")
async def get_contrato(contrato_id: str, db: AsyncSession = Depends(get_db)):
    import json, re as _re
    result = await db.execute(select(Contrato).where(Contrato.id == contrato_id))
    c = result.scalar_one_or_none()
    if not c:
        raise HTTPException(404, "Contrato não encontrado")

    hist_result = await db.execute(
        select(HistoricoStatus)
        .where(HistoricoStatus.contrato_id == contrato_id)
        .order_by(HistoricoStatus.registrado_em.desc())
        .limit(20)
    )
    historico = hist_result.scalars().all()

    # pedido_id_locadora: usa o valor salvo no banco (vem do Metabase diretamente)
    # Fallback: busca no mapa CPF Byetech se o banco não tiver
    byetech_pedido_id = c.pedido_id_locadora
    if not byetech_pedido_id:
        import json as _json2, re as _re2
        cpf_map_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".byetech_cpf_map.json")
        if os.path.exists(cpf_map_file):
            try:
                with open(cpf_map_file, encoding="utf-8") as f:
                    cpf_map = _json2.load(f)
                cpf_raw2 = _re2.sub(r"[^\d]", "", c.cliente_cpf_cnpj or "")
                if len(cpf_raw2) == 12 and cpf_raw2.endswith("0"):
                    cpf_norm2 = cpf_raw2[:-1]
                elif len(cpf_raw2) > 11:
                    cpf_norm2 = cpf_raw2[-11:]
                else:
                    cpf_norm2 = cpf_raw2.zfill(11)
                entry = cpf_map.get(cpf_norm2) or cpf_map.get(cpf_raw2) or cpf_map.get(cpf_norm2 + "0")
                if entry:
                    byetech_pedido_id = entry.get("pedido_id")
            except Exception:
                pass

    contrato_dict = _contrato_to_dict(c)
    contrato_dict["byetech_pedido_id"] = byetech_pedido_id

    return {
        "contrato": contrato_dict,
        "historico": [
            {
                "status_anterior": h.status_anterior,
                "status_novo": h.status_novo,
                "fonte": h.fonte,
                "registrado_em": h.registrado_em.isoformat() if h.registrado_em else None,
            }
            for h in historico
        ],
    }


class EntregarBody(BaseModel):
    data_entrega: str  # YYYY-MM-DD


@app.post("/api/contratos/{contrato_id}/entregar")
async def marcar_entregue(contrato_id: str, body: EntregarBody, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Contrato).where(Contrato.id == contrato_id))
    c = result.scalar_one_or_none()
    if not c:
        raise HTTPException(404, "Contrato não encontrado")

    try:
        data = datetime.strptime(body.data_entrega, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "Formato de data inválido. Use YYYY-MM-DD")

    status_anterior = c.status_atual          # captura ANTES de alterar
    c.data_entrega_definitiva = data
    c.status_atual = "Definitivo Entregue"
    c.ultima_atualizacao = datetime.utcnow()

    hist = HistoricoStatus(
        contrato_id=contrato_id,
        status_anterior=status_anterior,
        status_novo="Definitivo Entregue",
        fonte="MANUAL",
    )
    db.add(hist)
    await db.commit()

    # ── Espelha entrega no Lovable Cloud ──
    try:
        from app.services.lovable_client import marcar_entregue as _lv_entrega
        _lv_entrega(contrato_id, data)
    except Exception as _lv_err:
        logger.debug(f"[Lovable] marcar_entregue ignorado: {_lv_err}")

    # ── Byetech: atualiza de forma síncrona com feedback real ──
    byetech_status = "sem_cpf"
    byetech_msg    = "Contrato sem CPF — não foi possível atualizar o Byetech"

    if c.cliente_cpf_cnpj:
        from app.scrapers.byetech_crm import (
            update_delivery_by_cpf, _load_session_from_disk, _test_session
        )
        placa = c.placa if c.placa and str(c.placa).lower() not in ("nan", "n/a", "") else None

        cookies = _load_session_from_disk()
        if not cookies or not await _test_session(cookies):
            byetech_status = "sessao_expirada"
            byetech_msg    = "Sessão Byetech expirada — entrega enfileirada para retry"
            _queue_byetech_update(contrato_id, data, c.cliente_cpf_cnpj, placa)
        else:
            try:
                ok = await asyncio.wait_for(
                    update_delivery_by_cpf(
                        cpf_raw=c.cliente_cpf_cnpj, data_entrega=data, placa=placa
                    ),
                    timeout=20.0,
                )
                if ok:
                    byetech_status = "ok"
                    byetech_msg    = "Byetech atualizado com sucesso"
                else:
                    byetech_status = "erro"
                    byetech_msg    = "PATCH retornou falha — verifique manualmente no Byetech"
                    _queue_byetech_update(contrato_id, data, c.cliente_cpf_cnpj, placa)
            except asyncio.TimeoutError:
                byetech_status = "timeout"
                byetech_msg    = "Byetech demorou (timeout) — atualização enfileirada"
                _queue_byetech_update(contrato_id, data, c.cliente_cpf_cnpj, placa)
            except Exception as e:
                err = str(e)
                if "expirada" in err.lower() or "2FA" in err or "401" in err:
                    byetech_status = "sessao_expirada"
                    byetech_msg    = "Sessão expirada — entrega enfileirada para retry"
                else:
                    byetech_status = "erro"
                    byetech_msg    = err[:120]
                _queue_byetech_update(contrato_id, data, c.cliente_cpf_cnpj, placa)

    return {
        "ok":             True,
        "data_entrega":   body.data_entrega,
        "byetech_status": byetech_status,
        "byetech_msg":    byetech_msg,
    }


# ── Fila de atualizações Byetech pendentes (persistida em disco) ──────────
import json as _json

_PENDING_FILE = os.getenv("PENDING_FILE", os.path.join(os.path.dirname(__file__), "..", ".byetech_pending.json"))


def _load_pending() -> list[dict]:
    try:
        if os.path.exists(_PENDING_FILE):
            with open(_PENDING_FILE, encoding="utf-8") as f:
                items = _json.load(f)
            # Reconverte data de string para datetime
            for item in items:
                if isinstance(item.get("data"), str):
                    item["data"] = datetime.fromisoformat(item["data"])
            return items
    except Exception:
        pass
    return []


def _save_pending(items: list[dict]):
    try:
        serializable = []
        for item in items:
            d = dict(item)
            if isinstance(d.get("data"), datetime):
                d["data"] = d["data"].isoformat()
            serializable.append(d)
        with open(_PENDING_FILE, "w", encoding="utf-8") as f:
            _json.dump(serializable, f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"[Byetech] Não foi possível salvar fila pendente: {e}")


def _queue_byetech_update(db_contrato_id: str, data: datetime,
                          cpf: str, placa: Optional[str]):
    """Enfileira uma atualização Byetech persistindo em disco."""
    items = _load_pending()
    items = [p for p in items if p["db_id"] != db_contrato_id]  # remove duplicata
    items.append({"db_id": db_contrato_id, "data": data, "cpf": cpf, "placa": placa})
    _save_pending(items)
    logger.warning(f"[Byetech] {db_contrato_id} enfileirado ({len(items)} pendente(s))")


def _get_pending_count() -> int:
    return len(_load_pending())


async def _flush_byetech_pending():
    """Processa todas as atualizações pendentes após sync com sessão válida."""
    items = _load_pending()
    if not items:
        return
    from app.scrapers.byetech_crm import update_delivery_by_cpf
    logger.info(f"[Byetech] Processando {len(items)} pendente(s)...")
    done = []
    for item in list(items):
        try:
            ok = await update_delivery_by_cpf(
                cpf_raw=item["cpf"],
                data_entrega=item["data"] if isinstance(item["data"], datetime)
                             else datetime.fromisoformat(item["data"]),
                placa=item.get("placa"),
            )
            if ok:
                done.append(item["db_id"])
                logger.info(f"[Byetech] Pendente OK: {item['db_id']}")
            else:
                logger.error(f"[Byetech] Pendente falhou: {item['db_id']}")
        except Exception as e:
            logger.error(f"[Byetech] Erro pendente {item['db_id']}: {e}")
    # Salva apenas os que ainda falharam
    items = [p for p in items if p["db_id"] not in done]
    _save_pending(items)
    if done:
        logger.info(f"[Byetech] {len(done)} pendente(s) processado(s) com sucesso")


async def _update_byetech_crm(db_contrato_id: str, data: datetime):
    """
    Atualiza entrega_definitivo no Byetech CRM.
    Se sessão expirada, enfileira para retry no próximo sync.
    """
    try:
        from app.scrapers.byetech_crm import update_delivery_by_cpf

        async with SessionLocal() as s:
            res = await s.execute(select(Contrato).where(Contrato.id == db_contrato_id))
            c = res.scalar_one_or_none()
        if not c:
            logger.error(f"[Byetech] Contrato {db_contrato_id} não encontrado no banco")
            return
        if not c.cliente_cpf_cnpj:
            logger.error(f"[Byetech] Contrato {db_contrato_id} sem CPF — impossível atualizar")
            return

        placa = c.placa if c.placa and str(c.placa).lower() not in ("nan", "n/a", "") else None
        try:
            ok = await update_delivery_by_cpf(
                cpf_raw=c.cliente_cpf_cnpj,
                data_entrega=data,
                placa=placa,
            )
        except Exception as e:
            err = str(e)
            if "2FA_REQUIRED" in err or "expirada" in err.lower() or "401" in err:
                logger.warning(f"[Byetech] Sessão expirada — {db_contrato_id} enfileirado para retry")
                _queue_byetech_update(db_contrato_id, data, c.cliente_cpf_cnpj, placa)
            else:
                raise
            return

        if ok:
            logger.info(f"✅ [Byetech] {c.cliente_nome} → Definitivo Entregue em {data.date()}")
        else:
            logger.error(f"[Byetech] Falha ao atualizar {db_contrato_id}")
            _queue_byetech_update(db_contrato_id, data, c.cliente_cpf_cnpj, placa)
    except Exception as e:
        import traceback
        logger.error(f"[Byetech] Erro ao atualizar {db_contrato_id}: {e}\n{traceback.format_exc()}")


@app.post("/api/contratos/{contrato_id}/email-unidas")
async def enviar_email_unidas(
    contrato_id: str,
    db: AsyncSession = Depends(get_db),
    test_email: str | None = None,
):
    result = await db.execute(select(Contrato).where(Contrato.id == contrato_id))
    c = result.scalar_one_or_none()
    if not c:
        raise HTTPException(404, "Contrato não encontrado")

    destinatario = test_email or c.cliente_email
    if not destinatario:
        raise HTTPException(400, "E-mail do cliente não cadastrado. Use ?test_email= para teste.")

    data_fmt = ""
    if c.data_prevista_entrega:
        dp = c.data_prevista_entrega
        if isinstance(dp, datetime):
            data_fmt = dp.strftime("%d/%m/%Y")

    await send_unidas_confirmation(
        cliente_email=destinatario,
        cliente_nome=c.cliente_nome or "",
        veiculo=c.veiculo or "",
        data_prevista=data_fmt,
        contrato_id=c.id,
    )

    return {"ok": True, "enviado_para": destinatario, "teste": test_email is not None}


# ── Confirmação pública de entrega (link no e-mail) ──────
@app.get("/confirmar/{contrato_id}", response_class=HTMLResponse)
async def confirmar_page(contrato_id: str, request: Request, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Contrato).where(Contrato.id == contrato_id))
    c = result.scalar_one_or_none()
    if not c:
        raise HTTPException(404, "Contrato não encontrado")

    import os as _os
    wpp_num = _os.getenv("BYECAR_WHATSAPP", "5511999999999")
    import urllib.parse
    wpp_msg = urllib.parse.quote(
        f"Olá! Sou cliente Byecar e gostaria de falar com o pós-venda sobre o veículo {c.veiculo or ''}."
    )
    wpp_url = f"https://wa.me/{wpp_num}?text={wpp_msg}"

    ja_confirmado = bool(c.data_entrega_definitiva)
    data_confirmada = ""
    if ja_confirmado:
        de = c.data_entrega_definitiva
        if isinstance(de, datetime):
            data_confirmada = de.strftime("%d/%m/%Y")
        else:
            data_confirmada = str(de)

    return templates.TemplateResponse(request, "confirmar_entrega.html", {
        "contrato_id":   contrato_id,
        "cliente_nome":  c.cliente_nome or "Cliente",
        "primeiro_nome": (c.cliente_nome or "Cliente").split()[0],
        "veiculo":       c.veiculo or "",
        "whatsapp_url":  wpp_url,
        "hoje":          date.today().isoformat(),
        "ja_confirmado": ja_confirmado,
        "data_confirmada": data_confirmada,
    })


class ConfirmarBody(BaseModel):
    data_entrega: str  # YYYY-MM-DD


@app.post("/confirmar/{contrato_id}")
async def confirmar_entrega(contrato_id: str, body: ConfirmarBody, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Contrato).where(Contrato.id == contrato_id))
    c = result.scalar_one_or_none()
    if not c:
        raise HTTPException(404, "Contrato não encontrado")

    if c.data_entrega_definitiva:
        return {"ok": True, "ja_confirmado": True}

    try:
        data = datetime.strptime(body.data_entrega, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "Formato de data inválido")

    status_anterior = c.status_atual            # captura ANTES de alterar
    c.data_entrega_definitiva = data
    c.status_atual = "Definitivo Entregue"
    c.ultima_atualizacao = datetime.utcnow()

    hist = HistoricoStatus(
        contrato_id=contrato_id,
        status_anterior=status_anterior,
        status_novo="Definitivo Entregue",
        fonte="CLIENTE",
    )
    db.add(hist)
    await db.commit()

    logger.info(f"✅ Entrega confirmada pelo cliente: {c.cliente_nome} ({contrato_id}) em {data.date()}")

    # Atualiza Byetech CRM em background usando o ID do banco
    asyncio.create_task(_update_byetech_crm(contrato_id, data))

    return {"ok": True, "data_entrega": body.data_entrega}


# ── API: Resumo de entregas do dia ───────────────────────
@app.get("/api/entregas-hoje")
async def entregas_hoje(db: AsyncSession = Depends(get_db)):
    """Retorna todos os contratos marcados como entregues hoje."""
    hoje_inicio = datetime.combine(date.today(), datetime.min.time())
    hoje_fim    = datetime.combine(date.today(), datetime.max.time())

    result = await db.execute(
        select(Contrato).where(
            and_(
                Contrato.data_entrega_definitiva >= hoje_inicio,
                Contrato.data_entrega_definitiva <= hoje_fim,
            )
        ).order_by(Contrato.data_entrega_definitiva.asc())
    )
    contratos = result.scalars().all()

    return {
        "total": len(contratos),
        "data": date.today().isoformat(),
        "entregas": [
            {
                "id": c.id,
                "cliente_nome": c.cliente_nome,
                "veiculo": c.veiculo,
                "placa": c.placa,
                "fonte": c.fonte,
                "data_entrega": c.data_entrega_definitiva.isoformat() if c.data_entrega_definitiva else None,
            }
            for c in contratos
        ],
    }


# ── API: Sync ─────────────────────────────────────────────
@app.post("/api/sync")
async def trigger_sync():
    state = get_sync_state()
    if state["status"] in ("running", "needs_2fa"):
        return {"ok": False, "message": "Sync já em andamento", "needs_2fa": False}

    # Roda em background — popup de 2FA aparece quando status mudar para "needs_2fa"
    async def _sync_and_flush():
        await run_full_sync()
        # Após sync bem-sucedido, tenta processar pendentes do Byetech
        await _flush_byetech_pending()

    asyncio.create_task(_sync_and_flush())
    return {"ok": True, "needs_2fa": False, "message": "Sync iniciada — aguarde o popup de 2FA"}


class TwoFABody(BaseModel):
    code: str


@app.post("/api/sync/2fa")
async def submit_2fa(body: TwoFABody):
    global _twofa_renovar_code
    # Notifica o sync_service (orquestrador)
    await provide_twofa(body.code)
    # Notifica o scraper do Byetech CRM
    from app.scrapers.byetech_crm import provide_twofa_code as byetech_2fa
    await byetech_2fa(body.code)
    # Notifica o fluxo de renovação de sessão
    _twofa_renovar_code = body.code
    _twofa_renovar_event.set()
    return {"ok": True}


@app.get("/api/sync/status")
async def sync_status():
    state = get_sync_state()
    state["byetech_pending"] = _get_pending_count()
    return state


@app.get("/api/byetech/sessao-ok")
async def byetech_sessao_ok():
    """Verifica se a sessão Byetech em disco ainda é válida (chamada leve)."""
    from app.scrapers.byetech_crm import _load_session_from_disk, _test_session
    cookies = _load_session_from_disk()
    if not cookies:
        return {"ok": False, "motivo": "sem_sessao"}
    ok = await _test_session(cookies)
    return {"ok": ok, "motivo": None if ok else "expirada"}


@app.post("/api/sync/reset-session")
async def reset_byetech_session():
    """Limpa a sessão cacheada do Byetech — próxima sync vai pedir 2FA."""
    from app.scrapers.byetech_crm import clear_session
    clear_session()
    return {"ok": True, "message": "Sessão do Byetech CRM removida — próximo sync pedirá 2FA"}


@app.post("/api/sync/renovar-sessao")
async def renovar_sessao_byetech():
    """
    Renova apenas a sessão Byetech (login + 2FA), sem fazer scraping completo.
    Após login bem-sucedido, processa pendentes.
    """
    state = get_sync_state()
    if state["status"] in ("running", "needs_2fa"):
        return {"ok": False, "message": "Já há uma operação em andamento"}

    async def _renovar():
        set_sync_state(status="needs_2fa", message="Aguardando código 2FA...", system="Byetech CRM")
        try:
            async def _2fa_cb():
                return await _wait_twofa_renovar()

            from app.scrapers.byetech_crm import get_session, clear_session
            clear_session()
            cookies = await get_session(twofa_callback=_2fa_cb)
            set_sync_state(status="running", message="Sessão renovada! Processando pendentes...")
            logger.info("[Byetech] Sessão renovada com sucesso")
            await _flush_byetech_pending()
            pending = _get_pending_count()
            msg = "Sessão Byetech renovada!" + (f" {pending} pendente(s) restante(s)." if pending else " Todos os pendentes processados.")
            set_sync_state(status="done", message=msg, atualizados=0)
        except TimeoutError:
            set_sync_state(status="error", message="Tempo esgotado aguardando o código 2FA (5 min). Tente novamente e insira o código mais rápido.")
            logger.error("[Byetech] Timeout aguardando 2FA para renovação")
        except Exception as e:
            set_sync_state(status="error", message=f"Erro ao renovar sessão: {e}")
            logger.error(f"[Byetech] Erro ao renovar sessão: {e}")

    asyncio.create_task(_renovar())
    return {"ok": True, "message": "Renovação iniciada — aguarde o código 2FA"}


# Evento 2FA reutilizável para renovação de sessão
_twofa_renovar_event = asyncio.Event()
_twofa_renovar_code: Optional[str] = None


async def _wait_twofa_renovar() -> str:
    global _twofa_renovar_code
    _twofa_renovar_event.clear()
    await asyncio.wait_for(_twofa_renovar_event.wait(), timeout=300)
    code = _twofa_renovar_code
    _twofa_renovar_code = None
    return code


# ── API: Movida ───────────────────────────────────────────
@app.post("/api/movida/preview")
async def movida_preview(file: UploadFile = File(...)):
    content = await file.read()
    try:
        info = get_unmapped_columns(content, file.filename or "file.xlsx")
        contratos = parse_movida_spreadsheet(content, file.filename or "file.xlsx")
        return {
            **info,
            "total": len(contratos),
        }
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/movida/import")
async def movida_import(file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    content = await file.read()
    try:
        contratos = parse_movida_spreadsheet(content, file.filename or "file.xlsx")
    except Exception as e:
        raise HTTPException(400, str(e))

    importados = 0
    for c in contratos:
        from app.services.sync_service import _upsert_contrato
        await _upsert_contrato(db, c)
        importados += 1

    await db.commit()
    return {"ok": True, "importados": importados}


# ── API: Metabase ─────────────────────────────────────────
@app.post("/api/metabase/sync")
async def metabase_sync(full: bool = False):
    """
    Sincroniza contratos do Metabase.
    ?full=true → todos os ativos (bootstrap inicial).
    ?full=false → apenas contratos do dia (padrão diário).
    """
    try:
        result = await run_metabase_sync(full=full)
        return {"ok": True, **result}
    except Exception as e:
        raise HTTPException(500, str(e))


# ── API: Relatório completo manual ───────────────────────
@app.post("/api/slack/relatorio-completo")
async def slack_relatorio_completo(dias_vendas: int = 5, dias_entregas: int = 7):
    """Dispara o relatório completo no Slack imediatamente."""
    from app.services.slack_service import send_relatorio_completo
    try:
        await send_relatorio_completo(dias_vendas=dias_vendas, dias_entregas=dias_entregas)
        return {"ok": True, "message": "Relatório enviado no Slack!"}
    except Exception as e:
        raise HTTPException(500, str(e))


# ── API: Validação GWM / LM ──────────────────────────────
@app.post("/api/sync/validar-gwm-lm")
async def validar_gwm_lm(days: int = 4, max_por_fonte: Optional[int] = None):
    """
    Valida entregas GWM/LM (contratos mais urgentes) + sincroniza Metabase N dias.
    Roda em background. Acompanhe via /api/sync/status ou aguarde o relatório no Slack.
    max_por_fonte: limita clientes por fonte, priorizando os mais urgentes (default 80).
    """
    state = get_sync_state()
    if state.get("status") == "running":
        raise HTTPException(409, "Já existe uma operação em andamento — aguarde.")

    async def _run():
        from app.services.slack_service import send_validation_report
        set_sync_state(
            status="running",
            message="Validação GWM/LM: iniciando...",
            atualizados=0,
            iniciado_em=datetime.utcnow().isoformat(),
        )
        try:
            resultado = await run_gwm_lm_validation(days_back=days, max_por_fonte=max_por_fonte)
            n_ent = len(resultado.get("entregues", []))
            n_mud = len(resultado.get("mudancas_status", []))
            n_nov = sum(d.get("novos", 0) for d in resultado.get("novas_vendas_por_dia", {}).values())
            set_sync_state(
                status="done",
                message=(
                    f"Validação concluída — {n_ent} entregue(s) | "
                    f"{n_mud} mudança(s) de status | {n_nov} nova(s) venda(s)"
                ),
                atualizados=n_ent + n_mud,
            )
            await send_validation_report(resultado)
        except Exception as e:
            set_sync_state(status="error", message=str(e))
            logger.error(f"Erro validar-gwm-lm: {e}")

    asyncio.create_task(_run())
    limite_txt = f"{max_por_fonte} contratos por fonte" if max_por_fonte else "todos os contratos (sem limite)"
    return {
        "ok": True,
        "message": f"Validação iniciada — {limite_txt}. Relatório chegará no Slack.",
    }


@app.post("/api/sync/validar-cpfs")
async def validar_cpfs_lista(body: dict):
    """
    Valida uma lista específica de CPFs no portal Sign&Drive/LM.
    Body: {"cpfs": ["12345678901", ...], "fonte": "GWM"}
    Roda em background, envia relatório no Slack ao terminar.
    """
    cpfs: list[str] = body.get("cpfs", [])
    fonte: str = body.get("fonte", "GWM")
    if not cpfs:
        raise HTTPException(400, "Lista de CPFs vazia")

    state = get_sync_state()
    if state.get("status") == "running":
        raise HTTPException(409, "Já existe uma operação em andamento — aguarde.")

    async def _run():
        from app.scrapers.portaldealer import scrape_portaldealer
        from app.services.slack_service import get_client, get_or_create_channel, FONTE_EMOJI
        import re as _re

        set_sync_state(status="running",
                       message=f"Validando {len(cpfs)} CPFs no portal {fonte}...",
                       atualizados=0,
                       iniciado_em=datetime.utcnow().isoformat())
        try:
            # Monta lista de clientes a partir dos CPFs
            async with SessionLocal() as session:
                from sqlalchemy import or_, func as sqlfunc
                result = await session.execute(
                    select(Contrato).where(
                        Contrato.cliente_cpf_cnpj.isnot(None)
                    )
                )
                todos = result.scalars().all()

            # Indexa por dígitos do CPF
            def _d(s): return _re.sub(r'\D','', s or '')
            idx = {}
            for c in todos:
                d = _d(c.cliente_cpf_cnpj)
                idx.setdefault(d, c)
                if len(d) == 12: idx.setdefault(d[:-1], c)
                elif len(d) == 10: idx.setdefault('0'+d, c)

            clientes = []
            for cpf_raw in cpfs:
                cpf = _d(cpf_raw)
                c = idx.get(cpf) or idx.get(cpf+'0') or idx.get(cpf.lstrip('0'))
                if c:
                    clientes.append({
                        "cliente_cpf_cnpj":    c.cliente_cpf_cnpj,
                        "cliente_nome":        c.cliente_nome,
                        "cliente_email":       c.cliente_email,
                        "byetech_contrato_id": c.byetech_contrato_id,
                        "veiculo":             c.veiculo,
                        "placa":               c.placa,
                        "data_prevista_entrega": c.data_prevista_entrega,
                    })
                else:
                    clientes.append({
                        "cliente_cpf_cnpj": cpf_raw,
                        "cliente_nome": cpf_raw,
                        "cliente_email": "",
                        "byetech_contrato_id": "",
                        "veiculo": "", "placa": "", "data_prevista_entrega": None,
                    })

            logger.info(f"[validar-cpfs] Scraping {len(clientes)} clientes no portal {fonte}...")
            resultados = await scrape_portaldealer(clientes, fonte)

            # Monta relatório
            entregues, mudancas, erros_lst = [], [], []
            hoje = datetime.utcnow().date()
            for r in resultados:
                if r.get("erro"):
                    erros_lst.append(f"{r.get('cliente_nome','?')}: {r['erro']}")
                    continue
                status = r.get("status_atual") or r.get("etapa_atual") or "—"
                entregue = r.get("entregue", False)
                if entregue:
                    entregues.append(r)
                mudancas.append({
                    "cliente_nome": r.get("cliente_nome","—"),
                    "cpf": r.get("cliente_cpf_cnpj",""),
                    "veiculo": r.get("veiculo","—"),
                    "placa": r.get("placa",""),
                    "status_portal": status,
                    "entregue": entregue,
                    "data_entrega": r.get("data_entrega_definitiva") or r.get("data_ultima_etapa"),
                })

            # Envia Slack
            channel = await get_or_create_channel()
            client  = get_client()
            hoje_str = datetime.now().strftime("%d/%m/%Y %H:%M")
            fe = FONTE_EMOJI.get(fonte, "🚗")

            header = f"*{fe} Validação Portal {fonte} — {len(cpfs)} CPFs — {hoje_str}*\n"
            header += f"Entregues: *{len(entregues)}* | Verificados: *{len(mudancas)}* | Erros: *{len(erros_lst)}*\n"
            await client.chat_postMessage(channel=channel, text=header, mrkdwn=True)

            if entregues:
                linhas = []
                for e in entregues:
                    de = e.get("data_entrega_definitiva") or e.get("data_ultima_etapa")
                    de_fmt = de.strftime("%d/%m/%Y") if de and hasattr(de,"strftime") else str(de or "—")
                    linhas.append(f"✅ *{e.get('cliente_nome','—')}* — {e.get('veiculo','—')} | entregue {de_fmt}")
                MAX = 2600
                bloco, blocos = "", []
                for l in linhas:
                    if len(bloco)+len(l)+1 > MAX:
                        blocos.append(bloco); bloco = l+"\n"
                    else:
                        bloco += l+"\n"
                if bloco: blocos.append(bloco)
                for i, b in enumerate(blocos):
                    await client.chat_postMessage(channel=channel,
                        text=f"*Entregues ({len(entregues)}){' cont.' if i>0 else ''}*\n{b}", mrkdwn=True)

            if mudancas:
                linhas = []
                for m in mudancas:
                    tag = "✅" if m["entregue"] else "🔄"
                    linhas.append(f"{tag} *{m['cliente_nome']}* ({m['cpf']}) | {m['veiculo']} | _{m['status_portal']}_")
                MAX = 2600
                bloco, blocos = "", []
                for l in linhas:
                    if len(bloco)+len(l)+1 > MAX:
                        blocos.append(bloco); bloco = l+"\n"
                    else:
                        bloco += l+"\n"
                if bloco: blocos.append(bloco)
                for i, b in enumerate(blocos):
                    await client.chat_postMessage(channel=channel,
                        text=f"*Status no portal ({len(mudancas)}){' cont.' if i>0 else ''}*\n{b}", mrkdwn=True)

            set_sync_state(status="done",
                           message=f"Validação CPFs concluída — {len(entregues)} entregues, {len(mudancas)} verificados",
                           atualizados=len(entregues))
            logger.info(f"[validar-cpfs] Concluído: {len(entregues)} entregues, {len(erros_lst)} erros")
        except Exception as e:
            set_sync_state(status="error", message=str(e))
            logger.error(f"[validar-cpfs] Erro: {e}", exc_info=True)

    asyncio.create_task(_run())
    return {"ok": True, "message": f"Validando {len(cpfs)} CPFs no portal {fonte}. Resultado chegará no Slack."}


class SdSyncBody(BaseModel):
    fontes: list[str] | None = None

@app.post("/api/sync/signanddrive")
async def sync_signanddrive(body: SdSyncBody | None = None):
    """
    Sincroniza todos os contratos SIGN & DRIVE pendentes contra a API do portal.
    - Detecta novos entregues e atualiza Byetech CRM.
    - Atualiza mudancas de status (faturamento, transporte, disponivel).
    Roda em background. Acompanhe via /api/sync/status.
    """
    state = get_sync_state()
    if state.get("status") == "running":
        raise HTTPException(409, "Ja existe uma operacao em andamento — aguarde.")

    async def _run():
        from app.services.slack_service import get_client, get_or_create_channel

        set_sync_state(
            status="running",
            message="Sign & Drive: iniciando consulta ao portal...",
            atualizados=0,
            iniciado_em=datetime.utcnow().isoformat(),
        )
        try:
            resultado = await run_signanddrive_sync(fontes=body.fontes if body else None)
            n_ent = len(resultado.get("entregues", []))
            n_mud = len(resultado.get("mudancas_status", []))
            n_sem = len(resultado.get("sem_pedido", []))
            n_err = len(resultado.get("erros", []))

            set_sync_state(
                status="done",
                message=(
                    f"Sign & Drive concluido — {n_ent} entregue(s) | "
                    f"{n_mud} mudanca(s) de status | {n_sem} sem pedido"
                ),
                atualizados=n_ent + n_mud,
            )

            # Envia resumo no Slack
            try:
                channel = await get_or_create_channel()
                client  = get_client()
                hoje_str = datetime.now().strftime("%d/%m/%Y %H:%M")
                linhas = [f"*🚗 Sign & Drive Sync — {hoje_str}*"]
                linhas.append(f"Entregues: *{n_ent}* | Mudancas: *{n_mud}* | Sem pedido: *{n_sem}* | Erros: *{n_err}*")
                if resultado.get("entregues"):
                    linhas.append("")
                    for e in resultado["entregues"]:
                        de = e.get("data_entrega")
                        de_fmt = de.strftime("%d/%m/%Y") if de and hasattr(de, "strftime") else str(de or "—")
                        linhas.append(f"  ✅ *{e.get('cliente_nome','—')}* — {e.get('placa','')} | {de_fmt}")
                if resultado.get("mudancas_status"):
                    linhas.append("")
                    for m in resultado["mudancas_status"]:
                        linhas.append(
                            f"  🔄 *{m.get('cliente_nome','—')}* "
                            f"{m.get('status_anterior','—')} → {m.get('status_novo','—')}"
                        )
                await client.chat_postMessage(
                    channel=channel, text="\n".join(linhas), mrkdwn=True
                )
            except Exception as slack_err:
                logger.warning(f"Sign & Drive Slack nao enviado: {slack_err}")

        except Exception as e:
            set_sync_state(status="error", message=str(e))
            logger.error(f"Erro sync Sign & Drive: {e}", exc_info=True)

    asyncio.create_task(_run())
    return {"ok": True, "message": "Sync Sign & Drive iniciada. Resultado chegara no Slack."}


@app.get("/api/health")
async def health_check():
    """
    Valida todas as conexões do sistema em paralelo:
    - Database (SQLite)
    - Byetech CRM (sessão + API)
    - Metabase (URL pública)
    - Portal GWM/Sign&Drive (credenciais configuradas)
    - Portal LM (credenciais configuradas)
    - Slack (token + canal)
    - E-mail SMTP (configurado)
    """
    import asyncio as _asyncio
    import httpx
    import os

    results = {}

    async def check_database():
        try:
            async with SessionLocal() as s:
                total = await s.execute(select(func.count()).select_from(Contrato))
                n = total.scalar()
            return {"ok": True, "detail": f"{n} contratos no banco"}
        except Exception as e:
            return {"ok": False, "detail": str(e)}

    async def check_byetech():
        try:
            from app.scrapers.byetech_crm import _load_session_from_disk, _test_session
            cookies = _load_session_from_disk()
            if not cookies:
                return {"ok": False, "detail": "Sem sessão salva — faça login pelo portal"}
            ok = await _test_session(cookies)
            return {"ok": ok, "detail": "Sessão válida" if ok else "Sessão expirada — clique em 🔑 Renovar sessão"}
        except Exception as e:
            return {"ok": False, "detail": str(e)}

    async def check_metabase():
        try:
            from app.scrapers.metabase import METABASE_URL, CARD_ID
            import urllib.parse
            url = f"{METABASE_URL}/api/public/card/{CARD_ID}/query/json"
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                n = len(data) if isinstance(data, list) else "?"
                return {"ok": True, "detail": f"Acessível — {n} registros"}
            return {"ok": False, "detail": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"ok": False, "detail": str(e)}

    async def check_portal(fonte: str):
        try:
            from app.scrapers.portaldealer import ACCOUNTS, PORTAL_URL
            acc = ACCOUNTS.get(fonte, {})
            login = acc.get("login", "")
            pwd   = acc.get("password", "")
            if not login or not pwd:
                return {"ok": False, "detail": "Credenciais não configuradas no .env"}
            # Testa apenas se a URL do portal está acessível (sem login completo para não demorar)
            async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
                resp = await client.get(PORTAL_URL)
            reachable = resp.status_code < 500
            return {
                "ok": reachable,
                "detail": f"Portal acessível ({resp.status_code}) · Login: {login}" if reachable
                          else f"Portal inacessível ({resp.status_code})"
            }
        except Exception as e:
            return {"ok": False, "detail": str(e)}

    async def check_slack():
        try:
            from app.services.slack_service import get_client, get_or_create_channel, SLACK_TOKEN, CHANNEL_NAME
            if not SLACK_TOKEN:
                return {"ok": False, "detail": "SLACK_BOT_TOKEN não configurado no .env"}
            channel = await get_or_create_channel()
            client = get_client()
            resp = await client.auth_test()
            bot_name = resp.get("user", "?")
            return {"ok": True, "detail": f"Bot: {bot_name} · Canal: {CHANNEL_NAME} ({channel})"}
        except Exception as e:
            return {"ok": False, "detail": str(e)}

    async def check_email():
        try:
            smtp_host = os.getenv("SMTP_HOST", "")
            smtp_user = os.getenv("SMTP_USER", "") or os.getenv("EMAIL_USER", "")
            if not smtp_host or not smtp_user:
                return {"ok": False, "detail": "SMTP_HOST / SMTP_USER não configurados no .env"}
            return {"ok": True, "detail": f"Configurado: {smtp_user} via {smtp_host}"}
        except Exception as e:
            return {"ok": False, "detail": str(e)}

    # Roda todos em paralelo
    (
        results["database"],
        results["byetech_crm"],
        results["metabase"],
        results["portal_gwm"],
        results["portal_lm"],
        results["slack"],
        results["email"],
    ) = await _asyncio.gather(
        check_database(),
        check_byetech(),
        check_metabase(),
        check_portal("GWM"),
        check_portal("LM"),
        check_slack(),
        check_email(),
    )

    all_ok = all(v["ok"] for v in results.values())
    return {"ok": all_ok, "connections": results}


# ── Helpers ───────────────────────────────────────────────
def _contrato_to_dict(c: Contrato) -> dict:
    def iso(dt):
        if not dt:
            return None
        if isinstance(dt, datetime):
            return dt.isoformat()
        return str(dt)

    return {
        "id": c.id,
        "fonte": c.fonte,
        "id_externo": c.id_externo,
        "cliente_nome": c.cliente_nome,
        "cliente_cpf_cnpj": c.cliente_cpf_cnpj,
        "cliente_email": c.cliente_email,
        "veiculo": c.veiculo,
        "placa": c.placa,
        "status_atual": c.status_atual,
        "status_anterior": c.status_anterior,
        "data_prevista_entrega": iso(c.data_prevista_entrega),
        "data_entrega_definitiva": iso(c.data_entrega_definitiva),
        "byetech_contrato_id": c.byetech_contrato_id,
        "dias_para_entrega": c.dias_para_entrega,
        "atrasado": c.atrasado,
        "observacoes": c.observacoes,
        "data_venda": iso(c.data_venda),
        "pedido_id_locadora": c.pedido_id_locadora,
        "ultima_atualizacao": iso(c.ultima_atualizacao),
        "etapas": [],  # preenchido via detalhe
    }
