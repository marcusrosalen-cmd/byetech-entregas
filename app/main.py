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

from app.database import init_db, get_db, SessionLocal, Contrato, HistoricoStatus, AlertaEnviado, ByetechPendente
from app.services.sync_service import (
    get_sync_state, provide_twofa, set_sync_state,
)

# Imports opcionais — não travam o servidor se não estiverem disponíveis
try:
    from app.services.scheduler import start_scheduler
    _HAS_SCHEDULER = True
except Exception as _e:
    _HAS_SCHEDULER = False
    logging.getLogger("main").warning(f"Scheduler indisponível: {_e}")

try:
    from app.services.sync_service import run_full_sync, run_metabase_sync, run_gwm_lm_validation, run_signanddrive_sync, run_gwm_portaldealer_sync
    _HAS_SYNC = True
except Exception as _e:
    _HAS_SYNC = False
    logging.getLogger("main").warning(f"Sync service indisponível: {_e}")

try:
    from app.scrapers.movida import parse_movida_spreadsheet, get_unmapped_columns
    _HAS_MOVIDA = True
except Exception as _e:
    _HAS_MOVIDA = False

try:
    from app.services.email_service import send_unidas_confirmation
    _HAS_EMAIL = True
except Exception as _e:
    _HAS_EMAIL = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Banco de dados — crítico, mas trata erro sem derrubar o servidor
    try:
        await init_db()
        logger.info("✅ Banco de dados inicializado")
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar banco: {e}")

    # Scheduler — opcional
    if _HAS_SCHEDULER:
        try:
            start_scheduler()
            logger.info("✅ Scheduler iniciado")
        except Exception as e:
            logger.error(f"❌ Scheduler não iniciou: {e}")

    # Sync Metabase em background — falha silenciosa
    if _HAS_SYNC:
        asyncio.create_task(_startup_metabase_sync())

    logger.info("🚀 Byetech Entregas iniciado")
    yield
    logger.info("Encerrando...")


async def _startup_metabase_sync():
    """
    Roda Metabase sync no startup para garantir dados atualizados.
    Se o banco estiver vazio (reinício após deploy), faz sync completo automaticamente.
    """
    await asyncio.sleep(5)
    try:
        from app.database import SessionLocal as _SL, Contrato as _C
        from sqlalchemy import func as _func, select as _sel
        async with _SL() as s:
            cnt = (await s.execute(_sel(_func.count()).select_from(_C))).scalar() or 0

        if cnt < 100:
            logger.info(f"[startup] Banco com {cnt} contratos — executando sync completo Metabase...")
            result = await run_metabase_sync(full=True)
        else:
            result = await run_metabase_sync(full=False)

        logger.info(f"[startup] Metabase sync: {result.get('importados', 0)} contratos importados (banco={cnt})")
    except Exception as e:
        logger.warning(f"[startup] Metabase sync nao disponivel: {e}")


app = FastAPI(title="Byetech Entregas", lifespan=lifespan)

# Static files e templates
BASE_DIR = os.path.dirname(__file__)
_static_dir = os.path.join(BASE_DIR, "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


# ── Frontend ─────────────────────────────────────────────
def _static_ver(filename: str) -> str:
    """Gera versão baseada no mtime do arquivo para cache-busting."""
    try:
        path = os.path.join(BASE_DIR, "static", filename)
        return str(int(os.path.getmtime(path)))
    except Exception:
        return "1"


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request, "index.html", {
        "js_ver": _static_ver("js/main.js"),
        "css_ver": _static_ver("css/style.css"),
    })


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


@app.get("/api/contratos/entregues")
async def get_contratos_entregues(
    db: AsyncSession = Depends(get_db),
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    fonte: Optional[str] = None,
    search: Optional[str] = None,
    page: int = 1,
    per_page: int = 100,
):
    """
    Lista contratos já entregues com filtros opcionais.
    - from_date / to_date : YYYY-MM-DD (filtra data_entrega_definitiva)
    - fonte               : locadora (GWM, MOVIDA, etc.)
    - search              : nome, CPF, placa, veículo
    - page / per_page     : paginação
    """
    from sqlalchemy import and_
    from collections import Counter

    conditions = [Contrato.data_entrega_definitiva.is_not(None)]

    if from_date:
        try:
            dt_from = datetime.strptime(from_date, "%Y-%m-%d")
            conditions.append(Contrato.data_entrega_definitiva >= dt_from)
        except ValueError:
            pass

    if to_date:
        try:
            dt_to = datetime.strptime(to_date, "%Y-%m-%d")
            from datetime import timedelta
            conditions.append(Contrato.data_entrega_definitiva < dt_to + timedelta(days=1))
        except ValueError:
            pass

    if fonte:
        conditions.append(Contrato.fonte == fonte)

    result = await db.execute(
        select(Contrato)
        .where(and_(*conditions))
        .order_by(Contrato.data_entrega_definitiva.desc())
    )
    all_rows = result.scalars().all()

    if search:
        s = search.lower()
        all_rows = [
            c for c in all_rows
            if s in (c.cliente_nome or "").lower()
            or s in (c.cliente_cpf_cnpj or "").lower()
            or s in (c.placa or "").lower()
            or s in (c.veiculo or "").lower()
        ]

    total = len(all_rows)
    offset = (page - 1) * per_page
    page_rows = all_rows[offset: offset + per_page]

    por_fonte = dict(Counter(c.fonte for c in all_rows if c.fonte))

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": max(1, -(-total // per_page)),
        "por_fonte": por_fonte,
        "contratos": [_contrato_to_dict(c) for c in page_rows],
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


class PatchContratoBody(BaseModel):
    observacoes: Optional[str] = None
    nova_previsao_entrega: Optional[str] = None  # YYYY-MM-DD


@app.patch("/api/contratos/{contrato_id}")
async def patch_contrato(contrato_id: str, body: PatchContratoBody, db: AsyncSession = Depends(get_db)):
    """Atualiza observações e/ou nova previsão de entrega sem remover status de atraso."""
    result = await db.execute(select(Contrato).where(Contrato.id == contrato_id))
    c = result.scalar_one_or_none()
    if not c:
        raise HTTPException(404, "Contrato não encontrado")

    if body.observacoes is not None:
        c.observacoes = body.observacoes.strip() or None

    if body.nova_previsao_entrega is not None:
        if body.nova_previsao_entrega == "":
            c.nova_previsao_entrega = None
        else:
            try:
                nova_data = datetime.strptime(body.nova_previsao_entrega, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(400, "Formato de data inválido. Use YYYY-MM-DD")
            c.nova_previsao_entrega = nova_data
            # Recalcula dias com base na nova previsão, mas mantém atrasado=True
            hoje = datetime.utcnow().date()
            c.dias_para_entrega = (nova_data.date() - hoje).days

    c.ultima_atualizacao = datetime.utcnow()
    await db.commit()
    return {"ok": True, "id": contrato_id}


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

    # ── Byetech: tenta atualizar com Playwright se disponível, senão enfileira ──
    byetech_status = "sem_cpf"
    byetech_msg    = "Contrato sem CPF — não foi possível atualizar o Byetech"
    placa = c.placa if c.placa and str(c.placa).lower() not in ("nan", "n/a", "") else None

    if c.cliente_cpf_cnpj:
        _playwright_ok = False
        try:
            from app.scrapers.byetech_crm import (
                update_delivery_by_cpf, _load_session_from_disk, _test_session
            )
            _playwright_ok = True
        except ImportError:
            pass

        if not _playwright_ok:
            # Render: Playwright não disponível — enfileira no banco para processamento local
            byetech_status = "enfileirado"
            byetech_msg    = "Playwright indisponível neste servidor — entrega salva na fila. Rode processar_pendentes.py localmente."
            await _queue_byetech_update(contrato_id, data, c.cliente_cpf_cnpj, placa, c.cliente_nome or "")
        else:
            cookies = _load_session_from_disk()
            if not cookies or not await _test_session(cookies):
                byetech_status = "sessao_expirada"
                byetech_msg    = "Sessão Byetech expirada — entrega enfileirada para retry"
                await _queue_byetech_update(contrato_id, data, c.cliente_cpf_cnpj, placa, c.cliente_nome or "")
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
                        await _queue_byetech_update(contrato_id, data, c.cliente_cpf_cnpj, placa, c.cliente_nome or "")
                except asyncio.TimeoutError:
                    byetech_status = "timeout"
                    byetech_msg    = "Byetech demorou (timeout) — atualização enfileirada"
                    await _queue_byetech_update(contrato_id, data, c.cliente_cpf_cnpj, placa, c.cliente_nome or "")
                except Exception as e:
                    err = str(e)
                    if "expirada" in err.lower() or "2FA" in err or "401" in err:
                        byetech_status = "sessao_expirada"
                        byetech_msg    = "Sessão expirada — entrega enfileirada para retry"
                    else:
                        byetech_status = "erro"
                        byetech_msg    = err[:120]
                    await _queue_byetech_update(contrato_id, data, c.cliente_cpf_cnpj, placa, c.cliente_nome or "")

    return {
        "ok":             True,
        "data_entrega":   body.data_entrega,
        "byetech_status": byetech_status,
        "byetech_msg":    byetech_msg,
    }


# ── Fila de atualizações Byetech pendentes (persistida no banco de dados) ──
# Render não tem Playwright: gravamos no DB e a máquina local processa via
# GET /api/byetech/pendentes → POST /api/byetech/pendentes/{id}/done

async def _queue_byetech_update(db_contrato_id: str, data: datetime,
                                cpf: str, placa: Optional[str],
                                cliente_nome: str = ""):
    """Enfileira uma atualização Byetech no banco (sobrevive a deploys)."""
    try:
        async with SessionLocal() as s:
            # Remove duplicata se já existir pendente para o mesmo contrato
            from sqlalchemy import delete
            await s.execute(
                delete(ByetechPendente).where(
                    ByetechPendente.contrato_id == db_contrato_id,
                    ByetechPendente.processado_em.is_(None),
                    ByetechPendente.tipo == "entrega",
                )
            )
            p = ByetechPendente(
                contrato_id=db_contrato_id,
                cliente_nome=cliente_nome,
                cliente_cpf=cpf,
                placa=placa,
                data_entrega=data,
                tipo="entrega",
            )
            s.add(p)
            await s.commit()
            logger.warning(f"[Byetech] {db_contrato_id} enfileirado no banco para processamento local")
    except Exception as e:
        logger.error(f"[Byetech] Falha ao enfileirar {db_contrato_id}: {e}")


async def _get_pending_count() -> int:
    try:
        async with SessionLocal() as s:
            result = await s.execute(
                select(func.count()).select_from(ByetechPendente)
                .where(ByetechPendente.processado_em.is_(None))
            )
            return result.scalar() or 0
    except Exception:
        return 0


async def _rebuild_cpf_map() -> int:
    """
    Reconstrói o .byetech_cpf_map.json no Render fazendo scrape dos contratos ativos.

    O mapa local (.byetech_cpf_map.json) só existe na máquina do desenvolvedor.
    No Render, quando uma sessão válida é recebida, este método reconstrói o mapa
    a partir da API do Byetech para que update_delivery_by_cpf / update_phase_by_cpf
    funcionem sem precisar do arquivo local.

    Retorna o número de entradas escritas.
    """
    import re as _re, json as _json
    try:
        from app.scrapers.byetech_crm import scrape_contratos, SESSION_FILE
        contratos = await scrape_contratos()

        cpf_map: dict = {}
        for c in contratos:
            cpf_raw = c.get("cliente_cpf_cnpj", "")
            digits  = _re.sub(r"\D", "", cpf_raw)
            if not digits:
                continue

            raw = c.get("_raw") or {}
            cid = str(c.get("id_externo") or raw.get("id") or "")
            if not cid:
                continue

            entry = {
                "id":                       cid,
                "placa_carro":              c.get("placa") or raw.get("placa_carro") or raw.get("placaCarro"),
                "retirada_provisorio":      raw.get("retirada_provisorio") or raw.get("retiradaProvisorio"),
                "km_excedente_value":       str(raw.get("km_excedente_value") or raw.get("kmExcedenteValue") or "0.00"),
                "frequency_of_use":         raw.get("frequency_of_use") or raw.get("frequencyOfUse"),
                "usage_type":               raw.get("usage_type") or raw.get("usageType"),
                "is_reversal":              raw.get("is_reversal") or raw.get("isReversal") or 0,
                "reversal_value":           raw.get("reversal_value") or raw.get("reversalValue"),
                "franquia_coparticipacao":  str(raw.get("franquia_coparticipacao") or raw.get("franquiaCoparticipacao") or "0"),
                "cobertura_danos_materiais":raw.get("cobertura_danos_materiais") or raw.get("coberturaDanosMateriais") or "--",
                "cobertura_danos_corporais":raw.get("cobertura_danos_corporais") or raw.get("coberturaDanosCorporais") or "--",
                "is_active":                raw.get("is_active") if raw.get("is_active") is not None else (raw.get("isActive") if raw.get("isActive") is not None else 1),
                "is_extended":              raw.get("is_extended") or raw.get("isExtended") or 0,
                "extension_months":         raw.get("extension_months") or raw.get("extensionMonths"),
                "automatic_send_link":      raw.get("automatic_send_link") if raw.get("automatic_send_link") is not None else (raw.get("automaticSendLink") if raw.get("automaticSendLink") is not None else 1),
            }
            # Indexa por todas as variações do CPF para maximizar as chances de match
            for v in {digits, digits.zfill(11), digits[:-1] if len(digits)==12 else digits}:
                if v:
                    cpf_map[v] = entry

        # Salva no mesmo diretório do SESSION_FILE (raiz do projeto)
        map_path = os.path.join(os.path.dirname(SESSION_FILE), ".byetech_cpf_map.json")
        with open(map_path, "w", encoding="utf-8") as f:
            _json.dump(cpf_map, f)

        logger.info(f"[Byetech] Mapa CPF reconstruido: {len(contratos)} contratos → {len(cpf_map)} entradas → {map_path}")
        return len(contratos)
    except Exception as e:
        logger.warning(f"[Byetech] Falha ao reconstruir mapa CPF: {e}")
        return 0


async def _flush_byetech_pending():
    """Processa todas as atualizações pendentes quando a sessão Byetech estiver disponível."""
    try:
        from app.scrapers.byetech_crm import update_delivery_by_cpf
    except ImportError:
        logger.warning("[Byetech] Playwright nao disponivel — pendentes aguardam processamento local")
        return

    async with SessionLocal() as s:
        res = await s.execute(
            select(ByetechPendente)
            .where(ByetechPendente.processado_em.is_(None))
            .order_by(ByetechPendente.criado_em)
        )
        items = res.scalars().all()

    if not items:
        return

    logger.info(f"[Byetech] Processando {len(items)} pendente(s)...")
    for item in items:
        try:
            ok = await update_delivery_by_cpf(
                cpf_raw=item.cliente_cpf,
                data_entrega=item.data_entrega,
                placa=item.placa,
            )
            async with SessionLocal() as s:
                res2 = await s.execute(select(ByetechPendente).where(ByetechPendente.id == item.id))
                p = res2.scalar_one_or_none()
                if p:
                    if ok:
                        p.processado_em = datetime.utcnow()
                        p.erro_ultimo = None
                    else:
                        p.tentativas = (p.tentativas or 0) + 1
                        p.erro_ultimo = "update retornou False"
                    await s.commit()
            if ok:
                logger.info(f"[Byetech] Pendente OK: {item.contrato_id}")
            else:
                logger.error(f"[Byetech] Pendente falhou: {item.contrato_id}")
        except Exception as e:
            logger.error(f"[Byetech] Erro pendente {item.contrato_id}: {e}")
            async with SessionLocal() as s:
                res2 = await s.execute(select(ByetechPendente).where(ByetechPendente.id == item.id))
                p = res2.scalar_one_or_none()
                if p:
                    p.tentativas = (p.tentativas or 0) + 1
                    p.erro_ultimo = str(e)[:200]
                    await s.commit()


async def _update_byetech_crm(db_contrato_id: str, data: datetime):
    """
    Atualiza entrega_definitivo no Byetech CRM.
    Se Playwright indisponível ou sessão expirada, enfileira no banco para retry local.
    """
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
        from app.scrapers.byetech_crm import update_delivery_by_cpf
    except ImportError:
        logger.warning(f"[Byetech] Playwright indisponível — {db_contrato_id} enfileirado para retry local")
        await _queue_byetech_update(db_contrato_id, data, c.cliente_cpf_cnpj, placa, c.cliente_nome or "")
        return

    try:
        ok = await update_delivery_by_cpf(cpf_raw=c.cliente_cpf_cnpj, data_entrega=data, placa=placa)
    except Exception as e:
        err = str(e)
        if "2FA_REQUIRED" in err or "expirada" in err.lower() or "401" in err:
            logger.warning(f"[Byetech] Sessão expirada — {db_contrato_id} enfileirado para retry")
        else:
            logger.error(f"[Byetech] Erro ao atualizar {db_contrato_id}: {e}")
        await _queue_byetech_update(db_contrato_id, data, c.cliente_cpf_cnpj, placa, c.cliente_nome or "")
        return

    if ok:
        logger.info(f"[Byetech] {c.cliente_nome} marcado Definitivo Entregue em {data.date()}")
    else:
        logger.error(f"[Byetech] Falha ao atualizar {db_contrato_id} — enfileirado")
        await _queue_byetech_update(db_contrato_id, data, c.cliente_cpf_cnpj, placa, c.cliente_nome or "")


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
    if not _HAS_SYNC:
        return {"ok": False, "message": "Sync não disponível neste ambiente (rode localmente)"}
    state = get_sync_state()
    if state["status"] in ("running", "needs_2fa"):
        return {"ok": False, "message": "Sync já em andamento", "needs_2fa": False}

    async def _sync_and_flush():
        await run_full_sync()
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
    state["byetech_pending"] = await _get_pending_count()
    return state


# ── API: Fila Byetech pendentes ──────────────────────────
@app.get("/api/byetech/pendentes")
async def listar_pendentes(db: AsyncSession = Depends(get_db)):
    """
    Lista todas as atualizações pendentes no Byetech CRM.
    Usado pelo script processar_pendentes.py para processar localmente.
    """
    res = await db.execute(
        select(ByetechPendente)
        .where(ByetechPendente.processado_em.is_(None))
        .order_by(ByetechPendente.criado_em)
    )
    items = res.scalars().all()
    return {
        "total": len(items),
        "pendentes": [
            {
                "id":           p.id,
                "contrato_id":  p.contrato_id,
                "cliente_nome": p.cliente_nome,
                "cliente_cpf":  p.cliente_cpf,
                "placa":        p.placa,
                "data_entrega": p.data_entrega.isoformat() if p.data_entrega else None,
                "tipo":         p.tipo,
                "novo_status":  p.novo_status,
                "tentativas":   p.tentativas,
                "erro_ultimo":  p.erro_ultimo,
                "criado_em":    p.criado_em.isoformat() if p.criado_em else None,
            }
            for p in items
        ],
    }


class MarcarPendenteBody(BaseModel):
    sucesso: bool
    erro: Optional[str] = None


@app.post("/api/byetech/pendentes/{pendente_id}/done")
async def marcar_pendente_processado(
    pendente_id: int,
    body: MarcarPendenteBody,
    db: AsyncSession = Depends(get_db),
):
    """
    Marca um pendente como processado (chamado pelo script local após atualizar Byetech).
    """
    res = await db.execute(select(ByetechPendente).where(ByetechPendente.id == pendente_id))
    p = res.scalar_one_or_none()
    if not p:
        raise HTTPException(404, "Pendente não encontrado")

    if body.sucesso:
        p.processado_em = datetime.utcnow()
        p.erro_ultimo = None
        logger.info(f"[Byetech] Pendente {pendente_id} ({p.contrato_id}) marcado como processado")
    else:
        p.tentativas = (p.tentativas or 0) + 1
        p.erro_ultimo = (body.erro or "erro desconhecido")[:200]
        logger.warning(f"[Byetech] Pendente {pendente_id} falhou: {p.erro_ultimo}")

    await db.commit()
    return {"ok": True}


@app.get("/api/byetech/sessao-ok")
async def byetech_sessao_ok():
    """
    Verifica se a sessão Byetech está disponível e válida.
    Prioridade:
      1. Cache em memória (_session_cookies) → responde imediatamente sem chamar API
      2. Disco → testa com chamada leve à API Byetech
    Isso evita que o banner "Sessão expirada" apareça erroneamente quando a API
    do Byetech está lenta.
    """
    from app.scrapers.byetech_crm import _session_cookies, _load_session_from_disk, _test_session

    # 1. Memória: se a sessão está ativa no processo, considera válida
    if _session_cookies:
        return {"ok": True, "motivo": None, "source": "memory"}

    # 2. Disco: carrega e testa via API
    cookies = _load_session_from_disk()
    if not cookies:
        return {"ok": False, "motivo": "sem_sessao"}
    ok = await _test_session(cookies)
    return {"ok": ok, "motivo": None if ok else "expirada", "source": "disk"}


class PushSessionBody(BaseModel):
    cookies: dict
    secret: str = ""


class ByetechLoginBody(BaseModel):
    email: str
    senha: str
    codigo_2fa: Optional[str] = None


@app.post("/api/byetech/login")
async def byetech_login_manual(body: ByetechLoginBody):
    """Login manual no Byetech CRM via email + senha + 2FA opcional."""
    try:
        from app.scrapers.byetech_crm import _login_via_api, set_remote_session
        cookies = await _login_via_api(
            twofa_code=body.codigo_2fa or None,
            email=body.email,
            senha=body.senha,
        )
        if cookies:
            set_remote_session(cookies)
            return {"ok": True, "msg": "Sessão Byetech ativa!"}
        return {"ok": False, "dois_fatores": False, "msg": "Credenciais inválidas ou login falhou"}
    except Exception as e:
        if "2FA_REQUIRED" in str(e):
            return {"ok": False, "dois_fatores": True, "msg": "Código 2FA necessário"}
        raise HTTPException(500, str(e))


@app.post("/api/byetech/login-debug")
async def byetech_login_debug(body: ByetechLoginBody):
    """Diagnóstico do login Byetech — retorna status HTTP e body bruto para debug."""
    import urllib.parse as _up
    import httpx as _httpx
    from app.scrapers.byetech_crm import BYETECH_URL

    result = {}
    try:
        async with _httpx.AsyncClient(
            base_url=BYETECH_URL,
            follow_redirects=False,   # NÃO segue redirect — vê status real
            timeout=20,
        ) as client:
            # CSRF
            csrf_r = await client.get("/sanctum/csrf-cookie")
            result["csrf_status"] = csrf_r.status_code
            result["csrf_cookies"] = list(client.cookies.keys())

            xsrf_raw = client.cookies.get("XSRF-TOKEN", "")
            xsrf     = _up.unquote(xsrf_raw)
            result["xsrf_found"] = bool(xsrf_raw)

            hdrs = {
                "X-XSRF-TOKEN": xsrf,
                "Content-Type": "application/json",
                "Accept":       "application/json",
                "Referer":      BYETECH_URL + "/",
                "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0",
            }

            # Login POST
            r = await client.post("/login", json={
                "email": body.email,
                "password": body.senha,
            }, headers=hdrs)

            result["login_status"]   = r.status_code
            result["login_location"] = r.headers.get("location", "")
            result["login_cookies"]  = list(client.cookies.keys())
            try:
                result["login_body"] = r.json()
            except Exception:
                result["login_body_text"] = r.text[:300]

    except Exception as e:
        result["erro"] = str(e)

    return result


@app.post("/api/byetech/push-session")
async def push_byetech_session(body: PushSessionBody):
    """
    Recebe cookies de sessão do Byetech CRM enviados pela máquina local.
    Permite que o Render use a sessão sem precisar de Playwright.

    Uso (máquina local):
      python push_session_render.py
    """
    _secret = os.getenv("SESSION_PUSH_SECRET", "byetech-local")
    if body.secret != _secret:
        raise HTTPException(401, "Secret inválido — configure SESSION_PUSH_SECRET no .env")

    if not body.cookies:
        raise HTTPException(400, "Cookies vazios")

    from app.scrapers.byetech_crm import set_remote_session, _test_session

    # Armazena imediatamente (responde rápido) e valida + processa em background
    set_remote_session(body.cookies)
    pending = await _get_pending_count()

    async def _validar_e_processar():
        ok = await _test_session(body.cookies)
        if ok:
            logger.info("[Byetech] Sessao remota validada — reconstruindo mapa CPF...")
            n = await _rebuild_cpf_map()
            logger.info(f"[Byetech] Mapa CPF pronto ({n} contratos). Processando pendentes...")
            await _flush_byetech_pending()
        else:
            logger.warning("[Byetech] Sessao remota invalida ou expirada — verifique se a sessao e recente")

    asyncio.create_task(_validar_e_processar())

    return {
        "ok": True,
        "message": f"Sessão recebida. Validando e processando {pending} pendente(s) em background.",
    }


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
        set_sync_state(status="running", message="Renovando sessão Byetech...", system="Byetech CRM")
        try:
            from app.scrapers.byetech_crm import (
                _login_via_api, set_remote_session,
                get_session, clear_session,
            )

            # Tenta primeiro via httpx (sem Playwright) — funciona no Render
            cookies = await _login_via_api(twofa_code=None)
            if not cookies:
                # Fallback: Playwright (só funciona localmente)
                async def _2fa_cb():
                    return await _wait_twofa_renovar()
                clear_session()
                cookies = await get_session(twofa_callback=_2fa_cb)

            if cookies:
                set_remote_session(cookies)
                set_sync_state(status="running", message="Sessão renovada! Processando pendentes...")
                logger.info("[Byetech] Sessão renovada com sucesso")
                await _flush_byetech_pending()
                pending = _get_pending_count()
                msg = "Sessão Byetech renovada!" + (f" {pending} pendente(s) restante(s)." if pending else " Todos os pendentes processados.")
                set_sync_state(status="done", message=msg, atualizados=0)
            else:
                set_sync_state(status="error", message="Falha no login — credenciais inválidas ou 2FA necessário. Use o modal 🔑 Login Byetech.")
        except TimeoutError:
            set_sync_state(status="error", message="Tempo esgotado aguardando o código 2FA (5 min). Tente novamente.")
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
    if not _HAS_MOVIDA:
        raise HTTPException(503, "Importação Movida não disponível neste ambiente")
    content = await file.read()
    try:
        info = get_unmapped_columns(content, file.filename or "file.xlsx")
        contratos = parse_movida_spreadsheet(content, file.filename or "file.xlsx")
        return {**info, "total": len(contratos)}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/movida/import")
async def movida_import(file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    if not _HAS_MOVIDA:
        raise HTTPException(503, "Importação Movida não disponível neste ambiente")
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


@app.post("/api/sync/gwm")
async def sync_gwm_portal():
    """
    Consulta o Portal Dealer (GWM) para detectar entregas de contratos Haval/GWM.
    Busca individual por CPF — ~35s por contrato.
    Atualiza Byetech CRM diretamente (await, nao background task).
    Roda em background. Resultado chega no Slack.
    """
    if not _HAS_SYNC:
        raise HTTPException(503, "Sync service nao disponivel")

    state = get_sync_state()
    if state.get("status") == "running":
        raise HTTPException(409, "Ja existe uma operacao em andamento — aguarde.")

    async def _run():
        from app.services.slack_service import get_client, get_or_create_channel

        set_sync_state(
            status="running",
            message="GWM Portal: iniciando consulta...",
            atualizados=0,
            iniciado_em=datetime.utcnow().isoformat(),
        )
        try:
            resultado = await run_gwm_portaldealer_sync()
            n_ent  = len(resultado.get("entregues", []))
            n_nao  = len(resultado.get("nao_encontrados", []))
            n_err  = len(resultado.get("erros", []))

            set_sync_state(
                status="done",
                message=f"GWM Portal concluido — {n_ent} entregue(s) | {n_nao} nao encontrados",
                atualizados=n_ent,
            )

            # Slack
            try:
                channel = await get_or_create_channel()
                client  = get_client()
                hoje_str = datetime.now().strftime("%d/%m/%Y %H:%M")
                icon = "✅" if not n_err else "⚠️"
                linhas = [f"{icon} *GWM Portal Sync — {hoje_str}*"]
                linhas.append(f"Entregues: *{n_ent}* | Nao encontrados: *{n_nao}* | Erros: *{n_err}*")
                if resultado.get("entregues"):
                    linhas.append("")
                    for e in resultado["entregues"]:
                        de = e.get("data_entrega")
                        de_fmt = de.strftime("%d/%m/%Y") if de and hasattr(de, "strftime") else str(de or "—")
                        linhas.append(f"  ✅ *{e.get('cliente_nome','—')}* — {e.get('placa','')} | {de_fmt}")
                if resultado.get("erros"):
                    for err in resultado["erros"][:3]:
                        linhas.append(f"  ⚠️ {err[:100]}")
                await client.chat_postMessage(channel=channel, text="\n".join(linhas), mrkdwn=True)
            except Exception as slack_err:
                logger.warning(f"GWM Portal Slack nao enviado: {slack_err}")

        except Exception as e:
            set_sync_state(status="error", message=str(e))
            logger.error(f"Erro sync GWM Portal: {e}", exc_info=True)

    asyncio.create_task(_run())
    return {"ok": True, "message": "Sync GWM Portal iniciada. Resultado chegara no Slack."}


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


# ── Entregues: página de histórico de entregas ───────────
@app.get("/entregues", response_class=HTMLResponse)
async def entregues_page(request: Request):
    return templates.TemplateResponse(request, "entregues.html")


# ── Dashboard: página de analytics ──────────────────────
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return templates.TemplateResponse(request, "dashboard.html")


@app.get("/api/stats/dashboard")
async def stats_dashboard(db: AsyncSession = Depends(get_db)):
    """
    Retorna dados agregados para a aba de Dashboard:
    - KPIs gerais
    - Distribuição por fonte (ativos, entregues, atrasados)
    - Distribuição por status
    - Entregas por semana (últimas 8 semanas)
    - Novas vendas por semana (últimas 8 semanas)
    - Contratos mais críticos (top 10 atrasados)
    """
    from datetime import timedelta
    from collections import defaultdict, Counter

    hoje = datetime.now().date()
    hoje_ini = datetime.combine(hoje, datetime.min.time())

    # Todos os ativos
    res_ativos = await db.execute(
        select(Contrato).where(Contrato.data_entrega_definitiva.is_(None))
    )
    ativos = res_ativos.scalars().all()

    # Entregues últimas 8 semanas
    corte_ent = datetime.combine(hoje - timedelta(weeks=8), datetime.min.time())
    res_ent = await db.execute(
        select(Contrato).where(Contrato.data_entrega_definitiva >= corte_ent)
    )
    entregues = res_ent.scalars().all()

    # Novas vendas últimas 8 semanas
    corte_venda = datetime.combine(hoje - timedelta(weeks=8), datetime.min.time())
    res_vendas = await db.execute(
        select(Contrato).where(Contrato.data_venda >= corte_venda)
    )
    vendas = res_vendas.scalars().all()

    # ── KPIs ──────────────────────────────────────────────
    n_atrasados = sum(1 for c in ativos if c.atrasado or (c.dias_para_entrega or 0) < 0)
    n_urgentes  = sum(1 for c in ativos if not c.atrasado and 0 <= (c.dias_para_entrega or 999) <= 2)
    n_criticos  = sum(1 for c in ativos if not c.atrasado and 3 <= (c.dias_para_entrega or 999) <= 10)
    n_alertas   = sum(1 for c in ativos if not c.atrasado and 11 <= (c.dias_para_entrega or 999) <= 20)
    n_ok        = len(ativos) - n_atrasados - n_urgentes - n_criticos - n_alertas
    # Entregues na semana corrente
    inicio_semana = datetime.combine(hoje - timedelta(days=hoje.weekday()), datetime.min.time())
    n_ent_semana  = sum(1 for c in entregues if c.data_entrega_definitiva and c.data_entrega_definitiva >= inicio_semana)
    n_ent_mes     = sum(1 for c in entregues
                        if c.data_entrega_definitiva and c.data_entrega_definitiva >= datetime.combine(hoje.replace(day=1), datetime.min.time()))

    kpis = {
        "total_ativos": len(ativos),
        "atrasados": n_atrasados,
        "urgentes": n_urgentes,
        "criticos": n_criticos,
        "alertas": n_alertas,
        "ok": n_ok,
        "perc_atrasados": round(n_atrasados / len(ativos) * 100, 1) if ativos else 0,
        "entregas_semana": n_ent_semana,
        "entregas_mes": n_ent_mes,
        "entregas_total_periodo": len(entregues),
        "novas_vendas_8sem": len(vendas),
    }

    # ── Por fonte ─────────────────────────────────────────
    fontes_ativos   = Counter(c.fonte for c in ativos if c.fonte)
    fontes_ent      = Counter(c.fonte for c in entregues if c.fonte)
    fontes_atrasado = Counter(c.fonte for c in ativos if c.atrasado and c.fonte)
    todas_fontes    = sorted(set(list(fontes_ativos.keys()) + list(fontes_ent.keys())))

    por_fonte = [
        {
            "fonte": f,
            "ativos": fontes_ativos.get(f, 0),
            "entregues": fontes_ent.get(f, 0),
            "atrasados": fontes_atrasado.get(f, 0),
        }
        for f in todas_fontes
    ]

    # ── Por status ────────────────────────────────────────
    status_cnt = Counter(c.status_atual for c in ativos if c.status_atual)
    por_status = [{"status": s, "total": n} for s, n in status_cnt.most_common(10)]

    # ── Entregas por semana (últimas 8) ───────────────────
    semanas_ent: dict[str, int] = defaultdict(int)
    for c in entregues:
        if not c.data_entrega_definitiva:
            continue
        dt = c.data_entrega_definitiva
        if isinstance(dt, datetime):
            dt = dt.date()
        # ISO week
        iso = dt.isocalendar()
        semanas_ent[f"{iso.year}-W{iso.week:02d}"] += 1

    # Gera as últimas 8 semanas (mesmo que vazia = 0)
    semanas_labels = []
    semanas_valores = []
    for i in range(7, -1, -1):
        dia = hoje - timedelta(weeks=i)
        iso = dia.isocalendar()
        key = f"{iso.year}-W{iso.week:02d}"
        semanas_labels.append(f"Sem {iso.week}/{iso.year}")
        semanas_valores.append(semanas_ent.get(key, 0))

    # ── Vendas por semana (últimas 8) ─────────────────────
    semanas_venda: dict[str, int] = defaultdict(int)
    for c in vendas:
        dt_ref = c.data_venda or c.criado_em
        if not dt_ref:
            continue
        if isinstance(dt_ref, datetime):
            dt_ref = dt_ref.date()
        iso = dt_ref.isocalendar()
        semanas_venda[f"{iso.year}-W{iso.week:02d}"] += 1

    vendas_valores = []
    for i in range(7, -1, -1):
        dia = hoje - timedelta(weeks=i)
        iso = dia.isocalendar()
        key = f"{iso.year}-W{iso.week:02d}"
        vendas_valores.append(semanas_venda.get(key, 0))

    # ── Top 10 atrasados ─────────────────────────────────
    top_atrasados = sorted(
        [c for c in ativos if c.atrasado or (c.dias_para_entrega or 0) < 0],
        key=lambda c: c.dias_para_entrega or 0
    )[:10]

    criticos_lista = [
        {
            "id": c.id,
            "cliente_nome": c.cliente_nome or "—",
            "fonte": c.fonte or "—",
            "veiculo": c.veiculo or "—",
            "status": c.status_atual or "—",
            "data_prevista": c.data_prevista_entrega.strftime("%d/%m/%Y") if c.data_prevista_entrega else "—",
            "dias": c.dias_para_entrega or 0,
        }
        for c in top_atrasados
    ]

    return {
        "kpis": kpis,
        "por_fonte": por_fonte,
        "por_status": por_status,
        "semanas_labels": semanas_labels,
        "entregas_por_semana": semanas_valores,
        "vendas_por_semana": vendas_valores,
        "top_atrasados": criticos_lista,
        "gerado_em": datetime.now().isoformat(),
    }


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
        "nova_previsao_entrega": iso(c.nova_previsao_entrega),
        "data_venda": iso(c.data_venda),
        "pedido_id_locadora": c.pedido_id_locadora,
        "ultima_atualizacao": iso(c.ultima_atualizacao),
        "etapas": [],  # preenchido via detalhe
    }
