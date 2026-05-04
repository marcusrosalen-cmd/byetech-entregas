"""
Serviço de alertas via Slack.
Canal: byetech-entregas (criado automaticamente se não existir)
"""
import os
import asyncio
from datetime import datetime
from typing import Optional
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

load_dotenv()

SLACK_TOKEN   = os.getenv("SLACK_BOT_TOKEN", "")
CHANNEL_NAME  = os.getenv("SLACK_CHANNEL_NAME", "byetech-entregas")

_client: Optional[AsyncWebClient] = None
_channel_id: Optional[str] = None


def get_client() -> AsyncWebClient:
    global _client
    if not _client:
        _client = AsyncWebClient(token=SLACK_TOKEN)
    return _client


async def _post(channel: str, text: str = "", blocks=None, mrkdwn: bool = True):
    """Envia mensagem com retry automático em caso de rate limit (429)."""
    client = get_client()
    for attempt in range(3):
        try:
            kwargs = {"channel": channel, "mrkdwn": mrkdwn}
            if blocks:
                kwargs["blocks"] = blocks
                kwargs["text"] = text or "Byetech Entregas"
            else:
                kwargs["text"] = text
            await client.chat_postMessage(**kwargs)
            await asyncio.sleep(1.1)
            return
        except SlackApiError as e:
            if "ratelimited" in str(e).lower() and attempt < 2:
                retry_after = int(e.response.headers.get("Retry-After", 5))
                await asyncio.sleep(retry_after + 1)
            else:
                raise


async def get_or_create_channel() -> str:
    global _channel_id
    if _channel_id:
        return _channel_id

    if CHANNEL_NAME.startswith(("C", "D", "G", "W")):
        _channel_id = CHANNEL_NAME
        return _channel_id

    client = get_client()
    try:
        for tipos in ("public_channel", "private_channel"):
            cursor = ""
            while True:
                resp = await client.conversations_list(
                    types=tipos, limit=200, exclude_archived=True,
                    cursor=cursor or None
                )
                for ch in resp.get("channels", []):
                    if ch["name"] == CHANNEL_NAME.lstrip("#"):
                        _channel_id = ch["id"]
                        return _channel_id
                cursor = resp.get("response_metadata", {}).get("next_cursor", "")
                if not cursor:
                    break
    except SlackApiError as e:
        raise Exception(f"Erro ao listar canais Slack: {e}")

    raise Exception(
        f"Canal Slack '{CHANNEL_NAME}' não encontrado. "
        f"Crie o canal, adicione o bot e defina SLACK_CHANNEL_NAME=<ID> no .env"
    )


FONTE_EMOJI = {
    "GWM":         "🚙",
    "SIGN & DRIVE": "🚘",
    "LM":          "🚗",
    "LOCALIZA":    "🟡",
    "MOVIDA":      "🔵",
    "UNIDAS":      "🟣",
    "VW":          "🏎️",
    "FLUA":        "🛻",
    "NISSAN":      "🚐",
}

URGENCIA_EMOJI = {
    "atrasado": "🔴",
    "critico":  "🟠",
    "alerta":   "🟡",
    "ok":       "🟢",
}


def _urgencia(dias: Optional[int], atrasado: bool) -> str:
    if atrasado or (dias is not None and dias < 0):
        return "atrasado"
    if dias is not None and dias <= 5:
        return "critico"
    if dias is not None and dias <= 20:
        return "alerta"
    return "ok"


def _fmt_dias(dias: Optional[int], atrasado: bool) -> str:
    if atrasado or (dias is not None and dias < 0):
        return f"*ATRASADO {abs(dias or 0)} dias*"
    if dias is None:
        return "–"
    return f"{dias} dia{'s' if dias != 1 else ''}"


def _fmt_date(dt: Optional[datetime]) -> str:
    if not dt:
        return "–"
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return dt
    return dt.strftime("%d/%m/%Y")


async def send_daily_alert(contratos: list[dict]) -> bool:
    """Envia resumo diário de contratos pendentes com alertas de prazo."""
    if not SLACK_TOKEN:
        return False

    channel = await get_or_create_channel()
    client = get_client()

    atrasados = [c for c in contratos if c.get("atrasado") or (c.get("dias_para_entrega") or 0) < 0]
    criticos  = [c for c in contratos if not c.get("atrasado") and 0 <= (c.get("dias_para_entrega") or 999) <= 5]
    alertas   = [c for c in contratos if not c.get("atrasado") and 5 < (c.get("dias_para_entrega") or 999) <= 20]

    today = datetime.now().strftime("%d/%m/%Y")

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"📦 Byetech Entregas — {today}"}},
        {"type": "context", "elements": [{"type": "mrkdwn",
            "text": f"Total ativo: *{len(contratos)}* · 🔴 {len(atrasados)} atrasados · 🟠 {len(criticos)} críticos · 🟡 {len(alertas)} alertas"}]},
        {"type": "divider"},
    ]

    def contract_line(c):
        urg = _urgencia(c.get("dias_para_entrega"), c.get("atrasado", False))
        return (
            f"{URGENCIA_EMOJI.get(urg,'')} {FONTE_EMOJI.get(c.get('fonte',''),'📄')} "
            f"*{c.get('cliente_nome','–')}* ({c.get('fonte','?')}) · "
            f"_{c.get('status_atual','–')}_ · "
            f"Prev: {_fmt_date(c.get('data_prevista_entrega'))} · "
            f"{_fmt_dias(c.get('dias_para_entrega'), c.get('atrasado',False))}"
        )

    for titulo, grupo, limite in [
        ("*🔴 ATRASADOS*", atrasados, 20),
        ("*🟠 CRÍTICOS (≤5 dias)*", criticos, 20),
        ("*🟡 EM ALERTA (6–20 dias)*", alertas, 30),
    ]:
        if grupo:
            text = "\n".join(contract_line(c) for c in grupo[:limite])
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": titulo}})
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
            blocks.append({"type": "divider"})

    if not atrasados and not criticos and not alertas:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "✅ Nenhum contrato em alerta hoje."}})

    blocks.append({"type": "context", "elements": [{"type": "mrkdwn",
        "text": f"_Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}_"}]})

    try:
        await client.chat_postMessage(channel=channel, blocks=blocks, text="Byetech Entregas — resumo diário")
        return True
    except SlackApiError as e:
        raise Exception(f"Erro ao enviar Slack: {e}")


async def send_entregas_resumo(
    entregas_hoje: list[dict],
    atualizados: int = 0,
    erros: list[str] | None = None,
) -> bool:
    if not SLACK_TOKEN:
        return False
    if not entregas_hoje:
        return True

    channel = await get_or_create_channel()
    client = get_client()

    today = datetime.now().strftime("%d/%m/%Y")
    erros = erros or []

    por_fonte: dict[str, list] = {}
    for e in entregas_hoje:
        f = e.get("fonte", "OUTRO")
        por_fonte.setdefault(f, []).append(e)

    linhas = []
    for fonte, itens in sorted(por_fonte.items()):
        emoji_f = FONTE_EMOJI.get(fonte, "📄")
        linhas.append(f"*{emoji_f} {fonte}* ({len(itens)} veículo{'s' if len(itens) != 1 else ''})")
        for e in itens:
            nome    = e.get("cliente_nome", "—")
            veiculo = e.get("veiculo", "—")
            placa   = f" `{e['placa']}`" if e.get("placa") else ""
            hora    = e.get("data_entrega", "")
            hora_fmt = f" — _{hora}_" if hora else ""
            linhas.append(f"  • {nome} — {veiculo}{placa}{hora_fmt}")

    corpo = "\n".join(linhas)
    erros_bloco = ""
    if erros:
        lista_erros = "\n".join(f"  • {e}" for e in erros[:5])
        erros_bloco = f"\n\n⚠️ *Erros no sync ({len(erros)}):*\n{lista_erros}"

    texto = (
        f"📦 *Entregas do dia — {today}*\n"
        f"_{atualizados} contratos atualizados neste sync_\n\n"
        f"{corpo}{erros_bloco}"
    )

    try:
        await client.chat_postMessage(channel=channel, text=texto, mrkdwn=True)
        return True
    except SlackApiError as e:
        raise Exception(f"Erro ao enviar resumo de entregas no Slack: {e}")


async def send_relatorio_completo(dias_vendas: int = 5, dias_entregas: int = 7) -> bool:
    """
    Relatório diário conciso — UMA mensagem por seção para análise rápida.
    Estrutura: cabeçalho/KPIs | críticos | entregas recentes | performance por fonte
    """
    if not SLACK_TOKEN:
        return False

    from app.database import SessionLocal, Contrato, HistoricoStatus
    from sqlalchemy import select, and_
    from datetime import datetime as _dt, timedelta, date as _date
    from collections import Counter

    hoje      = _dt.now().date()
    hoje_str  = hoje.strftime("%d/%m/%Y")
    hoje_ini  = _dt.combine(hoje, _dt.min.time())

    async with SessionLocal() as session:
        # Contratos ativos (sem entrega)
        res_todos = await session.execute(
            select(Contrato).where(Contrato.data_entrega_definitiva.is_(None))
        )
        todos = res_todos.scalars().all()

        # Entregas recentes
        data_corte_ent = _dt.combine(hoje - timedelta(days=dias_entregas), _dt.min.time())
        res_ent = await session.execute(
            select(Contrato).where(Contrato.data_entrega_definitiva >= data_corte_ent)
        )
        entregues = res_ent.scalars().all()

        # Novas vendas
        data_corte_venda = _dt.combine(hoje - timedelta(days=dias_vendas), _dt.min.time())
        res_novas = await session.execute(
            select(Contrato).where(Contrato.criado_em >= data_corte_venda)
        )
        novas_list = res_novas.scalars().all()

    channel = await get_or_create_channel()
    now_str = _dt.now().strftime("%d/%m/%Y %H:%M")

    # ── Classificação de urgência ──────────────────────────
    atrasados = sorted([c for c in todos if c.atrasado or (c.dias_para_entrega or 0) < 0],
                       key=lambda c: c.dias_para_entrega or 0)
    urgentes  = sorted([c for c in todos if not c.atrasado and 0 <= (c.dias_para_entrega or 999) <= 2],
                       key=lambda c: c.dias_para_entrega or 0)
    criticos  = sorted([c for c in todos if not c.atrasado and 3 <= (c.dias_para_entrega or 999) <= 10],
                       key=lambda c: c.dias_para_entrega or 0)
    alertas   = [c for c in todos if not c.atrasado and 11 <= (c.dias_para_entrega or 999) <= 20]

    # ── Performance por fonte ──────────────────────────────
    fontes_ativas = Counter(c.fonte for c in todos if c.fonte)
    fontes_ent    = Counter(c.fonte for c in entregues if c.fonte)

    def _linha_critico(c) -> str:
        fe   = FONTE_EMOJI.get(c.fonte or "", "📄")
        dias = c.dias_para_entrega or 0
        dias_txt = f"*{abs(dias)}d atraso*" if dias < 0 else f"{dias}d"
        dp   = _fmt_date(c.data_prevista_entrega)
        return f"{fe} *{c.cliente_nome or '—'}* | {c.veiculo or '—'} | {dp} | {dias_txt}"

    # ── MSG 1: KPIs do dia (cabeçalho executivo) ──────────
    perc_atrasado = f"{len(atrasados)/len(todos)*100:.0f}%" if todos else "0%"
    linha_kpi = (
        f"*📊 Byetech Entregas — {hoje_str}*\n"
        f"```\n"
        f"  Ativos      : {len(todos):>4}  |  Atrasados : {len(atrasados):>3} ({perc_atrasado})\n"
        f"  Urgentes(≤2d): {len(urgentes):>3}  |  Críticos  : {len(criticos):>3} (3-10d)\n"
        f"  Alertas(≤20d): {len(alertas):>3}  |  Ok (>20d) : {len(todos)-len(atrasados)-len(urgentes)-len(criticos)-len(alertas):>3}\n"
        f"```\n"
        f"📦 *{len(entregues)}* entregas nos últimos {dias_entregas}d  |  🆕 *{len(novas_list)}* novas vendas"
    )
    await _post(channel=channel, text=linha_kpi)

    # ── MSG 2: Ação imediata (atrasados + urgentes) ────────
    linhas_acao = []
    if atrasados:
        top_atr = [_linha_critico(c) for c in atrasados[:15]]
        resto = f"\n_...e mais {len(atrasados)-15} atrasados_" if len(atrasados) > 15 else ""
        linhas_acao.append(f"*🔴 ATRASADOS ({len(atrasados)})*\n" + "\n".join(top_atr) + resto)
    if urgentes:
        top_urg = [_linha_critico(c) for c in urgentes[:10]]
        linhas_acao.append(f"*🚨 URGENTES — 1-2 dias ({len(urgentes)})*\n" + "\n".join(top_urg))

    if linhas_acao:
        await _post(channel=channel, text="\n\n".join(linhas_acao))

    # ── MSG 3: Monitoramento (críticos 3-10d) ─────────────
    if criticos:
        top_cr = [_linha_critico(c) for c in criticos[:12]]
        resto  = f"\n_...e mais {len(criticos)-12}_" if len(criticos) > 12 else ""
        await _post(channel=channel,
            text=f"*🟠 CRÍTICOS — 3 a 10 dias ({len(criticos)})*\n" + "\n".join(top_cr) + resto)

    # ── MSG 4: Entregas recentes + performance por fonte ──
    secoes_4 = []

    if entregues:
        ent_sorted = sorted(entregues, key=lambda c: c.data_entrega_definitiva or _dt.min, reverse=True)
        ent_linhas = []
        for c in ent_sorted[:12]:
            fe = FONTE_EMOJI.get(c.fonte or "", "📄")
            data_ent = _fmt_date(c.data_entrega_definitiva)
            ent_linhas.append(f"{fe} *{c.cliente_nome or '—'}* — {c.veiculo or '—'} | {data_ent}")
        resto = f"\n_...e mais {len(entregues)-12}_" if len(entregues) > 12 else ""
        secoes_4.append(f"*📦 Entregas — últimos {dias_entregas} dias ({len(entregues)})*\n"
                        + "\n".join(ent_linhas) + resto)

    # Performance por fonte (ativos x entregues)
    if fontes_ativas:
        perf_linhas = []
        for fonte in sorted(fontes_ativas.keys()):
            fe    = FONTE_EMOJI.get(fonte, "📄")
            ativ  = fontes_ativas[fonte]
            ent_f = fontes_ent.get(fonte, 0)
            # conta atrasados por fonte
            atr_f = sum(1 for c in atrasados if c.fonte == fonte)
            atr_txt = f"  🔴{atr_f}" if atr_f else ""
            perf_linhas.append(f"{fe} *{fonte}*: {ativ} ativos | {ent_f} entregues{atr_txt}")
        secoes_4.append("*📈 Performance por locadora*\n" + "\n".join(perf_linhas))

    if secoes_4:
        await _post(channel=channel, text="\n\n".join(secoes_4))

    # ── MSG 5: Novas vendas (compacto, por dia) ────────────
    if novas_list:
        vendas_por_dia: dict[str, list] = {}
        for c in novas_list:
            ref = c.data_venda or c.criado_em
            if not ref:
                continue
            dia = (ref.date() if hasattr(ref, "date") else ref).strftime("%d/%m")
            vendas_por_dia.setdefault(dia, []).append(c)

        linhas_v = []
        for dia in sorted(vendas_por_dia.keys(), reverse=True):
            itens = vendas_por_dia[dia]
            fontes_dia = Counter(c.fonte for c in itens if c.fonte)
            fontes_str = " · ".join(
                f"{FONTE_EMOJI.get(f,'📄')}{n}" for f, n in fontes_dia.most_common()
            )
            linhas_v.append(f"• *{dia}*: {len(itens)} contrato{'s' if len(itens)!=1 else ''} — {fontes_str}")

        await _post(channel=channel,
            text=f"*🆕 Novas vendas — {dias_vendas}d ({len(novas_list)} total)*\n" + "\n".join(linhas_v[:10]))

    await _post(channel=channel, text=f"_✅ {now_str} · Byecar_")
    return True


async def send_validation_report(resultado: dict) -> bool:
    """Relatório de validação GWM/LM + Metabase — conciso."""
    if not SLACK_TOKEN:
        return False

    channel = await get_or_create_channel()
    client  = get_client()

    today         = datetime.now().strftime("%d/%m/%Y %H:%M")
    entregues     = resultado.get("entregues", [])
    mudancas      = resultado.get("mudancas_status", [])
    vendas_por_dia = resultado.get("novas_vendas_por_dia", {})
    erros         = resultado.get("erros", [])

    linhas = [f"*🧪 Validação GWM/LM — {today}*"]

    # Entregas
    if entregues:
        linhas.append(f"\n*📦 {len(entregues)} veículo{'s' if len(entregues)!=1 else ''} entregue{'s' if len(entregues)!=1 else ''}*")
        for e in entregues[:10]:
            fe   = FONTE_EMOJI.get(e.get("fonte", ""), "📄")
            placa = f" `{e['placa']}`" if e.get("placa") else ""
            linhas.append(f"  ✅ {fe} *{e.get('cliente_nome','—')}* — {e.get('veiculo','—')}{placa} | {_fmt_date(e.get('data_entrega'))}")
    else:
        linhas.append("\n📦 Nenhuma entrega nova.")

    # Mudanças
    if mudancas:
        linhas.append(f"\n*🔄 {len(mudancas)} mudança{'s' if len(mudancas)!=1 else ''} de status*")
        for m in mudancas[:8]:
            fe = FONTE_EMOJI.get(m.get("fonte", ""), "📄")
            ok = " ✅" if m.get("byetech_ok") else (" ❌" if m.get("byetech_ok") is False else "")
            linhas.append(f"  {fe} *{m.get('cliente_nome','—')}* _{m.get('status_anterior','—')}_ → *{m.get('status_novo','—')}*{ok}")

    # Novas vendas (resumo)
    if vendas_por_dia:
        total_novos = sum(v.get("novos", 0) for v in vendas_por_dia.values())
        linhas.append(f"\n*🆕 {total_novos} novos contratos no período*")

    # Erros
    if erros:
        linhas.append(f"\n⚠️ *{len(erros)} erro{'s' if len(erros)!=1 else ''}*: {erros[0][:100]}")

    try:
        await client.chat_postMessage(channel=channel, text="\n".join(linhas), mrkdwn=True)
        return True
    except SlackApiError as e:
        raise Exception(f"Erro ao enviar relatório Slack: {e}")


async def send_prazo_alert(contrato: dict, dias_antes: int) -> bool:
    """Envia alerta pontual de prazo para um contrato específico."""
    if not SLACK_TOKEN:
        return False

    channel = await get_or_create_channel()
    client = get_client()

    emoji    = FONTE_EMOJI.get(contrato.get("fonte", ""), "📄")
    urgencia = "🔴" if dias_antes <= 1 else ("🟠" if dias_antes <= 5 else "🟡")

    text = (
        f"{urgencia} *Alerta de prazo — {dias_antes} dia{'s' if dias_antes != 1 else ''} restante{'s' if dias_antes != 1 else ''}*\n"
        f"{emoji} *{contrato.get('cliente_nome', '–')}* ({contrato.get('fonte')})\n"
        f"Veículo: {contrato.get('veiculo', '–')} {contrato.get('placa', '')}\n"
        f"Status: _{contrato.get('status_atual', '–')}_\n"
        f"Entrega prevista: *{_fmt_date(contrato.get('data_prevista_entrega'))}*"
    )

    try:
        await client.chat_postMessage(channel=channel, text=text, mrkdwn=True)
        return True
    except SlackApiError as e:
        raise Exception(f"Erro ao enviar alerta Slack: {e}")


async def send_sync_concluido(
    atualizados: int,
    entregas_hoje: list[dict],
    erros: list[str],
    duracao_seg: float,
) -> bool:
    """Envia mensagem de sync concluído com resumo executivo."""
    if not SLACK_TOKEN:
        return False

    channel = await get_or_create_channel()
    client = get_client()

    hoje = datetime.now().strftime("%d/%m/%Y %H:%M")
    n_entregas = len(entregas_hoje)
    dur = f"{duracao_seg/60:.1f} min" if duracao_seg >= 60 else f"{duracao_seg:.0f}s"
    status_icon = "✅" if not erros else "⚠️"

    linhas = [f"{status_icon} *Sync concluído — {hoje}* ({dur})"]
    linhas.append(f"• {atualizados} contrato{'s' if atualizados != 1 else ''} atualizado{'s' if atualizados != 1 else ''}")

    if n_entregas:
        por_fonte: dict[str, int] = {}
        for e in entregas_hoje:
            por_fonte[e.get("fonte", "?")] = por_fonte.get(e.get("fonte", "?"), 0) + 1
        entregas_txt = ", ".join(
            f"{FONTE_EMOJI.get(f,'📄')} {f}: {n}" for f, n in sorted(por_fonte.items())
        )
        linhas.append(f"• {n_entregas} entrega{'s' if n_entregas != 1 else ''} registrada{'s' if n_entregas != 1 else ''} — {entregas_txt}")
    else:
        linhas.append("• Nenhuma nova entrega")

    if erros:
        linhas.append(f"• ⚠️ {len(erros)} erro{'s' if len(erros) != 1 else ''}: {erros[0][:80]}" + (" ..." if len(erros) > 1 else ""))

    try:
        await client.chat_postMessage(channel=channel, text="\n".join(linhas), mrkdwn=True)
        return True
    except SlackApiError:
        return False
