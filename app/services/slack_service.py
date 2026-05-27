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
    """Relatório diário — apenas entregas confirmadas (data_entrega_definitiva definida)."""
    if not SLACK_TOKEN:
        return False

    from app.database import SessionLocal, Contrato
    from sqlalchemy import select
    from datetime import datetime as _dt, timedelta

    hoje = _dt.now().date()
    data_corte = _dt.combine(hoje - timedelta(days=dias_entregas), _dt.min.time())

    async with SessionLocal() as session:
        res = await session.execute(
            select(Contrato).where(Contrato.data_entrega_definitiva >= data_corte)
        )
        entregues = res.scalars().all()

    if not entregues:
        return True

    channel = await get_or_create_channel()

    entregues_sorted = sorted(entregues, key=lambda c: c.data_entrega_definitiva or _dt.min, reverse=True)

    por_fonte: dict[str, list] = {}
    for c in entregues_sorted:
        por_fonte.setdefault(c.fonte or "OUTRO", []).append(c)

    linhas = [f"*📦 Entregas confirmadas — últimos {dias_entregas} dias ({len(entregues)} total)*"]
    for fonte in sorted(por_fonte.keys()):
        fe = FONTE_EMOJI.get(fonte, "📄")
        itens = por_fonte[fonte]
        linhas.append(f"\n*{fe} {fonte}* ({len(itens)})")
        for c in itens:
            data_ent = _fmt_date(c.data_entrega_definitiva)
            veiculo = c.veiculo or "—"
            placa = f" `{c.placa}`" if c.placa else ""
            linhas.append(f"  ✅ *{c.cliente_nome or '—'}* — {veiculo}{placa} | {data_ent}")

    await _post(channel=channel, text="\n".join(linhas))
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
