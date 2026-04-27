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
            await asyncio.sleep(1.1)  # respeita o rate limit de 1 msg/s da Slack API
            return
        except SlackApiError as e:
            if "ratelimited" in str(e).lower() and attempt < 2:
                retry_after = int(e.response.headers.get("Retry-After", 5))
                await asyncio.sleep(retry_after + 1)
            else:
                raise


async def get_or_create_channel() -> str:
    """
    Retorna o ID do canal configurado.
    - Se CHANNEL_NAME já é um ID (começa com C/D/G), usa direto.
    - Senão, busca por nome na lista de canais do bot.
    - Se não encontrar, lança exceção com instrução clara.
    """
    global _channel_id
    if _channel_id:
        return _channel_id

    # ID direto
    if CHANNEL_NAME.startswith(("C", "D", "G", "W")):
        _channel_id = CHANNEL_NAME
        return _channel_id

    client = get_client()
    # Busca por nome em todos os tipos de canal que o bot tem acesso
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
    "GWM":        "🚙",
    "SIGN & DRIVE":"🚘",
    "LM":         "🚗",
    "LOCALIZA":   "🟡",
    "MOVIDA":     "🔵",
    "UNIDAS":     "🟣",
    "VW":         "🏎️",
    "FLUA":       "🛻",
    "NISSAN":     "🚐",
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
    """
    Envia resumo diário de contratos pendentes com alertas de prazo.
    """
    if not SLACK_TOKEN:
        return False

    channel = await get_or_create_channel()
    client = get_client()

    # Separa por urgência
    atrasados = [c for c in contratos if c.get("atrasado") or (c.get("dias_para_entrega") or 0) < 0]
    criticos  = [c for c in contratos if not c.get("atrasado") and 0 <= (c.get("dias_para_entrega") or 999) <= 5]
    alertas   = [c for c in contratos if not c.get("atrasado") and 5 < (c.get("dias_para_entrega") or 999) <= 20]

    today = datetime.now().strftime("%d/%m/%Y")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📦 Byetech Entregas — {today}"},
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn",
                "text": f"Total ativo: *{len(contratos)}* contratos · "
                        f"🔴 {len(atrasados)} atrasados · "
                        f"🟠 {len(criticos)} críticos · "
                        f"🟡 {len(alertas)} em alerta"}]
        },
        {"type": "divider"},
    ]

    def contract_line(c):
        urg = _urgencia(c.get("dias_para_entrega"), c.get("atrasado", False))
        emoji_fonte = FONTE_EMOJI.get(c.get("fonte", ""), "📄")
        emoji_urg   = URGENCIA_EMOJI.get(urg, "")
        return (
            f"{emoji_urg} {emoji_fonte} *{c.get('cliente_nome', '–')}* "
            f"({c.get('fonte', '?')}) · "
            f"_{c.get('status_atual', '–')}_ · "
            f"Prev: {_fmt_date(c.get('data_prevista_entrega'))} · "
            f"{_fmt_dias(c.get('dias_para_entrega'), c.get('atrasado', False))}"
        )

    # Seção atrasados
    if atrasados:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*🔴 ATRASADOS*"},
        })
        text = "\n".join(contract_line(c) for c in atrasados[:20])
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        blocks.append({"type": "divider"})

    # Seção críticos
    if criticos:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*🟠 CRÍTICOS (≤5 dias)*"},
        })
        text = "\n".join(contract_line(c) for c in criticos[:20])
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})
        blocks.append({"type": "divider"})

    # Seção alertas
    if alertas:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*🟡 EM ALERTA (6–20 dias)*"},
        })
        text = "\n".join(contract_line(c) for c in alertas[:30])
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    if not atrasados and not criticos and not alertas:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "✅ Nenhum contrato em alerta hoje."},
        })

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"_Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}_"}]
    })

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
    """
    Envia resumo das entregas registradas no dia ao fim do sync.
    Agrupa por fonte e lista cliente + veículo + hora.
    """
    if not SLACK_TOKEN:
        return False
    if not entregas_hoje:
        return True

    channel = await get_or_create_channel()
    client = get_client()

    today = datetime.now().strftime("%d/%m/%Y")
    erros = erros or []

    # Agrupa por fonte
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
        f"{corpo}"
        f"{erros_bloco}"
    )

    try:
        await client.chat_postMessage(channel=channel, text=texto, mrkdwn=True)
        return True
    except SlackApiError as e:
        raise Exception(f"Erro ao enviar resumo de entregas no Slack: {e}")


async def send_relatorio_completo(dias_vendas: int = 5, dias_entregas: int = 7) -> bool:
    """
    Relatório diário completo — TODOS os casos, sem limites.
    Estrutura:
      1. Cabeçalho + resumo
      2. Alertas 1/2 dias (todos)
      3. Alertas 3-5/6-10 dias (todos)
      4. Alertas 11-15/16-20 dias (todos)
      5. Atrasados por locadora (todos, auto-dividido em mensagens)
      6. Movimentações + Entregas (todos) + Novas vendas (todos)
    """
    if not SLACK_TOKEN:
        return False

    from app.database import SessionLocal, Contrato, HistoricoStatus
    from sqlalchemy import select, and_
    from datetime import datetime as _dt, timedelta

    hoje      = _dt.now().date()
    hoje_str  = hoje.strftime("%d/%m/%Y")
    hoje_ini  = _dt.combine(hoje, _dt.min.time())
    ontem_ini = hoje_ini - timedelta(days=1)

    async with SessionLocal() as session:
        res_todos = await session.execute(
            select(Contrato).where(Contrato.data_entrega_definitiva.is_(None))
        )
        todos = res_todos.scalars().all()

        data_corte_ent = _dt.combine(hoje - timedelta(days=dias_entregas), _dt.min.time())
        res_ent = await session.execute(
            select(Contrato).where(Contrato.data_entrega_definitiva >= data_corte_ent)
        )
        entregues = res_ent.scalars().all()

        data_corte_venda = _dt.combine(hoje - timedelta(days=dias_vendas), _dt.min.time())
        res_novas = await session.execute(
            select(Contrato).where(Contrato.criado_em >= data_corte_venda)
        )
        novas_list = sorted(res_novas.scalars().all(),
                            key=lambda c: c.criado_em or _dt.min, reverse=True)

        res_mv = await session.execute(
            select(HistoricoStatus, Contrato)
            .join(Contrato, Contrato.id == HistoricoStatus.contrato_id)
            .where(HistoricoStatus.registrado_em >= hoje_ini)
            .order_by(HistoricoStatus.registrado_em.desc())
        )
        mv_hoje = res_mv.all()

        res_mv_on = await session.execute(
            select(HistoricoStatus, Contrato)
            .join(Contrato, Contrato.id == HistoricoStatus.contrato_id)
            .where(and_(HistoricoStatus.registrado_em >= ontem_ini,
                        HistoricoStatus.registrado_em < hoje_ini))
            .order_by(HistoricoStatus.registrado_em.desc())
        )
        mv_ontem = res_mv_on.all()

    # ── Classificação ─────────────────────────────────────
    atrasados = sorted(
        [c for c in todos if c.atrasado or (c.dias_para_entrega or 0) < 0],
        key=lambda c: c.dias_para_entrega or 0
    )
    def _faixa(d_min, d_max):
        return sorted(
            [c for c in todos if not c.atrasado
             and d_min <= (c.dias_para_entrega or 999) <= d_max],
            key=lambda c: c.dias_para_entrega or 0
        )
    a1  = _faixa(1, 1);  a2  = _faixa(2, 2);  a5  = _faixa(3, 5)
    a10 = _faixa(6, 10); a15 = _faixa(11, 15); a20 = _faixa(16, 20)
    total_alertas = len(a1)+len(a2)+len(a5)+len(a10)+len(a15)+len(a20)

    atr_por_fonte: dict[str, list] = {}
    for c in atrasados:
        atr_por_fonte.setdefault(c.fonte or "?", []).append(c)

    channel = await get_or_create_channel()
    client  = get_client()
    now_str = _dt.now().strftime("%d/%m/%Y %H:%M")

    # ── Helpers ───────────────────────────────────────────
    def _linha(c, mostrar_dias=True) -> str:
        fe   = FONTE_EMOJI.get(c.fonte or "", "📄")
        dp   = _fmt_date(c.data_prevista_entrega)
        dias = c.dias_para_entrega or 0
        dias_txt = f"*{abs(dias)}d atraso*" if dias < 0 else f"{dias}d"
        linha = f"{fe} [{c.fonte or '?'}] *{c.cliente_nome or '—'}* — {c.veiculo or '—'} | prev {dp}"
        if mostrar_dias:
            linha += f" | {dias_txt}"
        linha += f" | _{c.status_atual or '—'}_"
        return linha

    async def _post_lista(titulo: str, rows: list, mostrar_dias=True):
        """Envia lista completa dividindo em múltiplas mensagens conforme necessário."""
        if not rows:
            await _post(channel=channel, text=f"*{titulo}*: nenhum contrato.")
            return
        MAX = 2600
        blocos_txt = []
        atual = ""
        for i, c in enumerate(rows):
            linha = _linha(c, mostrar_dias) + "\n"
            if len(atual) + len(linha) > MAX:
                blocos_txt.append(atual.rstrip())
                atual = linha
            else:
                atual += linha
        if atual:
            blocos_txt.append(atual.rstrip())

        for idx, bloco in enumerate(blocos_txt):
            parte = f" ({idx+1}/{len(blocos_txt)})" if len(blocos_txt) > 1 else ""
            header = f"*{titulo} ({len(rows)} total){parte}*\n" if idx == 0 else f"*{titulo} — cont.{parte}*\n"
            await _post(channel=channel, text=header + bloco)

    # ── 1. Cabeçalho (apenas título, sem resumo) ──────────
    await _post(
        channel=channel,
        blocks=[
            {"type":"header","text":{"type":"plain_text",
                "text": f"📋 Relatório Byetech Entregas — {hoje_str}"}},
        ],
        text=f"Relatório Byetech Entregas — {hoje_str}",
    )

    # ── 2. Alertas urgentes: 1 e 2 dias ───────────────────
    await _post_lista("🚨 ALERTAS — 1 DIA", a1)
    await _post_lista("🔴 ALERTAS — 2 DIAS", a2)

    # ── 3. Alertas 3-5 e 6-10 dias ────────────────────────
    await _post_lista("🟠 ALERTAS — 3 a 5 DIAS", a5)
    await _post_lista("🟡 ALERTAS — 6 a 10 DIAS", a10)

    # ── 4. Alertas 11-15 e 16-20 dias ─────────────────────
    await _post_lista("🟡 ALERTAS — 11 a 15 DIAS", a15)
    await _post_lista("🟢 ALERTAS — 16 a 20 DIAS", a20)

    # ── 5. Atrasados por locadora (todos) ─────────────────
    for fonte, lst in sorted(atr_por_fonte.items(), key=lambda x: -len(x[1])):
        fe = FONTE_EMOJI.get(fonte, "📄")
        pior = abs(min((c.dias_para_entrega or 0) for c in lst))
        await _post_lista(f"🔴 ATRASADOS {fe} {fonte} (maior atraso: {pior}d)", lst)

    # ── 6. Movimentações ──────────────────────────────────
    mv_usar  = mv_hoje  if mv_hoje  else mv_ontem
    mv_label = "hoje"   if mv_hoje  else "ontem"
    if mv_usar:
        mv_linhas = [
            f"{FONTE_EMOJI.get(hist.fonte or '','📄')} [{hist.fonte or '?'}] "
            f"*{c.cliente_nome or '—'}* — {c.veiculo or '—'}\n"
            f"   _{hist.status_anterior or '—'}_ → *{hist.status_novo or '—'}*"
            for hist, c in mv_usar
        ]
        MAX = 2600
        blocos_mv, atual = [], ""
        for linha in mv_linhas:
            if len(atual) + len(linha) + 1 > MAX and atual:
                blocos_mv.append(atual)
                atual = linha + "\n"
            else:
                atual += linha + "\n"
        if atual: blocos_mv.append(atual)
        for idx, bloco in enumerate(blocos_mv):
            parte = f" ({idx+1}/{len(blocos_mv)})" if len(blocos_mv) > 1 else ""
            await _post(channel=channel,
                text=f"*🔄 Movimentações {mv_label} ({len(mv_usar)}){parte}*\n{bloco}")
    else:
        await _post(channel=channel, text="*🔄 Movimentações:* nenhuma registrada hoje.")

    # ── 7. Entregas ───────────────────────────────────────
    await _post_lista(
        f"📦 VEÍCULOS ENTREGUES — últimos {dias_entregas} dias",
        sorted(entregues, key=lambda c: c.data_entrega_definitiva or _dt.min, reverse=True),
        mostrar_dias=False,
    )

    # ── 8. Novas vendas (todos, com nome) ─────────────────
    novas_por_dia: dict[str, list] = {}
    for c in novas_list:
        ref_dt = c.data_venda or c.criado_em
        if not ref_dt: continue
        dia = (ref_dt.date() if hasattr(ref_dt, "date") else ref_dt).strftime("%Y-%m-%d")
        novas_por_dia.setdefault(dia, []).append(c)

    if novas_por_dia:
        for dia in sorted(novas_por_dia.keys(), reverse=True):
            try: dia_fmt = _dt.strptime(dia, "%Y-%m-%d").strftime("%d/%m/%Y")
            except: dia_fmt = dia
            itens = novas_por_dia[dia]
            await _post_lista(
                f"🆕 NOVAS VENDAS — {dia_fmt} ({len(itens)} contratos)",
                sorted(itens, key=lambda c: c.fonte or ""),
                mostrar_dias=False,
            )
    else:
        await _post(channel=channel,
            text=f"*🆕 Novas vendas:* nenhuma nos últimos {dias_vendas} dias.")

    # ── Rodapé ────────────────────────────────────────────
    await _post(channel=channel,
        text=f"_✅ Relatório completo gerado em {now_str} · Byecar Central de Entregas_")
    return True


async def send_validation_report(resultado: dict) -> bool:
    """
    Envia relatório consolidado de validação GWM/LM + Metabase:
    - Veículos entregues
    - Mudanças de status
    - Novas vendas por dia (últimos N dias)
    """
    if not SLACK_TOKEN:
        return False

    channel = await get_or_create_channel()
    client  = get_client()

    today  = datetime.now().strftime("%d/%m/%Y %H:%M")
    entregues       = resultado.get("entregues", [])
    mudancas        = resultado.get("mudancas_status", [])
    vendas_por_dia  = resultado.get("novas_vendas_por_dia", {})
    erros           = resultado.get("erros", [])

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"🧪 Relatório de Validação GWM / LM — {today}"},
        },
        {"type": "divider"},
    ]

    # ── Entregas confirmadas ──────────────────────────────
    if entregues:
        linhas = []
        for e in entregues:
            emoji = FONTE_EMOJI.get(e.get("fonte", ""), "📄")
            data  = _fmt_date(e.get("data_entrega")) if e.get("data_entrega") else "—"
            placa = f" `{e['placa']}`" if e.get("placa") else ""
            linhas.append(
                f"✅ {emoji} *{e.get('cliente_nome','—')}* — "
                f"{e.get('veiculo','—')}{placa} · entregue {data}"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"*📦 Veículos entregues ({len(entregues)})*\n" + "\n".join(linhas[:20])},
        })
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*📦 Veículos entregues:* nenhuma nova entrega confirmada."},
        })
    blocks.append({"type": "divider"})

    # ── Mudanças de status ────────────────────────────────
    if mudancas:
        linhas = []
        for m in mudancas:
            emoji = FONTE_EMOJI.get(m.get("fonte", ""), "📄")
            byetech_ok  = m.get("byetech_ok")
            byetech_msg = m.get("byetech_msg", "")
            if byetech_ok is True and byetech_msg not in ("sem_mapeamento", "ja_na_fase"):
                bye_txt = " ✅ _Byetech atualizado_"
            elif byetech_ok is False:
                bye_txt = f" ❌ _Byetech erro: {byetech_msg}_"
            else:
                bye_txt = ""
            linhas.append(
                f"🔄 {emoji} *{m.get('cliente_nome','—')}* — {m.get('veiculo','—')}\n"
                f"   _{m.get('status_anterior','—')}_ → *{m.get('status_novo','—')}*{bye_txt}"
            )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"*🔄 Mudanças de status ({len(mudancas)})*\n" + "\n".join(linhas[:20])},
        })
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*🔄 Mudanças de status:* nenhuma alteração detectada."},
        })
    blocks.append({"type": "divider"})

    # ── Novas vendas por dia ──────────────────────────────
    if vendas_por_dia:
        linhas_v = []
        total_novos = 0
        for dt_str in sorted(vendas_por_dia.keys()):
            info   = vendas_por_dia[dt_str]
            total  = info.get("total", 0)
            novos  = info.get("novos", 0)
            total_novos += novos
            # Formata a data para pt-BR
            try:
                from datetime import datetime as _dt
                dt_fmt = _dt.strptime(dt_str, "%Y-%m-%d").strftime("%d/%m/%Y")
            except Exception:
                dt_fmt = dt_str
            novos_txt = f" *(+{novos} novo{'s' if novos != 1 else ''})*" if novos else ""
            linhas_v.append(f"• {dt_fmt}: {total} contrato{'s' if total != 1 else ''}{novos_txt}")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": (
                         f"*📊 Novas vendas — últimos {len(vendas_por_dia)} dias*\n"
                         + "\n".join(linhas_v)
                         + f"\n\n_Total de novos contratos no período: *{total_novos}*_"
                     )},
        })
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*📊 Novas vendas:* sem dados do Metabase."},
        })

    # ── Erros ─────────────────────────────────────────────
    if erros:
        blocks.append({"type": "divider"})
        lista = "\n".join(f"  • {e}" for e in erros[:5])
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"⚠️ *Erros ({len(erros)}):*\n{lista}"},
        })

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"_Gerado em {today}_"}],
    })

    try:
        await client.chat_postMessage(
            channel=channel,
            blocks=blocks,
            text=f"Relatório de Validação GWM/LM — {today}",
        )
        return True
    except SlackApiError as e:
        raise Exception(f"Erro ao enviar relatório Slack: {e}")


async def send_prazo_alert(contrato: dict, dias_antes: int) -> bool:
    """
    Envia alerta pontual de prazo para um contrato específico.
    Disparado em: 20, 15, 10, 5 e 1 dia antes.
    """
    if not SLACK_TOKEN:
        return False

    channel = await get_or_create_channel()
    client = get_client()

    emoji = FONTE_EMOJI.get(contrato.get("fonte", ""), "📄")
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
