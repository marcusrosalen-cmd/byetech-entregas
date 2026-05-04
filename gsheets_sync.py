"""
╔══════════════════════════════════════════════════════════════════╗
║   BYETECH ENTREGAS — SYNC PARA GOOGLE SHEETS                     ║
║                                                                  ║
║  Puxa dados do Metabase + Byetech CRM e escreve no Google Sheets.║
║  O time visualiza pelo Glide (app bonito em cima da planilha).   ║
║                                                                  ║
║  SETUP (só fazer uma vez):                                       ║
║    1. pip install gspread google-auth httpx python-dotenv        ║
║    2. Crie credenciais Google (veja instruções abaixo)           ║
║    3. Coloque o arquivo credentials.json nesta pasta             ║
║    4. python gsheets_sync.py --setup   (cria as abas)            ║
║    5. python gsheets_sync.py --teste   (verifica conexão)        ║
║    6. python gsheets_sync.py --inicial (importa tudo)            ║
║    7. python gsheets_sync.py           (sync diário)             ║
║                                                                  ║
║  CREDENCIAIS GOOGLE:                                             ║
║    1. Acesse: console.cloud.google.com                           ║
║    2. Crie projeto → "Byetech Entregas"                          ║
║    3. APIs → ative "Google Sheets API" e "Google Drive API"      ║
║    4. Credenciais → "Conta de Serviço" → baixe o JSON            ║
║    5. Renomeie para credentials.json e coloque nesta pasta       ║
║    6. Copie o e-mail da conta de serviço (termina em             ║
║       @...iam.gserviceaccount.com)                               ║
║    7. Abra a planilha → Compartilhar → cole o e-mail (Editor)    ║
╚══════════════════════════════════════════════════════════════════╝
"""

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════

import os
from dotenv import load_dotenv
load_dotenv()

# Nome exato da sua planilha Google Sheets
SHEET_NAME = os.getenv("GSHEET_NAME", "Byetech Entregas")

# Caminho para o arquivo de credenciais da conta de serviço
CREDENTIALS_FILE = os.getenv("GSHEET_CREDENTIALS", "credentials.json")

# ═══════════════════════════════════════════════════════════════
#  IMPORTS
# ═══════════════════════════════════════════════════════════════

import sys
import asyncio
import argparse
import logging
import re
from datetime import datetime, date, timedelta
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("gsheets_sync.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("gsheets")

# ═══════════════════════════════════════════════════════════════
#  ESTRUTURA DAS ABAS DO GOOGLE SHEETS
# ═══════════════════════════════════════════════════════════════

# Aba principal — todos os contratos
CABECALHO_CONTRATOS = [
    "ID",              # ID interno (GWM_123456)
    "Fonte",           # GWM | LM | UNIDAS | LOCALIZA | MOVIDA | SIGN_DRIVE
    "Cliente",
    "CPF_CNPJ",
    "Email",
    "Veiculo",
    "Placa",
    "Status",
    "Etapa_Kanban",    # faturamento | transporte | disponivel_loja | etc
    "Data_Prevista",   # DD/MM/YYYY
    "Data_Entrega",    # DD/MM/YYYY — preenchido quando entregue
    "Data_Venda",      # DD/MM/YYYY
    "Atrasado",        # TRUE / FALSE
    "Dias_Restantes",  # número (negativo = atrasado)
    "Byetech_ID",      # ID no Byetech CRM
    "ID_Externo",      # ID na locadora
    "Origem",          # METABASE | BYETECH_CRM | MANUAL
    "Ultima_Sync",     # DD/MM/YYYY HH:MM
]

# Aba de histórico de status
CABECALHO_HISTORICO = [
    "ID_Contrato",
    "Status_Anterior",
    "Status_Novo",
    "Etapa_Anterior",
    "Etapa_Nova",
    "Fonte",
    "Data_Hora",       # DD/MM/YYYY HH:MM
    "Observacao",
]

# Aba de log de sincronizações
CABECALHO_SYNC_LOG = [
    "Fonte",
    "Status",          # sucesso | erro | em_andamento
    "Inicio",
    "Fim",
    "Importados",
    "Atualizados",
    "Erro",
]

# ═══════════════════════════════════════════════════════════════
#  CONEXÃO COM GOOGLE SHEETS
# ═══════════════════════════════════════════════════════════════

_gc = None      # cliente gspread
_sheet = None   # planilha aberta


def _conectar():
    """Conecta ao Google Sheets via conta de serviço."""
    global _gc, _sheet
    if _sheet:
        return _sheet

    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(
            f"\n❌ Arquivo '{CREDENTIALS_FILE}' não encontrado!\n"
            f"   Siga as instruções no topo deste script para criar as credenciais.\n"
        )

    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
        _gc    = gspread.authorize(creds)
        _sheet = _gc.open(SHEET_NAME)
        log.info(f"✅ Google Sheets conectado: '{SHEET_NAME}'")
        return _sheet
    except gspread.exceptions.SpreadsheetNotFound:
        raise RuntimeError(
            f"\n❌ Planilha '{SHEET_NAME}' não encontrada!\n"
            f"   1. Crie uma planilha com este nome exato no Google Drive\n"
            f"   2. Compartilhe com o e-mail da conta de serviço (Editor)\n"
            f"   3. Rode: python gsheets_sync.py --setup\n"
        )
    except Exception as e:
        raise RuntimeError(f"\n❌ Erro ao conectar no Google Sheets: {e}\n")


def _aba(nome: str):
    """Retorna uma aba pelo nome, criando se não existir."""
    sheet = _conectar()
    try:
        return sheet.worksheet(nome)
    except Exception:
        log.info(f"[Sheets] Criando aba '{nome}'...")
        return sheet.add_worksheet(title=nome, rows=5000, cols=30)


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _fmt_data(dt) -> str:
    """Converte datetime/date para string DD/MM/YYYY."""
    if not dt:
        return ""
    if isinstance(dt, datetime):
        return dt.strftime("%d/%m/%Y")
    if isinstance(dt, date):
        return dt.strftime("%d/%m/%Y")
    s = str(dt)[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%d/%m/%Y")
    except Exception:
        return s


def _fmt_datetime(dt) -> str:
    if not dt:
        return ""
    if isinstance(dt, datetime):
        return dt.strftime("%d/%m/%Y %H:%M")
    return _fmt_data(dt)


def _etapa_kanban(status: str) -> str:
    if not status:
        return "faturamento"
    s = status.lower()
    if any(x in s for x in ["faturad", "fatura"]):             return "faturamento"
    if any(x in s for x in ["saiu", "saída", "saida", "fabric"]): return "saida_fabrica"
    if any(x in s for x in ["transport", "trânsito", "transito"]): return "transporte"
    if any(x in s for x in ["disponível", "disponivel", "loja"]): return "disponivel_loja"
    if any(x in s for x in ["aguardando", "retirada"]):         return "aguardando_cliente"
    if any(x in s for x in ["entregue", "definitivo"]):         return "entregue"
    return "faturamento"


def _dias_restantes(data_prevista) -> int:
    """Calcula dias até a entrega (negativo = atrasado)."""
    if not data_prevista:
        return 0
    if isinstance(data_prevista, datetime):
        dp = data_prevista.date()
    elif isinstance(data_prevista, date):
        dp = data_prevista
    else:
        try:
            dp = datetime.strptime(str(data_prevista)[:10], "%Y-%m-%d").date()
        except Exception:
            return 0
    return (dp - date.today()).days


def _id_contrato(fonte: str, id_externo: str, cpf: str) -> str:
    chave = id_externo or cpf or "unknown"
    return f"{fonte}_{chave}".upper()


def _limpar(s) -> str:
    return str(s or "").strip()


# ═══════════════════════════════════════════════════════════════
#  OPERAÇÕES NA PLANILHA
# ═══════════════════════════════════════════════════════════════

def _carregar_indice_contratos(ws) -> dict[str, int]:
    """
    Lê a coluna ID da aba Contratos e retorna {id: numero_da_linha}.
    Usado para saber se um contrato já existe (upsert).
    """
    try:
        ids = ws.col_values(1)  # coluna A = ID
        return {v: i + 1 for i, v in enumerate(ids) if v and v != "ID"}
    except Exception:
        return {}


def _montar_linha_contrato(c: dict, cid: str) -> list:
    """Monta a linha da planilha a partir do dict do contrato."""
    status = _limpar(c.get("status_atual"))
    dp     = c.get("data_prevista_entrega")
    dias   = _dias_restantes(dp)
    atrasado = "TRUE" if dias < 0 else "FALSE"

    return [
        cid,                                          # ID
        _limpar(c.get("fonte")),                      # Fonte
        _limpar(c.get("cliente_nome")),               # Cliente
        _limpar(c.get("cliente_cpf_cnpj")),           # CPF_CNPJ
        _limpar(c.get("cliente_email")),              # Email
        _limpar(c.get("veiculo")),                    # Veiculo
        _limpar(c.get("placa")),                      # Placa
        status,                                       # Status
        _etapa_kanban(status),                        # Etapa_Kanban
        _fmt_data(dp),                                # Data_Prevista
        _fmt_data(c.get("data_entrega_definitiva")),  # Data_Entrega
        _fmt_data(c.get("data_venda")),               # Data_Venda
        atrasado,                                     # Atrasado
        dias,                                         # Dias_Restantes
        _limpar(c.get("byetech_contrato_id")),        # Byetech_ID
        _limpar(c.get("id_externo")),                 # ID_Externo
        _limpar(c.get("origem_dados", "SYNC")),       # Origem
        datetime.now().strftime("%d/%m/%Y %H:%M"),    # Ultima_Sync
    ]


def sheets_upsert_contrato(c: dict, cid: str, ws=None, indice: dict = None) -> bool:
    """
    Insere ou atualiza um contrato na aba Contratos.
    ws e indice podem ser passados para batch (evita releitura).
    """
    try:
        if ws is None:
            ws = _aba("Contratos")
        if indice is None:
            indice = _carregar_indice_contratos(ws)

        linha = _montar_linha_contrato(c, cid)

        if cid in indice:
            # Atualiza linha existente
            num = indice[cid]
            ws.update(f"A{num}:R{num}", [linha])
        else:
            # Adiciona nova linha
            ws.append_row(linha, value_input_option="USER_ENTERED")
            indice[cid] = len(indice) + 2  # aproximado

        return True
    except Exception as e:
        log.warning(f"[Sheets] upsert_contrato({cid}): {e}")
        return False


def sheets_upsert_lote(contratos: list[tuple[dict, str]]) -> tuple[int, int]:
    """
    Envia múltiplos contratos de forma eficiente.
    Lê o índice uma vez e faz todas as atualizações em lote.
    Retorna (ok, erros).
    """
    if not contratos:
        return 0, 0

    try:
        ws = _aba("Contratos")
    except Exception as e:
        log.error(f"[Sheets] Não foi possível abrir aba Contratos: {e}")
        return 0, len(contratos)

    # Lê índice atual
    indice = _carregar_indice_contratos(ws)

    # Separa novos de existentes
    novos    = []
    updates  = []
    for c, cid in contratos:
        linha = _montar_linha_contrato(c, cid)
        if cid in indice:
            updates.append((indice[cid], linha))
        else:
            novos.append(linha)

    ok_total = err_total = 0

    # Insere novos em lote
    if novos:
        try:
            # gspread não tem append_rows em lote direto, usamos update na última linha
            ultima = ws.row_count
            for i, linha in enumerate(novos):
                ws.append_row(linha, value_input_option="USER_ENTERED")
            ok_total += len(novos)
            log.info(f"[Sheets] {len(novos)} novos contratos inseridos")
        except Exception as e:
            log.error(f"[Sheets] Erro ao inserir novos: {e}")
            err_total += len(novos)

    # Atualiza existentes (um por um — Google Sheets não suporta batch update em linhas diferentes)
    for num, linha in updates:
        try:
            ws.update(f"A{num}:R{num}", [linha])
            ok_total += 1
        except Exception as e:
            log.warning(f"[Sheets] Erro update linha {num}: {e}")
            err_total += 1

    log.info(f"[Sheets] Lote: {ok_total} ok | {err_total} erros")
    return ok_total, err_total


def sheets_add_historico(
    cid: str,
    status_ant: str,
    status_novo: str,
    fonte: str,
    etapa_ant: str = "",
    etapa_nova: str = "",
    obs: str = "",
):
    """Adiciona linha na aba Historico."""
    try:
        ws = _aba("Historico")
        ws.append_row([
            cid,
            status_ant or "",
            status_novo or "",
            etapa_ant or "",
            etapa_nova or _etapa_kanban(status_novo or ""),
            fonte,
            datetime.now().strftime("%d/%m/%Y %H:%M"),
            obs,
        ], value_input_option="USER_ENTERED")
    except Exception as e:
        log.debug(f"[Sheets] historico: {e}")


def sheets_log_sync(fonte: str) -> Optional[int]:
    """Registra início de sync. Retorna número da linha para atualizar depois."""
    try:
        ws = _aba("Sync_Log")
        ws.append_row([
            fonte,
            "em_andamento",
            datetime.now().strftime("%d/%m/%Y %H:%M"),
            "", "", "", "",
        ], value_input_option="USER_ENTERED")
        return ws.row_count
    except Exception as e:
        log.debug(f"[Sheets] log_sync: {e}")
        return None


def sheets_log_fim(
    linha: Optional[int],
    status: str,
    importados: int = 0,
    atualizados: int = 0,
    erro: str = "",
):
    """Atualiza linha do log de sync com resultado."""
    if not linha:
        return
    try:
        ws = _aba("Sync_Log")
        ws.update(f"B{linha}:G{linha}", [[
            status,
            ws.cell(linha, 3).value,  # mantém hora início
            datetime.now().strftime("%d/%m/%Y %H:%M"),
            importados,
            atualizados,
            erro,
        ]])
    except Exception as e:
        log.debug(f"[Sheets] log_fim: {e}")


def sheets_marcar_entregue(cid: str, data_entrega: datetime) -> bool:
    """Marca contrato como entregue na planilha."""
    try:
        ws     = _aba("Contratos")
        indice = _carregar_indice_contratos(ws)
        if cid not in indice:
            return False

        num = indice[cid]
        # Atualiza: Status, Etapa, Data_Entrega, Atrasado, Dias
        ws.update(f"H{num}:N{num}", [[
            "Definitivo Entregue",           # Status (col H)
            "entregue",                      # Etapa_Kanban (col I)
            _fmt_data(data_entrega),         # Data_Prevista (col J) — mantém
            _fmt_data(data_entrega),         # Data_Entrega (col K)
            ws.cell(num, 12).value,          # Data_Venda (col L) — mantém
            "FALSE",                         # Atrasado (col M)
            "0",                             # Dias_Restantes (col N)
        ]])

        sheets_add_historico(cid, "Pendente", "Definitivo Entregue", "MANUAL",
                             obs=f"Entregue em {_fmt_data(data_entrega)}")
        log.info(f"[Sheets] ✅ Entregue: {cid}")
        return True
    except Exception as e:
        log.warning(f"[Sheets] marcar_entregue({cid}): {e}")
        return False


# ═══════════════════════════════════════════════════════════════
#  SETUP — cria estrutura inicial da planilha
# ═══════════════════════════════════════════════════════════════

def cmd_setup():
    """Cria as abas e cabeçalhos na planilha Google Sheets."""
    print("\n⚙️  Configurando planilha Google Sheets...\n")

    try:
        # Aba Contratos
        ws = _aba("Contratos")
        primeira = ws.row_values(1)
        if primeira != CABECALHO_CONTRATOS:
            ws.clear()
            ws.append_row(CABECALHO_CONTRATOS)
            # Formata cabeçalho (negrito)
            ws.format("A1:R1", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
            })
            print("  ✅ Aba 'Contratos' criada")
        else:
            print("  ✅ Aba 'Contratos' já existe")

        # Aba Historico
        wh = _aba("Historico")
        if wh.row_values(1) != CABECALHO_HISTORICO:
            wh.clear()
            wh.append_row(CABECALHO_HISTORICO)
            wh.format("A1:H1", {"textFormat": {"bold": True}})
            print("  ✅ Aba 'Historico' criada")
        else:
            print("  ✅ Aba 'Historico' já existe")

        # Aba Sync_Log
        wl = _aba("Sync_Log")
        if wl.row_values(1) != CABECALHO_SYNC_LOG:
            wl.clear()
            wl.append_row(CABECALHO_SYNC_LOG)
            wl.format("A1:G1", {"textFormat": {"bold": True}})
            print("  ✅ Aba 'Sync_Log' criada")
        else:
            print("  ✅ Aba 'Sync_Log' já existe")

        print("\n✅ Planilha configurada! Próximo passo:")
        print("   python gsheets_sync.py --teste\n")

    except Exception as e:
        print(f"\n❌ Erro: {e}\n")
        sys.exit(1)


# ═══════════════════════════════════════════════════════════════
#  TESTE DE CONEXÃO
# ═══════════════════════════════════════════════════════════════

def cmd_teste():
    print("\n" + "═" * 55)
    print("  TESTE DE CONEXÃO — Google Sheets")
    print("═" * 55 + "\n")

    # Verifica credentials.json
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"  ❌ '{CREDENTIALS_FILE}' não encontrado!")
        print("\n  COMO CRIAR:")
        print("  1. Acesse: console.cloud.google.com")
        print("  2. Novo projeto → 'Byetech Entregas'")
        print("  3. Menu → APIs → Biblioteca")
        print("     → Ative 'Google Sheets API'")
        print("     → Ative 'Google Drive API'")
        print("  4. APIs → Credenciais → + Criar credencial")
        print("     → 'Conta de serviço' → preencha o nome → Criar")
        print("     → Clique na conta criada → Chaves → Adicionar chave → JSON")
        print("     → Salve como 'credentials.json' nesta pasta")
        print("  5. Copie o e-mail da conta (client_email no JSON)")
        print("  6. Abra sua planilha → Compartilhar → cole o e-mail → Editor")
        print("  7. Rode: python gsheets_sync.py --setup\n")
        sys.exit(1)

    # Testa leitura do JSON
    import json
    try:
        with open(CREDENTIALS_FILE) as f:
            creds_data = json.load(f)
        email_sa = creds_data.get("client_email", "?")
        print(f"  ✅ credentials.json encontrado")
        print(f"     Conta de serviço: {email_sa}")
    except Exception as e:
        print(f"  ❌ Erro ao ler credentials.json: {e}")
        sys.exit(1)

    # Testa conexão
    try:
        _conectar()
        print(f"  ✅ Conectado na planilha '{SHEET_NAME}'")
    except Exception as e:
        print(f"  ❌ {e}")
        sys.exit(1)

    # Testa escrita
    try:
        ws = _aba("Contratos")
        _ = ws.row_values(1)
        print("  ✅ Aba 'Contratos' acessível")
    except Exception as e:
        print(f"  ⚠️  Aba 'Contratos' não encontrada — rode: python gsheets_sync.py --setup")

    print(f"\n  ✅ Tudo OK! Próximo:")
    print(f"     python gsheets_sync.py --setup")
    print(f"     python gsheets_sync.py --inicial\n")


# ═══════════════════════════════════════════════════════════════
#  SYNC INICIAL — exporta SQLite → Sheets
# ═══════════════════════════════════════════════════════════════

async def cmd_inicial():
    print("\n⚠️  Sync inicial: exporta TODOS os contratos para o Google Sheets.")
    ok = input("Confirmar? (s/n): ").strip().lower()
    if ok != "s":
        print("Cancelado.")
        return

    linha_log = sheets_log_sync("INICIAL")

    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from app.database import init_db, SessionLocal, Contrato
        from sqlalchemy import select

        await init_db()
        async with SessionLocal() as session:
            result = await session.execute(select(Contrato))
            todos  = result.scalars().all()

        print(f"\n📦 {len(todos)} contratos encontrados. Enviando...\n")

        lote = []
        for c in todos:
            data = {
                "fonte": c.fonte, "id_externo": c.id_externo,
                "byetech_contrato_id": c.byetech_contrato_id,
                "cliente_nome": c.cliente_nome, "cliente_cpf_cnpj": c.cliente_cpf_cnpj,
                "cliente_email": c.cliente_email, "veiculo": c.veiculo, "placa": c.placa,
                "status_atual": c.status_atual,
                "data_prevista_entrega": c.data_prevista_entrega,
                "data_entrega_definitiva": c.data_entrega_definitiva,
                "data_venda": c.data_venda, "origem_dados": "METABASE",
            }
            lote.append((data, c.id))

        enviados, erros = sheets_upsert_lote(lote)
        sheets_log_fim(linha_log, "sucesso" if erros == 0 else "erro", importados=enviados)

        print(f"\n{'✅' if erros == 0 else '⚠️ '} Concluído!")
        print(f"   Enviados : {enviados}")
        print(f"   Erros    : {erros}")
        print("\n▶ Agora abra o Google Sheets e veja os dados!")
        print("  Depois acesse glideapps.com para criar o app.\n")

    except ImportError:
        print("\n⚠️  SQLite local não encontrado.")
        print("   Rode: python gsheets_sync.py --fonte metabase\n")
        sheets_log_fim(linha_log, "erro", erro="SQLite não encontrado")


# ═══════════════════════════════════════════════════════════════
#  SYNC METABASE
# ═══════════════════════════════════════════════════════════════

async def sync_metabase(full: bool = False) -> dict:
    linha_log = sheets_log_sync("METABASE")
    log.info(f"[Metabase] Iniciando sync...")

    try:
        # Usa scraper do projeto se disponível
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from app.services.sync_service import run_metabase_sync
        from app.database import SessionLocal, Contrato
        from sqlalchemy import select

        await run_metabase_sync(full=full)

        async with SessionLocal() as session:
            result = await session.execute(select(Contrato))
            todos  = result.scalars().all()

        lote = [
            ({
                "fonte": c.fonte, "id_externo": c.id_externo,
                "byetech_contrato_id": c.byetech_contrato_id,
                "cliente_nome": c.cliente_nome, "cliente_cpf_cnpj": c.cliente_cpf_cnpj,
                "cliente_email": c.cliente_email, "veiculo": c.veiculo, "placa": c.placa,
                "status_atual": c.status_atual,
                "data_prevista_entrega": c.data_prevista_entrega,
                "data_entrega_definitiva": c.data_entrega_definitiva,
                "data_venda": c.data_venda, "origem_dados": "METABASE",
            }, c.id)
            for c in todos
        ]

        ok, err = sheets_upsert_lote(lote)
        sheets_log_fim(linha_log, "sucesso", importados=ok)
        log.info(f"[Metabase] ✅ {ok} contratos no Sheets")
        return {"ok": True, "importados": ok}

    except Exception as e:
        log.error(f"[Metabase] ❌ {e}")
        sheets_log_fim(linha_log, "erro", erro=str(e)[:200])
        return {"ok": False, "erro": str(e)}


# ═══════════════════════════════════════════════════════════════
#  SYNC BYETECH CRM
# ═══════════════════════════════════════════════════════════════

async def sync_byetech() -> dict:
    linha_log = sheets_log_sync("BYETECH")
    log.info("[Byetech] Iniciando scrape...")

    try:
        from app.scrapers.byetech_crm import scrape_contratos

        async def twofa_cb():
            print("\n🔐 Byetech CRM pediu verificação em dois fatores.")
            return input("   Digite o código 2FA: ").strip()

        contratos = await scrape_contratos(twofa_callback=twofa_cb)
        log.info(f"[Byetech] {len(contratos)} contratos recebidos")

    except Exception as e:
        log.error(f"[Byetech] ❌ {e}")
        sheets_log_fim(linha_log, "erro", erro=str(e)[:200])
        return {"ok": False, "erro": str(e)}

    if not contratos:
        sheets_log_fim(linha_log, "sucesso", importados=0)
        return {"ok": True, "importados": 0}

    lote = []
    for c in contratos:
        c["origem_dados"] = "BYETECH_CRM"
        id_ext = c.get("id_externo") or c.get("byetech_contrato_id", "")
        cid    = _id_contrato(c.get("fonte", "BYETECH"), id_ext, c.get("cliente_cpf_cnpj", ""))
        lote.append((c, cid))

    ok, err = sheets_upsert_lote(lote)
    sheets_log_fim(linha_log, "sucesso", importados=ok, atualizados=ok)
    log.info(f"[Byetech] ✅ {ok} enviados")
    return {"ok": True, "importados": ok}


# ═══════════════════════════════════════════════════════════════
#  SYNC COMPLETO
# ═══════════════════════════════════════════════════════════════

async def sync_completo(full_metabase: bool = False):
    print("\n" + "═" * 45)
    print(f"  SYNC — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("═" * 45)

    inicio = datetime.now()

    print("\n📊 [1/2] Metabase...")
    r1 = await sync_metabase(full=full_metabase)
    print(f"   {'✅' if r1['ok'] else '❌'} {r1.get('importados', 0)} contratos")

    print("\n🏢 [2/2] Byetech CRM...")
    r2 = await sync_byetech()
    print(f"   {'✅' if r2['ok'] else '❌'} {r2.get('importados', 0)} contratos")

    s = int((datetime.now() - inicio).total_seconds())
    print(f"\n✅ Sync concluído em {s}s")
    print("   Abra o Google Sheets para ver os dados atualizados.\n")


# ═══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Byetech Entregas — Sync para Google Sheets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python gsheets_sync.py --setup    Cria abas e cabeçalhos na planilha
  python gsheets_sync.py --teste    Verifica conexão
  python gsheets_sync.py --inicial  1ª vez: exporta SQLite → Sheets
  python gsheets_sync.py            Sync completo (Metabase + Byetech)
  python gsheets_sync.py --full     Metabase completo
        """
    )
    parser.add_argument("--setup",   action="store_true", help="Cria abas e cabeçalhos")
    parser.add_argument("--teste",   action="store_true", help="Testa conexão")
    parser.add_argument("--inicial", action="store_true", help="Exporta tudo (1ª vez)")
    parser.add_argument("--full",    action="store_true", help="Metabase completo")
    parser.add_argument("--fonte",
        choices=["metabase", "byetech", "completo"],
        default="completo"
    )
    args = parser.parse_args()

    if args.setup:
        cmd_setup()
    elif args.teste:
        cmd_teste()
    elif args.inicial:
        asyncio.run(cmd_inicial())
    elif args.fonte == "metabase":
        asyncio.run(sync_metabase(full=args.full))
    elif args.fonte == "byetech":
        asyncio.run(sync_byetech())
    else:
        asyncio.run(sync_completo(full_metabase=args.full))


if __name__ == "__main__":
    main()
