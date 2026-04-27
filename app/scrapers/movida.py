"""
Importador Movida — Planilha de atualização

Aceita: .xlsx, .xls, .csv
Mapeamento de colunas flexível: tenta detectar automaticamente por nome.
Colunas esperadas (qualquer combinação de nomes):
  - CPF/CNPJ do cliente
  - Nome do cliente
  - Status da entrega / Etapa atual
  - Data prevista de entrega
  - Data de entrega real (se houver)
  - Número do contrato / pedido
  - Placa do veículo (opcional)
"""
import re
import pandas as pd
from datetime import datetime
from typing import Optional
import io


# Mapeamento de nomes de colunas possíveis → campo padronizado
COLUMN_MAP = {
    "cliente_cpf_cnpj": [
        "cpf", "cnpj", "cpf/cnpj", "documento", "doc", "cpf_cnpj",
        "cpfcnpj", "cpf cnpj", "cliente cpf", "cliente cnpj",
    ],
    "cliente_nome": [
        "nome", "cliente", "nome do cliente", "razão social", "razao social",
        "nome cliente",
    ],
    "status_atual": [
        "status", "etapa", "situação", "situacao", "fase",
        "status entrega", "status da entrega", "etapa atual",
    ],
    "data_prevista_entrega": [
        "data prevista", "previsão", "previsao", "data previsão",
        "prazo", "data prazo", "entrega prevista", "data entrega prevista",
        "previsão de entrega",
    ],
    "data_entrega_definitiva": [
        "data entrega", "entregue em", "data de entrega",
        "entrega real", "data real", "entrega definitiva",
    ],
    "id_externo": [
        "contrato", "pedido", "número do contrato", "n° contrato",
        "numero contrato", "id contrato", "n pedido",
    ],
    "placa": [
        "placa", "placa do veiculo", "placa veículo", "placa veiculo",
    ],
    "veiculo": [
        "veículo", "veiculo", "modelo", "carro", "modelo do veiculo",
    ],
}


def _normalize(text: str) -> str:
    """Normaliza texto para comparação."""
    import unicodedata
    text = str(text).lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text


def _detect_columns(df: pd.DataFrame) -> dict[str, Optional[str]]:
    """Detecta mapeamento de colunas do DataFrame."""
    mapping = {}
    df_cols_normalized = {_normalize(c): c for c in df.columns}

    for field, aliases in COLUMN_MAP.items():
        found = None
        for alias in aliases:
            alias_norm = _normalize(alias)
            if alias_norm in df_cols_normalized:
                found = df_cols_normalized[alias_norm]
                break
            # Busca parcial
            for col_norm, col_orig in df_cols_normalized.items():
                if alias_norm in col_norm or col_norm in alias_norm:
                    found = col_orig
                    break
            if found:
                break
        mapping[field] = found

    return mapping


def _parse_date(value) -> Optional[datetime]:
    if pd.isna(value) if hasattr(value, '__class__') and value.__class__.__name__ in ['float', 'NaT'] else not value:
        return None
    if isinstance(value, datetime):
        return value
    if hasattr(value, 'to_pydatetime'):
        return value.to_pydatetime()
    text = str(value).strip()
    for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y %H:%M"]:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _clean_cpf(value) -> str:
    if not value or (isinstance(value, float) and pd.isna(value)):
        return ""
    return re.sub(r"[^\d]", "", str(value))


STATUS_ENTREGUE = ["entregue", "entrega realizada", "definitivo", "concluído", "concluido"]


def _is_entregue(status: str) -> bool:
    if not status:
        return False
    return any(s in status.lower() for s in STATUS_ENTREGUE)


def parse_movida_spreadsheet(file_content: bytes, filename: str) -> list[dict]:
    """
    Processa planilha Movida e retorna lista de contratos.
    file_content: bytes do arquivo
    filename: nome do arquivo para detectar extensão
    """
    ext = filename.lower().split(".")[-1]

    try:
        if ext in ["xlsx", "xls"]:
            df = pd.read_excel(io.BytesIO(file_content), dtype=str)
        elif ext == "csv":
            # Tenta diferentes encodings e separadores
            for enc in ["utf-8", "latin-1", "cp1252"]:
                for sep in [",", ";", "\t"]:
                    try:
                        df = pd.read_csv(io.BytesIO(file_content), encoding=enc, sep=sep, dtype=str)
                        if len(df.columns) > 1:
                            break
                    except Exception:
                        continue
                else:
                    continue
                break
        else:
            raise ValueError(f"Formato não suportado: {ext}")
    except Exception as e:
        raise ValueError(f"Erro ao ler planilha: {e}")

    # Remove linhas completamente vazias
    df = df.dropna(how="all")

    # Detecta colunas
    col_map = _detect_columns(df)

    contratos = []
    for _, row in df.iterrows():
        def get(field):
            col = col_map.get(field)
            if col and col in row:
                val = row[col]
                if pd.isna(val) if isinstance(val, float) else not str(val).strip():
                    return None
                return str(val).strip()
            return None

        cpf = _clean_cpf(get("cliente_cpf_cnpj"))
        if not cpf:
            continue

        status = get("status_atual") or "Não informado"
        data_prevista = _parse_date(get("data_prevista_entrega"))
        data_real = _parse_date(get("data_entrega_definitiva"))

        contrato = {
            "fonte": "MOVIDA",
            "id_externo": get("id_externo") or "",
            "cliente_cpf_cnpj": cpf,
            "cliente_nome": get("cliente_nome") or "",
            "status_atual": status,
            "data_prevista_entrega": data_prevista,
            "data_entrega_definitiva": data_real,
            "placa": get("placa") or "",
            "veiculo": get("veiculo") or "",
            "entregue": _is_entregue(status),
        }
        contratos.append(contrato)

    # Filtra entregues e ordena por data prevista
    contratos = [c for c in contratos if not c["entregue"]]
    contratos.sort(key=lambda x: x.get("data_prevista_entrega") or datetime.max)

    return contratos


def get_unmapped_columns(file_content: bytes, filename: str) -> dict:
    """Retorna quais colunas foram detectadas e quais ficaram sem mapear."""
    ext = filename.lower().split(".")[-1]
    if ext in ["xlsx", "xls"]:
        df = pd.read_excel(io.BytesIO(file_content), dtype=str, nrows=0)
    else:
        df = pd.read_csv(io.BytesIO(file_content), dtype=str, nrows=0)

    col_map = _detect_columns(df)
    return {
        "colunas_encontradas": list(df.columns),
        "mapeamento": col_map,
        "nao_mapeadas": [f for f, c in col_map.items() if c is None],
    }
