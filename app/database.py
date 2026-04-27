from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, String, DateTime, Integer, Text, Boolean, Float, text
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./byetech.db")

engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Contrato(Base):
    __tablename__ = "contratos"

    id = Column(String, primary_key=True)          # ID único (fonte + id_externo)
    fonte = Column(String, nullable=False)          # GWM | LM | LOCALIZA | MOVIDA | UNIDAS
    id_externo = Column(String)                     # ID no sistema de origem
    cliente_nome = Column(String)
    cliente_cpf_cnpj = Column(String)
    cliente_email = Column(String)
    veiculo = Column(String)
    placa = Column(String)
    status_atual = Column(String)
    status_anterior = Column(String)
    data_prevista_entrega = Column(DateTime)
    data_entrega_definitiva = Column(DateTime)
    byetech_contrato_id = Column(String)           # ID no CRM Byetech
    dias_para_entrega = Column(Integer)
    atrasado = Column(Boolean, default=False)
    observacoes = Column(Text)
    data_venda = Column(DateTime)                  # Data de venda (do Metabase)
    pedido_id_locadora = Column(Integer)           # ID do pedido na locadora (do Metabase)
    ultima_atualizacao = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    criado_em = Column(DateTime, default=datetime.utcnow)


class HistoricoStatus(Base):
    __tablename__ = "historico_status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    contrato_id = Column(String, nullable=False)
    status_anterior = Column(String)
    status_novo = Column(String)
    fonte = Column(String)
    registrado_em = Column(DateTime, default=datetime.utcnow)


class AlertaEnviado(Base):
    __tablename__ = "alertas_enviados"

    id = Column(Integer, primary_key=True, autoincrement=True)
    contrato_id = Column(String, nullable=False)
    tipo = Column(String)                           # slack | email
    dias_antes = Column(Integer)
    enviado_em = Column(DateTime, default=datetime.utcnow)


class ConfigScraper(Base):
    __tablename__ = "config_scraper"

    chave = Column(String, primary_key=True)
    valor = Column(Text)
    atualizado_em = Column(DateTime, default=datetime.utcnow)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Migrations: add columns that may not exist on older DBs
        for stmt in [
            "ALTER TABLE contratos ADD COLUMN data_venda DATETIME",
            "ALTER TABLE contratos ADD COLUMN pedido_id_locadora INTEGER",
        ]:
            try:
                await conn.execute(text(stmt))
            except Exception:
                pass  # Column already exists


async def get_db() -> AsyncSession:
    async with SessionLocal() as session:
        yield session
