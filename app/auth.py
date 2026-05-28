"""
Autenticação por sessão — Byecar Entregas.

Fluxo:
  1. Usuário acessa /login e entra com credenciais Byetech.
  2. POST /api/byetech/login valida contra o Byetech CRM.
  3. Se OK → create_session() gera token e a resposta define o cookie "byetech_app_session".
  4. Todas as rotas protegidas usam Depends(require_auth).
  5. Rotas HTML verificam o cookie e redirecionam para /login se inválido.

Sessões ficam em memória (dict). O Render free reinicia a cada deploy/idle,
então os usuários fazem login no máximo uma vez por dia.
"""
import os
import secrets
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Cookie, HTTPException, Request

logger = logging.getLogger("auth")

SESSION_COOKIE  = "byetech_app_session"
SESSION_TTL_H   = int(os.getenv("SESSION_TTL_HOURS", "12"))

# { token: expira_em }
_sessions: dict[str, datetime] = {}


# ── Gestão de sessões ──────────────────────────────────────────────────────────

def create_session() -> str:
    """Cria uma sessão nova e retorna o token."""
    token = secrets.token_urlsafe(32)
    _sessions[token] = datetime.utcnow() + timedelta(hours=SESSION_TTL_H)
    _purge_expired()
    logger.info(f"[auth] Nova sessão criada (total ativas: {len(_sessions)})")
    return token


def validate_session(token: str) -> bool:
    """Retorna True se o token existe e não expirou."""
    exp = _sessions.get(token)
    if not exp:
        return False
    if datetime.utcnow() > exp:
        del _sessions[token]
        return False
    return True


def revoke_session(token: str):
    _sessions.pop(token, None)
    logger.info(f"[auth] Sessão revogada (total ativas: {len(_sessions)})")


def active_sessions() -> int:
    _purge_expired()
    return len(_sessions)


def _purge_expired():
    now = datetime.utcnow()
    expired = [k for k, v in _sessions.items() if v < now]
    for k in expired:
        del _sessions[k]


# ── FastAPI dependencies ───────────────────────────────────────────────────────

async def require_auth(request: Request) -> bool:
    """
    Dependency para rotas de API.
    Lança HTTP 401 se não houver sessão válida.
    """
    token = request.cookies.get(SESSION_COOKIE)
    if not token or not validate_session(token):
        raise HTTPException(
            status_code=401,
            detail="Sessão expirada ou inválida — faça login novamente.",
        )
    return True


def check_auth_cookie(request: Request) -> bool:
    """
    Verificação sem exceção — usado nas rotas HTML para decidir redirect.
    Retorna True se autenticado, False caso contrário.
    """
    token = request.cookies.get(SESSION_COOKIE)
    return bool(token and validate_session(token))
