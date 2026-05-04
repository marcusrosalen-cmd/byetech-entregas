"""
processar_pendentes.py
======================
Roda LOCALMENTE (onde Playwright está instalado).
Busca as entregas pendentes no servidor Render e as processa no Byetech CRM.

Uso:
    python processar_pendentes.py               # processa tudo
    python processar_pendentes.py --listar      # só lista, sem processar
    python processar_pendentes.py --id 42       # processa apenas o pendente de ID 42
"""

import asyncio
import argparse
import sys
import os
import httpx
from datetime import datetime

# URL do servidor Render
RENDER_URL = os.getenv("RENDER_URL", "https://byetech-entregas.onrender.com")


async def listar_pendentes() -> list[dict]:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{RENDER_URL}/api/byetech/pendentes")
        r.raise_for_status()
        data = r.json()
    return data.get("pendentes", [])


async def marcar_done(pendente_id: int, sucesso: bool, erro: str = None):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{RENDER_URL}/api/byetech/pendentes/{pendente_id}/done",
            json={"sucesso": sucesso, "erro": erro},
        )
        r.raise_for_status()


async def processar(pendentes: list[dict], dry_run: bool = False):
    if not pendentes:
        print("✅ Nenhuma entrega pendente.")
        return

    print(f"\n{'='*60}")
    print(f"  {len(pendentes)} entrega(s) pendente(s) para atualizar no Byetech")
    print(f"{'='*60}\n")

    # Verifica sessão Byetech
    try:
        from app.scrapers.byetech_crm import (
            update_delivery_by_cpf, _load_session_from_disk, _test_session
        )
    except ImportError:
        print("❌ Playwright não instalado. Execute: pip install playwright && playwright install chromium")
        sys.exit(1)

    cookies = _load_session_from_disk()
    if not cookies or not await _test_session(cookies):
        print("❌ Sessão Byetech expirada.")
        print("   Acesse o portal local (python -m uvicorn app.main:app --port 8001)")
        print("   e clique em '🔑 Renovar sessão' para reativar.")
        sys.exit(1)

    print("✅ Sessão Byetech válida\n")

    ok_count = 0
    fail_count = 0

    for p in pendentes:
        cpf    = p["cliente_cpf"]
        nome   = p["cliente_nome"] or "?"
        placa  = p.get("placa") or None
        pid    = p["id"]
        tipo   = p.get("tipo", "entrega")

        try:
            data_str = p.get("data_entrega", "")
            data_dt  = datetime.fromisoformat(data_str) if data_str else datetime.utcnow()
        except Exception:
            data_dt = datetime.utcnow()

        print(f"→ [{pid}] {nome} | CPF: {cpf[:6]}... | Placa: {placa or '—'} | {data_dt.strftime('%d/%m/%Y')}")

        if dry_run:
            print("  (dry-run — não executado)\n")
            continue

        try:
            ok = await update_delivery_by_cpf(
                cpf_raw=cpf,
                data_entrega=data_dt,
                placa=placa,
            )
            if ok:
                print(f"  ✅ Byetech atualizado com sucesso")
                await marcar_done(pid, sucesso=True)
                ok_count += 1
            else:
                print(f"  ⚠️  update_delivery retornou False — verifique manualmente")
                await marcar_done(pid, sucesso=False, erro="update_delivery retornou False")
                fail_count += 1
        except Exception as e:
            print(f"  ❌ Erro: {e}")
            await marcar_done(pid, sucesso=False, erro=str(e)[:200])
            fail_count += 1

        print()

    if not dry_run:
        print(f"\n{'='*60}")
        print(f"  Concluído: {ok_count} atualizados | {fail_count} falhas")
        print(f"{'='*60}")


async def main():
    parser = argparse.ArgumentParser(description="Processa entregas pendentes no Byetech CRM")
    parser.add_argument("--listar", action="store_true", help="Apenas lista, não processa")
    parser.add_argument("--dry-run", action="store_true", help="Simula sem executar no Byetech")
    parser.add_argument("--id", type=int, help="Processa apenas o pendente com este ID")
    args = parser.parse_args()

    print(f"🔗 Conectando em {RENDER_URL}...")
    pendentes = await listar_pendentes()

    if args.id:
        pendentes = [p for p in pendentes if p["id"] == args.id]
        if not pendentes:
            print(f"❌ Nenhum pendente com ID={args.id}")
            sys.exit(1)

    if args.listar or not pendentes:
        if not pendentes:
            print("✅ Sem pendentes.")
            return
        print(f"\n{len(pendentes)} pendente(s):\n")
        for p in pendentes:
            data_fmt = p.get("data_entrega", "")[:10]
            print(f"  [{p['id']:3d}] {p['cliente_nome'] or '?':30s} | CPF: {p['cliente_cpf'][:6]}... | {data_fmt} | tentativas: {p['tentativas']}")
            if p.get("erro_ultimo"):
                print(f"        Último erro: {p['erro_ultimo'][:80]}")
        return

    await processar(pendentes, dry_run=args.dry_run)


if __name__ == "__main__":
    # Adiciona o diretório raiz do projeto ao path
    sys.path.insert(0, os.path.dirname(__file__))
    asyncio.run(main())
