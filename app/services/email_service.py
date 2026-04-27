"""
Serviço de e-mail para fluxo Unidas.
Envia e-mail ao cliente solicitando confirmação de entrega do veículo.
"""
import os
import ssl
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "cs@byecar.com.br")
SMTP_PASS = os.getenv("SMTP_PASS", "")

BASE_URL        = os.getenv("BASE_URL", "http://localhost:8001").rstrip("/")
BYECAR_WHATSAPP = os.getenv("BYECAR_WHATSAPP", "5511999999999")

FROM_NAME = "Byecar Customer Success"
FROM_ADDR = SMTP_USER


def _whatsapp_url(mensagem: str = "") -> str:
    import urllib.parse
    msg = urllib.parse.quote(mensagem)
    return f"https://wa.me/{BYECAR_WHATSAPP}?text={msg}"


def _build_unidas_email(
    cliente_nome: str,
    veiculo: str,
    data_prevista: str,
    contrato_id: str,
) -> tuple[str, str, str]:
    """Retorna (assunto, texto_plain, texto_html)."""
    assunto = f"Seu veículo chegou? Confirme a retirada — {veiculo or 'Byecar'}"

    primeiro_nome = (cliente_nome or "Cliente").split()[0]

    confirmar_url = f"{BASE_URL}/confirmar/{contrato_id}"
    wpp_msg = (
        f"Olá! Sou cliente Byecar e gostaria de falar com o pós-venda "
        f"sobre o veículo {veiculo or ''}."
    )
    wpp_url = _whatsapp_url(wpp_msg)

    texto_plain = f"""Olá, {primeiro_nome}!

Passando para verificar como está o processo de entrega do seu veículo {veiculo or ''}.

Conforme nosso acompanhamento, a previsão de entrega era {data_prevista or 'em breve'}.

Você já retirou o veículo? Se sim, confirme pelo link abaixo — leva menos de 30 segundos:
{confirmar_url}

Caso precise de suporte, nosso time de pós-venda está disponível pelo WhatsApp:
{wpp_url}

Atenciosamente,
Equipe Customer Success — Byecar
"""

    texto_html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#f0f2f7;font-family:'Segoe UI',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f2f7;padding:40px 16px;">
  <tr><td align="center">
    <table width="560" cellpadding="0" cellspacing="0"
           style="background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,.09);max-width:560px;width:100%">

      <!-- Header -->
      <tr>
        <td style="background:#1a1d27;padding:28px 36px;">
          <table cellpadding="0" cellspacing="0">
            <tr>
              <td style="background:#4f7cff;border-radius:8px;width:32px;height:32px;text-align:center;vertical-align:middle;">
                <span style="color:#fff;font-size:16px;font-weight:800;line-height:32px;display:block;">B</span>
              </td>
              <td style="padding-left:10px;">
                <span style="font-size:18px;font-weight:700;color:#fff;">Byecar</span>
              </td>
            </tr>
          </table>
          <p style="font-size:15px;font-weight:600;color:#e8ecf6;margin:18px 0 4px 0;line-height:1.4;">
            Seu veículo chegou? Confirme a retirada ✓
          </p>
          <p style="font-size:12px;color:#8c91a8;margin:0;">
            Leva menos de 30 segundos — é só clicar no botão abaixo.
          </p>
        </td>
      </tr>

      <!-- Body -->
      <tr>
        <td style="padding:32px 36px;">

          <p style="font-size:15px;color:#333;margin:0 0 16px;">
            Olá, <strong>{primeiro_nome}</strong>!
          </p>

          <p style="font-size:14px;color:#555;line-height:1.6;margin:0 0 8px;">
            Passando para verificar a entrega do seu veículo
            <strong style="color:#1a1d27;">{veiculo or ''}</strong>.
          </p>

          <!-- Info box -->
          <table width="100%" cellpadding="0" cellspacing="0"
                 style="background:#f5f7ff;border:1px solid #dde3ff;border-radius:10px;margin:20px 0;">
            <tr>
              <td style="padding:14px 18px;">
                <p style="font-size:11px;font-weight:600;color:#8c91a8;text-transform:uppercase;letter-spacing:.06em;margin:0 0 3px;">
                  Previsão de entrega
                </p>
                <p style="font-size:15px;font-weight:600;color:#1a1d27;margin:0;">
                  {data_prevista or 'Em breve'}
                </p>
              </td>
            </tr>
          </table>

          <p style="font-size:14px;color:#555;line-height:1.6;margin:0 0 24px;">
            Já retirou o veículo? Confirme abaixo para que possamos atualizar seu processo
            e garantir que tudo está em ordem.
          </p>

          <!-- CTA principal -->
          <table cellpadding="0" cellspacing="0" width="100%">
            <tr>
              <td align="center" style="padding-bottom:12px;">
                <a href="{confirmar_url}"
                   style="display:inline-block;background:#4f7cff;color:#fff;text-decoration:none;
                          font-weight:700;font-size:15px;padding:14px 36px;border-radius:10px;
                          letter-spacing:.01em;">
                  ✓ &nbsp; Confirmar recebimento do veículo
                </a>
              </td>
            </tr>
            <!-- CTA secundário -->
            <tr>
              <td align="center">
                <a href="{wpp_url}"
                   style="display:inline-block;background:#25d366;color:#fff;text-decoration:none;
                          font-weight:600;font-size:14px;padding:12px 28px;border-radius:10px;">
                  💬 &nbsp; Falar com o pós-venda Byecar
                </a>
              </td>
            </tr>
          </table>

          <p style="font-size:12px;color:#aaa;margin:20px 0 0;text-align:center;line-height:1.5;">
            Ou acesse diretamente:<br/>
            <a href="{confirmar_url}" style="color:#4f7cff;word-break:break-all;">{confirmar_url}</a>
          </p>

        </td>
      </tr>

      <!-- Footer -->
      <tr>
        <td style="background:#f9f9f9;padding:18px 36px;border-top:1px solid #eee;">
          <p style="font-size:11px;color:#bbb;margin:0;line-height:1.6;">
            Equipe Customer Success — Byecar &nbsp;|&nbsp;
            Este e-mail foi enviado automaticamente. Para dúvidas, use o botão de WhatsApp acima.
          </p>
        </td>
      </tr>

    </table>
  </td></tr>
</table>
</body>
</html>"""

    return assunto, texto_plain, texto_html


async def send_unidas_confirmation(
    cliente_email: str,
    cliente_nome: str,
    veiculo: str = "",
    data_prevista: str = "",
    contrato_id: str = "",
) -> bool:
    """
    Envia e-mail de confirmação de entrega para cliente Unidas.
    Retorna True se enviado com sucesso.
    """
    if not SMTP_PASS:
        raise Exception("SMTP_PASS não configurado no .env")

    if not cliente_email:
        raise Exception("E-mail do cliente não informado")

    assunto, texto_plain, texto_html = _build_unidas_email(
        cliente_nome, veiculo, data_prevista, contrato_id
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = assunto
    msg["From"]    = f"{FROM_NAME} <{FROM_ADDR}>"
    msg["To"]      = cliente_email
    msg["Reply-To"] = FROM_ADDR

    msg.attach(MIMEText(texto_plain, "plain", "utf-8"))
    msg.attach(MIMEText(texto_html,  "html",  "utf-8"))

    tls_context = ssl.create_default_context()
    tls_context.check_hostname = False
    tls_context.verify_mode = ssl.CERT_NONE

    await aiosmtplib.send(
        msg,
        hostname=SMTP_HOST,
        port=SMTP_PORT,
        start_tls=True,
        username=SMTP_USER,
        password=SMTP_PASS,
        tls_context=tls_context,
    )
    return True


async def send_batch_unidas(contratos: list[dict]) -> dict:
    """
    Envia e-mails para todos os contratos Unidas sem data de entrega.
    Retorna resumo de enviados/erros.
    """
    enviados = 0
    erros = []

    for c in contratos:
        if c.get("fonte") != "UNIDAS":
            continue
        if c.get("data_entrega_definitiva"):
            continue

        email = c.get("cliente_email", "")
        if not email:
            erros.append(f"{c.get('cliente_nome', '?')}: sem e-mail cadastrado")
            continue

        data_fmt = ""
        dp = c.get("data_prevista_entrega")
        if dp:
            try:
                if isinstance(dp, str):
                    dp = datetime.fromisoformat(dp)
                data_fmt = dp.strftime("%d/%m/%Y")
            except Exception:
                data_fmt = str(dp)

        try:
            await send_unidas_confirmation(
                cliente_email=email,
                cliente_nome=c.get("cliente_nome", ""),
                veiculo=c.get("veiculo", ""),
                data_prevista=data_fmt,
                contrato_id=c.get("id", ""),
            )
            enviados += 1
        except Exception as e:
            erros.append(f"{c.get('cliente_nome', '?')}: {e}")

    return {"enviados": enviados, "erros": erros}
