# Byetech Entregas — Como rodar

## 1. Configurar o .env

Abra o arquivo `.env` e preencha os campos marcados:

```
SMTP_PASS=         ← senha do e-mail cs@byecar.com.br
SLACK_BOT_TOKEN=   ← token do bot Slack (ver abaixo)
```

### Obter o Slack Bot Token
1. Acesse https://api.slack.com/apps → "Create New App"
2. Nome: `Byetech Entregas` | Workspace: Byetech
3. Em "OAuth & Permissions" adicione os scopes:
   - `chat:write`, `channels:manage`, `channels:read`, `groups:read`
4. "Install to Workspace" → copie o `Bot User OAuth Token` (começa com `xoxb-`)
5. Cole no `.env` em `SLACK_BOT_TOKEN`

---

## 2. Rodar o portal

```bash
cd C:\Users\verid\byetech-entregas
python run.py
```

Acesse: **http://localhost:8000**

---

## 3. Configurar senha do e-mail (Gmail)

Se usar Gmail para `cs@byecar.com.br`:
1. Ative "Verificação em 2 etapas" na conta Google
2. Gere uma "Senha de app" em myaccount.google.com/apppasswords
3. Cole essa senha no `.env` em `SMTP_PASS`

---

## 4. Primeiro uso

1. Clique em **"Sincronizar agora"** — vai pedir o código 2FA do Byetech
2. Forneça o código quando solicitado
3. Os contratos serão carregados automaticamente

**Sincronização automática:** todos os dias às 08:00
**Alertas Slack:** todos os dias às 08:30

---

## 5. Importar Movida

Clique em **"Importar Movida"** → arraste ou selecione a planilha exportada.
O sistema detecta automaticamente as colunas.

---

## Estrutura de arquivos

```
byetech-entregas/
├── .env                    ← credenciais (NÃO versionar)
├── byetech.db              ← banco de dados local (criado automaticamente)
├── run.py                  ← inicia o servidor
├── app/
│   ├── main.py             ← FastAPI + rotas
│   ├── database.py         ← modelos do banco
│   ├── scrapers/
│   │   ├── byetech_crm.py  ← Byetech CRM (com 2FA)
│   │   ├── portaldealer.py ← GWM / LM
│   │   ├── localiza.py     ← Localiza
│   │   └── movida.py       ← importação de planilha
│   ├── services/
│   │   ├── sync_service.py ← orquestrador central
│   │   ├── scheduler.py    ← agendamento diário
│   │   ├── slack_service.py← alertas Slack
│   │   └── email_service.py← e-mails Unidas
│   └── templates/
│       └── index.html      ← dashboard
```
