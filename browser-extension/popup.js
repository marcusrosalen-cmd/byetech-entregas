const DEFAULT_URL = "https://byetech-entregas.onrender.com";
const DEFAULT_SECRET = "byetech-local";
const BYETECH_DOMAIN = ".byetech.pro";

// ── Elementos ──────────────────────────────────────────────
const crmBadge      = document.getElementById("crm-status");
const platformBadge = document.getElementById("platform-status");
const syncBtn       = document.getElementById("sync-btn");
const resultDiv     = document.getElementById("result");
const urlInput      = document.getElementById("platform-url");
const secretInput   = document.getElementById("secret");
const saveBtn       = document.getElementById("save-config");
const savedMsg      = document.getElementById("saved-msg");

// ── Configuração (persistida em chrome.storage.local) ─────
async function loadConfig() {
  return new Promise((resolve) => {
    chrome.storage.local.get({ platformUrl: DEFAULT_URL, secret: DEFAULT_SECRET }, resolve);
  });
}

async function saveConfig(url, secret) {
  return new Promise((resolve) => {
    chrome.storage.local.set({ platformUrl: url, secret }, resolve);
  });
}

// ── Helpers de badge ──────────────────────────────────────
function setBadge(el, type, text) {
  el.className = "badge " + type;
  el.textContent = text;
}

function showResult(type, msg) {
  resultDiv.className = type;
  resultDiv.textContent = msg;
  resultDiv.style.display = "block";
}

function hideResult() {
  resultDiv.style.display = "none";
}

// ── Cookies do Byetech CRM ────────────────────────────────
async function getByetechCookies() {
  return new Promise((resolve) => {
    chrome.cookies.getAll({ domain: BYETECH_DOMAIN }, (cookies) => {
      const map = {};
      for (const c of cookies) {
        map[c.name] = c.value;
      }
      resolve(map);
    });
  });
}

function isLoggedIn(cookies) {
  // Laravel Sanctum: laravel_session é o cookie de sessão principal
  const sessionKeys = ["laravel_session", "byetech_session", "XSRF-TOKEN"];
  return sessionKeys.some((k) => cookies[k]);
}

// ── Status da plataforma ───────────────────────────────────
async function checkPlatformStatus(baseUrl) {
  try {
    const resp = await fetch(`${baseUrl}/api/byetech/session-status`, {
      method: "GET",
      signal: AbortSignal.timeout(8000),
    });
    if (!resp.ok) return { ok: false };
    return await resp.json();
  } catch {
    return { ok: false, motivo: "inaccessivel" };
  }
}

// ── Inicialização ──────────────────────────────────────────
async function init() {
  const cfg = await loadConfig();
  urlInput.value    = cfg.platformUrl;
  secretInput.value = cfg.secret;

  // Verifica cookies do CRM
  const cookies = await getByetechCookies();
  const loggedIn = isLoggedIn(cookies);

  if (loggedIn) {
    setBadge(crmBadge, "ok", "Logado");
  } else {
    setBadge(crmBadge, "error", "Sem sessão");
  }

  // Verifica sessão na plataforma
  const status = await checkPlatformStatus(cfg.platformUrl);
  if (status.ok) {
    setBadge(platformBadge, "ok", "Sessão válida");
  } else if (status.motivo === "expirada") {
    setBadge(platformBadge, "warn", "Expirada");
  } else if (status.motivo === "sem_sessao") {
    setBadge(platformBadge, "error", "Sem sessão");
  } else {
    setBadge(platformBadge, "warn", "Inacessível");
  }

  // Habilita botão apenas se o CRM está logado
  syncBtn.disabled = !loggedIn;
  if (!loggedIn) {
    showResult("error", "Faça login no Byetech CRM primeiro e reabra esta extensão.");
  }
}

// ── Sincronizar ────────────────────────────────────────────
syncBtn.addEventListener("click", async () => {
  hideResult();
  syncBtn.disabled = true;
  syncBtn.classList.add("loading");

  const cfg = await loadConfig();

  try {
    const cookies = await getByetechCookies();

    if (!isLoggedIn(cookies)) {
      showResult("error", "Sem sessão no Byetech CRM. Faça login e tente novamente.");
      return;
    }

    const resp = await fetch(`${cfg.platformUrl}/api/byetech/push-session`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cookies, secret: cfg.secret }),
      signal: AbortSignal.timeout(15000),
    });

    const data = await resp.json().catch(() => ({}));

    if (resp.ok && data.ok) {
      showResult("ok", `✓ ${data.message || "Sessão sincronizada com sucesso!"}`);
      setBadge(platformBadge, "ok", "Sessão válida");
    } else if (resp.status === 401) {
      showResult("error", "Secret inválido. Verifique as configurações abaixo.");
    } else {
      showResult("error", data.detail || data.message || `Erro ${resp.status} — tente novamente.`);
    }
  } catch (err) {
    if (err.name === "TimeoutError") {
      showResult("error", "Tempo esgotado. Verifique se a plataforma está online.");
    } else {
      showResult("error", `Falha na conexão: ${err.message}`);
    }
  } finally {
    syncBtn.classList.remove("loading");
    syncBtn.disabled = false;
  }
});

// ── Salvar configurações ───────────────────────────────────
saveBtn.addEventListener("click", async () => {
  const url    = urlInput.value.trim().replace(/\/$/, "") || DEFAULT_URL;
  const secret = secretInput.value.trim() || DEFAULT_SECRET;
  await saveConfig(url, secret);
  savedMsg.style.display = "block";
  setTimeout(() => { savedMsg.style.display = "none"; }, 2000);
  // Re-checar status com a nova URL
  const status = await checkPlatformStatus(url);
  if (status.ok) {
    setBadge(platformBadge, "ok", "Sessão válida");
  } else {
    setBadge(platformBadge, status.motivo === "expirada" ? "warn" : "error",
      status.motivo === "expirada" ? "Expirada" : "Sem sessão");
  }
});

// ── Start ──────────────────────────────────────────────────
init();
