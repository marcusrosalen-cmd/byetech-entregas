// ── State ────────────────────────────────────────────────
let allContracts   = [];
let filteredContracts = [];
let syncPolling    = null;
let activeStage    = '';
let _totpTimer     = null;
let _currentSystem = 'Byetech CRM';
let _2faShown      = false;
let todayDeliveries = 0;

// Links das plataformas externas
const PLATFORM_URLS = {
  gwm:      'https://portaldealer.lmmobilidade.com.br/orders',
  byetech:  'https://crm.byetech.pro',
  metabase: 'https://analytics.byetech.pro',
  localiza: 'https://gestao.localiza.com',
  movida:   'https://www.movida.com.br',
  unidas:   'https://unidas.com.br',
};

// Mapeamento status portal → stage
const STAGE_MAP = {
  'aguardando faturamento': 'faturamento',
  'aguardando transporte':  'transporte',
  'veículo disponível':     'disponivel',
  'veiculo disponivel':     'disponivel',
  'definitivo entregue':    'entregue',
  'definitivo_entregue':    'entregue',
  'pedido concluído':       'entregue',
  'pedido concluido':       'entregue',
  'veículo entregue':       'entregue',
  'veiculo entregue':       'entregue',
};

// ── Init ─────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  loadContracts();
  setInterval(loadContracts, 5 * 60 * 1000);  // auto-refresh 5min (inclui check pendentes)
  updateDateTime();
  setInterval(updateDateTime, 60 * 1000);
});

function updateDateTime() {
  const el = document.getElementById('last-sync');
  if (el && !el.dataset.synced) {
    const now = new Date();
    el.textContent = now.toLocaleDateString('pt-BR', {
      weekday: 'short', day: '2-digit', month: '2-digit'
    });
  }
}

// ── API helpers ──────────────────────────────────────────
async function api(path, opts = {}) {
  const res = await fetch('/api' + path, {
    headers: { 'Content-Type': 'application/json', ...opts.headers },
    ...opts,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || 'Erro na requisição');
  }
  return res.json();
}

// ── Load contracts ────────────────────────────────────────
async function loadContracts() {
  try {
    const [data] = await Promise.all([
      api('/contratos'),
      checkByetechPending(),   // atualiza _byetechSessionOk em paralelo
    ]);
    allContracts = data.contratos || [];
    updateStats(data.stats || {});
    updatePipelineCounts();
    buildReminders();
    applyFilters();
    updatePlatformStatus(data);
    if (data.ultima_sync) {
      const el = document.getElementById('last-sync');
      el.textContent = 'Sync: ' + formatDateTime(data.ultima_sync);
      el.dataset.synced = '1';
    }
  } catch (e) {
    showToast('Erro ao carregar contratos: ' + e.message, 'error');
  }
}

// ── Stats ─────────────────────────────────────────────────
function updateStats(stats) {
  setStatNum('stat-total',       stats.total       ?? allContracts.length);
  setStatNum('stat-atrasados',   stats.atrasados   ?? 0);
  setStatNum('stat-criticos',    stats.criticos    ?? 0);
  setStatNum('stat-hoje',        todayDeliveries);
  setStatNum('stat-disponiveis', countByStage('disponivel'));
  setStatNum('stat-unidas',      countByFonte('UNIDAS'));
}

function setStatNum(id, val) {
  const el = document.getElementById(id);
  if (el) el.querySelector('.stat-num').textContent = val;
}

function countByStage(stage) {
  return allContracts.filter(c => getStage(c) === stage).length;
}

function countByFonte(fonte) {
  return allContracts.filter(c => c.fonte === fonte).length;
}

// ── Pipeline counts ───────────────────────────────────────
function updatePipelineCounts() {
  const counts = { all: allContracts.length, faturamento: 0, transporte: 0, disponivel: 0, entregue: 0 };
  allContracts.forEach(c => {
    const s = getStage(c);
    if (counts[s] !== undefined) counts[s]++;
  });
  setCount('pill-count-all',          counts.all);
  setCount('pill-count-faturamento',  counts.faturamento);
  setCount('pill-count-transporte',   counts.transporte);
  setCount('pill-count-disponivel',   counts.disponivel);
  setCount('pill-count-entregue',     counts.entregue);
}

function setCount(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

// ── Stage helper ──────────────────────────────────────────
function getStage(c) {
  if (c.data_entrega_definitiva) return 'entregue';
  const s = (c.status_atual || '').toLowerCase();
  return STAGE_MAP[s] || 'outro';
}

// ── Reminders ─────────────────────────────────────────────
function buildReminders() {
  const banner = document.getElementById('reminder-banner');
  const reminders = [];

  const hoje = new Date(); hoje.setHours(0,0,0,0);
  const amanha = new Date(hoje); amanha.setDate(amanha.getDate()+1);
  const semana  = new Date(hoje); semana.setDate(semana.getDate()+7);

  const atrasados = allContracts.filter(c => c.atrasado && !c.data_entrega_definitiva);
  const criticos  = allContracts.filter(c => !c.atrasado && c.dias_para_entrega != null
                                          && c.dias_para_entrega <= 5
                                          && !c.data_entrega_definitiva);
  const disponiveis = allContracts.filter(c => getStage(c) === 'disponivel');
  const semPlaca   = allContracts.filter(c => !c.placa && !c.data_entrega_definitiva
                                           && getStage(c) !== 'faturamento');

  if (atrasados.length > 0) {
    reminders.push({
      type: 'danger',
      icon: '⛔',
      text: `<strong>${atrasados.length} contrato${atrasados.length>1?'s':''} atrasado${atrasados.length>1?'s':''}!</strong> Ação urgente necessária.`,
      action: `onclick="filterByUrgencia('atrasado')"`,
      actionLabel: 'Ver atrasados',
    });
  }

  if (criticos.length > 0) {
    reminders.push({
      type: 'warning',
      icon: '🔴',
      text: `<strong>${criticos.length} contrato${criticos.length>1?'s':''}</strong> vencem em até 5 dias.`,
      action: `onclick="filterByUrgencia('critico')"`,
      actionLabel: 'Ver críticos',
    });
  }

  if (disponiveis.length > 0) {
    reminders.push({
      type: 'info',
      icon: '✅',
      text: `<strong>${disponiveis.length} veículo${disponiveis.length>1?'s':''} disponível${disponiveis.length>1?'is':''}.</strong> Aguardando retirada pelo cliente.`,
      action: `onclick="filterByStage('disponivel')"`,
      actionLabel: 'Ver disponíveis',
    });
  }

  if (semPlaca.length > 0) {
    reminders.push({
      type: 'warning',
      icon: '🔢',
      text: `<strong>${semPlaca.length} contrato${semPlaca.length>1?'s':''} sem placa</strong> cadastrada no sistema.`,
      action: '',
      actionLabel: '',
    });
  }

  banner.innerHTML = reminders.map(r => `
    <div class="reminder-card ${r.type}">
      <span class="reminder-icon">${r.icon}</span>
      <span class="reminder-text">${r.text}</span>
      ${r.actionLabel ? `
      <div class="reminder-actions">
        <button class="btn btn-sm btn-outline" ${r.action}>${r.actionLabel}</button>
      </div>` : ''}
    </div>
  `).join('');
}

// ── Byetech session + pending check ──────────────────────
let _byetechPending = 0;
let _byetechSessionOk = true;

async function checkByetechPending() {
  try {
    const s = await api('/sync/status');
    _byetechPending   = s.byetech_pending || 0;
    // Verifica sessão Byetech
    const sessaoOk = await fetch('/api/byetech/sessao-ok').then(r => r.json()).catch(() => ({ok: true}));
    _byetechSessionOk = sessaoOk.ok !== false;

    // Botão renovar: aparece só quando sessão expirada
    const btnRenovar = document.getElementById('btn-renovar');
    if (btnRenovar) btnRenovar.style.display = _byetechSessionOk ? 'none' : '';

    const el = document.getElementById('byetech-pending-banner');
    if (!el) return;

    if (_byetechPending > 0 || !_byetechSessionOk) {
      el.style.display = 'flex';
      const sessaoMsg = !_byetechSessionOk
        ? '🔑 <strong>Sessão Byetech expirada.</strong>'
        : '';
      const pendMsg = _byetechPending > 0
        ? `⚠️ <strong>${_byetechPending} entrega(s) pendente(s)</strong> aguardando sincronização.`
        : '';
      el.innerHTML = `
        <span style="font-size:.88rem">${[sessaoMsg, pendMsg].filter(Boolean).join(' ')}</span>
        <button class="btn btn-sm btn-outline" onclick="triggerRenovarSessao()" style="margin-left:auto;white-space:nowrap">
          🔑 Renovar sessão
        </button>`;
    } else {
      el.style.display = 'none';
    }
  } catch (_) {}
}

async function triggerRenovarSessao() {
  const btn = document.getElementById('btn-renovar');
  if (btn) { btn.disabled = true; btn.textContent = 'Aguardando 2FA...'; }
  try {
    const res = await api('/sync/renovar-sessao', { method: 'POST' });
    if (!res.ok) { showToast(res.message, 'info'); return; }
    showToast('🔑 Informe o código 2FA no popup.', 'info');
    _2faShown = false;
    pollSyncStatus();
  } catch (e) {
    showToast('Erro: ' + e.message, 'error');
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '🔑 Renovar sessão'; }
  }
}

// ── Platform status chips ─────────────────────────────────
function updatePlatformStatus(data) {
  const chips = {
    'chip-gwm':      { ok: true,  label: '🚗 GWM' },
    'chip-byetech':  { ok: _byetechSessionOk, label: _byetechSessionOk ? '🏢 Byetech' : '🏢 Byetech ⚠' },
    'chip-metabase': { ok: !!data.ultima_sync, label: '📊 Metabase' },
    'chip-movida':   { ok: countByFonte('MOVIDA') > 0, label: '🔄 Movida' },
  };
  Object.entries(chips).forEach(([id, info]) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = info.label;
    el.style.borderColor = info.ok ? 'var(--primary)' : 'var(--warning)';
    el.style.color = info.ok ? 'var(--primary)' : 'var(--warning)';
    el.title = info.ok ? '' : 'Sessão expirada — clique em Renovar sessão';
  });
}

// ── Render table ──────────────────────────────────────────
function renderTable(contracts) {
  const tbody = document.getElementById('table-body');
  document.getElementById('filter-count').textContent = `${contracts.length} contrato${contracts.length !== 1 ? 's' : ''}`;

  if (!contracts.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="empty">Nenhum contrato encontrado.</td></tr>';
    return;
  }

  tbody.innerHTML = contracts.map(c => {
    const urgencia = getUrgencia(c.dias_para_entrega, c.atrasado);
    const stage    = getStage(c);
    const entregue = !!c.data_entrega_definitiva;

    const stageHtml = buildStageBadge(c.status_atual, stage, entregue);
    const byetechHtml = buildByetechCell(c, entregue);
    const diasHtml  = buildDiasCell(c, urgencia, entregue);
    const acoesHtml = buildAcoesCell(c, entregue);

    return `
    <tr class="${urgencia.rowClass}" data-id="${c.id}" ondblclick="showDetail('${c.id}')">
      <td>
        <span class="badge badge-${(c.fonte||'').toLowerCase()}">${c.fonte || '–'}</span>
      </td>
      <td>
        <div style="font-weight:600;color:var(--cream)">${esc(c.cliente_nome || '–')}</div>
        <div style="font-size:.72rem;color:var(--muted)">${formatDoc(c.cliente_cpf_cnpj)}</div>
        ${c.data_venda ? `<div style="font-size:.68rem;color:var(--cream-muted)">venda: ${formatDate(c.data_venda)}</div>` : ''}
      </td>
      <td>
        <div style="font-size:.83rem">${esc(c.veiculo || '–')}</div>
        ${c.placa
          ? `<span class="placa-tag">${esc(c.placa)}</span>`
          : `<span style="font-size:.7rem;color:var(--danger)">sem placa</span>`}
      </td>
      <td>${stageHtml}</td>
      <td>${byetechHtml}</td>
      <td style="white-space:nowrap;font-size:.82rem;color:var(--muted)">
        ${formatDate(c.data_prevista_entrega)}
      </td>
      <td>${diasHtml}</td>
      <td>${acoesHtml}</td>
    </tr>`;
  }).join('');
}

function buildStageBadge(status, stage, entregue) {
  if (entregue) {
    return `<span class="stage-badge stage-entregue">📦 Definitivo Entregue</span>`;
  }
  const stageClass = {
    faturamento: 'stage-faturamento',
    transporte:  'stage-transporte',
    disponivel:  'stage-disponivel',
  }[stage] || 'stage-outro';
  const stageIcon = {
    faturamento: '💰',
    transporte:  '🚛',
    disponivel:  '✅',
  }[stage] || '📋';
  return `<span class="stage-badge ${stageClass}">${stageIcon} ${esc(status || 'Não informado')}</span>`;
}

function buildByetechCell(c, entregue) {
  if (entregue) {
    return `<span class="bt-synced">✅ Sincronizado</span>`;
  }
  // Mostra se tem id externo (byetech_contrato_id)
  if (c.byetech_contrato_id) {
    return `<span class="bt-synced">✓ Vinculado</span>`;
  }
  return `<span class="bt-missing">— Pendente</span>`;
}

function buildDiasCell(c, urgencia, entregue) {
  if (entregue) {
    return `<span class="dias-num entregue">✓ ${formatDate(c.data_entrega_definitiva)}</span>`;
  }
  if (c.dias_para_entrega == null) return '<span class="dias-num" style="color:var(--muted)">–</span>';
  const abs = Math.abs(c.dias_para_entrega);
  const label = c.atrasado ? `⚠ ${abs}d atrasado` : `${c.dias_para_entrega}d`;
  return `<span class="dias-num ${urgencia.diasClass}">${label}</span>`;
}

function buildAcoesCell(c, entregue) {
  const btns = [];

  btns.push(`<button class="btn btn-sm btn-outline" onclick="showDetail('${c.id}')">🔍</button>`);

  if (!entregue) {
    btns.push(`<button class="btn btn-sm btn-success" onclick="markDelivered('${c.id}')">✓ Entregar</button>`);
  }

  if (c.fonte === 'UNIDAS') {
    btns.push(`<button class="btn btn-sm btn-outline" onclick="sendUnidasEmail('${c.id}')">📧</button>`);
  }

  // Link rápido Byetech
  btns.push(`<a class="platform-link" href="${PLATFORM_URLS.byetech}" target="_blank" title="Abrir Byetech CRM">🏢</a>`);

  if (c.fonte === 'GWM') {
    btns.push(`<a class="platform-link" href="${PLATFORM_URLS.gwm}" target="_blank" title="Abrir Portal GWM">🚗</a>`);
  }

  return `<div style="display:flex;gap:.3rem;flex-wrap:wrap;align-items:center">${btns.join('')}</div>`;
}

// ── Urgência helpers ──────────────────────────────────────
function getUrgencia(dias, atrasado) {
  if (atrasado || dias < 0)
    return { rowClass: 'atrasado', badgeClass: 'atrasado', diasClass: 'atrasado' };
  if (dias <= 5)
    return { rowClass: 'critico',  badgeClass: 'critico',  diasClass: 'critico' };
  if (dias <= 20)
    return { rowClass: '',         badgeClass: 'alerta',   diasClass: 'alerta' };
  return   { rowClass: '',         badgeClass: 'ok',       diasClass: 'ok' };
}

// ── Filters ───────────────────────────────────────────────
function applyFilters() {
  const search   = (document.getElementById('filter-search').value || '').toLowerCase();
  const fonte    = document.getElementById('filter-fonte').value;
  const urgencia = document.getElementById('filter-urgencia').value;
  const sort     = document.getElementById('filter-sort').value;

  let filtered = allContracts.filter(c => {
    // Stage pill filter
    if (activeStage && getStage(c) !== activeStage) return false;

    // Text search
    if (search && ![
      c.cliente_nome, c.cliente_cpf_cnpj, c.placa, c.veiculo, c.status_atual, c.id_externo,
    ].some(f => (f || '').toLowerCase().includes(search))) return false;

    // Source filter
    if (fonte && c.fonte !== fonte) return false;

    // Urgency filter
    if (urgencia) {
      const urg = getUrgencia(c.dias_para_entrega, c.atrasado);
      if (urgencia === 'atrasado' && !c.atrasado) return false;
      if (urgencia === 'critico'  && (c.atrasado || c.dias_para_entrega > 5)) return false;
      if (urgencia === 'alerta'   && (c.atrasado || c.dias_para_entrega <= 5 || c.dias_para_entrega > 20)) return false;
      if (urgencia === 'ok'       && (c.atrasado || c.dias_para_entrega <= 20)) return false;
    }

    return true;
  });

  // Sort
  filtered = sortContracts(filtered, sort);
  filteredContracts = filtered;
  renderTable(filtered);
}

function sortContracts(list, by) {
  return [...list].sort((a, b) => {
    if (by === 'nome')  return (a.cliente_nome||'').localeCompare(b.cliente_nome||'');
    if (by === 'data' || by === 'prazo') {
      return (a.dias_para_entrega??9999) - (b.dias_para_entrega??9999);
    }
    if (by === 'fonte') return (a.fonte||'').localeCompare(b.fonte||'');
    if (by === 'atraso') {
      // Atrasados primeiro (maior atraso no topo), depois por dias restantes crescente
      const da = a.atrasado ? (a.dias_para_entrega??0) : 9999;
      const db_ = b.atrasado ? (b.dias_para_entrega??0) : 9999;
      return da - db_;
    }
    if (by === 'data_venda') {
      const da = a.data_venda ? new Date(a.data_venda).getTime() : 0;
      const db_ = b.data_venda ? new Date(b.data_venda).getTime() : 0;
      return da - db_;
    }
    // urgencia (default): atrasados > criticos > alerta > ok > entregues
    const urgOrder = c => {
      if (c.data_entrega_definitiva) return 10;
      if (c.atrasado) return 0;
      if (c.dias_para_entrega <= 5)  return 1;
      if (c.dias_para_entrega <= 20) return 2;
      return 3;
    };
    return urgOrder(a) - urgOrder(b);
  });
}

function filterByStage(stage) {
  activeStage = stage;
  // Update pill active state
  document.querySelectorAll('.stage-pill').forEach(p => {
    p.classList.toggle('active', p.dataset.stage === stage);
  });
  applyFilters();
}

function filterByUrgencia(urg) {
  document.getElementById('filter-urgencia').value = urg;
  applyFilters();
}

function filterByFonte(fonte) {
  document.getElementById('filter-fonte').value = fonte;
  applyFilters();
}

function clearAllFilters() {
  document.getElementById('filter-search').value  = '';
  document.getElementById('filter-fonte').value   = '';
  document.getElementById('filter-urgencia').value = '';
  activeStage = '';
  document.querySelectorAll('.stage-pill').forEach(p => {
    p.classList.toggle('active', p.dataset.stage === '');
  });
  applyFilters();
}

// ── Detail modal ──────────────────────────────────────────
async function showDetail(id) {
  try {
    const data = await api(`/contratos/${id}`);
    const c = data.contrato;
    const hist = data.historico || [];
    const stage = getStage(c);

    // Etapas visuais de progresso
    const stagesFlow = [
      { key: 'faturamento', label: 'Ag. Faturamento',     icon: '💰' },
      { key: 'transporte',  label: 'Ag. Transporte',      icon: '🚛' },
      { key: 'disponivel',  label: 'Veículo Disponível',  icon: '✅' },
      { key: 'entregue',    label: 'Definitivo Entregue', icon: '📦' },
    ];
    const stageOrder = ['faturamento','transporte','disponivel','entregue'];
    const currentIdx = stageOrder.indexOf(stage);

    const etapasHtml = `
      <div style="display:flex;gap:.5rem;flex-wrap:wrap;margin-bottom:.5rem">
        ${stagesFlow.map((s, i) => {
          const done    = i < currentIdx;
          const current = i === currentIdx;
          return `
            <div style="flex:1;min-width:90px;text-align:center;padding:.5rem .3rem;
                        border-radius:6px;border:1px solid ${current ? 'var(--primary)' : done ? 'var(--success)' : 'var(--border)'};
                        background:${current ? 'var(--primary-dim)' : done ? 'rgba(0,229,160,.08)' : 'var(--surface2)'}">
              <div style="font-size:1.1rem">${s.icon}</div>
              <div style="font-size:.67rem;font-weight:600;color:${current ? 'var(--primary)' : done ? 'var(--success)' : 'var(--muted)'}">
                ${s.label}
              </div>
              ${current ? '<div style="font-size:.62rem;color:var(--primary)">◉ atual</div>' : ''}
              ${done    ? '<div style="font-size:.62rem;color:var(--success)">✓ ok</div>' : ''}
            </div>`;
        }).join('')}
      </div>`;

    // Histórico
    const histHtml = hist.length
      ? `<ul class="etapas-list">${hist.map(h => `
          <li class="etapa-item">
            <div class="etapa-dot done"></div>
            <div class="etapa-info">
              <div class="etapa-nome">${esc(h.status_anterior||'–')} → ${esc(h.status_novo||'–')}</div>
              <div class="etapa-data">${formatDateTime(h.registrado_em)}</div>
            </div>
          </li>`).join('')}</ul>`
      : '<p style="color:var(--muted);font-size:.83rem">Sem histórico registrado.</p>';

    // Plataformas vinculadas
    const platformLinks = buildDetailPlatforms(c);

    document.getElementById('modal-title').textContent = c.cliente_nome || 'Contrato';
    document.getElementById('modal-content').innerHTML = `
      <!-- Dados principais -->
      <div class="detail-section">
        <div class="detail-section-title">Dados do Contrato</div>
        <div class="detail-grid">
          <div class="detail-item"><label>Locadora</label><span>${c.fonte || '–'}</span></div>
          <div class="detail-item"><label>ID externo</label><span>${c.id_externo || '–'}</span></div>
          <div class="detail-item"><label>ID na locadora (Byetech)</label>
            <span style="color:${c.byetech_pedido_id ? 'var(--primary)' : 'var(--muted)'}">
              ${c.byetech_pedido_id || '–'}
            </span>
          </div>
          <div class="detail-item"><label>CPF/CNPJ</label><span>${formatDoc(c.cliente_cpf_cnpj)}</span></div>
          <div class="detail-item"><label>Email</label><span>${c.cliente_email || '–'}</span></div>
          <div class="detail-item"><label>Veículo</label><span>${c.veiculo || '–'}</span></div>
          <div class="detail-item"><label>Placa</label>
            <span style="color:${c.placa ? 'var(--cream)' : 'var(--danger)'}">
              ${c.placa || 'Não informada'}
            </span>
          </div>
          <div class="detail-item"><label>Data da venda</label>
            <span style="color:${c.data_venda ? 'var(--text)' : 'var(--muted)'}">
              ${c.data_venda ? formatDate(c.data_venda) : '–'}
            </span>
          </div>
          <div class="detail-item" style="grid-column:1/-1">
            <label>📅 Data prevista de entrega</label>
            <span style="font-size:1.05rem;font-weight:700;color:${
              !c.data_prevista_entrega ? 'var(--muted)' :
              c.atrasado ? 'var(--danger)' :
              c.dias_para_entrega <= 5 ? 'var(--warning)' : 'var(--primary)'
            }">
              ${c.data_prevista_entrega ? formatDate(c.data_prevista_entrega) : '–'}
              ${c.dias_para_entrega != null && !c.data_entrega_definitiva
                ? ` <small style="font-size:.8rem;font-weight:400">(${c.atrasado ? '⚠ ' + Math.abs(c.dias_para_entrega) + 'd atrasado' : c.dias_para_entrega + 'd restantes'})</small>`
                : ''}
            </span>
          </div>
          <div class="detail-item"><label>Entrega definitiva</label>
            <span style="color:${c.data_entrega_definitiva ? 'var(--success)' : 'var(--muted)'}">
              ${c.data_entrega_definitiva ? formatDate(c.data_entrega_definitiva) : '–'}
            </span>
          </div>
        </div>
      </div>

      <!-- Progresso de etapas -->
      <div class="detail-section">
        <div class="detail-section-title">Progresso de Entrega</div>
        ${etapasHtml}
        <div style="margin-top:.5rem;font-size:.82rem;color:var(--muted)">
          Status portal: <strong style="color:var(--text)">${c.status_atual || '–'}</strong>
        </div>
      </div>

      <!-- Plataformas -->
      <div class="detail-section">
        <div class="detail-section-title">Plataformas & Links</div>
        ${platformLinks}
      </div>

      <!-- Ações rápidas -->
      <div class="detail-section">
        <div class="detail-section-title">Ações</div>
        <div style="display:flex;gap:.5rem;flex-wrap:wrap">
          ${!c.data_entrega_definitiva
            ? `<button class="btn btn-success" onclick="markDelivered('${c.id}');closeModal('modal-detail')">
                📦 Marcar como Entregue
               </button>`
            : `<span style="font-size:.85rem;color:var(--success)">✓ Entregue em ${formatDate(c.data_entrega_definitiva)}</span>`}
          ${c.fonte === 'UNIDAS'
            ? `<button class="btn btn-outline" onclick="sendUnidasEmail('${c.id}')">📧 Enviar Email Unidas</button>`
            : ''}
        </div>
      </div>

      <!-- Histórico -->
      <div class="detail-section">
        <div class="detail-section-title">Histórico de Mudanças</div>
        ${histHtml}
      </div>
    `;
    openModal('modal-detail');
  } catch (e) {
    showToast('Erro ao carregar detalhes: ' + e.message, 'error');
  }
}

function buildDetailPlatforms(c) {
  const items = [
    { name: 'Byetech CRM', icon: '🏢', url: PLATFORM_URLS.byetech, active: !!c.byetech_contrato_id, sub: c.byetech_contrato_id || 'Não vinculado' },
  ];

  if (c.fonte === 'GWM') {
    items.push({ name: 'Portal GWM', icon: '🚗', url: PLATFORM_URLS.gwm, active: true, sub: c.id_externo || '' });
  }
  if (c.fonte === 'LOCALIZA') {
    items.push({ name: 'Localiza', icon: '🟢', url: PLATFORM_URLS.localiza, active: true, sub: '' });
  }
  if (c.fonte === 'MOVIDA') {
    items.push({ name: 'Movida', icon: '🔄', url: PLATFORM_URLS.movida, active: true, sub: '' });
  }
  items.push({ name: 'Metabase', icon: '📊', url: PLATFORM_URLS.metabase, active: true, sub: 'Analytics' });

  return `<div class="platforms-panel">
    ${items.map(p => `
      <a class="platform-card ${p.active ? 'active' : ''}" href="${p.url}" target="_blank">
        <span class="platform-card-icon">${p.icon}</span>
        <span class="platform-card-name">${p.name}</span>
        ${p.sub ? `<span class="platform-card-sub">${esc(p.sub)}</span>` : ''}
      </a>`).join('')}
  </div>`;
}

// ── Mark delivered ────────────────────────────────────────
function markDelivered(id) {
  const today = new Date().toISOString().split('T')[0];
  _pendingDeliveryId = id;

  // Preenche a data de hoje no input do modal
  const input = document.getElementById('entregar-data-input');
  if (input) input.value = today;

  // Abre o modal de confirmação de entrega
  openModal('modal-entregar');
}

let _pendingDeliveryId = null;

async function confirmDelivery() {
  const id    = _pendingDeliveryId;
  const input = document.getElementById('entregar-data-input');
  const date  = input ? input.value.trim() : '';

  if (!id || !date) {
    showToast('Selecione a data de entrega.', 'error');
    return;
  }

  const btn = document.getElementById('btn-confirmar-entrega');
  if (btn) { btn.disabled = true; btn.textContent = 'Registrando...'; }

  try {
    const res = await api(`/contratos/${id}/entregar`, {
      method: 'POST',
      body: JSON.stringify({ data_entrega: date }),
    });
    closeModal('modal-entregar');
    closeModal('modal-detail');
    const bs = res.byetech_status;
    if (bs === 'ok') {
      showToast('✅ Entrega registrada e Byetech atualizado!', 'success');
    } else if (bs === 'sessao_expirada') {
      showToast('✅ Entrega registrada. ⚠️ Byetech pendente — sessão expirada. Clique em 🔑 Renovar sessão.', 'warning');
    } else if (bs === 'timeout') {
      showToast('✅ Entrega registrada. ⏳ Byetech demorou — atualização enfileirada para retry.', 'warning');
    } else if (bs === 'erro') {
      showToast(`❌ Entrega registrada, mas ERRO no Byetech: ${res.byetech_msg || 'verifique manualmente.'}`, 'error');
    } else if (bs === 'sem_cpf') {
      showToast('✅ Entrega registrada. ⚠️ Sem CPF — não foi possível atualizar o Byetech.', 'warning');
    } else {
      showToast('✅ Entrega registrada.', 'success');
    }
    todayDeliveries++;
    _pendingDeliveryId = null;
    loadContracts();
  } catch (e) {
    showToast('Erro ao registrar entrega: ' + e.message, 'error');
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Confirmar Entrega'; }
  }
}

// ── Unidas email ──────────────────────────────────────────
async function sendUnidasEmail(id) {
  if (!confirm('Enviar e-mail de confirmação de entrega para este cliente?')) return;
  try {
    await api(`/contratos/${id}/email-unidas`, { method: 'POST' });
    showToast('📧 E-mail enviado!', 'success');
  } catch (e) {
    showToast('Erro ao enviar e-mail: ' + e.message, 'error');
  }
}

// ── Sync ──────────────────────────────────────────────────
async function triggerSync() {
  const btn = document.getElementById('btn-sync');
  btn.innerHTML = '<span class="syncing">⟳</span> Sincronizando...';
  btn.disabled = true;

  try {
    const res = await api('/sync', { method: 'POST' });
    if (!res.ok) {
      showToast(res.message || 'Sync já em andamento.', 'info');
      resetSyncBtn();
      return;
    }
    showToast('Sincronização iniciada — aguarde o popup de 2FA...', 'info');
    _2faShown = false;
    pollSyncStatus();
  } catch (e) {
    showToast('Erro ao iniciar sync: ' + e.message, 'error');
    resetSyncBtn();
  }
}

function show2FAModal(systemName) {
  document.getElementById('twofa-system-name').textContent = systemName;
  document.getElementById('twofa-input').value = '';
  openModal('modal-2fa');
  document.getElementById('twofa-input').focus();
  startTotpTimer();
}

function startTotpTimer() {
  if (_totpTimer) clearInterval(_totpTimer);
  const timerEl = document.getElementById('twofa-timer');
  const update = () => {
    const secs = 30 - (Math.floor(Date.now() / 1000) % 30);
    timerEl.textContent = `Código expira em ${secs}s`;
    timerEl.style.color = secs <= 8 ? 'var(--warning)' : 'var(--muted)';
  };
  update();
  _totpTimer = setInterval(update, 1000);
}

function cancel2FA() {
  if (_totpTimer) { clearInterval(_totpTimer); _totpTimer = null; }
  closeModal('modal-2fa');
}

async function submit2FA() {
  const code = document.getElementById('twofa-input').value.trim();
  if (code.length < 6) { showToast('Digite os 6 dígitos completos.', 'error'); return; }
  if (_totpTimer) { clearInterval(_totpTimer); _totpTimer = null; }
  try {
    await api('/sync/2fa', {
      method: 'POST',
      body: JSON.stringify({ code, system: _currentSystem }),
    });
    closeModal('modal-2fa');
    _2faShown = false;
    showToast('Código enviado. Continuando...', 'info');
    if (!syncPolling) pollSyncStatus();
  } catch (e) {
    showToast('Erro: ' + e.message, 'error');
  }
}

function pollSyncStatus() {
  if (syncPolling) clearInterval(syncPolling);
  syncPolling = setInterval(async () => {
    try {
      const status = await api('/sync/status');
      if (status.status === 'needs_2fa') {
        if (!_2faShown) {
          _2faShown = true;
          _currentSystem = status.system || 'Byetech CRM';
          show2FAModal(_currentSystem);
        }
      } else if (status.status === 'running') {
        if (_2faShown) _2faShown = false;
      } else if (status.status === 'done') {
        clearInterval(syncPolling); syncPolling = null; _2faShown = false;
        resetSyncBtn();
        const entregasHoje = status.entregas_hoje || [];
        let msg = `Sync concluída! ${status.atualizados || 0} atualizados.`;
        if (entregasHoje.length > 0) {
          msg += ` ${entregasHoje.length} entrega(s) hoje.`;
          todayDeliveries += entregasHoje.length;
          showEntregasHoje(entregasHoje);
        }
        if (status.byetech_pending > 0) {
          msg += ` ⚠ ${status.byetech_pending} entrega(s) Byetech pendente(s) — processando...`;
        }
        showToast(msg, 'success');
        loadContracts();
      } else if (status.status === 'error') {
        clearInterval(syncPolling); syncPolling = null; _2faShown = false;
        resetSyncBtn();
        showToast('Erro na sincronização: ' + status.message, 'error');
      }
    } catch (_) {}
  }, 2000);
}

function resetSyncBtn() {
  const btn = document.getElementById('btn-sync');
  btn.innerHTML = '<span>⟳</span> Byetech CRM';
  btn.disabled = false;
}

// ── Painel entregas do dia ────────────────────────────────
function showEntregasHoje(entregas) {
  const wrap = document.getElementById('painel-entregas-hoje');
  wrap.style.display = 'block';

  const linhas = entregas.map(e => `
    <tr>
      <td><strong>${esc(e.cliente_nome)}</strong></td>
      <td>${esc(e.veiculo)}${e.placa ? ` <span class="placa-tag">${esc(e.placa)}</span>` : ''}</td>
      <td><span class="badge badge-${(e.fonte||'').toLowerCase()}">${esc(e.fonte)}</span></td>
      <td style="font-size:.78rem;color:var(--muted)">${esc(e.data_entrega || '–')}</td>
    </tr>`).join('');

  wrap.innerHTML = `
    <div class="painel-entregas-inner">
      <div class="painel-entregas-header">
        <span>📦 Entregas registradas hoje — ${entregas.length} veículo(s)</span>
        <button class="painel-close" onclick="document.getElementById('painel-entregas-hoje').style.display='none'">✕</button>
      </div>
      <div class="painel-entregas-body">
        <table class="entregas-table">
          <thead><tr><th>Cliente</th><th>Veículo</th><th>Locadora</th><th>Data</th></tr></thead>
          <tbody>${linhas}</tbody>
        </table>
      </div>
    </div>`;
}

// ── Metabase sync ─────────────────────────────────────────
async function triggerMetabaseSync() {
  const btn = document.getElementById('btn-metabase');
  btn.disabled = true;
  btn.innerHTML = '<span class="syncing">⟳</span> Buscando...';
  try {
    const res = await api('/metabase/sync', { method: 'POST' });
    showToast(`📊 Metabase: ${res.importados} contratos sincronizados.`, 'success');
    loadContracts();
  } catch (e) {
    showToast('Erro Metabase: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<span>⟳</span> Metabase';
  }
}

// ── Relatório completo manual ─────────────────────────────
async function triggerRelatorioCompleto() {
  showToast('📋 Gerando relatório completo...', 'info');
  try {
    await api('/slack/relatorio-completo', { method: 'POST' });
    showToast('✅ Relatório enviado no Slack!', 'success');
  } catch (e) {
    showToast('Erro ao gerar relatório: ' + e.message, 'error');
  }
}

// ── Validação GWM / LM ───────────────────────────────────
let _validarPolling = null;

async function triggerValidarGwmLm() {
  const btn = document.getElementById('btn-validar');
  btn.disabled = true;
  btn.textContent = '⏳ Validando...';

  try {
    await api('/sync/validar-gwm-lm?days=4', { method: 'POST' });
    showToast('🧪 Validação iniciada — consultando portais GWM/LM + Metabase...', 'info');
    // Polling de status
    if (_validarPolling) clearInterval(_validarPolling);
    _validarPolling = setInterval(async () => {
      try {
        const st = await api('/sync/status');
        if (st.status === 'running') {
          btn.textContent = '⏳ ' + (st.message || 'Validando...').slice(0, 40) + '...';
        } else if (st.status === 'done' || st.status === 'error') {
          clearInterval(_validarPolling);
          _validarPolling = null;
          btn.disabled = false;
          btn.textContent = '🧪 Validar GWM/LM';
          if (st.status === 'done') {
            showToast('✅ ' + st.message + ' — relatório enviado no Slack!', 'success');
          } else {
            showToast('❌ Erro: ' + st.message, 'error');
          }
          loadContracts();
        }
      } catch (_) {}
    }, 5000);
  } catch (e) {
    showToast('Erro na validação: ' + e.message, 'error');
    btn.disabled = false;
    btn.textContent = '🧪 Validar GWM/LM';
  }
}

// ── Sign & Drive Sync ─────────────────────────────────────
let _sdPolling = null;

async function triggerSignAndDriveSync() {
  const btn = document.getElementById('btn-sd');
  btn.disabled = true;
  btn.innerHTML = '⏳ Consultando...';

  try {
    await api('/sync/signanddrive', { method: 'POST' });
    showToast('🚗 Sign & Drive: consultando portal...', 'info');
    if (_sdPolling) clearInterval(_sdPolling);
    _sdPolling = setInterval(async () => {
      try {
        const st = await api('/sync/status');
        if (st.status === 'running') {
          btn.innerHTML = '⏳ ' + (st.message || 'Consultando...').slice(0, 38) + '...';
        } else if (st.status === 'done' || st.status === 'error') {
          clearInterval(_sdPolling);
          _sdPolling = null;
          btn.disabled = false;
          btn.innerHTML = '<span>⟳</span> Sign &amp; Drive';
          if (st.status === 'done') {
            showToast('✅ ' + st.message, 'success');
          } else {
            showToast('❌ Erro Sign & Drive: ' + st.message, 'error');
          }
          loadContracts();
        }
      } catch (e) {
        clearInterval(_sdPolling);
        _sdPolling = null;
        btn.disabled = false;
        btn.innerHTML = '<span>⟳</span> Sign &amp; Drive';
      }
    }, 3000);
  } catch (e) {
    btn.disabled = false;
    btn.innerHTML = '<span>⟳</span> Sign &amp; Drive';
    showToast('Erro: ' + e.message, 'error');
  }
}

// ── Health Check ──────────────────────────────────────────
async function triggerHealthCheck() {
  openModal('modal-health');
  const box = document.getElementById('health-content');
  box.innerHTML = '<div style="text-align:center;padding:2rem;color:var(--muted)">🔄 Verificando conexões...</div>';

  try {
    const res = await fetch('/api/health');
    const data = await res.json();
    const conn = data.connections || {};

    const LABELS = {
      database:    '🗄️  Banco de dados (SQLite)',
      byetech_crm: '🏢 Byetech CRM',
      metabase:    '📊 Metabase Analytics',
      portal_gwm:  '🚙 Portal GWM / Sign & Drive',
      portal_lm:   '🚗 Portal LM / AssineCar',
      slack:       '💬 Slack',
      email:       '📧 E-mail SMTP',
    };

    const rows = Object.entries(conn).map(([key, val]) => {
      const icon   = val.ok ? '✅' : '❌';
      const color  = val.ok ? 'var(--success)' : 'var(--danger)';
      const label  = LABELS[key] || key;
      return `
        <div style="display:flex;align-items:flex-start;gap:.75rem;padding:.6rem 0;border-bottom:1px solid var(--border)">
          <span style="font-size:1.1rem;min-width:1.5rem">${icon}</span>
          <div style="flex:1">
            <div style="font-weight:600;color:var(--cream)">${label}</div>
            <div style="font-size:.82rem;color:${color};margin-top:.15rem">${val.detail || ''}</div>
          </div>
        </div>`;
    }).join('');

    const globalIcon  = data.ok ? '✅' : '⚠️';
    const globalColor = data.ok ? 'var(--success)' : 'var(--warning)';
    const globalMsg   = data.ok ? 'Todas as conexões OK' : 'Uma ou mais conexões com problema';

    box.innerHTML = `
      <div style="display:flex;align-items:center;gap:.5rem;margin-bottom:1rem;
                  padding:.65rem 1rem;border-radius:8px;background:${globalColor}22;
                  border:1px solid ${globalColor};color:${globalColor};font-weight:600">
        ${globalIcon} ${globalMsg}
      </div>
      ${rows}
      <div style="font-size:.75rem;color:var(--muted);margin-top:.75rem;text-align:right">
        Verificado em ${new Date().toLocaleTimeString('pt-BR')}
      </div>`;
  } catch (e) {
    box.innerHTML = `<div style="color:var(--danger)">Erro ao verificar: ${e.message}</div>`;
  }
}

// ── Movida import ─────────────────────────────────────────
function openMovidaModal() { openModal('modal-movida'); }

function handleDrop(e) {
  e.preventDefault();
  const file = e.dataTransfer.files[0];
  if (file) {
    document.getElementById('movida-file').files = e.dataTransfer.files;
    previewMovida(file);
  }
  document.getElementById('upload-zone').classList.remove('drag-over');
}

async function previewMovida(file) {
  file = file || document.getElementById('movida-file').files[0];
  if (!file) return;
  document.getElementById('file-name').textContent = file.name + ' · ' + formatBytes(file.size);
  try {
    const fd = new FormData();
    fd.append('file', file);
    const res  = await fetch('/api/movida/preview', { method: 'POST', body: fd });
    const data = await res.json();
    const preview = document.getElementById('movida-preview');
    preview.classList.remove('hidden');
    preview.innerHTML = `
      <strong>${data.total} contratos</strong> encontrados<br>
      Colunas: ${Object.entries(data.mapeamento).filter(([,v]) => v).map(([,v]) => `<em>${v}</em>`).join(', ')}<br>
      ${data.nao_mapeadas.length
        ? `<span style="color:var(--warning)">⚠ Não mapeadas: ${data.nao_mapeadas.join(', ')}</span>`
        : '<span style="color:var(--success)">✓ Todas as colunas mapeadas</span>'}
    `;
  } catch (e) { console.error(e); }
}

async function uploadMovida() {
  const file = document.getElementById('movida-file').files[0];
  if (!file) { showToast('Selecione um arquivo primeiro.', 'error'); return; }
  const fd = new FormData();
  fd.append('file', file);
  try {
    const res  = await fetch('/api/movida/import', { method: 'POST', body: fd });
    const data = await res.json();
    closeModal('modal-movida');
    showToast(`🔄 Movida: ${data.importados} contratos atualizados.`, 'success');
    loadContracts();
  } catch (e) {
    showToast('Erro ao importar: ' + e.message, 'error');
  }
}

// ── Modal helpers ─────────────────────────────────────────
function openModal(id)  { document.getElementById(id).classList.remove('hidden'); }
function closeModal(id) {
  document.getElementById(id).classList.add('hidden');
  if (id === 'modal-2fa' && !syncPolling) resetSyncBtn();
}
document.addEventListener('click', e => {
  if (e.target.classList.contains('modal')) closeModal(e.target.id);
});

// ── Toast ─────────────────────────────────────────────────
let toastTimer = null;
function showToast(msg, type = 'info') {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = `toast ${type}`;
  if (toastTimer) clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.add('hidden'), 5000);
}

// ── Format helpers ────────────────────────────────────────
function formatDate(iso) {
  if (!iso) return '–';
  const d = new Date(iso);
  if (isNaN(d)) return iso;
  return d.toLocaleDateString('pt-BR');
}
function formatDateTime(iso) {
  if (!iso) return '–';
  const d = new Date(iso);
  if (isNaN(d)) return iso;
  return d.toLocaleString('pt-BR');
}
function formatDoc(doc) {
  if (!doc) return '–';
  const d = doc.replace(/\D/g, '');
  if (d.length === 11) return d.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, '$1.$2.$3-$4');
  if (d.length === 14) return d.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, '$1.$2.$3/$4-$5');
  return doc;
}
function formatBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1024*1024) return (b/1024).toFixed(1) + ' KB';
  return (b/1024/1024).toFixed(1) + ' MB';
}
function esc(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
