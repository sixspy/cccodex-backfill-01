const msg = document.getElementById('msg');
const view = document.getElementById('view');
let windows = [];

async function api(path, opts = {}) {
  const r = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...opts });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
  return j;
}
function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
function setMsg(t, err = false) { msg.textContent = t || ''; msg.className = err ? 'err' : 'muted'; }
async function loadWindows() { windows = (await api('/api/windows')).items || []; }

function mutexChip(w) {
  if (w.mutex_no && w.own_mutex) return `<span class="chip chip-warn">拒灌 · ${esc(w.mutex_no)}</span>`;
  if (w.mutex_no) return `<span class="chip chip-warn">他窗持有 ${esc(w.mutex_no)}</span>`;
  return `<span class="chip chip-ok">日增可写</span>`;
}
function alignChip(w) {
  return w.aligned
    ? `<span class="chip chip-ok">门槛对齐</span>`
    : `<span class="chip chip-bad" title="${esc(w.problem)}">门槛失配</span>`;
}

async function renderDaily() {
  view.innerHTML = `<div class="card">
    <h3>日增量灌数</h3>
    <p class="muted">与开灌共用一把排他锁：目标表有互斥号时直接拒灌，落点一行不写。</p>
    <label>目标表<input id="d-table" value="dw_orders_d" /></label>
    <label>业务日期<input id="d-date" value="2026-11-11" /></label>
    <label>行数<input id="d-rows" type="number" value="5" min="1" max="1000" /></label>
    <button class="primary" id="d-go" type="button">写日增量</button>
    <div id="d-out" class="wall" style="margin-top:0.8rem"></div>
  </div>`;
  document.getElementById('d-go').onclick = async () => {
    try {
      const j = await api('/api/daily-rows', {
        method: 'POST',
        body: JSON.stringify({
          table_name: document.getElementById('d-table').value.trim(),
          biz_date: document.getElementById('d-date').value.trim(),
          rows: Number(document.getElementById('d-rows').value),
        }),
      });
      setMsg(`已写入日增量 ${j.result.written} 行 · ${j.result.biz_date}`);
      document.getElementById('d-out').innerHTML =
        `<span class="chip chip-ok">可写 · 已落 ${j.result.written} 行</span>`;
    } catch (e) {
      setMsg(e.message, true);
      document.getElementById('d-out').innerHTML = `<span class="chip chip-warn">拒灌</span>`;
    }
    await loadWindows();
  };
}

async function renderWindows() {
  view.innerHTML = `<div class="wall">${windows.map(w => `
    <article class="card">
      <h3>${esc(w.table_name)}</h3>
      <p class="muted">#${w.id} · ${esc(w.start_date)} → ${esc(w.end_date)}</p>
      <p class="chips-row"><span class="tag">${esc(w.status)}</span>${mutexChip(w)}${alignChip(w)}</p>
      <p class="muted">切片 ${w.slice_count} · 已落库 ${w.loaded_count}</p>
      <div class="row">
        <button class="primary" type="button" data-open="${w.id}"
          ${w.status === '已计划' ? '' : 'disabled'}>点开灌</button>
        <button type="button" data-close="${w.id}"
          ${w.status === '执行中' ? '' : 'disabled'}>收灌</button>
      </div>
    </article>`).join('') || '<p class="muted">暂无窗口</p>'}</div>`;

  document.querySelectorAll('[data-open]').forEach(btn => {
    btn.onclick = async () => {
      btn.disabled = true;
      try {
        const j = await api(`/api/windows/${btn.dataset.open}/open`, { method: 'POST', body: '{}' });
        setMsg(`已开灌 · 互斥号 ${j.mutex_no} · 日增量即刻拒灌`);
      } catch (e) { setMsg(e.message, true); }
      await loadWindows(); renderWindows();
    };
  });
  document.querySelectorAll('[data-close]').forEach(btn => {
    btn.onclick = async () => {
      btn.disabled = true;
      try {
        const j = await api(`/api/windows/${btn.dataset.close}/close`, { method: 'POST', body: '{}' });
        setMsg(`已收灌 · ${j.mutex_no} 释放 · 日增量恢复可写`);
      } catch (e) { setMsg(e.message, true); }
      await loadWindows(); renderWindows();
    };
  });
}

function winOptions(sel) {
  return windows.map(w => `<option value="${w.id}" ${String(w.id) === String(sel) ? 'selected' : ''}>#${w.id} ${esc(w.table_name)} (${esc(w.status)})</option>`).join('');
}

function renderPlan() {
  view.innerHTML = `<div class="card">
    <label>窗口<select id="wid">${winOptions()}</select></label>
    <button class="primary" id="go" type="button">生成切片计划</button>
    <pre id="out" class="muted"></pre>
  </div>`;
  document.getElementById('go').onclick = async () => {
    try {
      const wid = document.getElementById('wid').value;
      const j = await api(`/api/windows/${wid}/plan`, { method: 'POST', body: '{}' });
      document.getElementById('out').textContent = JSON.stringify(j.slices, null, 2);
      setMsg(`已生成 ${j.slices.length} 片`);
      await loadWindows();
    } catch (e) { setMsg(e.message, true); }
  };
}

async function renderReplay() {
  await loadWindows();
  view.innerHTML = `<div class="card">
    <label>窗口<select id="wid">${winOptions()}</select></label>
    <p id="gate" class="chips-row"></p>
    <div id="list" class="wall" style="margin-top:0.8rem"></div>
  </div>`;
  const draw = async () => {
    const id = document.getElementById('wid').value;
    const j = await api(`/api/windows/${id}/slices`);
    const g = j.gate || {};
    document.getElementById('gate').innerHTML =
      `<span class="tag">${esc(j.window.status)}</span>` +
      `<span class="chip ${g.gate_replay === '可写' ? 'chip-ok' : 'chip-bad'}">重放${g.gate_replay === '可写' ? '可写' : '拒灌'}</span>` +
      `<span class="chip ${g.gate_increment === '拒灌' ? 'chip-warn' : 'chip-ok'}">日增量${g.gate_increment}</span>` +
      (g.mutex_no ? `<span class="chip chip-warn">${esc(g.mutex_no)}</span>` : '') +
      (g.aligned ? '' : `<span class="chip chip-bad">门槛失配</span>`);
    const items = j.items || [];
    document.getElementById('list').innerHTML = items.map(s => `
      <article class="card">
        <h3>${esc(s.slice_date)}</h3>
        <span class="tag">${esc(s.status)}</span>
        <p class="muted">行数 ${s.row_count}</p>
        <button data-id="${s.id}" class="primary" type="button" ${s.can_replay ? '' : 'disabled'}>重放</button>
      </article>`).join('') || '<p class="muted">先生成切片计划</p>';
    document.querySelectorAll('#list button[data-id]').forEach(btn => {
      btn.onclick = async () => {
        try {
          const r = await api(`/api/slices/${btn.dataset.id}/replay`, { method: 'POST', body: '{}' });
          setMsg(`已落库 ${r.slice.slice_date} · ${r.slice.row_count} 行`);
          await draw(); await loadWindows();
        } catch (e) { setMsg(e.message, true); await draw(); }
      };
    });
  };
  document.getElementById('wid').onchange = draw;
  await draw();
}

async function renderMutex() {
  const items = (await api('/api/mutexes')).items || [];
  view.innerHTML = `<div class="card">
    <h3>互斥账册</h3>
    <p class="muted">持有中的互斥号即拒灌芯片来源；同一目标表同时只允许一条持有。</p>
    <div class="wall" style="margin-top:0.8rem">${items.map(m => `
      <article class="card">
        <h3>${esc(m.mutex_no)}</h3>
        <p class="muted">${esc(m.table_name)} · 窗口 #${m.window_id}</p>
        <p class="chips-row">
          <span class="chip ${m.state === '持有' ? 'chip-warn' : 'chip-ok'}">${esc(m.state)}</span>
          <span class="tag">${esc(m.window_status)}</span>
        </p>
        <p class="muted">${esc(m.start_date)} → ${esc(m.end_date)}</p>
      </article>`).join('') || '<p class="muted">暂无互斥记录</p>'}</div>
  </div>`;
}

async function renderCoverage() {
  const items = (await api('/api/coverage-reports')).items || [];
  view.innerHTML = `<div class="card">
    <label>窗口<select id="wid">${winOptions()}</select></label>
    <button class="primary" id="go" type="button">跑覆盖对账</button>
    <div class="wall" style="margin-top:0.8rem">${items.map(r => `
      <article class="card">
        <h3>窗口 #${r.window_id} <span class="chip ${r.verdict === '通过' ? 'chip-ok' : 'chip-bad'}">${esc(r.verdict)}</span></h3>
        <p>${esc(r.summary)}</p>
        <p class="muted">落点 ${r.landing_rows} 行 · 日增混入 ${r.mixed_rows} 行 · 门槛 ${esc(r.gate)}${r.mutex_no ? ' · ' + esc(r.mutex_no) : ''}</p>
      </article>`).join('') || '<p class="muted">还没有对账记录</p>'}</div>
  </div>`;
  document.getElementById('go').onclick = async () => {
    try {
      const j = await api('/api/coverage-reports', { method: 'POST', body: JSON.stringify({ window_id: Number(document.getElementById('wid').value) }) });
      setMsg(j.report.summary);
      await renderCoverage();
    } catch (e) { setMsg(e.message, true); }
  };
}

async function renderLog() {
  const items = (await api('/api/orchestration-log')).items || [];
  view.innerHTML = `<div class="card">
    <h3>编排留痕</h3>
    <div class="wall" style="margin-top:0.8rem">${items.map(l => `
      <article class="card">
        <h3>${esc(l.kind)} · ${esc(l.outcome)}</h3>
        <p class="muted">${esc(l.at)}${l.table_name ? ' · ' + esc(l.table_name) : ''}${l.mutex_no ? ' · ' + esc(l.mutex_no) : ''}</p>
        <p>${esc(l.detail)}</p>
      </article>`).join('') || '<p class="muted">暂无记录</p>'}</div>
  </div>`;
}

async function show(tab) {
  setMsg(''); await loadWindows();
  if (tab === 'windows') renderWindows();
  if (tab === 'plan') renderPlan();
  if (tab === 'replay') await renderReplay();
  if (tab === 'daily') await renderDaily();
  if (tab === 'mutex') await renderMutex();
  if (tab === 'coverage') await renderCoverage();
  if (tab === 'log') await renderLog();
}
document.querySelectorAll('#tabs button').forEach(btn => {
  btn.onclick = () => { document.querySelectorAll('#tabs button').forEach(b => b.classList.remove('on')); btn.classList.add('on'); show(btn.dataset.tab); };
});
document.getElementById('btn-logout').onclick = async () => { await api('/api/logout', { method: 'POST', body: '{}' }); location.href = '/login'; };
show('windows');
