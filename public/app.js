const $ = (sel) => document.querySelector(sel);

const state = {
  snapshot: null,
  events: [],
  lastSeq: 0,
  busy: false,
  timer: null,
  modalResolve: null,
};

const STATUS_LABEL = {
  PENDING: '未投递',
  DISPATCHED: '已发送·待确认',
  ACKED: '已确认生效',
  FAILED: '投递失败',
  TIMEOUT: '确认超时',
  REJECTED: '结构性拒绝',
};

const RELEASE_STATE = {
  DRAFT: '草稿',
  PUBLISHED: '已发布·投递中',
  PROMOTED: '正式版',
  SUPERSEDED: '已被取代',
  ROLLED_BACK: '已回退',
  REVOKED: '已撤销',
};

const CONSUMER_LABEL = { alpha: 'Alpha', beta: 'Beta', gamma: 'Gamma' };

function esc(value) {
  return String(value ?? '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

function timeOf(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? '—' : d.toLocaleTimeString('zh-CN', { hour12: false });
}

function ago(iso) {
  if (!iso) return '—';
  const ms = Date.now() - Date.parse(iso);
  if (!Number.isFinite(ms)) return '—';
  if (ms < 1000) return '刚刚';
  if (ms < 60000) return `${Math.floor(ms / 1000)} 秒前`;
  if (ms < 3600000) return `${Math.floor(ms / 60000)} 分前`;
  return `${Math.floor(ms / 3600000)} 时前`;
}

function dur(ms) {
  if (!Number.isFinite(ms)) return '—';
  const neg = ms < 0;
  let s = Math.floor(Math.abs(ms) / 1000);
  const parts = [];
  if (s >= 3600) { parts.push(`${Math.floor(s / 3600)} 小时`); s %= 3600; }
  if (s >= 60) { parts.push(`${Math.floor(s / 60)} 分`); s %= 60; }
  if (!parts.length) parts.push(`${s} 秒`);
  return `${neg ? '已超期 ' : ''}${parts.join(' ')}`;
}

let toastTimer = null;
function toast(message, kind = 'info') {
  const el = $('#toast');
  el.textContent = message;
  el.className = `toast ${kind}`;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.hidden = true; }, 4200);
}

async function api(path, { method = 'GET', body } = {}) {
  const res = await fetch(path, {
    method,
    headers: body ? { 'content-type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  let data = null;
  try { data = await res.json(); } catch { /* ignore */ }
  if (!res.ok) {
    const detail = data?.details?.blockers?.join('；');
    const err = new Error(detail || data?.error || `HTTP ${res.status}`);
    err.status = res.status;
    err.data = data;
    throw err;
  }
  return data;
}

function openModal(title, body, confirmLabel = '确认执行', danger = true) {
  $('#modal-title').textContent = title;
  $('#modal-body').textContent = body;
  $('#modal-reason').value = '';
  $('#modal-confirm').textContent = confirmLabel;
  $('#modal-confirm').className = danger ? 'danger' : 'primary';
  $('#modal').hidden = false;
  return new Promise((resolve) => { state.modalResolve = resolve; });
}

function closeModal(result) {
  $('#modal').hidden = true;
  const resolve = state.modalResolve;
  state.modalResolve = null;
  if (resolve) resolve(result);
}

$('#modal-cancel').addEventListener('click', () => closeModal(null));
$('#modal-confirm').addEventListener('click', () => {
  closeModal({ reason: $('#modal-reason').value.trim() });
});
$('#modal').addEventListener('click', (e) => {
  if (e.target.id === 'modal') closeModal(null);
});

/**
 * Serialize operator actions. A second click while a request is in flight is
 * queued rather than dropped, so a fast demo cannot silently skip a step.
 */
function act(fn, successMessage) {
  state.actionChain = (state.actionChain ?? Promise.resolve()).then(async () => {
    state.busy = true;
    try {
      const result = await fn();
      if (successMessage) toast(successMessage, 'good');
      await refresh(true);
      return result;
    } catch (err) {
      toast(err.message, 'bad');
      await refresh(true);
      return null;
    } finally {
      state.busy = false;
    }
  });
  return state.actionChain;
}

function policyDescription(policy) {
  if (policy.promotionMode === 'all-converged') {
    return `策略：<b>全员一致后切版</b>。发布后等待至多 <b>${dur(policy.waitWindowMs)}</b>，全部下游确认前不切正式版；` +
      `任一下游结构性拒绝则阻塞。切换后允许未确认者落后 <b>${dur(policy.lagWindowMs)}</b>，超期需人工处置。`;
  }
  return `策略：<b>法定人数 + 允许落后</b>。获得 <b>${policy.quorum}/3</b> 确认即可切版，其余下游允许落后 <b>${dur(policy.lagWindowMs)}</b>；` +
    `结构不兼容的下游在 <b>${dur(policy.compatWindowMs)}</b> 兼容窗口内可不升级，窗口超期需业务例外。`;
}

function renderSummary(snap) {
  const active = snap.active;
  const prod = snap.production;
  const rel = active;
  const cards = [];
  cards.push(`<div class="stat"><div class="k">当前正式版</div><div class="v mono">${prod ? esc(prod.version) : '—'}</div></div>`);
  cards.push(`<div class="stat"><div class="k">进行中发布</div><div class="v mono">${rel ? `${esc(rel.version)} <span class="muted" style="font-size:11px">${esc(RELEASE_STATE[rel.state] ?? rel.state)}</span>` : '无'}</div></div>`);
  cards.push(`<div class="stat"><div class="k">确认进度</div><div class="v">${rel ? `${rel.ackedCount}/${rel.total}` : '—'}</div></div>`);
  cards.push(`<div class="stat"><div class="k">Schema</div><div class="v mono">${rel ? `s${rel.schema}` : '—'}</div></div>`);
  cards.push(`<div class="stat"><div class="k">事件序号</div><div class="v mono">#${snap.journalHead}</div></div>`);
  cards.push(`<div class="stat"><div class="k">重复/迟到确认</div><div class="v mono">${snap.counters.duplicateAcks} / ${snap.counters.staleAcks + snap.counters.lateAcks}</div></div>`);
  $('#active-summary').innerHTML = cards.join('');
}

function renderPromotion(snap) {
  const active = snap.active;
  const promo = snap.promotion;
  const box = $('#promotion-box');
  if (!active) {
    const revoked = snap.releases.find((r) => r.state === 'REVOKED')
      ?? snap.releases.find((r) => r.state === 'ROLLED_BACK');
    if (revoked) {
      const byId = new Map(snap.releases.map((r) => [r.releaseId, r]));
      const previous = snap.promotions
        .map((p) => byId.get(p.releaseId))
        .filter((r) => r && r.releaseId !== revoked.releaseId && r.state !== 'REVOKED')
        .sort((a, b) => b.ordinal - a.ordinal)[0] ?? null;
      const why = revoked.state === 'REVOKED' ? '撤销' : '回退';
      const action = previous
        ? `<div class="gate"><button id="btn-restore" class="warn">以 ${esc(previous.version)} 内容向前恢复</button></div>` +
          `<p class="policy-note">恢复采用向前发布：用上一正式版 <b>${esc(previous.version)}</b> 的内容创建新版本，` +
          '重新走完整投递与确认，不修改任何下游的版本号。已隔离的数据保留在本地日志中。</p>'
        : '<p class="policy-note">没有更早的正式版本可供恢复。请新建一个草稿（建议换一个版本号）并重新发布。</p>';
      box.innerHTML = `<ul class="blockers"><li class="bad">当前没有正式版：最近版本 <b>${esc(revoked.version)}</b> 已${why}。` +
        `撤销不会自动回退，需要人工决定恢复哪个版本。</li></ul>${action}`;
      $('#btn-restore')?.addEventListener('click', async () => {
        const ok = await openModal(
          '向前恢复',
          `将基于 ${previous.version} 的内容创建一个新版本并重新投递、确认。确认？`,
          '创建恢复版本',
        );
        if (!ok) return;
        await act(() => api(`/api/releases/${revoked.releaseId}/rollback`, {
          method: 'POST', body: { reason: ok.reason || '人工恢复' },
        }), '已创建恢复版本，等待确认');
      });
      return;
    }
    box.innerHTML = '<p class="policy-note">尚无发布。新建草稿 → 发布 → 等待下游确认 → 切正式版。</p>';
    return;
  }
  const policy = snap.policy;
  const pct = Math.round((active.ackedCount / Math.max(1, active.total)) * 100);
  const blockers = promo?.blockers ?? [];
  const lagging = promo?.lagging ?? [];
  const compat = (promo?.compat ?? []).filter((c) => !c.consumer || c.requiredSchema > c.supportedSchema);
  const canPromote = (promo?.eligible || promo?.eligibleWithException) && active.state === 'PUBLISHED';
  const compatExceptionNeeded = promo?.compatExpired;
  const rows = [];

  rows.push(`<div class="gate">
    <span class="pill ${pct === 100 ? 'pill-good' : 'pill-warn'}">${pct}% 已确认</span>
    <div class="bar"><i style="width:${pct}%"></i></div>
    <button id="btn-promote" class="primary" ${canPromote ? '' : 'disabled'}>切为正式版</button>
    <button id="btn-dispatch" class="ghost">立即重试投递</button>
    <button id="btn-pub-restart" class="ghost" title="SIGKILL 发布进程后重启，验证状态不回退">重启发布进程</button>
  </div>`);
  rows.push(`<p class="policy-note">${policyDescription(policy)}</p>`);

  const promotable = active.state === 'PUBLISHED';
  if (promotable && promo?.eligibleWithException && !promo.eligible) {
    rows.push(`<ul class="blockers">${(promo.blockersWithException ?? []).map((b) => `<li>${esc(b)}</li>`).join('')}</ul>`);
    rows.push('<p class="policy-note">兼容窗口已超期，可以用<strong>业务例外</strong>继续切版；点按钮后需要再次确认，例外会记入发布记录。</p>');
  } else if (promotable && blockers.length) {
    rows.push(`<ul class="blockers">${blockers.map((b) => `<li>${esc(b)}</li>`).join('')}</ul>`);
  } else if (active.state === 'PUBLISHED') {
    rows.push('<p class="policy-note" style="color:var(--good)">切换条件已满足，可以切为正式版。</p>');
  } else if (active.state === 'PROMOTED') {
    const inFlight = snap.releases.find((r) => r.state === 'PUBLISHED');
    rows.push(`<p class="policy-note">已于 ${esc(timeOf(active.promotedAt))} 切为正式版；${active.promotion?.forced ? '存在落后下游，已记录落后截止时间。' : '全部下游已确认。'}${inFlight ? ` 新发布 <b>${esc(inFlight.version)}</b> 正在投递，确认进度 ${inFlight.ackedCount}/${inFlight.total}。` : ''}</p>`);
  } else {
    rows.push(`<p class="policy-note">当前状态 <b>${esc(RELEASE_STATE[active.state] ?? active.state)}</b>，无需切版。</p>`);
  }

  if (lagging.length) {
    const desc = lagging.map((l) => {
      const deadline = l.lagDeadline ? `，落后截止 ${esc(timeOf(l.lagDeadline))}${l.lagOverdue ? '（已超期）' : ''}` : '';
      return `<b>${esc(CONSUMER_LABEL[l.consumer] ?? l.consumer)}</b>（${esc(STATUS_LABEL[l.status] ?? l.status)}${deadline}）`;
    }).join('，');
    rows.push(`<div class="policy-note">落后下游：${desc}${promo?.waitDeadline && active.state === 'PUBLISHED' ? `；等待窗口至 ${esc(timeOf(promo.waitDeadline))}${promo.waitOverdue ? '（已到期）' : ''}` : ''}</div>`);
  }
  if (compat.length) {
    rows.push(`<div class="policy-note">兼容窗口：${compat.map((c) => `<b>${esc(CONSUMER_LABEL[c.consumer] ?? c.consumer)}</b> 支持 s${c.supportedSchema}，本版本要求 s${c.requiredSchema}，窗口至 ${esc(timeOf(c.expiresAt))}（${c.expired ? '<span style="color:var(--bad)">已超期</span>' : '剩余 ' + dur(c.remainingMs)}）`).join('；')}</div>`);
  }
  if (compatExceptionNeeded && canPromote) {
    rows.push('<p class="policy-note" style="color:var(--warn)">兼容窗口已超期：切版将要求确认业务例外（再次确认对话框）。</p>');
  }
  box.innerHTML = rows.join('');

  const promoteBtn = $('#btn-promote');
  if (promoteBtn) {
    promoteBtn.addEventListener('click', async () => {
      let acknowledge = false;
      if (promo?.compatExpired) {
        const ok = await openModal(
          `切版确认 · ${active.version}`,
          `${promo.compat.filter((c) => c.expired).map((c) => CONSUMER_LABEL[c.consumer] ?? c.consumer).join('、')} 的 schema 兼容窗口已过，仍按法定人数切版属于业务例外。确认继续？`,
          '确认业务例外并切版',
        );
        if (!ok) return;
        acknowledge = true;
      }
      await act(
        () => api(`/api/releases/${active.releaseId}/promote`, { method: 'POST', body: { acknowledgeCompatException: acknowledge } }),
        `${active.version} 已切为正式版`,
      );
    });
  }
  $('#btn-dispatch')?.addEventListener('click', () => act(
    () => api(`/api/releases/${active.releaseId}/dispatch`, { method: 'POST' }),
    '已按当前状态重新投递未确认下游',
  ));
  $('#btn-pub-restart')?.addEventListener('click', async () => {
    const ok = await openModal('重启发布进程', '将向进程管理器发送重启指令。发布器从磁盘事件日志重放，状态不应回退。', '重启');
    if (!ok) return;
    await act(() => api('/api/supervisor/publisher/restart', { method: 'POST' }), '已请求重启发布进程，状态从磁盘日志恢复');
    setTimeout(() => refresh(true), 1200);
  });
}

function renderConsumers(snap) {
  const activeId = snap.active?.releaseId ?? null;
  $('#consumers').innerHTML = snap.consumers.map((c) => {
    const isOnline = c.online;
    const activeDelivery = snap.active?.deliveries?.[c.name] ?? null;
    const consumedActive = c.consumed.find((x) => x.releaseId === activeId);
    const eff = c.effectiveVersion;
    const drift = c.drift;
    let versionChip = eff
      ? `<span class="ver-chip effective">${esc(eff)}</span>`
      : '<span class="ver-chip">未确认任何版本</span>';
    if (c.effectiveWithdrawn) versionChip += ' <span class="pill pill-bad">该版本已撤销</span>';
    if (c.withdrawalPending) versionChip += ' <span class="pill pill-warn">撤销待确认</span>';
    if (c.lagOverdue) versionChip += ' <span class="pill pill-bad">落后超期</span>';
    const reported = c.consumerReportedVersion
      ? `<span class="ver-chip ${drift ? 'drift' : ''}">${esc(c.consumerReportedVersion)}</span>${drift ? ' <span class="pill pill-bad">与确认不一致</span>' : ''}`
      : '<span class="muted">离线 / 未上报</span>';
    const activeState = activeDelivery
      ? `<span class="pill ${activeDelivery.status === 'ACKED' ? 'pill-good' : activeDelivery.status === 'REJECTED' ? 'pill-bad' : 'pill-warn'}">${esc(STATUS_LABEL[activeDelivery.status] ?? activeDelivery.status)}</span>`
      : '<span class="muted">—</span>';
    const rejection = activeDelivery?.rejection?.reason
      ? `<div class="kv"><span class="k">拒绝原因</span><span class="v" style="color:#ffb4ad">${esc(activeDelivery.rejection.reason)}</span></div>`
      : '';
    const consumedList = c.consumed.length
      ? c.consumed.map((x) => `<span class="ver-chip ${x.quarantined ? 'drift' : ''}" title="${esc(x.quarantined ? '已隔离，不再提供数据' : '已确认生效')}">${esc(x.version)}${x.quarantined ? ' ⛔' : ''}</span>`).join(' ')
      : '<span class="muted">无</span>';
    return `<div class="consumer ${isOnline ? '' : 'offline'}">
      <div class="consumer-head">
        <div class="consumer-name"><span class="dot" style="color:${isOnline ? 'var(--good)' : 'var(--bad)'}"></span>${esc(CONSUMER_LABEL[c.name] ?? c.name)}<span class="port">${c.port ? `:${esc(c.port)}` : ''}</span></div>
        <span class="pill ${isOnline ? 'pill-good' : 'pill-bad'}">${isOnline ? '在线' : '离线'}</span>
      </div>
      <div class="kv">
        <span class="k">${c.effectiveWithdrawn ? '最后确认版本' : '已确认生效'}</span><span class="v">${versionChip}</span>
        <span class="k">进程自报版本</span><span class="v">${reported}</span>
        <span class="k">支持 schema</span><span class="v">s${esc(c.profile?.schema ?? '?')}</span>
        <span class="k">当前发布投递</span><span class="v">${activeState}</span>
        ${rejection}
        <span class="k">已消费版本</span><span class="v">${consumedList}</span>
      </div>
      <div class="consumer-actions">
        <button class="small" data-consumer="${esc(c.name)}" data-op="start" ${isOnline ? 'disabled' : ''}>启动</button>
        <button class="small" data-consumer="${esc(c.name)}" data-op="stop" ${isOnline ? '' : 'disabled'}>停止</button>
        <button class="small" data-consumer="${esc(c.name)}" data-op="restart">重启</button>
        <button class="small warn" data-consumer="${esc(c.name)}" data-op="crash" ${isOnline ? '' : 'disabled'} title="SIGKILL 模拟崩溃，不自动拉起">崩溃</button>
        <button class="small ghost" data-consumer="${esc(c.name)}" data-op="details">本地状态</button>
      </div>
    </div>`;
  }).join('');

  document.querySelectorAll('[data-op]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const name = btn.dataset.consumer;
      const op = btn.dataset.op;
      const label = CONSUMER_LABEL[name] ?? name;
      if (op === 'details') {
        try {
          const detail = await api(`/api/consumers/${name}/state`);
          showConsumerDetail(name, detail);
        } catch (err) {
          toast(err.message, 'bad');
        }
        return;
      }
      if (op === 'crash') {
        const ok = await openModal(`模拟崩溃 · ${label}`, 'SIGKILL 杀掉进程且不自动重启。之后可点“启动”验证从磁盘日志恢复且不重复应用。', '杀掉进程');
        if (!ok) return;
        await act(() => api(`/api/supervisor/${name}/crash`, { method: 'POST' }), `${label} 已被杀掉`);
        return;
      }
      if (op === 'stop' || op === 'restart') {
        const ok = await openModal(
          `${op === 'stop' ? '停止' : '重启'} · ${label}`,
          op === 'stop'
            ? '停止后发布时间内该下游不会收到投递；未确认的内容会显示为超时。重新启动后，未确认的投递会继续重试。'
            : '进程将从磁盘日志重放状态，已完成的应用不会重复执行。',
          op === 'stop' ? '停止进程' : '重启进程',
          op === 'stop',
        );
        if (!ok) return;
      }
      await act(() => api(`/api/supervisor/${name}/${op}`, { method: 'POST' }),
        `${label} 已${op === 'start' ? '启动' : op === 'stop' ? '停止' : '重启'}`);
      setTimeout(() => refresh(true), op === 'start' || op === 'restart' ? 900 : 200);
    });
  });
}

function showConsumerDetail(name, detail) {
  const lines = [
    `当前进程版本: ${detail.currentVersion ?? '无'}（appliedSeq=${detail.appliedSeq}，serving=${detail.serving}）`,
    `支持 schema: s${detail.supportedSchema}`,
    `已应用: ${detail.applications.map((a) => `${a.version}${a.state === 'quarantined' ? '（隔离）' : ''}`).join(', ') || '无'}`,
    `已拒绝: ${detail.rejected.map((r) => `${r.releaseId.slice(0, 10)}: ${r.reason}`).join('；') || '无'}`,
    `待重发确认: ${detail.outbox.length}`,
    `计数器: 应用 ${detail.counters.applied} / 重复投递 ${detail.counters.duplicateDeliveries} / 旧投递 ${detail.counters.staleDeliveries} / 拒绝 ${detail.counters.rejected} / 隔离 ${detail.counters.quarantined}`,
    '',
    '最近本地事件:',
    ...detail.recentEvents.slice(-10).map((e) => `#${e.seq} ${e.at.slice(11, 19)} ${e.type} ${e.releaseId ? e.releaseId.slice(0, 12) : ''} ${e.reason ?? ''}`),
  ];
  openModal(`${CONSUMER_LABEL[name] ?? name} 本地状态`, lines.join('\n'), '关闭', false).then(() => {});
  const confirm = $('#modal-confirm');
  confirm.textContent = '关闭';
  confirm.onclick = () => {
    confirm.onclick = null;
    closeModal({ reason: '' });
  };
}

function deliveryRow(rel, name, d) {
  const status = d.status;
  const cls = status === 'ACKED' ? 'pill-good' : status === 'REJECTED' ? 'pill-bad' : status === 'PENDING' ? 'pill-muted' : 'pill-warn';
  const reason = d.rejection?.reason ? `<td class="reason">${esc(d.rejection.reason)}</td>` : '<td class="muted">—</td>';
  const retry = ['PENDING', 'FAILED', 'TIMEOUT', 'DISPATCHED'].includes(status) && !['REVOKED', 'ROLLED_BACK', 'DRAFT'].includes(rel.state)
    ? `<td><button class="small ghost" data-retry="${esc(rel.releaseId)}" data-name="${esc(name)}">重试</button></td>`
    : '<td></td>';
  return `<tr>
    <td>${esc(name)}</td>
    <td><span class="pill ${cls}">${esc(STATUS_LABEL[status] ?? status)}</span></td>
    <td>${esc(d.attempts)}</td>
    <td>${esc(timeOf(d.dispatchedAt))}</td>
    <td>${esc(timeOf(d.ackAt))}</td>
    <td>${d.appliedSeq ?? '—'}</td>
    ${reason}
    ${retry}
  </tr>`;
}

function releaseCard(rel, snap) {
  const actions = [];
  if (rel.state === 'DRAFT') {
    actions.push(`<button class="primary small" data-act="publish" data-id="${esc(rel.releaseId)}">发布并投递</button>`);
  }
  if (rel.state === 'PUBLISHED') {
    const canPromote = snap.promotion?.eligible;
    actions.push(`<button class="primary small" data-act="promote" data-id="${esc(rel.releaseId)}" ${canPromote ? '' : 'disabled'}>切为正式版</button>`);
    actions.push(`<button class="small ghost" data-act="dispatch" data-id="${esc(rel.releaseId)}">立即重试投递</button>`);
  }
  if (['PUBLISHED', 'PROMOTED'].includes(rel.state)) {
    actions.push(`<button class="danger small" data-act="revoke" data-id="${esc(rel.releaseId)}">紧急撤销</button>`);
    actions.push(`<button class="warn small" data-act="rollback" data-id="${esc(rel.releaseId)}">回退</button>`);
  }
  if (rel.state === 'REVOKED') {
    actions.push(`<button class="warn small" data-act="rollback" data-id="${esc(rel.releaseId)}">恢复上一版本</button>`);
  }
  if (rel.state === 'REVOKED' && rel.revocation) {
    const pending = Object.values(rel.revocation.results ?? {}).filter((r) => !r.ok).map((r) => r.consumer);
    if (pending.length) {
      actions.push(`<button class="small" data-act="revoke" data-id="${esc(rel.releaseId)}">重试撤销（${esc(pending.join(','))}）</button>`);
    }
  }
  if (rel.deliveries && Object.values(rel.deliveries).some((d) => ['ACKED', 'TIMEOUT'].includes(d.status))) {
    actions.push(`<button class="small ghost" data-act="dup-ack" data-id="${esc(rel.releaseId)}">重放一条确认</button>`);
    actions.push(`<button class="small ghost" data-act="late-ack" data-id="${esc(rel.releaseId)}" title="模拟旧投递的确认迟到">注入迟到确认</button>`);
  }
  const delivRows = Object.entries(rel.deliveries).map(([name, d]) => deliveryRow(rel, name, d)).join('');
  const promoInfo = rel.promotion
    ? `<div class="policy-note">切版时生效版本：${Object.entries(rel.promotion.effectiveVersions).map(([n, v]) => `${esc(CONSUMER_LABEL[n] ?? n)}=${esc(v ?? '无')}`).join('，')}${rel.promotion.forced ? '（存在落后下游）' : ''}</div>`
    : '';
  const revokeInfo = rel.revocation
    ? `<div class="policy-note">撤销处置：${Object.entries(rel.revocation.targets ?? {}).map(([n, t]) => `${esc(n)} ${t.consumed ? '已消费→隔离' : '未消费→拉黑'}`).join('；')}
       ${Object.entries(rel.revocation.results ?? {}).map(([n, r]) => `<span class="pill ${r.ok ? 'pill-good' : 'pill-bad'}">${esc(n)}: ${esc(r.disposition)}${r.expect && r.expect !== r.disposition ? `（预期 ${esc(r.expect)}）` : ''}</span>`).join(' ')}</div>`
    : '';
  const rollbackInfo = rel.rollback ? `<div class="policy-note">回退原因：${esc(rel.rollback.reason)}</div>` : '';
  return `<div class="release ${rel.state}">
    <div class="release-head">
      <span class="name">${esc(rel.version)}</span>
      <span class="pill ${rel.state === 'PROMOTED' ? 'pill-good' : rel.state === 'REVOKED' ? 'pill-bad' : rel.state === 'ROLLED_BACK' ? 'pill-warn' : 'pill-info'}">${esc(RELEASE_STATE[rel.state] ?? rel.state)}</span>
      <span class="muted mono" style="font-size:11.5px">#${rel.ordinal} · s${rel.schema} · ${rel.rowCount} 行 · ${rel.payloadHash.slice(0, 10)}</span>
      <span class="muted" style="font-size:11.5px">确认 ${rel.ackedCount}/${rel.total}</span>
      <span class="spacer"></span>
      <span class="muted" style="font-size:11.5px">${ago(rel.createdAt)}</span>
    </div>
    <div class="release-body">
      ${rel.notes ? `<div class="policy-note">${esc(rel.notes)}</div>` : ''}
      <div class="release-actions">${actions.join('')}</div>
      <table class="deliv-table">
        <thead><tr><th>下游</th><th>状态</th><th>尝试</th><th>发送</th><th>确认</th><th>appliedSeq</th><th>原因</th><th></th></tr></thead>
        <tbody>${delivRows}</tbody>
      </table>
      ${promoInfo}
      ${revokeInfo}
      ${rollbackInfo}
    </div>
  </div>`;
}

function renderReleases(snap) {
  const list = snap.releases;
  $('#releases').innerHTML = list.length
    ? list.map((r) => releaseCard(r, snap)).join('')
    : '<p class="policy-note">还没有发布。填写版本号后点“新建草稿”。</p>';

  document.querySelectorAll('[data-act]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const id = btn.dataset.id;
      const op = btn.dataset.act;
      const rel = list.find((r) => r.releaseId === id);
      if (op === 'publish') {
        await act(() => api(`/api/releases/${id}/publish`, { method: 'POST' }), `${rel.version} 已发布，开始投递`);
      } else if (op === 'promote') {
        await act(() => api(`/api/releases/${id}/promote`, { method: 'POST' }), `${rel.version} 已切为正式版`);
      } else if (op === 'dispatch') {
        await act(() => api(`/api/releases/${id}/dispatch`, { method: 'POST' }), '已重新投递未确认下游');
      } else if (op === 'revoke') {
        const ok = await openModal(
          `紧急撤销 · ${rel.version}`,
          '已消费该版本的下游将被要求隔离数据（保留审计、不再对外提供）；未消费的下游将被拉黑，之后即使收到投递也不会应用。撤销不会被后来的重复确认翻回。',
          '撤销该版本',
        );
        if (!ok) return;
        await act(() => api(`/api/releases/${id}/revoke`, { method: 'POST', body: { reason: ok.reason || '紧急撤销' } }), `${rel.version} 已撤销`);
      } else if (op === 'rollback') {
        const ok = await openModal(
          `回退 · ${rel.version}`,
          '回退采用“向前发布”方式：用上一正式版本的内容创建一个新版本并投递，而不是让下游版本号倒退。确认回退？',
          '确认回退',
        );
        if (!ok) return;
        await act(() => api(`/api/releases/${id}/rollback`, { method: 'POST', body: { reason: ok.reason || '回退' } }), '已创建回退恢复版本');
      } else if (op === 'dup-ack') {
        const consumer = Object.entries(rel.deliveries).find(([, d]) => d.status === 'ACKED')?.[0];
        if (!consumer) return;
        const res = await act(
          () => api(`/api/releases/${id}/simulate/duplicate-ack`, { method: 'POST', body: { consumer } }),
          '已重放重复确认（应被识别为 duplicate）',
        );
        if (res) toast(`重复确认处理结果：${JSON.stringify(res.result)}`, 'info');
      } else if (op === 'late-ack') {
        const consumer = Object.entries(rel.deliveries).find(([, d]) => ['ACKED', 'TIMEOUT', 'DISPATCHED'].includes(d.status))?.[0];
        if (!consumer) return;
        const res = await act(
          () => api(`/api/releases/${id}/simulate/late-ack`, { method: 'POST', body: { consumer } }),
          '已注入旧投递的迟到确认（应被判定 stale）',
        );
        if (res) toast(`迟到确认处理结果：${JSON.stringify(res.result)}`, 'info');
      }
    });
  });
  document.querySelectorAll('[data-retry]').forEach((btn) => {
    btn.addEventListener('click', () => act(
      () => api(`/api/releases/${btn.dataset.retry}/deliveries/${btn.dataset.name}/retry`, { method: 'POST' }),
      `已重试 ${btn.dataset.name}`,
    ));
  });
  const modeSel = $('#policy-mode');
  if (modeSel.value !== snap.policy.promotionMode) modeSel.value = snap.policy.promotionMode;
  $('#policy-quorum').value = String(snap.policy.quorum);
  // Keep window inputs in sync with the server, but never overwrite a field
  // the operator is currently editing.
  const syncMinutes = (sel, ms, unit) => {
    const el = $(sel);
    if (document.activeElement === el) return;
    el.value = String(Math.round(ms / unit));
  };
  syncMinutes('#policy-wait', snap.policy.waitWindowMs, 60000);
  syncMinutes('#policy-lag', snap.policy.lagWindowMs, 60000);
  syncMinutes('#policy-compat', snap.policy.compatWindowMs, 3600000);
}

const EVENT_DESC = {
  'release.created': (e) => `创建草稿 ${e.version}（#${e.ordinal}, schema s${e.schema}）`,
  'release.published': () => '发布，开始点对点投递',
  'release.promoted': (e) => `切为正式版（${e.mode}${e.forced ? '，存在落后下游' : ''}）`,
  'release.superseded': () => '被更新版本取代',
  'release.rolled_back': (e) => `回退：${e.reason}`,
  'release.revoked': (e) => `紧急撤销：${e.reason}`,
  'delivery.dispatched': (e) => `发送给 ${e.consumer}（第 ${e.dispatchSeq} 次投递，等待确认）`,
  'delivery.rejected': (e) => `${e.consumer} 拒绝：${e.reason}${e.terminal ? '（终结）' : ''}`,
  'delivery.timed_out': (e) => `${e.consumer} 确认超时（不视为已生效）`,
  'delivery.retried': (e) => `${e.consumer} 重新排队投递`,
  'delivery.acked': (e) => `${e.consumer} 确认已应用 ${e.version}（appliedSeq=${e.appliedSeq}）`,
  'ack.duplicate': (e) => `${e.consumer} 重复确认已忽略`,
  'ack.stale': (e) => `${e.consumer} 旧确认迟到（seq ${e.arrivalSeq} < ${e.expectedDispatchSeq}），未采信`,
  'revocation.result': (e) => `撤销处置 ${e.consumer}: ${e.disposition}${e.ok ? '' : ` (失败: ${e.error})`}`,
  'policy.updated': () => '更新发布策略',
  'error': (e) => `错误：${e.message}`,
};

function eventClass(type) {
  if (type.startsWith('delivery.acked')) return 'good';
  if (type.includes('rejected') || type.includes('revoked') || type.includes('stale')) return 'bad';
  if (type.includes('timed_out') || type.includes('retry') || type.includes('duplicate')) return 'warn';
  return '';
}

function renderTimeline() {
  const events = state.events.slice(-120).reverse();
  $('#timeline').innerHTML = events.map((e) => {
    const desc = EVENT_DESC[e.type]?.(e) ?? e.type;
    const id = e.releaseId ? ` <span class="muted">${esc(e.releaseId.slice(0, 12))}</span>` : '';
    return `<li><span class="seq">#${e.seq}</span><span class="ts">${esc(timeOf(e.at))}</span><span><span class="t ${eventClass(e.type)}">${esc(e.type)}</span> <span class="d">${esc(desc)}${id}</span></span></li>`;
  }).join('') || '<li class="muted">暂无事件</li>';
  $('#journal-head').textContent = `head #${state.lastSeq}`;
}

async function fetchEvents() {
  const follow = $('#follow').checked;
  const since = follow ? state.lastSeq : 0;
  const data = await api(`/api/events?since=${since}`);
  if (follow) {
    state.events = [...state.events, ...data.events].slice(-400);
    state.lastSeq = data.head;
  } else {
    state.events = data.events;
  }
}

async function refresh(full = false) {
  try {
    const [snap] = await Promise.all([api('/api/state'), fetchEvents()]);
    state.snapshot = snap;
    renderSummary(snap);
    renderPromotion(snap);
    renderConsumers(snap);
    renderReleases(snap);
    renderTimeline();
    $('#conn').textContent = '已连接';
    $('#conn').className = 'pill pill-good';
    $('#clock').textContent = new Date().toLocaleTimeString('zh-CN', { hour12: false });
  } catch (err) {
    $('#conn').textContent = '连接断开';
    $('#conn').className = 'pill pill-bad';
    if (full) toast(`无法连接发布器：${err.message}`, 'bad');
  }
}

$('#btn-refresh').addEventListener('click', () => refresh(true));
$('#btn-policy').addEventListener('click', () => act(() => api('/api/policy', {
  method: 'POST',
  body: {
    promotionMode: $('#policy-mode').value,
    quorum: Number($('#policy-quorum').value),
    waitWindowMs: Number($('#policy-wait').value || 15) * 60 * 1000,
    lagWindowMs: Number($('#policy-lag').value || 15) * 60 * 1000,
    compatWindowMs: Number($('#policy-compat').value || 24) * 3600 * 1000,
  },
}), '策略已保存'));

$('#btn-create').addEventListener('click', () => {
  const version = $('#new-version').value.trim();
  const schema = Number($('#new-schema').value);
  const cohort = $('#new-cohort').value.trim();
  act(() => api('/api/releases', {
    method: 'POST',
    body: { version: version || undefined, schema, cohort: cohort || undefined, rowCount: 24 },
  }), '草稿已创建，可以点“发布并投递”').then(() => {
    $('#new-version').value = '';
    $('#new-cohort').value = '';
  });
});

$('#policy-mode').addEventListener('change', () => {
  $('#quorum-wrap').style.opacity = $('#policy-mode').value === 'quorum-with-lag' ? '1' : '.4';
});

state.timer = setInterval(() => { refresh(false); }, 1200);
refresh(true);
