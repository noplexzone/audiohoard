'use strict';

window.AudiohoardNavigation.registerPage('downloads', function (region) {
  var controller = new AbortController();
  var signal = controller.signal;
  var pollTimer = null;
  var pollInterval = 10000; // POLL_INTERVAL_MS = 10000

  function queueContainer() { return region.querySelector('#downloads-queue'); }
  function hasActiveGroup(container) {
    return container && Array.from(container.querySelectorAll('.badge')).some(function (element) {
      return ['pending', 'running'].includes(element.textContent.trim());
    });
  }
  function groupDetails(row) {
    var ownDetails = row.querySelector('details');
    if (ownDetails) return ownDetails;
    var next = row.nextElementSibling;
    return next && next.classList.contains('download-detail-row') ? next.querySelector('details') : null;
  }
  function detailsState(container) {
    var state = Object.create(null);
    container.querySelectorAll('[data-download-group]').forEach(function (row) {
      var details = groupDetails(row);
      if (details) state[row.dataset.downloadGroup] = details.open;
    });
    return state;
  }
  function restoreDetails(container, state) {
    container.querySelectorAll('[data-download-group]').forEach(function (row) {
      var details = groupDetails(row);
      if (details && Object.prototype.hasOwnProperty.call(state, row.dataset.downloadGroup)) {
        details.open = state[row.dataset.downloadGroup];
      }
    });
  }
  function poll() {
    var container = queueContainer();
    if (document.hidden || !container) return;
    var params = new URLSearchParams(window.location.search);
    var statusParam = params.get('status');
    var url = '/downloads/queue' + (statusParam ? '?status=' + encodeURIComponent(statusParam) : '');
    window.fetch(url, { credentials: 'same-origin', signal: signal }).then(function (response) {
      return response.ok ? response.text() : null;
    }).then(function (html) {
      var current = queueContainer();
      if (html === null || !current) return;
      var saved = detailsState(current);
      current.innerHTML = html;
      restoreDetails(current, saved);
      if (!hasActiveGroup(current) && pollTimer) { window.clearInterval(pollTimer); pollTimer = null; }
    }).catch(function (error) { if (error.name !== 'AbortError') return; });
  }

  region.addEventListener('submit', function (event) {
    var form = event.target.closest('form[data-confirm]');
    if (form && !window.confirm(form.dataset.confirm)) event.preventDefault();
  }, { signal: signal });
  if (hasActiveGroup(queueContainer())) pollTimer = window.setInterval(poll, pollInterval);
  return function () { controller.abort(); if (pollTimer) window.clearInterval(pollTimer); };
});
