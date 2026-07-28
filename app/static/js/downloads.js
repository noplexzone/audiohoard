(function () {
  'use strict';

  var POLL_INTERVAL_MS = 10000;
  var pollTimer = null;

  function queueContainer() {
    return document.getElementById('downloads-queue');
  }

  function hasActiveGroup(container) {
    if (!container) return false;
    return Array.from(container.querySelectorAll('.badge')).some(function (el) {
      var text = el.textContent.trim();
      return text === 'pending' || text === 'running';
    });
  }

  function captureDetailsState(container) {
    var state = Object.create(null);
    container.querySelectorAll('[data-download-group]').forEach(function (row) {
      var key = row.getAttribute('data-download-group');
      var details = row.querySelector('details');
      if (details) {
        state[key] = details.open;
      }
    });
    return state;
  }

  function restoreDetailsState(container, state) {
    container.querySelectorAll('[data-download-group]').forEach(function (row) {
      var key = row.getAttribute('data-download-group');
      if (Object.prototype.hasOwnProperty.call(state, key)) {
        var details = row.querySelector('details');
        if (details) {
          details.open = state[key];
        }
      }
    });
  }

  function buildQueueUrl() {
    var params = new URLSearchParams(window.location.search);
    var statusParam = params.get('status');
    return '/downloads/queue' + (statusParam ? '?status=' + encodeURIComponent(statusParam) : '');
  }

  function poll() {
    if (document.hidden) return;
    var container = queueContainer();
    if (!container) return;

    fetch(buildQueueUrl(), { credentials: 'same-origin' })
      .then(function (resp) {
        if (!resp.ok) return null;
        return resp.text();
      })
      .then(function (html) {
        if (html === null) return;
        var container = queueContainer();
        if (!container) return;
        var saved = captureDetailsState(container);
        var scrollY = window.scrollY;
        container.innerHTML = html;
        restoreDetailsState(container, saved);
        window.scrollTo(0, scrollY);
        if (!hasActiveGroup(container)) {
          clearInterval(pollTimer);
          pollTimer = null;
        }
      })
      .catch(function () {
        // network error — keep polling
      });
  }


  document.addEventListener('submit', function (event) {
    var form = event.target.closest('form[data-confirm]');
    if (form && !window.confirm(form.getAttribute('data-confirm'))) {
      event.preventDefault();
    }
  });

  document.addEventListener('DOMContentLoaded', function () {
    var container = queueContainer();
    if (hasActiveGroup(container)) {
      pollTimer = setInterval(poll, POLL_INTERVAL_MS);
    }
  });
})();
