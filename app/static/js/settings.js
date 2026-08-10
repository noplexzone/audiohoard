'use strict';

window.AudiohoardNavigation.registerPage('settings', function (region) {
  var controller = new AbortController();
  var dirtyForms = new Set();

  function setDirty(form, dirty) {
    var indicator = form.querySelector('.unsaved-indicator');
    if (indicator) indicator.hidden = !dirty;
    if (dirty) dirtyForms.add(form);
    else dirtyForms.delete(form);
  }

  region.querySelectorAll('[data-settings-form]').forEach(function (form) {
    form.addEventListener('input', function () { setDirty(form, true); }, { signal: controller.signal });
    form.addEventListener('change', function () { setDirty(form, true); }, { signal: controller.signal });
  });

  region.addEventListener('click', function (event) {
    var button = event.target.closest('.dismiss-btn');
    if (button) button.closest('.alert')?.remove();
  }, { signal: controller.signal });

  region.addEventListener('submit', async function (event) {
    var form = event.target.closest('[data-save-and-test-form]');
    if (!form || !window.fetch) return;
    var submitter = event.submitter;
    var action = new URL(submitter?.formAction || form.action, window.location.href);
    if (action.pathname !== '/settings/save-and-test') return;

    event.preventDefault();
    var feedback = form.querySelector('[data-connection-feedback]');
    var status = form.closest('[data-provider-card]')?.querySelector('[data-connection-status]');
    if (submitter) submitter.disabled = true;
    if (feedback) {
      feedback.className = 'connection-feedback hint';
      feedback.textContent = 'Saving and testing connection…';
    }

    try {
      var response = await fetch(action.href, {
        method: 'POST',
        body: new FormData(form),
        headers: { 'X-Requested-With': 'fetch' },
        signal: controller.signal
      });
      var payload = await response.json();
      if (!response.ok || !payload.saved) throw new Error(payload.error || 'Connection test failed');
      if (status) {
        status.classList.toggle('ok', Boolean(payload.available));
        status.classList.toggle('error', !payload.available);
        status.classList.remove('info');
        var label = status.querySelector('[data-status-label]');
        var detail = status.querySelector('[data-status-detail]');
        if (label) label.textContent = payload.status;
        if (detail && payload.elapsed_ms !== null) detail.textContent = ' · just checked · ' + payload.elapsed_ms + 'ms';
      }
      if (feedback) {
        feedback.className = 'connection-feedback ' + (payload.available ? 'ok' : 'error');
        feedback.textContent = payload.available ? 'Saved and connected.' : 'Saved, but the connection test failed: ' + (payload.reason || payload.status);
        feedback.focus();
      }
      setDirty(form, false);
    } catch (error) {
      if (error.name === 'AbortError') return;
      if (feedback) {
        feedback.className = 'connection-feedback error';
        feedback.setAttribute('role', 'alert');
        feedback.textContent = error.message || 'Settings could not be saved and tested.';
        feedback.focus();
      }
    } finally {
      if (submitter) submitter.disabled = false;
    }
  }, { signal: controller.signal });

  function beforeUnload(event) {
    if (!dirtyForms.size) return;
    event.preventDefault();
    event.returnValue = '';
  }
  window.addEventListener('beforeunload', beforeUnload, { signal: controller.signal });
  return function () { controller.abort(); };
});
