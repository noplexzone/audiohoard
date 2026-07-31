'use strict';

window.AudiohoardNavigation.registerPage('album', function (region) {
  var controller = new AbortController();
  var signal = controller.signal;

  function setButton(button, text, disabled) {
    if (!button) return;
    button.disabled = disabled;
    button.textContent = text;
  }

  region.querySelectorAll('form[data-autosave="monitor-upgrades"]').forEach(function (form) {
    var checkbox = form.querySelector('input[type="checkbox"]');
    var status = form.querySelector('[data-save-status]');
    if (!checkbox || !status) return;
    checkbox.addEventListener('change', async function () {
      var requested = checkbox.checked;
      checkbox.disabled = true;
      status.textContent = 'Saving…';
      status.className = 'save-status pending';
      try {
        var response = await window.fetch(form.action, {
          method: 'POST', body: new FormData(form), credentials: 'same-origin',
          headers: { 'X-Requested-With': 'fetch' }, signal: signal,
        });
        if (!response.ok) throw new Error('Monitoring update failed');
        status.textContent = 'Saved';
        status.className = 'save-status saved';
      } catch (error) {
        if (error.name === 'AbortError') return;
        checkbox.checked = !requested;
        status.textContent = 'Could not save';
        status.className = 'save-status error';
      } finally {
        if (checkbox.isConnected) checkbox.disabled = false;
      }
    }, { signal: signal });
  });

  region.addEventListener('submit', async function (event) {
    var confirmForm = event.target.closest('form[data-confirm]');
    if (confirmForm && !window.confirm(confirmForm.getAttribute('data-confirm'))) {
      event.preventDefault();
      return;
    }

    var removeForm = event.target.closest('form[data-remove-form]');
    if (removeForm) {
      event.preventDefault();
      var removeButton = removeForm.querySelector('button[type="submit"]');
      setButton(removeButton, 'Removing…', true);
      try {
        var removal = await window.fetch(removeForm.action, {
          method: 'POST', body: new FormData(removeForm), credentials: 'same-origin',
          headers: { 'Accept': 'application/json', 'X-Requested-With': 'fetch' }, signal: signal,
        });
        if (!removal.ok) throw new Error('Removal failed');
        await removal.json();
        if (window.AudiohoardNavigation && window.AudiohoardNavigation.refresh) {
          await window.AudiohoardNavigation.refresh();
        } else {
          window.location.reload();
        }
      } catch (error) {
        if (error.name === 'AbortError') return;
        setButton(removeButton, 'Try again', false);
      }
      return;
    }

    var form = event.target.closest('form[data-download-form]');
    if (!form) return;
    event.preventDefault();
    var button = form.querySelector('button[type="submit"]');
    var original = button ? button.textContent : '';
    setButton(button, 'Queueing…', true);
    try {
      var response = await window.fetch(form.action, {
        method: 'POST', body: new FormData(form), credentials: 'same-origin',
        headers: { 'X-Requested-With': 'fetch' }, signal: signal,
      });
      if (!response.ok) throw new Error('Download request failed');
      var data = await response.json();
      setButton(button, data.queued > 0 ? 'Queued' : 'Nothing to queue', true);
      window.setTimeout(function () {
        if (button && button.isConnected) setButton(button, original, false);
      }, 1600);
    } catch (error) {
      if (error.name === 'AbortError') return;
      setButton(button, 'Try again', false);
    }
  }, { signal: signal });

  return function () { controller.abort(); };
});
