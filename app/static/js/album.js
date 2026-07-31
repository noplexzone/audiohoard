'use strict';

window.AudiohoardNavigation.registerPage('album', function (region) {
  var controller = new AbortController();
  var signal = controller.signal;

  region.addEventListener('submit', function (event) {
    var confirmForm = event.target.closest('form[data-confirm]');
    if (confirmForm && !window.confirm(confirmForm.getAttribute('data-confirm'))) {
      event.preventDefault();
      return;
    }
    var form = event.target.closest('form[data-download-form]');
    if (!form) return;
    event.preventDefault();
    var button = form.querySelector('button[type="submit"]');
    var original = button ? button.textContent : '';
    if (button) { button.disabled = true; button.textContent = 'Queueing…'; }
    window.fetch(form.action, {
      method: 'POST', body: new FormData(form), credentials: 'same-origin',
      headers: { 'X-Requested-With': 'fetch' }, signal: signal,
    }).then(function (response) {
      if (!response.ok) throw new Error('Download request failed');
      return response.json();
    }).then(function (data) {
      if (button) button.textContent = data.queued > 0 ? 'Queued' : 'Nothing to queue';
      if (form.hasAttribute('data-album-download')) {
        var progressLine = region.querySelector('.release-progress span');
        if (progressLine && data.queued > 0) {
          progressLine.textContent = progressLine.textContent.replace(/ · queued.*$/, '') + ' · queued ' + data.queued;
        }
      }
      window.setTimeout(function () {
        if (button && button.isConnected) { button.disabled = false; button.textContent = original; }
      }, 1600);
    }).catch(function (error) {
      if (error.name === 'AbortError') return;
      if (button) { button.disabled = false; button.textContent = 'Try again'; }
    });
  }, { signal: signal });

  return function () { controller.abort(); };
});
