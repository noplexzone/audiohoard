'use strict';

document.addEventListener('submit', function (event) {
  var form = event.target.closest('form[data-confirm]');
  if (form && !window.confirm(form.getAttribute('data-confirm'))) {
    event.preventDefault();
  }
});


document.addEventListener('submit', function (event) {
  var form = event.target.closest('form[data-download-form]');
  if (!form) {
    return;
  }
  event.preventDefault();
  var button = form.querySelector('button[type="submit"]');
  var original = button ? button.textContent : '';
  if (button) {
    button.disabled = true;
    button.textContent = 'Queueing…';
  }
  window.fetch(form.action, {
    method: 'POST',
    body: new FormData(form),
    credentials: 'same-origin',
    headers: { 'X-Requested-With': 'fetch' },
  }).then(function (response) {
    if (!response.ok) {
      throw new Error('Download request failed');
    }
    return response.json();
  }).then(function (data) {
    if (button) {
      button.textContent = data.queued > 0 ? 'Queued' : 'Nothing to queue';
    }
    if (form.hasAttribute('data-album-download')) {
      var progressLine = document.querySelector('.release-progress span');
      if (progressLine && data.queued > 0) {
        progressLine.textContent = progressLine.textContent.replace(/ · queued.*$/, '') + ' · queued ' + data.queued;
      }
    }
    window.setTimeout(function () {
      if (button) {
        button.disabled = false;
        button.textContent = original;
      }
    }, 1600);
  }).catch(function () {
    if (button) {
      button.disabled = false;
      button.textContent = 'Try again';
    }
  });
});
