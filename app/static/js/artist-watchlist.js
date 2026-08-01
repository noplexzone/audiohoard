'use strict';

window.AudiohoardNavigation.registerPage('artist-watchlist', function (region) {
  var controller = new AbortController();
  var signal = controller.signal;
  var discographyTimer = null;

  function submitForm(form, status) {
    if (!form || form.dataset.submitting === 'true') return;
    form.dataset.submitting = 'true';
    if (status) status.textContent = 'Saving…';
    if (form.requestSubmit) {
      form.requestSubmit();
    } else {
      form.submit();
    }
  }

  var dialog = region.querySelector('[data-watchlist-dialog]');
  region.querySelectorAll('[data-open-watchlist]').forEach(function (button) {
    button.addEventListener('click', function () {
      if (!dialog) return;
      if (dialog.showModal) dialog.showModal();
      else dialog.setAttribute('open', '');
    }, { signal: signal });
  });
  if (dialog) {
    dialog.addEventListener('click', function (event) {
      if (event.target === dialog) dialog.close();
    }, { signal: signal });
  }

  region.querySelectorAll('[data-auto-submit-form]').forEach(function (form) {
    var status = form.querySelector('[data-save-status]');
    var controls = Array.from(form.querySelectorAll('select, input[type="checkbox"]'));
    controls = controls.concat(Array.from(region.querySelectorAll('input[form="' + form.id + '"]')));
    controls.forEach(function (control) {
      control.addEventListener('change', function () { submitForm(form, status); }, { signal: signal });
    });
  });

  region.querySelectorAll('[data-primary-source-select]').forEach(function (select) {
    select.addEventListener('change', function () {
      if (select.form && select.form.requestSubmit) select.form.requestSubmit();
    }, { signal: signal });
  });
  region.querySelectorAll('.dismiss-btn').forEach(function (button) {
    button.addEventListener('click', function () { button.closest('[role="alert"]')?.remove(); }, { signal: signal });
  });

  var discography = region.querySelector('#discography-region');
  if (discography && discography.dataset.artistRefresh === 'true') {
    var artistId = discography.dataset.artistId;
    var pollDiscography = async function () {
      if (document.hidden) return;
      try {
        var stateResponse = await window.fetch('/artists/catalog/' + artistId + '/state', {
          credentials: 'same-origin', signal: signal,
        });
        if (!stateResponse.ok) return;
        var state = await stateResponse.json();
        if (state.enrichment_state === 'queued' || state.enrichment_state === 'running') return;
        var pageResponse = await window.fetch(window.location.href, { credentials: 'same-origin', signal: signal });
        if (!pageResponse.ok) return;
        var html = await pageResponse.text();
        var fresh = new DOMParser().parseFromString(html, 'text/html').getElementById('discography-region');
        if (fresh && discography.isConnected) {
          discography.innerHTML = fresh.innerHTML;
          discography.dataset.artistRefresh = fresh.dataset.artistRefresh || 'false';
        }
        if (discographyTimer) { window.clearInterval(discographyTimer); discographyTimer = null; }
      } catch (error) {
        if (error.name === 'AbortError') return;
      }
    };
    discographyTimer = window.setInterval(pollDiscography, 5000);
  }

  region.addEventListener('submit', async function (event) {
    var form = event.target.closest('form[data-download-form]');
    if (!form) return;
    event.preventDefault();
    var button = form.querySelector('button[type="submit"]');
    var original = button ? button.textContent : '';
    if (button) { button.disabled = true; button.textContent = 'Queueing…'; }
    try {
      var response = await window.fetch(form.action, {
        method: 'POST', body: new FormData(form), credentials: 'same-origin',
        headers: { 'X-Requested-With': 'fetch' }, signal: signal,
      });
      if (!response.ok) throw new Error('Download request failed');
      var data = await response.json();
      if (button) button.textContent = data.queued > 0 ? 'Queued' : 'Nothing to queue';
      window.setTimeout(function () {
        if (button && button.isConnected) { button.disabled = false; button.textContent = original; }
      }, 1600);
    } catch (error) {
      if (error.name === 'AbortError') return;
      if (button) { button.disabled = false; button.textContent = 'Try again'; }
    }
  }, { signal: signal });

  return function () {
    controller.abort();
    if (discographyTimer) window.clearInterval(discographyTimer);
  };
});
