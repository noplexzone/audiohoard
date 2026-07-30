(() => {
  const inputs = Array.from(document.querySelectorAll('input[form="monitor-form"]'));
  const initialState = new Map(inputs.map((input) => [input, input.checked]));
  const bar = document.createElement("div");
  const count = document.createElement("span");
  const save = document.createElement("button");
  bar.className = "unsaved-bar";
  bar.hidden = true;
  bar.setAttribute("role", "status");
  save.type = "submit";
  save.className = "btn";
  save.setAttribute("form", "monitor-form");
  save.textContent = "Save";
  bar.append(count, save);
  document.body.append(bar);

  const updateBar = () => {
    const unsaved = inputs.filter((input) => input.checked !== initialState.get(input)).length;
    count.textContent = `${unsaved} unsaved selection${unsaved === 1 ? "" : "s"}`;
    bar.hidden = unsaved === 0;
  };

  inputs.forEach((input) => input.addEventListener("change", updateBar));
  document.querySelectorAll(".dismiss-btn").forEach((button) => {
    button.addEventListener("click", () => button.closest('[role="alert"]')?.remove());
  });



  const region = document.getElementById('discography-region');
  if (region && region.dataset.artistRefresh === 'true') {
    const artistId = region.dataset.artistId;
    const pollDiscography = async () => {
      if (document.hidden) return;
      try {
        const stateResponse = await window.fetch(`/artists/catalog/${artistId}/state`, { credentials: 'same-origin' });
        if (!stateResponse.ok) return;
        const state = await stateResponse.json();
        if (state.enrichment_state === 'queued' || state.enrichment_state === 'running') return;
        const pageResponse = await window.fetch(window.location.href, { credentials: 'same-origin' });
        if (!pageResponse.ok) return;
        const html = await pageResponse.text();
        const doc = new DOMParser().parseFromString(html, 'text/html');
        const fresh = doc.getElementById('discography-region');
        if (fresh) {
          region.innerHTML = fresh.innerHTML;
          region.dataset.artistRefresh = fresh.dataset.artistRefresh || 'false';
        }
        window.clearInterval(discographyTimer);
      } catch (_error) {
        // Provider/background timing can race the first page. Keep polling.
      }
    };
    const discographyTimer = window.setInterval(pollDiscography, 5000);
  }

  document.querySelectorAll('form[data-download-form]').forEach((form) => {
    form.addEventListener('submit', async (event) => {
      event.preventDefault();
      const button = form.querySelector('button[type="submit"]');
      const original = button?.textContent ?? '';
      if (button) {
        button.disabled = true;
        button.textContent = 'Queueing…';
      }
      try {
        const response = await window.fetch(form.action, {
          method: 'POST',
          body: new FormData(form),
          credentials: 'same-origin',
          headers: { 'X-Requested-With': 'fetch' },
        });
        if (!response.ok) {
          throw new Error('Download request failed');
        }
        const data = await response.json();
        if (button) {
          button.textContent = data.queued > 0 ? 'Queued' : 'Nothing to queue';
        }
        window.setTimeout(() => {
          if (button) {
            button.disabled = false;
            button.textContent = original;
          }
        }, 1600);
      } catch (_error) {
        if (button) {
          button.disabled = false;
          button.textContent = 'Try again';
        }
      }
    });
  });
})();
