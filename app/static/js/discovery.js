(() => {
  "use strict";

  const pendingForms = new WeakSet();
  let lastDialogTrigger = null;

  const cardFor = (element) => element.closest("[data-provider][data-provider-id]");

  function setError(card, message = "") {
    const error = card?.querySelector("[data-watchlist-error]");
    if (!error) return;
    error.textContent = message;
    error.hidden = !message;
  }

  function setStatus(card, message) {
    const status = card?.querySelector("[data-watchlist-status]");
    if (status) status.textContent = message;
  }

  function applyCheckboxes(form, data) {
    for (const name of [
      "watchlist_release_albums",
      "watchlist_release_singles",
      "watchlist_release_eps",
      "watchlist_monitor_upgrades",
    ]) {
      const input = form.elements.namedItem(name);
      if (input instanceof HTMLInputElement) input.checked = Boolean(data[name]);
    }
  }

  function showDialog(card, trigger) {
    const dialog = card?.querySelector("[data-watchlist-dialog]");
    if (!(dialog instanceof HTMLDialogElement) || typeof dialog.showModal !== "function") {
      return false;
    }
    lastDialogTrigger = trigger;
    if (!dialog.open) dialog.showModal();
    const firstInput = dialog.querySelector('input[type="checkbox"]');
    if (firstInput instanceof HTMLInputElement) firstInput.focus();
    return true;
  }

  async function jsonResponse(response) {
    let data = null;
    try {
      data = await response.json();
    } catch (_error) {
      data = null;
    }
    if (!response.ok) {
      throw new Error(data?.message || "Could not update this artist. Please try again.");
    }
    if (!data || typeof data.artist_id !== "number") {
      throw new Error("The server returned an invalid watchlist response.");
    }
    return data;
  }

  async function submitWatchlist(form) {
    if (pendingForms.has(form)) return;
    const card = cardFor(form);
    if (!card || card.dataset.watched === "true") return;
    const submit = form.querySelector("[data-watchlist-submit]");
    pendingForms.add(form);
    setError(card);
    setStatus(card, "Adding artist to watchlist…");
    if (submit instanceof HTMLButtonElement) {
      submit.disabled = true;
      submit.textContent = "Adding…";
    }

    try {
      const response = await fetch(form.action, {
        method: "POST",
        body: new FormData(form),
        headers: {Accept: "application/json", "X-Requested-With": "fetch"},
      });
      const data = await jsonResponse(response);
      card.dataset.watched = "true";
      setStatus(card, "Watched");
      if (submit instanceof HTMLButtonElement) submit.textContent = "Watched";

      const discography = card.querySelector("[data-discography-link]");
      if (discography instanceof HTMLAnchorElement) discography.href = data.discography_url;
      const configure = card.querySelector("[data-watchlist-configure]");
      if (configure instanceof HTMLAnchorElement) {
        configure.href = data.discography_url;
        configure.hidden = false;
      }
      const configForm = card.querySelector("[data-watchlist-config]");
      if (configForm instanceof HTMLFormElement) {
        configForm.action = data.configure_url;
        applyCheckboxes(configForm, data);
      }
      showDialog(card, submit);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Could not update this artist.";
      setError(card, message);
      setStatus(card, "Watchlist update failed");
      if (submit instanceof HTMLButtonElement) {
        submit.disabled = false;
        submit.textContent = "Add to watchlist";
      }
    } finally {
      pendingForms.delete(form);
    }
  }

  async function submitConfiguration(form) {
    if (pendingForms.has(form)) return;
    const card = cardFor(form);
    const save = form.querySelector("[data-watchlist-config-save]");
    pendingForms.add(form);
    setError(card);
    if (save instanceof HTMLButtonElement) save.disabled = true;

    try {
      const response = await fetch(form.action, {
        method: "POST",
        body: new FormData(form),
        headers: {Accept: "application/json", "X-Requested-With": "fetch"},
      });
      const data = await jsonResponse(response);
      applyCheckboxes(form, data);
      setStatus(card, "Watchlist settings saved");
      const dialog = form.closest("dialog");
      if (dialog instanceof HTMLDialogElement) dialog.close();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Could not save watchlist settings.";
      setError(card, message);
      setStatus(card, "Watchlist settings were not saved");
    } finally {
      pendingForms.delete(form);
      if (save instanceof HTMLButtonElement) save.disabled = false;
    }
  }

  document.addEventListener("submit", (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) return;
    if (form.matches("[data-watchlist-form]")) {
      event.preventDefault();
      void submitWatchlist(form);
    } else if (form.matches("[data-watchlist-config]")) {
      event.preventDefault();
      void submitConfiguration(form);
    }
  });

  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    const configure = target.closest("[data-watchlist-configure]");
    if (configure instanceof HTMLAnchorElement && showDialog(cardFor(configure), configure)) {
      event.preventDefault();
      return;
    }
    const close = target.closest("[data-watchlist-dialog-close]");
    if (close instanceof HTMLButtonElement) {
      const dialog = close.closest("dialog");
      if (dialog instanceof HTMLDialogElement) dialog.close();
    }
  });

  document.querySelectorAll("[data-watchlist-dialog]").forEach((dialog) => {
    dialog.addEventListener("close", () => {
      if (lastDialogTrigger instanceof HTMLElement && lastDialogTrigger.isConnected) {
        lastDialogTrigger.focus();
      }
      lastDialogTrigger = null;
    });
  });
})();
