(() => {
  "use strict";

  const pendingForms = new WeakSet();
  let lastDialogTrigger = null;

  function bindDialogs(root) {
    root.querySelectorAll("[data-watchlist-dialog]").forEach((dialog) => {
      if (dialog.dataset.discoveryBound === "true") return;
      dialog.dataset.discoveryBound = "true";
      dialog.addEventListener("close", () => {
        if (lastDialogTrigger instanceof HTMLElement && lastDialogTrigger.isConnected) {
          lastDialogTrigger.focus();
        }
        lastDialogTrigger = null;
      });
    });
  }

  function showFragmentError(container) {
    container.dataset.discoverState = "error";
    const body = container.querySelector("[data-discover-body]");
    if (!body) return;
    const alert = document.createElement("div");
    alert.className = "alert error";
    alert.setAttribute("role", "alert");
    const message = document.createElement("p");
    message.textContent = "This discovery feed could not be loaded.";
    const retry = document.createElement("a");
    retry.className = "btn secondary";
    retry.href = `/search#${container.id}`;
    retry.dataset.discoverRetry = "";
    retry.textContent = "Retry this section";
    alert.append(message, retry);
    body.replaceChildren(alert);
  }

  function initializeDiscovery(root) {
    const controller = new AbortController();
    const signal = controller.signal;

    const loadPending = () => {
      if (document.hidden || signal.aborted) return;
      root.querySelectorAll("[data-discover-fragment-url]").forEach((container) => {
        if (container.dataset.discoverState !== "pending" ||
            container.dataset.discoverRequested === "true") return;
        container.dataset.discoverRequested = "true";
        void fetch(container.dataset.discoverFragmentUrl, {
          method: "GET",
          credentials: "same-origin",
          headers: {Accept: "text/html", "X-Requested-With": "discover-fragment"},
          signal,
        }).then(async (response) => {
          if (!response.ok) throw new Error("fragment request failed");
          const contentType = response.headers.get("content-type")?.split(";", 1)[0].trim().toLowerCase();
          if (contentType !== "text/html") throw new Error("invalid fragment content type");
          const documentFragment = new DOMParser().parseFromString(await response.text(), "text/html");
          const fresh = documentFragment.querySelector("[data-discover-section]");
          const expectedUrl = container.dataset.discoverFragmentUrl;
          if (!fresh ||
              !["pending", "ready", "stale", "error"].includes(fresh.dataset.discoverState) ||
              fresh.id !== container.id ||
              fresh.dataset.discoverFragmentUrl !== expectedUrl) {
            throw new Error("invalid fragment response");
          }
          container.replaceWith(fresh);
          bindDialogs(fresh);
        }).catch((error) => {
          if (error?.name !== "AbortError") showFragmentError(container);
        });
      });
    };

    root.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const retry = target.closest("[data-discover-retry]");
      if (!(retry instanceof HTMLAnchorElement)) return;
      const container = retry.closest("[data-discover-section]");
      if (!(container instanceof HTMLElement)) return;
      event.preventDefault();
      container.dataset.discoverState = "pending";
      delete container.dataset.discoverRequested;
      const body = container.querySelector("[data-discover-body]");
      if (body) {
        const status = document.createElement("div");
        status.className = "empty-state";
        status.setAttribute("role", "status");
        status.textContent = "Retrying discovery feed…";
        body.replaceChildren(status);
      }
      loadPending();
    }, {signal});

    bindDialogs(root);
    loadPending();
    document.addEventListener("visibilitychange", loadPending, {signal});
    document.addEventListener("audiohoard:page-dispose", () => controller.abort(), {
      signal,
      once: true,
    });
    window.addEventListener("pagehide", () => controller.abort(), {signal, once: true});
    return () => controller.abort();
  }

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

  if (window.AudiohoardNavigation) {
    window.AudiohoardNavigation.registerPage("discovery", initializeDiscovery);
  } else {
    document.addEventListener("DOMContentLoaded", () => initializeDiscovery(document), {once: true});
  }
})();
