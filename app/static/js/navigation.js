'use strict';

(function () {
  var shell = document.querySelector('[data-progressive-shell]');
  if (!shell) return;

  var controller = null;
  var loadedScripts = new Set();
  var pageModules = new Map();
  var pageCleanups = [];
  var scrollFrame = null;
  var navigationStatus = document.createElement('p');
  navigationStatus.className = 'sr-only';
  navigationStatus.setAttribute('aria-live', 'polite');
  navigationStatus.setAttribute('aria-atomic', 'true');
  shell.append(navigationStatus);

  window.AudiohoardNavigation = {
    registerPage: function (name, initializer) {
      pageModules.set(name, initializer);
    },
    refresh: function () {
      return navigate(new URL(window.location.href), { replace: true, preserveScroll: true });
    },
  };

  function currentRegion() {
    return document.querySelector('[data-page-region]');
  }

  function disposePage() {
    document.dispatchEvent(new CustomEvent('audiohoard:page-dispose'));
    pageCleanups.splice(0).forEach(function (cleanup) {
      try { cleanup(); } catch (_error) { /* cleanup must not block navigation */ }
    });
  }

  function moduleScripts(root) {
    return Array.from(root.querySelectorAll('script[data-page-module][src]'));
  }

  function canonicalScript(source) {
    var url = new URL(source, window.location.href);
    return url.origin + url.pathname;
  }

  function loadScript(script) {
    var key = canonicalScript(script.src);
    if (loadedScripts.has(key)) return Promise.resolve();
    loadedScripts.add(key);
    return new Promise(function (resolve, reject) {
      var element = document.createElement('script');
      element.src = script.src;
      element.defer = true;
      element.addEventListener('load', resolve, { once: true });
      element.addEventListener('error', reject, { once: true });
      document.head.append(element);
    });
  }

  async function initializePage(region, scriptRoot) {
    var scripts = moduleScripts(scriptRoot || region);
    await Promise.all(scripts.map(loadScript));
    var initialized = new Set();
    scripts.forEach(function (script) {
      var name = script.dataset.pageModule;
      if (!name || initialized.has(name)) return;
      initialized.add(name);
      var initializer = pageModules.get(name);
      if (initializer) {
        var cleanup = initializer(region);
        if (typeof cleanup === 'function') pageCleanups.push(cleanup);
      }
    });
    document.dispatchEvent(new CustomEvent('audiohoard:page-init', { detail: { region: region } }));
  }

  function synchronizeNavigation(freshDocument) {
    ['.primary-nav', '.utility-nav', '.mobile-nav'].forEach(function (selector) {
      var current = document.querySelector(selector);
      var fresh = freshDocument.querySelector(selector);
      if (!current || !fresh) return;
      var freshLinks = Array.from(fresh.querySelectorAll('a[href]'));
      current.querySelectorAll('a[href]').forEach(function (link) {
        var match = freshLinks.find(function (candidate) {
          return candidate.getAttribute('href') === link.getAttribute('href');
        });
        link.classList.toggle('active', Boolean(match && match.classList.contains('active')));
        if (match && match.hasAttribute('aria-current')) {
          link.setAttribute('aria-current', match.getAttribute('aria-current'));
        } else {
          link.removeAttribute('aria-current');
        }
        var badge = link.querySelector('.nav-badge');
        var freshBadge = match ? match.querySelector('.nav-badge') : null;
        if (freshBadge) {
          if (!badge) {
            badge = freshBadge.cloneNode(true);
            link.append(badge);
          } else {
            badge.textContent = freshBadge.textContent;
          }
        } else if (badge) {
          badge.remove();
        }
      });
    });
  }

  function hardNavigate(url) {
    window.location.assign(url.href || String(url));
  }

  function captureScrollState() {
    var state = Object.assign({}, history.state || {}, {
      audiohoard: true,
      scrollX: window.scrollX,
      scrollY: window.scrollY,
    });
    history.replaceState(state, '', window.location.href);
  }

  function focusTarget(target) {
    var hadTabindex = target.hasAttribute('tabindex');
    if (!hadTabindex) target.setAttribute('tabindex', '-1');
    target.focus({ preventScroll: true });
    if (!hadTabindex) {
      target.addEventListener('blur', function () { target.removeAttribute('tabindex'); }, { once: true });
    }
  }

  function restoreDestination(url, options, region) {
    var opts = options || {};
    if (opts.preserveScroll) {
      window.scrollTo({ left: opts.scrollX, top: opts.scrollY, behavior: 'auto' });
      return;
    }
    if (opts.popstate && opts.state && Number.isFinite(opts.state.scrollY)) {
      window.scrollTo({
        left: Number.isFinite(opts.state.scrollX) ? opts.state.scrollX : 0,
        top: opts.state.scrollY,
        behavior: 'auto',
      });
      return;
    }
    if (url.hash) {
      var fragment;
      try { fragment = decodeURIComponent(url.hash.slice(1)); } catch (_error) { fragment = url.hash.slice(1); }
      var target = document.getElementById(fragment) || document.getElementsByName(fragment)[0];
      if (target) {
        target.scrollIntoView({ block: 'start', behavior: 'auto' });
        focusTarget(target);
        return;
      }
    }
    window.scrollTo({ top: 0, left: 0, behavior: 'auto' });
    region.focus({ preventScroll: true });
  }

  async function navigate(url, options) {
    var opts = options || {};
    var preservedX = window.scrollX;
    var preservedY = window.scrollY;
    if (!opts.popstate && !opts.replace) captureScrollState();
    if (controller) controller.abort();
    controller = new AbortController();
    var activeController = controller;
    shell.classList.add('is-navigating');

    try {
      var response = await window.fetch(url.href, {
        method: 'GET',
        credentials: 'same-origin',
        headers: { 'Accept': 'text/html', 'X-Requested-With': 'progressive-navigation' },
        signal: activeController.signal,
      });
      var contentType = response.headers.get('content-type') || '';
      if (!response.ok || !contentType.toLowerCase().includes('text/html')) {
        throw new Error('Navigation did not return an HTML page');
      }
      var html = await response.text();
      var freshDocument = new DOMParser().parseFromString(html, 'text/html');
      var freshShell = freshDocument.querySelector('[data-progressive-shell]');
      var freshRegion = freshShell && freshShell.querySelector('[data-page-region]');
      if (!freshRegion || !freshDocument.title) {
        throw new Error('Navigation response does not contain the application shell');
      }
      if (activeController !== controller) return;

      disposePage();
      var region = currentRegion();
      region.replaceChildren.apply(region, Array.from(freshRegion.childNodes));
      document.title = freshDocument.title;
      synchronizeNavigation(freshDocument);
      var finalUrl = new URL(response.url || url.href);
      finalUrl.hash = url.hash;
      if (opts.replace) {
        history.replaceState(history.state || { audiohoard: true }, '', finalUrl.href);
      } else if (!opts.popstate) {
        history.pushState({ audiohoard: true, scrollX: 0, scrollY: 0 }, '', finalUrl.href);
      }
      await initializePage(region, freshDocument);
      restoreDestination(finalUrl, Object.assign({}, opts, {
        scrollX: preservedX,
        scrollY: preservedY,
      }), region);
      navigationStatus.textContent = document.title + ' loaded';
    } catch (error) {
      if (error && error.name === 'AbortError') return;
      hardNavigate(url);
    } finally {
      if (activeController === controller) {
        controller = null;
        shell.classList.remove('is-navigating');
      }
    }
  }

  function eligibleUrl(rawUrl) {
    var url = new URL(rawUrl, window.location.href);
    if (url.origin !== window.location.origin) return null;
    if (!/^https?:$/.test(url.protocol)) return null;
    if (url.pathname.startsWith('/static/') || url.pathname.startsWith('/artwork')) return null;
    if (/\.(?:aac|flac|m4a|mp3|ogg|opus|wav|zip|pdf)$/i.test(url.pathname)) return null;
    return url;
  }


  function setSubmitButton(button, text, disabled) {
    if (!button) return;
    button.disabled = disabled;
    button.textContent = text;
  }

  function primarySubmitButton(form, event) {
    if (event && event.submitter && event.submitter.matches('button[type="submit"], input[type="submit"]')) {
      return event.submitter;
    }
    return form.querySelector('button[type="submit"], input[type="submit"]');
  }

  document.addEventListener('submit', async function (event) {
    if (event.defaultPrevented) return;
    var form = event.target.closest('form[data-download-form]');
    if (!form) return;
    event.preventDefault();
    var button = primarySubmitButton(form, event);
    var original = button ? button.textContent : '';
    setSubmitButton(button, 'Queueing…', true);
    try {
      var response = await window.fetch(form.action, {
        method: 'POST', body: new FormData(form), credentials: 'same-origin',
        headers: { 'Accept': 'application/json', 'X-Requested-With': 'fetch' },
      });
      if (!response.ok) throw new Error('Download request failed');
      var data = await response.json();
      setSubmitButton(button, data.queued > 0 ? 'Queued' : 'Nothing to queue', true);
      window.setTimeout(function () {
        if (button && button.isConnected) setSubmitButton(button, original, false);
      }, 1600);
    } catch (error) {
      setSubmitButton(button, 'Try again', false);
    }
  });

  document.addEventListener('click', function (event) {
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey ||
        event.shiftKey || event.altKey) return;
    var anchor = event.target.closest('a[href]');
    if (!anchor || anchor.download || (anchor.target && anchor.target !== '_self') ||
        anchor.hasAttribute('data-native-navigation') ||
        /(?:^|\s)external(?:\s|$)/i.test(anchor.rel || '')) return;
    var url = eligibleUrl(anchor.href);
    if (!url || (url.pathname === window.location.pathname && url.search === window.location.search && url.hash)) return;
    event.preventDefault();
    navigate(url);
  });

  window.addEventListener('popstate', function (event) {
    var url = eligibleUrl(window.location.href);
    if (url) navigate(url, { popstate: true, state: event.state || {} });
  });

  window.addEventListener('scroll', function () {
    if (scrollFrame !== null) return;
    scrollFrame = window.requestAnimationFrame(function () {
      scrollFrame = null;
      if (!controller) captureScrollState();
    });
  }, { passive: true });

  document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('script[src]').forEach(function (script) {
      loadedScripts.add(canonicalScript(script.src));
    });
    if ('scrollRestoration' in history) history.scrollRestoration = 'manual';
    history.replaceState(Object.assign({}, history.state || {}, {
      audiohoard: true,
      scrollX: window.scrollX,
      scrollY: window.scrollY,
    }), '', window.location.href);
    initializePage(currentRegion(), document);
  }, { once: true });
}());
