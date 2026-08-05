'use strict';

(function () {
  var SWIPE_THRESHOLD = 72;
  var SWIPE_INTENT_THRESHOLD = 10;
  var SWIPE_DIRECTION_THRESHOLD = 18;
  var SWIPE_HORIZONTAL_INTENT_RATIO = 1.5;
  var INTERACTIVE_SELECTOR = 'a, button, input, select, textarea, audio, video, label, summary, [contenteditable="true"]';
  var DOWNLOADED_VOLUME_KEY = 'audiohoard.importReview.downloadedVolume';
  var REFERENCE_VOLUME_KEY = 'audiohoard.importReview.referenceVolume';

  function initializeReviewDeck(root) {
    var deck = root.querySelector('[data-review-deck]');
    if (!deck) return;

    var controller = new AbortController();
    var signal = controller.signal;
    var downloaded = deck.querySelector('[data-downloaded-audio]');
    var reference = deck.querySelector('[data-reference-audio]');
    var approve = deck.querySelector('[data-approve-button]');
    var deny = deck.querySelector('[data-deny-button]');
    var skip = deck.querySelector('[data-skip-button]');
    var forms = Array.from(deck.querySelectorAll('[data-review-action]'));
    var matchSection = deck.querySelector('[data-match-section]');
    var abToggle = deck.querySelector('[data-ab-toggle]');
    var alignmentStatus = deck.querySelector('[data-alignment-status]');
    var nudgeButtons = Array.from(deck.querySelectorAll('[data-alignment-nudge]'));
    var alignmentOffset = null;
    var matching = false;
    var playbackStarted = false;
    var working = false;
    var swipe = null;
    var touchIdentifier = null;

    function preferredVolume(storageKey) {
      try {
        var stored = window.localStorage.getItem(storageKey);
        if (stored === null) return 1;
        var parsed = Number(stored);
        if (!Number.isFinite(parsed)) return 1;
        return Math.min(1, Math.max(0, parsed));
      } catch (_error) {
        return 1;
      }
    }

    function persistVolume(storageKey, value) {
      try {
        window.localStorage.setItem(storageKey, String(value));
      } catch (_error) {
        // Storage can be unavailable in privacy-restricted browser contexts.
      }
    }

    function bindVolumePreference(player, storageKey) {
      if (!player) return;
      player.volume = preferredVolume(storageKey);
      player.addEventListener('volumechange', function () {
        persistVolume(storageKey, player.volume);
      }, { signal: signal });
    }

    function toggle(player) {
      if (!player || player.getAttribute('aria-disabled') === 'true') return;
      if (player.paused) void player.play();
      else player.pause();
    }

    function submit(button) {
      if (working || !button) return;
      var form = button.closest('form');
      if (form) form.requestSubmit(button);
    }

    function clampTime(player, value) {
      var maximum = Number.isFinite(player.duration) && player.duration > 0 ? player.duration : Number.POSITIVE_INFINITY;
      return Math.max(0, Math.min(maximum, value));
    }

    function seekPlayer(player, value) {
      if (!player || !Number.isFinite(value)) return;
      var apply = function () { player.currentTime = clampTime(player, value); };
      if (player.readyState >= 1) apply();
      else player.addEventListener('loadedmetadata', apply, { once: true, signal: signal });
    }

    function setAlignmentState(payload, seekDownloaded) {
      var offset = Number(payload.downloaded_offset_sec);
      if (!Number.isFinite(offset) || offset < 0) throw new Error('Invalid alignment response');
      alignmentOffset = offset;
      if (seekDownloaded !== false) seekPlayer(downloaded, alignmentOffset);
      var linkedPlayback = payload.linked_playback === true;
      if (abToggle) abToggle.disabled = !linkedPlayback;
      nudgeButtons.forEach(function (button) { button.disabled = !linkedPlayback; });
      var label = payload.status === 'matched' ? 'Matched' : 'Estimated';
      alignmentStatus.textContent = label + ' downloaded start at ' + alignmentOffset.toFixed(1) + 's. ' + payload.message;
      alignmentStatus.dataset.alignmentState = payload.status;
    }

    function matchReferenceSection(automatic) {
      if (matching || !matchSection || !deck.dataset.alignmentUrl) return;
      matching = true;
      matchSection.disabled = true;
      matchSection.textContent = 'Matching…';
      alignmentStatus.textContent = 'Analyzing the reference and downloaded file…';
      var alignmentUrl = new URL(deck.dataset.alignmentUrl, window.location.href);
      if (deck.dataset.referenceSource) alignmentUrl.searchParams.set('reference_source', deck.dataset.referenceSource);
      if (deck.dataset.referenceUrl) alignmentUrl.searchParams.set('reference_url', deck.dataset.referenceUrl);
      fetch(alignmentUrl, { headers: { Accept: 'application/json' }, signal: signal })
        .then(function (response) {
          if (!response.ok) throw new Error('Alignment request failed');
          return response.json();
        })
        .then(function (payload) {
          if (payload.status === 'unavailable') throw new Error(payload.message || 'No reliable match was found');
          setAlignmentState(payload, !automatic || !playbackStarted);
          if (!automatic) {
            downloaded.pause();
            reference.pause();
          }
        })
        .catch(function (error) {
          alignmentStatus.textContent = error.message || 'The reference could not be matched.';
          alignmentStatus.dataset.alignmentState = 'unavailable';
        })
        .finally(function () {
          matching = false;
          matchSection.disabled = false;
          matchSection.textContent = 'Match section';
        });
    }

    function switchAB() {
      if (!Number.isFinite(alignmentOffset) || !downloaded || !reference) return;
      if (!downloaded.paused) {
        var referenceTime = clampTime(reference, downloaded.currentTime - alignmentOffset);
        downloaded.pause();
        seekPlayer(reference, referenceTime);
        void reference.play();
        alignmentStatus.textContent = 'Playing reference at the equivalent passage.';
      } else {
        var downloadedTime = clampTime(downloaded, reference.currentTime + alignmentOffset);
        reference.pause();
        seekPlayer(downloaded, downloadedTime);
        void downloaded.play();
        alignmentStatus.textContent = 'Playing downloaded file at the equivalent passage.';
      }
    }

    function clearSwipe() {
      swipe = null;
      touchIdentifier = null;
      deck.classList.remove('is-swiping', 'swipe-approve', 'swipe-deny');
    }

    function ignoresSwipe(target) {
      return target instanceof Element && Boolean(target.closest(INTERACTIVE_SELECTOR));
    }

    function beginSwipe(identifier, clientX, clientY, target) {
      if (working || ignoresSwipe(target)) return false;
      swipe = {
        identifier: identifier,
        startX: clientX,
        startY: clientY,
        deltaX: 0,
        deltaY: 0,
        horizontal: false
      };
      return true;
    }

    function moveSwipe(identifier, clientX, clientY, event) {
      if (!swipe || identifier !== swipe.identifier) return;
      var deltaX = clientX - swipe.startX;
      var deltaY = clientY - swipe.startY;
      var absoluteX = Math.abs(deltaX);
      var absoluteY = Math.abs(deltaY);

      if (!swipe.horizontal) {
        if (Math.max(absoluteX, absoluteY) < SWIPE_INTENT_THRESHOLD) return;
        if (absoluteY >= absoluteX) {
          clearSwipe();
          return;
        }
        if (absoluteX < absoluteY * SWIPE_HORIZONTAL_INTENT_RATIO) return;
        swipe.horizontal = true;
        deck.classList.add('is-swiping');
      } else if (absoluteY >= absoluteX) {
        clearSwipe();
        return;
      }

      swipe.deltaX = deltaX;
      swipe.deltaY = deltaY;
      event.preventDefault();
      deck.classList.toggle('swipe-approve', deltaX >= SWIPE_DIRECTION_THRESHOLD);
      deck.classList.toggle('swipe-deny', deltaX <= -SWIPE_DIRECTION_THRESHOLD);
    }

    function finishSwipe(identifier, clientX, clientY) {
      if (!swipe || identifier !== swipe.identifier) return;
      if (Number.isFinite(clientX)) swipe.deltaX = clientX - swipe.startX;
      if (Number.isFinite(clientY)) swipe.deltaY = clientY - swipe.startY;
      var deltaX = swipe.deltaX;
      var deltaY = swipe.deltaY;
      var horizontal = swipe.horizontal;
      clearSwipe();
      if (!horizontal || Math.abs(deltaY) >= Math.abs(deltaX)) return;
      if (deltaX >= SWIPE_THRESHOLD) submit(approve);
      else if (deltaX <= -SWIPE_THRESHOLD) submit(deny);
    }

    function findTouch(touchList, identifier) {
      return Array.from(touchList).find(function (touch) {
        return touch.identifier === identifier;
      });
    }

    deck.addEventListener('touchstart', function (event) {
      if (event.touches.length !== 1) {
        clearSwipe();
        return;
      }
      var touch = event.changedTouches[0];
      if (touch && beginSwipe(touch.identifier, touch.clientX, touch.clientY, event.target)) {
        touchIdentifier = touch.identifier;
      }
    }, { signal: signal, passive: true });

    deck.addEventListener('touchmove', function (event) {
      if (touchIdentifier === null) return;
      var touch = findTouch(event.touches, touchIdentifier);
      if (touch) moveSwipe(touchIdentifier, touch.clientX, touch.clientY, event);
    }, { signal: signal, passive: false });

    deck.addEventListener('touchend', function (event) {
      if (touchIdentifier === null) return;
      var touch = findTouch(event.changedTouches, touchIdentifier);
      if (touch) finishSwipe(touchIdentifier, touch.clientX, touch.clientY);
    }, { signal: signal, passive: true });

    deck.addEventListener('touchcancel', clearSwipe, { signal: signal, passive: true });

    deck.addEventListener('pointerdown', function (event) {
      if (event.pointerType !== 'pen' || !event.isPrimary) return;
      beginSwipe(event.pointerId, event.clientX, event.clientY, event.target);
    }, { signal: signal });

    deck.addEventListener('pointermove', function (event) {
      if (event.pointerType !== 'pen') return;
      moveSwipe(event.pointerId, event.clientX, event.clientY, event);
    }, { signal: signal });

    deck.addEventListener('pointerup', function (event) {
      if (event.pointerType !== 'pen') return;
      finishSwipe(event.pointerId, event.clientX, event.clientY);
    }, { signal: signal });

    deck.addEventListener('pointercancel', clearSwipe, { signal: signal });

    bindVolumePreference(downloaded, DOWNLOADED_VOLUME_KEY);
    bindVolumePreference(reference, REFERENCE_VOLUME_KEY);

    if (downloaded && reference) {
      downloaded.addEventListener('play', function () {
        playbackStarted = true;
        reference.pause();
      }, { signal: signal });
      reference.addEventListener('play', function () {
        playbackStarted = true;
        downloaded.pause();
      }, { signal: signal });
    }

    if (skip) {
      skip.addEventListener('click', function () {
        if (downloaded) downloaded.pause();
        if (reference) reference.pause();
      }, { signal: signal });
    }

    if (matchSection) matchSection.addEventListener('click', function () { matchReferenceSection(false); }, { signal: signal });
    if (abToggle) abToggle.addEventListener('click', switchAB, { signal: signal });
    nudgeButtons.forEach(function (button) {
      button.addEventListener('click', function () {
        if (!Number.isFinite(alignmentOffset)) return;
        alignmentOffset = Math.max(0, alignmentOffset + Number(button.dataset.alignmentNudge || 0));
        if (reference.paused) seekPlayer(downloaded, alignmentOffset + reference.currentTime);
        alignmentStatus.textContent = 'Adjusted downloaded start to ' + alignmentOffset.toFixed(1) + 's.';
      }, { signal: signal });
    });

    forms.forEach(function (form) {
      form.addEventListener('submit', function (event) {
        if (working) {
          event.preventDefault();
          return;
        }
        if (form.dataset.confirm && !window.confirm(form.dataset.confirm)) {
          event.preventDefault();
          return;
        }
        working = true;
        clearSwipe();
        [approve, deny].forEach(function (button) {
          if (button) button.disabled = true;
        });
        var active = event.submitter || form.querySelector("button[type='submit']");
        if (active) active.textContent = 'Working…';
      }, { signal: signal });
    });

    if (matchSection && deck.dataset.alignmentUrl) {
      matchReferenceSection(true);
    }

    document.addEventListener('keydown', function (event) {
      var target = event.target;
      if (target instanceof Element && target.closest(INTERACTIVE_SELECTOR)) return;
      if (event.altKey || event.ctrlKey || event.metaKey) return;

      if (event.key === 'ArrowRight') {
        event.preventDefault();
        submit(approve);
      } else if (event.key === 'ArrowLeft') {
        event.preventDefault();
        submit(deny);
      } else if (event.key.toLowerCase() === 'n' && skip) {
        event.preventDefault();
        skip.click();
      } else if (event.key === ' ') {
        event.preventDefault();
        toggle(downloaded);
      } else if (event.key.toLowerCase() === 'r') {
        event.preventDefault();
        toggle(reference);
      }
    }, { signal: signal });

    return function () {
      if (downloaded) downloaded.pause();
      if (reference) reference.pause();
      controller.abort();
    };
  }

  if (window.AudiohoardNavigation) {
    window.AudiohoardNavigation.registerPage('review-deck', initializeReviewDeck);
  } else {
    document.addEventListener('DOMContentLoaded', function () {
      initializeReviewDeck(document);
    }, { once: true });
  }
}());
