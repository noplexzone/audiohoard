'use strict';

(function () {
  var SWIPE_THRESHOLD = 72;
  var SWIPE_INTENT_THRESHOLD = 10;
  var SWIPE_DIRECTION_THRESHOLD = 18;
  var SWIPE_HORIZONTAL_INTENT_RATIO = 1.5;
  var INTERACTIVE_SELECTOR = 'a, button, input, select, textarea, audio, video, label, summary, [contenteditable="true"]';

  function initializeReviewDeck(root) {
    var deck = root.querySelector('[data-review-deck]');
    if (!deck) return;

    var controller = new AbortController();
    var signal = controller.signal;
    var downloaded = deck.querySelector('[data-downloaded-audio]');
    var reference = deck.querySelector('[data-reference-audio]');
    var approve = deck.querySelector('[data-approve-button]');
    var deny = deck.querySelector('[data-deny-button]');
    var forms = Array.from(deck.querySelectorAll('[data-review-action]'));
    var working = false;
    var swipe = null;
    var touchIdentifier = null;

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

    document.addEventListener('keydown', function (event) {
      var target = event.target;
      if (target instanceof Element && target.matches('input, textarea, select, [contenteditable="true"]')) return;
      if (event.altKey || event.ctrlKey || event.metaKey) return;

      if (event.key === 'ArrowRight') {
        event.preventDefault();
        submit(approve);
      } else if (event.key === 'ArrowLeft') {
        event.preventDefault();
        submit(deny);
      } else if (event.key === ' ') {
        event.preventDefault();
        toggle(downloaded);
      } else if (event.key.toLowerCase() === 'r') {
        event.preventDefault();
        toggle(reference);
      }
    }, { signal: signal });

    return function () { controller.abort(); };
  }

  if (window.AudiohoardNavigation) {
    window.AudiohoardNavigation.registerPage('review-deck', initializeReviewDeck);
  } else {
    document.addEventListener('DOMContentLoaded', function () {
      initializeReviewDeck(document);
    }, { once: true });
  }
}());
