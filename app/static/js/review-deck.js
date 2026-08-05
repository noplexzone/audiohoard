'use strict';

(function () {
  var SWIPE_THRESHOLD = 84;
  var SWIPE_INTENT_THRESHOLD = 12;
  var SWIPE_DIRECTION_THRESHOLD = 18;
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

    function toggle(player) {
      if (!player || player.getAttribute('aria-disabled') === 'true') return;
      if (player.paused) {
        void player.play();
      } else {
        player.pause();
      }
    }

    function submit(button) {
      if (working || !button) return;
      var form = button.closest('form');
      if (form) form.requestSubmit(button);
    }

    function clearSwipe() {
      swipe = null;
      deck.classList.remove('is-swiping', 'swipe-approve', 'swipe-deny');
    }

    function ignoresSwipe(target) {
      return target instanceof Element && Boolean(target.closest(INTERACTIVE_SELECTOR));
    }

    deck.addEventListener('pointerdown', function (event) {
      if (working || !event.isPrimary || event.pointerType === 'mouse' || ignoresSwipe(event.target)) return;
      swipe = {
        pointerId: event.pointerId,
        startX: event.clientX,
        startY: event.clientY,
        deltaX: 0,
        horizontal: false
      };
    }, { signal: signal });

    deck.addEventListener('pointermove', function (event) {
      if (!swipe || event.pointerId !== swipe.pointerId) return;
      var deltaX = event.clientX - swipe.startX;
      var deltaY = event.clientY - swipe.startY;
      var absoluteX = Math.abs(deltaX);
      var absoluteY = Math.abs(deltaY);

      if (!swipe.horizontal) {
        if (Math.max(absoluteX, absoluteY) < SWIPE_INTENT_THRESHOLD) return;
        if (absoluteY >= absoluteX) {
          clearSwipe();
          return;
        }
        swipe.horizontal = true;
        deck.classList.add('is-swiping');
        try {
          deck.setPointerCapture(event.pointerId);
        } catch (error) {
          // Pointer capture can fail if the browser already cancelled the gesture.
        }
      }

      swipe.deltaX = deltaX;
      event.preventDefault();
      deck.classList.toggle('swipe-approve', deltaX >= SWIPE_DIRECTION_THRESHOLD);
      deck.classList.toggle('swipe-deny', deltaX <= -SWIPE_DIRECTION_THRESHOLD);
    }, { signal: signal });

    deck.addEventListener('pointerup', function (event) {
      if (!swipe || event.pointerId !== swipe.pointerId) return;
      var deltaX = swipe.deltaX;
      var horizontal = swipe.horizontal;
      clearSwipe();
      if (!horizontal) return;
      if (deltaX >= SWIPE_THRESHOLD) {
        submit(approve);
      } else if (deltaX <= -SWIPE_THRESHOLD) {
        submit(deny);
      }
    }, { signal: signal });

    deck.addEventListener('pointercancel', clearSwipe, { signal: signal });
    deck.addEventListener('lostpointercapture', function () {
      if (swipe && swipe.horizontal) clearSwipe();
    }, { signal: signal });

    var midpoint = deck.querySelector('[data-jump-midpoint]');
    if (midpoint) {
      midpoint.addEventListener('click', function () {
        if (downloaded && Number.isFinite(downloaded.duration) && downloaded.duration > 0) {
          downloaded.currentTime = downloaded.duration / 2;
        }
      }, { signal: signal });
    }

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
