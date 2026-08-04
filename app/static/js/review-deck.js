'use strict';

(function () {
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
      if (form) form.requestSubmit();
    }

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
