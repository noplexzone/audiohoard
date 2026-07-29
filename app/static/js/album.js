'use strict';

document.addEventListener('submit', function (event) {
  var form = event.target.closest('form[data-confirm]');
  if (form && !window.confirm(form.getAttribute('data-confirm'))) {
    event.preventDefault();
  }
});
