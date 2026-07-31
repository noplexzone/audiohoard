'use strict';

window.AudiohoardNavigation.registerPage('settings', function (region) {
  var controller = new AbortController();
  region.addEventListener('click', function (event) {
    var button = event.target.closest('.dismiss-btn');
    if (button) button.closest('.alert')?.remove();
  }, { signal: controller.signal });
  return function () { controller.abort(); };
});
