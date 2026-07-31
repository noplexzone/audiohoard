'use strict';

window.AudiohoardNavigation.registerPage('wanted', function (region) {
  var controller = new AbortController();
  var form = region.querySelector('[data-wanted-bulk-form]');
  if (!form) return function () { controller.abort(); };
  var selectAll = form.querySelector('[data-wanted-select-all]');
  var checkboxes = Array.from(region.querySelectorAll('[data-wanted-release-checkbox]'));
  var queueSelected = form.querySelector('[data-queue-selected]');
  var queueAll = region.querySelector('[data-queue-all]');

  function updateSelectAll() {
    if (!selectAll) return;
    var selected = checkboxes.filter(function (checkbox) { return checkbox.checked; }).length;
    selectAll.checked = selected > 0 && selected === checkboxes.length;
    selectAll.indeterminate = selected > 0 && selected < checkboxes.length;
  }
  if (selectAll) selectAll.addEventListener('change', function () {
    checkboxes.forEach(function (checkbox) { checkbox.checked = selectAll.checked; });
    updateSelectAll();
  }, { signal: controller.signal });
  checkboxes.forEach(function (checkbox) {
    checkbox.addEventListener('change', updateSelectAll, { signal: controller.signal });
  });
  if (queueAll) queueAll.addEventListener('click', function () {
    checkboxes.forEach(function (checkbox) { checkbox.checked = true; });
    updateSelectAll();
  }, { signal: controller.signal });
  if (queueSelected) queueSelected.addEventListener('click', updateSelectAll, { signal: controller.signal });
  updateSelectAll();
  return function () { controller.abort(); };
});
