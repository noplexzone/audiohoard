'use strict';

(function () {
  var form = document.querySelector('[data-wanted-bulk-form]');
  if (!form) {
    return;
  }
  var selectAll = form.querySelector('[data-wanted-select-all]');
  var checkboxes = Array.prototype.slice.call(
    document.querySelectorAll('[data-wanted-release-checkbox]')
  );
  var queueSelected = form.querySelector('[data-queue-selected]');
  var queueAll = document.querySelector('[data-queue-all]');

  function updateSelectAll() {
    if (!selectAll) {
      return;
    }
    var selected = checkboxes.filter(function (checkbox) { return checkbox.checked; }).length;
    selectAll.checked = selected > 0 && selected === checkboxes.length;
    selectAll.indeterminate = selected > 0 && selected < checkboxes.length;
  }

  if (selectAll) {
    selectAll.addEventListener('change', function () {
      checkboxes.forEach(function (checkbox) { checkbox.checked = selectAll.checked; });
      updateSelectAll();
    });
  }

  checkboxes.forEach(function (checkbox) {
    checkbox.addEventListener('change', updateSelectAll);
  });

  if (queueAll) {
    queueAll.addEventListener('click', function () {
      checkboxes.forEach(function (checkbox) { checkbox.checked = true; });
      updateSelectAll();
    });
  }

  if (queueSelected) {
    queueSelected.addEventListener('click', function () {
      updateSelectAll();
    });
  }

  updateSelectAll();
}());
