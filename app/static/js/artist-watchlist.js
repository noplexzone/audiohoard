(() => {
  const inputs = Array.from(document.querySelectorAll('input[form="monitor-form"]'));
  const initialState = new Map(inputs.map((input) => [input, input.checked]));
  const bar = document.createElement("div");
  const count = document.createElement("span");
  const save = document.createElement("button");
  bar.className = "unsaved-bar";
  bar.hidden = true;
  bar.setAttribute("role", "status");
  save.type = "submit";
  save.className = "btn";
  save.setAttribute("form", "monitor-form");
  save.textContent = "Save";
  bar.append(count, save);
  document.body.append(bar);

  const updateBar = () => {
    const unsaved = inputs.filter((input) => input.checked !== initialState.get(input)).length;
    count.textContent = `${unsaved} unsaved selection${unsaved === 1 ? "" : "s"}`;
    bar.hidden = unsaved === 0;
  };

  inputs.forEach((input) => input.addEventListener("change", updateBar));
  document.querySelectorAll(".dismiss-btn").forEach((button) => {
    button.addEventListener("click", () => button.closest('[role="alert"]')?.remove());
  });
})();
