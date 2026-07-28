document.addEventListener("click", (event) => {
  const button = event.target.closest(".dismiss-btn");
  if (button) button.closest(".alert")?.remove();
});
