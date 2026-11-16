// Give the theme's small-screen label controls a keyboard action and a name.
for (const [id, name, focusTarget] of [
  ['__drawer', 'Open navigation', '.md-sidebar--primary .md-nav__link'],
  ['__search', 'Open search', '.md-search__input'],
]) {
  const control = document.querySelector(`.md-header label[for="${id}"]`);
  const toggle = document.getElementById(id);
  if (!control || !toggle) continue;
  control.setAttribute('role', 'button');
  control.setAttribute('aria-label', name);
  control.tabIndex = 0;
  const update = () => control.setAttribute('aria-expanded', String(toggle.checked));
  toggle.addEventListener('change', update);
  update();
  control.addEventListener('keydown', event => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    event.stopPropagation();
    control.click();
    if (toggle.checked) requestAnimationFrame(() => document.querySelector(focusTarget)?.focus());
  });
}
