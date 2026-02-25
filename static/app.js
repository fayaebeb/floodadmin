(() => {
  const openBtn = document.querySelector('[data-sidebar-open]');
  const closeBtn = document.querySelector('[data-sidebar-close]');
  const overlay = document.querySelector('[data-sidebar-overlay]');
  const collapseBtns = Array.from(document.querySelectorAll('[data-sidebar-collapse]'));

  const open = () => document.body.classList.add('sidebar-open');
  const close = () => document.body.classList.remove('sidebar-open');

  const syncCollapseButtons = () => {
    const collapsed = document.body.classList.contains('sidebar-collapsed');
    collapseBtns.forEach((btn) => {
      btn.setAttribute('aria-pressed', collapsed ? 'true' : 'false');
      btn.setAttribute('aria-label', collapsed ? 'Expand sidebar' : 'Collapse sidebar');
      btn.title = collapsed ? 'Expand sidebar' : 'Collapse sidebar';
    });
  };

  const setCollapsed = (collapsed) => {
    document.body.classList.toggle('sidebar-collapsed', Boolean(collapsed));
    try {
      localStorage.setItem('sidebarCollapsed', collapsed ? '1' : '0');
    } catch {}
    syncCollapseButtons();
  };

  openBtn?.addEventListener('click', () => open());
  closeBtn?.addEventListener('click', () => close());
  overlay?.addEventListener('click', () => close());

  collapseBtns.forEach((btn) => {
    btn.addEventListener('click', () => {
      setCollapsed(!document.body.classList.contains('sidebar-collapsed'));
    });
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') close();
  });

  try {
    setCollapsed(localStorage.getItem('sidebarCollapsed') === '1');
  } catch {
    syncCollapseButtons();
  }
})();

(() => {
  const logoutForm = document.querySelector('[data-logout-form]');
  const modal = document.querySelector('[data-logout-modal]');
  const cancelBtns = Array.from(document.querySelectorAll('[data-logout-cancel]'));
  const confirmBtn = document.querySelector('[data-logout-confirm]');

  if (!logoutForm || !modal || !confirmBtn) return;

  const open = () => {
    modal.hidden = false;
    requestAnimationFrame(() => modal.classList.add('is-open'));
    confirmBtn.focus?.();
  };

  const close = () => {
    modal.classList.remove('is-open');
    window.setTimeout(() => {
      modal.hidden = true;
    }, 180);
  };

  logoutForm.addEventListener('submit', (e) => {
    if (logoutForm.dataset.logoutConfirmed === '1') return;
    e.preventDefault();
    open();
  });

  confirmBtn.addEventListener('click', () => {
    logoutForm.dataset.logoutConfirmed = '1';
    if (typeof logoutForm.requestSubmit === 'function') {
      logoutForm.requestSubmit();
    } else {
      logoutForm.submit();
    }
  });

  cancelBtns.forEach((btn) => btn.addEventListener('click', () => close()));

  modal.addEventListener('click', (e) => {
    if (e.target === modal) close();
  });

  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    if (modal.hidden) return;
    e.preventDefault();
    close();
  });
})();
