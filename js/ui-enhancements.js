// MC Leaderboards - UI Enhancements
// Toast notifications, back-to-top button, keyboard shortcuts, character counters, confirmation dialogs

// ===== 1. Toast Notification System =====
const Toast = (() => {
  let container = null;

  function getContainer() {
    if (!container) {
      container = document.createElement('div');
      container.id = 'toastContainer';
      container.className = 'toast-container';
      document.body.appendChild(container);
    }
    return container;
  }

  function show(message, type = 'info', duration = 4000) {
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;

    const icons = {
      success: 'fa-circle-check',
      error: 'fa-circle-xmark',
      warning: 'fa-triangle-exclamation',
      info: 'fa-circle-info'
    };

    toast.innerHTML = `
      <i class="fas ${icons[type] || icons.info} toast-icon"></i>
      <span class="toast-message">${message}</span>
      <button class="toast-close" onclick="this.parentElement.remove()"><i class="fas fa-xmark"></i></button>
    `;

    getContainer().appendChild(toast);

    // Trigger entrance animation
    requestAnimationFrame(() => toast.classList.add('toast-visible'));

    if (duration > 0) {
      setTimeout(() => {
        toast.classList.add('toast-exit');
        toast.addEventListener('animationend', () => toast.remove());
      }, duration);
    }

    return toast;
  }

  return {
    success: (msg, dur) => show(msg, 'success', dur),
    error: (msg, dur) => show(msg, 'error', dur),
    warning: (msg, dur) => show(msg, 'warning', dur),
    info: (msg, dur) => show(msg, 'info', dur)
  };
})();


// ===== 2. Confirmation Dialog =====
function confirmAction(title, message, confirmText = 'Confirm', type = 'warning') {
  if (typeof Swal !== 'undefined') {
    return Swal.fire({
      title,
      text: message,
      icon: type,
      showCancelButton: true,
      confirmButtonText: confirmText,
      cancelButtonText: 'Cancel',
      confirmButtonColor: type === 'danger' ? '#ef4444' : '#1eb681',
      background: '#1a1d20',
      color: '#f3f4f6'
    }).then(result => result.isConfirmed);
  }
  return Promise.resolve(window.confirm(`${title}\n\n${message}`));
}


// ===== 3. Back-to-Top Button =====
function initBackToTop() {
  const btn = document.createElement('button');
  btn.id = 'backToTop';
  btn.className = 'back-to-top';
  btn.innerHTML = '<i class="fas fa-chevron-up"></i>';
  btn.title = 'Back to top';
  btn.setAttribute('aria-label', 'Scroll to top');
  document.body.appendChild(btn);

  let ticking = false;
  window.addEventListener('scroll', () => {
    if (!ticking) {
      requestAnimationFrame(() => {
        btn.classList.toggle('visible', window.scrollY > 300);
        ticking = false;
      });
      ticking = true;
    }
  }, { passive: true });

  btn.addEventListener('click', () => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  });
}


// ===== 4. Character Counter for Textareas =====
function initCharCounters() {
  document.querySelectorAll('textarea[maxlength], textarea[data-max-chars]').forEach(textarea => {
    const max = parseInt(textarea.getAttribute('maxlength') || textarea.dataset.maxChars, 10);
    if (!max || textarea.dataset.counterInit) return;
    textarea.dataset.counterInit = 'true';

    const counter = document.createElement('div');
    counter.className = 'char-counter';
    counter.textContent = `0 / ${max}`;
    textarea.parentNode.insertBefore(counter, textarea.nextSibling);

    textarea.addEventListener('input', () => {
      const len = textarea.value.length;
      counter.textContent = `${len} / ${max}`;
      counter.classList.toggle('char-counter-warn', len > max * 0.9);
      counter.classList.toggle('char-counter-full', len >= max);
    });
  });
}


// ===== 5. Keyboard Shortcuts =====
function initKeyboardShortcuts() {
  document.addEventListener('keydown', (e) => {
    // Don't trigger when typing in inputs
    if (e.target.matches('input, textarea, select, [contenteditable]')) return;

    // Ctrl/Cmd + K -> Focus search
    if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
      e.preventDefault();
      const searchInput = document.getElementById('searchInput');
      if (searchInput) searchInput.focus();
    }

    // G then H -> Go home (leaderboard)
    if (e.key === '?' && !e.ctrlKey && !e.metaKey) {
      showKeyboardShortcutsHelp();
    }
  });

  // Sequence shortcuts (g+h, g+d, g+i, g+a)
  let lastKey = '';
  let lastKeyTime = 0;
  document.addEventListener('keydown', (e) => {
    if (e.target.matches('input, textarea, select, [contenteditable]')) return;
    if (e.ctrlKey || e.metaKey || e.altKey) return;

    const now = Date.now();
    if (now - lastKeyTime > 1000) lastKey = '';

    if (lastKey === 'g') {
      switch (e.key) {
        case 'h': window.location.href = 'index.html'; break;
        case 'd': window.location.href = 'dashboard.html'; break;
        case 'i': window.location.href = 'inbox.html'; break;
        case 'a': window.location.href = 'account.html'; break;
      }
      lastKey = '';
      return;
    }

    lastKey = e.key;
    lastKeyTime = now;
  });
}

function showKeyboardShortcutsHelp() {
  if (typeof Swal !== 'undefined') {
    Swal.fire({
      title: 'Keyboard Shortcuts',
      html: `
        <div style="text-align:left; font-size:0.9rem; line-height:2;">
          <div><kbd>Ctrl+K</kbd> — Focus search</div>
          <div><kbd>G</kbd> then <kbd>H</kbd> — Go to Leaderboards</div>
          <div><kbd>G</kbd> then <kbd>D</kbd> — Go to Dashboard</div>
          <div><kbd>G</kbd> then <kbd>I</kbd> — Go to Inbox</div>
          <div><kbd>G</kbd> then <kbd>A</kbd> — Go to Account</div>
          <div><kbd>?</kbd> — Show this help</div>
        </div>
      `,
      background: '#1a1d20',
      color: '#f3f4f6',
      confirmButtonColor: '#1eb681',
      confirmButtonText: 'Got it'
    });
  }
}


// ===== 6. Staggered List Animations =====
function animateListItems(containerSelector) {
  const container = document.querySelector(containerSelector);
  if (!container) return;

  const items = container.children;
  for (let i = 0; i < items.length; i++) {
    items[i].style.opacity = '0';
    items[i].style.animation = `staggerFadeIn 0.3s ease forwards`;
    items[i].style.animationDelay = `${i * 0.04}s`;
  }
}


// ===== Initialize on DOM Load =====
document.addEventListener('DOMContentLoaded', () => {
  initBackToTop();
  initCharCounters();
  initKeyboardShortcuts();

  // Observe for dynamically added textareas (debounced to avoid CPU thrashing)
  let charCounterDebounce = null;
  const observer = new MutationObserver(() => {
    clearTimeout(charCounterDebounce);
    charCounterDebounce = setTimeout(initCharCounters, 500);
  });
  observer.observe(document.body, { childList: true, subtree: true });
});
