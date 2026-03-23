// MC Leaderboards - Navigation Management

/**
 * Toggle mobile menu
 */
function toggleMobileMenu() {
  const navbarNav = document.getElementById('navbarNav');
  const hamburgerBtn = document.getElementById('hamburgerBtn');
  if (!navbarNav) return;

  const isOpen = navbarNav.classList.toggle('active');
  document.body.classList.toggle('mobile-menu-open', isOpen);
  if (hamburgerBtn) {
    hamburgerBtn.setAttribute('aria-expanded', String(isOpen));
  }
}

function closeMobileMenu() {
  const navbarNav = document.getElementById('navbarNav');
  const hamburgerBtn = document.getElementById('hamburgerBtn');
  if (!navbarNav) return;

  navbarNav.classList.remove('active');
  document.body.classList.remove('mobile-menu-open');
  if (hamburgerBtn) {
    hamburgerBtn.setAttribute('aria-expanded', 'false');
  }
}

function syncMobileNavOffset() {
  const navbar = document.querySelector('.navbar');
  if (!navbar) return;
  const navHeight = Math.ceil(navbar.getBoundingClientRect().height);
  document.documentElement.style.setProperty('--mobile-nav-offset', `${navHeight}px`);
}

/**
 * Update navigation based on auth state
 */
function updateNavigation() {
  const isAuthenticated = AppState.isAuthenticated();
  const isAdmin = AppState.isAdmin();

  const navLogin = document.getElementById('navLogin');
  const navLogout = document.getElementById('navLogout');
  const navDashboard = document.getElementById('navDashboard');
  const navAccount = document.getElementById('navAccount');
  const navAdmin = document.getElementById('navAdmin');

  if (isAuthenticated) {
    if (navLogin) navLogin.classList.add('d-none');
    if (navLogout) navLogout.classList.remove('d-none');
    if (navDashboard) navDashboard.style.display = 'block';
    if (navAccount) navAccount.style.display = 'block';
    
    if (isAdmin && navAdmin) {
      navAdmin.classList.remove('d-none');
    }
  } else {
    if (navLogin) navLogin.classList.remove('d-none');
    if (navLogout) navLogout.classList.add('d-none');
    if (navDashboard) navDashboard.style.display = 'none';
    if (navAccount) navAccount.style.display = 'none';
    if (navAdmin) navAdmin.classList.add('d-none');
  }
}

/**
 * Handle logout
 */
async function handleLogout() {
  try {
    await firebaseAuthService.signOut();
    AppState.reset();
    updateNavigation();
    window.location.href = 'index.html';
  } catch (error) {
    console.error('Logout error:', error);
    Swal.fire({
      icon: 'error',
      title: 'Logout Failed',
      text: error.message
    });
  }
}

// Update navigation on state changes
AppState.addListener('user', updateNavigation);
AppState.addListener('profile', updateNavigation);

// Close mobile menu when clicking outside
document.addEventListener('click', (e) => {
  const navbarNav = document.getElementById('navbarNav');
  const hamburgerBtn = document.getElementById('hamburgerBtn');
  
  if (navbarNav && hamburgerBtn && 
      !navbarNav.contains(e.target) && 
      !hamburgerBtn.contains(e.target) &&
      navbarNav.classList.contains('active')) {
    closeMobileMenu();
  }
});

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    closeMobileMenu();
  }
});

window.addEventListener('resize', () => {
  syncMobileNavOffset();
  if (window.innerWidth > 768) {
    closeMobileMenu();
  }
});

document.addEventListener('DOMContentLoaded', () => {
  syncMobileNavOffset();

  const hamburgerBtn = document.getElementById('hamburgerBtn');
  if (hamburgerBtn) {
    hamburgerBtn.setAttribute('aria-expanded', 'false');
    hamburgerBtn.setAttribute('aria-controls', 'navbarNav');
    hamburgerBtn.setAttribute('aria-label', 'Toggle navigation menu');
  }

  document.querySelectorAll('#navbarNav a').forEach((link) => {
    link.addEventListener('click', () => {
      closeMobileMenu();
    });
  });

  // Add subtle easter egg trigger to every footer that has footer links.
  document.querySelectorAll('.footer-links').forEach((footerLinks) => {
    if (footerLinks.querySelector('[data-easter-trigger="just-dont"]')) {
      return;
    }

    const trigger = document.createElement('a');
    trigger.href = 'easteregg.html';
    trigger.textContent = 'just dont';
    trigger.dataset.easterTrigger = 'just-dont';
    trigger.style.opacity = '0.18';
    trigger.style.fontSize = '0.62rem';
    trigger.style.letterSpacing = '0.08em';
    trigger.style.textTransform = 'lowercase';
    footerLinks.appendChild(trigger);
  });
});

