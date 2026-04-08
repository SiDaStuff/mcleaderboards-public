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
 * Rebuild navigation with icons and inbox (Apple-style header)
 * Called once on DOMContentLoaded to inject proper nav items
 */
function rebuildNavigation() {
  const navbarNav = document.getElementById('navbarNav');
  if (!navbarNav || navbarNav.dataset.rebuilt === 'true') return;
  navbarNav.dataset.rebuilt = 'true';

  // Detect which page is active
  const path = window.location.pathname.split('/').pop() || 'index.html';

  const isActive = (page) => path === page ? ' class="active"' : '';

  const authed = AppState.isAuthenticated();

  navbarNav.innerHTML = `
    <li><a href="index.html"${isActive('index.html')}><i class="fas fa-trophy"></i><span class="nav-label"> Leaderboards</span></a></li>
    <li><a href="dashboard.html" id="navDashboard" style="${authed ? '' : 'display:none'}"${isActive('dashboard.html')}><i class="fas fa-gamepad"></i><span class="nav-label"> Dashboard</span></a></li>
    <li><a href="inbox.html" id="navInbox" style="${authed ? '' : 'display:none'}"${isActive('inbox.html')}><i class="fas fa-inbox"></i><span class="nav-label"> Inbox</span><span class="inbox-badge d-none" id="navInboxBadge"></span></a></li>
    <li><a href="account.html" id="navAccount" style="${authed ? '' : 'display:none'}"${isActive('account.html')}><i class="fas fa-circle-user"></i><span class="nav-label"> Account</span></a></li>
    <li><a href="admin.html" id="navAdmin" class="d-none"><i class="fas fa-shield-halved"></i><span class="nav-label"> Admin</span></a></li>
    <li><a href="login.html" id="navLogin" style="${authed ? 'display:none' : ''}"${isActive('login.html')}><i class="fas fa-right-to-bracket"></i><span class="nav-label"> Login</span></a></li>
    <li><a href="#" id="navLogout" style="${authed ? '' : 'display:none'}" onclick="handleLogout()"><i class="fas fa-right-from-bracket"></i><span class="nav-label"> Logout</span></a></li>
  `;

  // Re-attach mobile menu close listeners
  navbarNav.querySelectorAll('a').forEach((link) => {
    link.addEventListener('click', () => closeMobileMenu());
  });
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
  const navInbox = document.getElementById('navInbox');

  if (isAuthenticated) {
    if (navLogin) navLogin.style.display = 'none';
    if (navLogout) navLogout.style.display = '';
    if (navDashboard) navDashboard.style.display = '';
    if (navAccount) navAccount.style.display = '';
    if (navInbox) navInbox.style.display = '';
    
    if (isAdmin && navAdmin) {
      navAdmin.style.display = '';
    } else if (navAdmin) {
      navAdmin.style.display = 'none';
    }

    // Fetch inbox unread count
    updateInboxBadge();
  } else {
    if (navLogin) navLogin.style.display = '';
    if (navLogout) navLogout.style.display = 'none';
    if (navDashboard) navDashboard.style.display = 'none';
    if (navAccount) navAccount.style.display = 'none';
    if (navInbox) navInbox.style.display = 'none';
    if (navAdmin) navAdmin.style.display = 'none';
  }
}

/**
 * Fetch and update the inbox unread badge in the navbar
 */
async function updateInboxBadge() {
  try {
    if (typeof apiService === 'undefined') return;
    const res = await apiService.getInboxUnreadCount();
    const count = res?.unreadCount || 0;
    const badge = document.getElementById('navInboxBadge');
    if (badge) {
      if (count > 0) {
        badge.textContent = count > 99 ? '99+' : count;
        badge.classList.remove('d-none');
      } else {
        badge.classList.add('d-none');
      }
    }
  } catch (e) {
    // Silently fail - badge just won't show
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
  rebuildNavigation();

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

