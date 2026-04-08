// MC Leaderboards - Dashboard Functionality

let queueCheckInterval = null;
let activeMatch = null;
let activeMatchPollInterval = null;
let gamemodeStatsInterval = null;
let gamemodeStatsBackoffUntilMs = 0;
let hasLoadedRecentMatches = false;
let hasLoadedGamemodeActivity = false;
let hasLoadedNotificationSettings = false;
let hasLoadedQueueCooldowns = false;
let dashboardInitialized = false;
let notificationPollInterval = null; // legacy, removed
let dashboardRefreshInterval = null; // legacy, removed
let cooldownDisplayInterval = null;
let joinQueueButtonInterval = null;
const seenNotificationIds = new Set();
const MAX_SEEN_NOTIFICATIONS = 500;
const DASHBOARD_ACTIVE_MATCH_POLL_MS = 5000;

const DASHBOARD_ADMIN_CAPABILITY_MATRIX = {
  owner: ['*'],
  lead_admin: ['users:view', 'users:manage', 'blacklist:view', 'blacklist:manage', 'audit:view', 'matches:view', 'matches:manage', 'reports:manage', 'disputes:manage', 'queue:inspect', 'settings:manage'],
  moderator: ['users:view', 'blacklist:view', 'blacklist:manage', 'audit:view', 'matches:view', 'reports:manage', 'disputes:manage'],
  support: ['users:view', 'audit:view', 'matches:view']
};

const DASHBOARD_ADMIN_TAB_REQUIREMENTS = {
  management: ['users:view'],
  moderation: ['blacklist:view'],
  reported: ['reports:manage'],
  matches: ['matches:view'],
  operations: ['matches:view'],
  'security-scores': ['audit:view'],
  servers: ['settings:manage'],
  'staff-roles': ['settings:manage']
};

// Restore configureUnifiedQueueExperience for dashboard UI
function configureUnifiedQueueExperience() {
  const title = document.getElementById('queueCardTitle');
  const subtitle = document.getElementById('queueCardSubtitle');
  const flowMessage = document.getElementById('queueFlowMessage');
  const testerOptions = document.getElementById('testerQueueOptions');

  if (isTierTesterUser()) {
    if (title) title.textContent = 'Join Shared Queue';
    if (subtitle) subtitle.textContent = 'Use the same shared queue as everyone else. Your tier tester preference is applied automatically.';
    if (flowMessage) {
      flowMessage.innerHTML = '<strong>Queue flow:</strong> Select your gamemodes and regions, choose a whitelisted server, then join the same live queue players use.';
    }
    if (testerOptions) testerOptions.style.display = 'block';
  } else {
    if (title) title.textContent = 'Join Shared Queue';
    if (subtitle) subtitle.textContent = 'One shared queue for players and tier testers.';
    if (flowMessage) {
      flowMessage.innerHTML = '<strong>Queue flow:</strong> Select one or more gamemodes and regions, choose a whitelisted server, then join queue.';
    }
    if (testerOptions) testerOptions.style.display = 'none';
  }

  updateJoinQueueButtonState();
}

function getDashboardAdminCapabilities(profile = {}) {
  const contextCapabilities = Array.isArray(profile?.adminContext?.capabilities) ? profile.adminContext.capabilities : null;
  if (contextCapabilities && contextCapabilities.length > 0) {
    return contextCapabilities;
  }

  const role = typeof profile?.adminContext?.role === 'string'
    ? profile.adminContext.role
    : (typeof profile?.adminRole === 'string' ? profile.adminRole : (profile?.admin === true ? 'lead_admin' : null));
  return DASHBOARD_ADMIN_CAPABILITY_MATRIX[role] || [];
}

function dashboardAdminHasCapability(profile, capability) {
  const capabilities = getDashboardAdminCapabilities(profile);
  return capabilities.includes('*') || capabilities.includes(capability);
}

function isDashboardAdminTabVisible(profile, tab) {
  const requirements = DASHBOARD_ADMIN_TAB_REQUIREMENTS[tab] || [];
  if (!requirements.length) return true;
  return requirements.some((capability) => dashboardAdminHasCapability(profile, capability));
}

function scrollToTesterDashboard() {
  const testerSection = document.getElementById('sharedQueueCard');
  if (!testerSection) return;

  if (testerSection.style.display === 'none') {
    testerSection.style.display = 'block';
  }

  const navbar = document.querySelector('.navbar');
  const offset = (navbar ? navbar.offsetHeight : 80) + 16;
  const targetTop = testerSection.getBoundingClientRect().top + window.pageYOffset - offset;
  window.scrollTo({ top: Math.max(0, targetTop), behavior: 'smooth' });
}

function isTierTesterUser() {
  return Boolean(AppState.isTierTester && AppState.isTierTester());
}

function getJoinQueueButtonLabel() {
  return isTierTesterUser() ? 'Join as Tier Tester' : 'Join Shared Queue';
}

function renderGamemodeSelectionControls() {
  const gamemodes = (CONFIG?.GAMEMODES || []).filter(gm => gm.id && gm.id !== 'overall');
  if (!gamemodes.length) return;

  const playerGamemodeContainer = document.getElementById('playerGamemodeSelections');
  if (playerGamemodeContainer) {
    playerGamemodeContainer.innerHTML = gamemodes.map(gm => `
      <label class="gamemode-choice">
        <input type="checkbox" class="player-gamemode-checkbox" value="${gm.id}">
        <span class="gamemode-choice-content">
          <img src="${gm.icon}" alt="${escapeHtml(gm.name)}" class="gamemode-choice-icon">
          <span>${escapeHtml(gm.name)}</span>
        </span>
      </label>
    `).join('');
  }

  const playerRegionContainer = document.getElementById('playerRegionSelections');
  if (playerRegionContainer) {
    playerRegionContainer.innerHTML = ['NA', 'EU', 'AS', 'SA', 'AU'].map((region) =>
      `<label class="tester-region-choice"><input type="checkbox" class="player-region-checkbox" value="${region}"> ${region}</label>`
    ).join('');
  }

  const testerContainer = document.getElementById('testerGamemodeSelections');
  if (testerContainer) {
    testerContainer.innerHTML = gamemodes.map(gm => `
      <label class="gamemode-choice">
        <input type="checkbox" class="tester-gamemode-checkbox" value="${gm.id}">
        <span class="gamemode-choice-content">
          <img src="${gm.icon}" alt="${escapeHtml(gm.name)}" class="gamemode-choice-icon">
          <span>${escapeHtml(gm.name)}</span>
        </span>
      </label>
    `).join('');
  }
}

function getSelectedValues(selector) {
  return Array.from(document.querySelectorAll(selector))
    .filter((input) => input.checked)
    .map((input) => input.value);
}

function getSelectedPlayerQueueGamemodes() {
  return getSelectedValues('.player-gamemode-checkbox');
}

function getSelectedPlayerQueueRegions() {
  return getSelectedValues('.player-region-checkbox');
}

function formatQueueSelectionText(values = [], fallback = null, formatter = (value) => value) {
  const normalizedValues = Array.isArray(values) && values.length > 0
    ? values
    : (fallback ? [fallback] : []);

  if (!normalizedValues.length) return '-';
  return normalizedValues.map((value) => formatter(String(value))).join(', ');
}

function calculateQueueTotals(queueStats, queueEntry) {
  const gamemodes = Array.isArray(queueEntry?.gamemodes) && queueEntry.gamemodes.length > 0
    ? queueEntry.gamemodes
    : (queueEntry?.gamemode ? [queueEntry.gamemode] : []);
  const regions = Array.isArray(queueEntry?.regions) && queueEntry.regions.length > 0
    ? queueEntry.regions
    : (queueEntry?.region ? [queueEntry.region] : []);

  let playersInQueue = 0;
  let availableTesters = 0;

  gamemodes.forEach((gamemode) => {
    regions.forEach((region) => {
      playersInQueue += queueStats.playersQueued?.[gamemode]?.[region] || 0;
      availableTesters += queueStats.testersAvailable?.[gamemode]?.[region] || 0;
    });
  });

  return { playersInQueue, availableTesters };
}

async function getBlockedQueueCooldown(gamemodes) {
  for (const gamemode of gamemodes) {
    const cooldownCheck = await checkQueueCooldown(gamemode);
    if (!cooldownCheck.allowed) {
      return { gamemode, ...cooldownCheck };
    }
  }

  return null;
}

/**
 * Initialize dashboard
 */
async function initDashboard() {
  if (dashboardInitialized) return;
  dashboardInitialized = true;

  // Authentication is already verified by auth-guard.js
  // Just verify it's still authenticated
  if (!AppState.isAuthenticated()) {
    dashboardInitialized = false;
    return; // Will be handled by auth guard
  }

  renderGamemodeSelectionControls();

  // Update loading status
  if (window.mclbLoadingOverlay) {
    window.mclbLoadingOverlay.updateStatus('Loading dashboard data...', 85);
  }

  // Load essential data only on page load
  await Promise.all([
    loadUserProfile(),
    checkQueueStatus(),
    checkActiveMatch(),
    checkUserWarnings()
  ]);

  configureUnifiedQueueExperience();

  await loadUserCooldowns();
  hasLoadedQueueCooldowns = true;

  if (window.mclbLoadingOverlay) {
    window.mclbLoadingOverlay.updateStatus('Loading gamemode activity...', 89);
  }
  await window.loadGamemodeActivityOnDemand();

  // Start cooldown timer updates
  startCooldownTimers();

  // Security hardening: avoid broad client-side Firebase reads (queue/matches/users).
  // Use backend endpoints only for dashboard state and polling.
  // (Firebase Admin SDK bypasses rules on the server; clients should not access privileged data.)

  // OPTIMIZED: Periodic refresh with exponential backoff on errors
  let refreshInterval = 30000; // Start with 30s
  let consecutiveErrors = 0;
  const maxInterval = 120000; // Max 2 minutes
  
  const refreshDashboard = async () => {
    if (document.visibilityState !== 'visible') return;
    try {
      await Promise.all([
        loadUserProfile(),
        checkQueueStatus(),
        checkActiveMatch()
      ]);
      configureUnifiedQueueExperience();
      // Reset on success
      consecutiveErrors = 0;
      refreshInterval = 30000;
    } catch (error) {
      // If rate limited, back off more aggressively
      if (error?.message?.includes('429') || error?.status === 429) {
        consecutiveErrors++;
        refreshInterval = Math.min(refreshInterval * 1.5, maxInterval);
        return;
      }
      console.error('Dashboard refresh error:', error);
      consecutiveErrors++;
      if (consecutiveErrors > 3) {
        refreshInterval = Math.min(refreshInterval * 1.5, maxInterval);
      }
    }
  };
  
  // Start interval (don't run immediately since we just loaded everything above)
  dashboardRefreshInterval = setInterval(refreshDashboard, refreshInterval);
  startActiveMatchPolling();

  // Show tier tester application banner if user doesn't have tester role
  showTierTesterBanner();
  renderStaffActionsSection();

  if (isTierTesterUser()) {
    // Update loading status
    if (window.mclbLoadingOverlay) {
      window.mclbLoadingOverlay.updateStatus('Loading tester data...', 90);
    }

    await loadSharedQueueSettings();

    // Set up stay in queue setting listener
    const stayInQueueCheckbox = document.getElementById('stayInQueueAfterMatch');
    if (stayInQueueCheckbox) {
      stayInQueueCheckbox.addEventListener('change', async () => {
        try {
          await apiService.updateProfile({
            stayInQueueAfterMatch: stayInQueueCheckbox.checked
          });
        } catch (error) {
          console.error('Error saving stay in queue setting:', error);
          // Revert the checkbox on error
          stayInQueueCheckbox.checked = !stayInQueueCheckbox.checked;
        }
      });
    }

  }

  // Signal that all initial loading is complete
  if (window.mclbLoadingOverlay) {
    window.mclbLoadingOverlay.updateStatus('Dashboard ready!', 100);
  }
}

function renderStaffActionsSection() {
  const card = document.getElementById('staffActionsCard');
  const intro = document.getElementById('staffActionsIntro');
  const grid = document.getElementById('staffActionsGrid');
  if (!card || !intro || !grid) return;

  const profile = AppState.getProfile?.() || AppState.userProfile || {};
  const staffRole = profile.staffRole || null;
  if (!staffRole) {
    card.style.display = 'none';
    return;
  }

  card.style.display = 'block';
  const introIcon = staffRole.iconUrl
    ? `<img src="${escapeHtml(staffRole.iconUrl)}" alt="${escapeHtml(staffRole.name || 'Staff')}" class="staff-role-inline-icon-image">`
    : `<i class="${escapeHtml(staffRole.iconClass || 'fas fa-shield-alt')} staff-role-inline-icon-glyph"></i>`;
  intro.innerHTML = `Role: <strong style="color:${escapeHtml(staffRole.color || '#38bdf8')};">${introIcon}${escapeHtml(staffRole.name || 'Staff')}</strong>`;

  const actionDefs = {
    open_admin_management: { label: 'User Management', icon: 'fa-users-cog', adminTab: 'management', run: () => openAdminTabShortcut('management') },
    open_admin_moderation: { label: 'Blacklist & Applications', icon: 'fa-ban', adminTab: 'moderation', run: () => openAdminTabShortcut('moderation') },
    open_admin_reports: { label: 'Reports Review', icon: 'fa-flag', adminTab: 'reported', run: () => openAdminTabShortcut('reported') },
    open_admin_matches: { label: 'Match Manager', icon: 'fa-gamepad', adminTab: 'matches', run: () => openAdminTabShortcut('matches') },
    open_admin_operations: { label: 'Queue & Match Ops', icon: 'fa-diagram-project', adminTab: 'operations', run: () => openAdminTabShortcut('operations') },
    open_admin_security_scores: { label: 'Security Scores', icon: 'fa-shield-alt', adminTab: 'security-scores', run: () => openAdminTabShortcut('security-scores') },
    open_admin_support: { label: 'Support Tickets', icon: 'fa-life-ring', adminTab: 'support', run: () => openAdminTabShortcut('support') },
    open_admin_servers: { label: 'Whitelisted Servers', icon: 'fa-server', adminTab: 'servers', run: () => openAdminTabShortcut('servers') },
    open_admin_staff_roles: { label: 'Staff Roles', icon: 'fa-user-shield', adminTab: 'staff-roles', run: () => openAdminTabShortcut('staff-roles') },
    queue_open: { label: 'Join Queue', icon: 'fa-play', run: () => document.getElementById('queueForm')?.scrollIntoView({ behavior: 'smooth', block: 'center' }) },
    queue_leave: { label: 'Leave Queue', icon: 'fa-sign-out-alt', run: () => handleLeaveQueue() },
    queue_refresh: { label: 'Refresh Queue', icon: 'fa-sync-alt', run: () => checkQueueStatus() },
    load_activity: { label: 'Load Activity', icon: 'fa-chart-line', run: () => window.loadGamemodeActivityOnDemand?.() },
    load_cooldowns: { label: 'Load Cooldowns', icon: 'fa-clock', run: () => window.loadQueueCooldownsOnDemand?.() },
    open_reports_page: { label: 'Open Reports', icon: 'fa-flag', run: () => { window.location.href = 'report.html'; } },
    open_support_page: { label: 'Open Support', icon: 'fa-life-ring', run: () => { window.location.href = 'support.html'; } },
    open_testing_page: { label: 'Open Testing', icon: 'fa-flask', run: () => openTestingPage() }
  };

  function openAdminTabShortcut(tab) {
    window.location.href = `admin.html?tab=${encodeURIComponent(tab)}`;
  }

  const configuredActions = Array.isArray(staffRole.dashboardActions) ? staffRole.dashboardActions : [];
  const visibleActions = configuredActions.filter((actionId) => {
    const def = actionDefs[actionId];
    if (!def) {
      return false;
    }

    if (!def.adminTab) {
      return true;
    }

    return isDashboardAdminTabVisible(profile, def.adminTab);
  });

  if (!visibleActions.length) {
    grid.innerHTML = '<div class="text-muted">No accessible dashboard shortcuts are configured for this role.</div>';
    return;
  }

  grid.innerHTML = visibleActions.map((actionId) => {
    const def = actionDefs[actionId];
    if (!def) return '';
    return `
      <button class="btn btn-secondary" type="button" onclick="runStaffDashboardAction('${escapeHtml(actionId)}')">
        <i class="fas ${escapeHtml(def.icon)}"></i> ${escapeHtml(def.label)}
      </button>
    `;
  }).join('');

  window.runStaffDashboardAction = (actionId) => {
    if (!visibleActions.includes(actionId)) return;
    const def = actionDefs[actionId];
    if (!def || typeof def.run !== 'function') return;
    try {
      def.run();
    } catch (error) {
      console.error('Failed running staff dashboard action:', error);
    }
  };
}

async function getCachedProfile({ forceRefresh = false } = {}) {
  try {
    const existing = AppState.getProfile?.();
    if (existing && !forceRefresh) return existing;
    const profile = await apiService.getProfile();
    if (profile) {
      AppState.setProfile(profile);
    }
    return profile;
  } catch (error) {
    console.error('Error getting profile:', error);
    return AppState.getProfile?.() || null;
  }
}

/**
 * Load notification settings from profile
 */
async function loadNotificationSettings() {
  try {
    const profile = await getCachedProfile();
    const notifySettings = profile.notificationSettings || {};
    
    const notifyMatchCreated = document.getElementById('notifyMatchCreated');
    const notifyMatchFinalized = document.getElementById('notifyMatchFinalized');
    const notifyTesterAvailable = document.getElementById('notifyTesterAvailable');
    const selectedTesterGamemodes = Array.isArray(notifySettings.testerAvailabilityGamemodes)
      ? notifySettings.testerAvailabilityGamemodes
      : [];
    
    if (notifyMatchCreated) {
      notifyMatchCreated.checked = notifySettings.notifyMatchCreated !== false; // Default true
    }
    if (notifyMatchFinalized) {
      notifyMatchFinalized.checked = notifySettings.notifyMatchFinalized !== false; // Default true
    }
    if (notifyTesterAvailable) {
      if (notifySettings.notifyTesterAvailable === true) {
        notifyTesterAvailable.checked = true;
      } else if (notifySettings.notifyTesterAvailable === false) {
        notifyTesterAvailable.checked = false;
      } else {
        // Backward compatibility: if unset, treat existing gamemode selections as enabled.
        notifyTesterAvailable.checked = selectedTesterGamemodes.length > 0;
      }
    }
    const configureBtn = document.getElementById('configureTesterNotificationsBtn');
    const saveBtn = document.getElementById('saveNotificationSettingsBtn');
    if (configureBtn) configureBtn.disabled = false;
    if (saveBtn) saveBtn.disabled = false;
    hasLoadedNotificationSettings = true;
  } catch (error) {
    console.error('Error loading notification settings:', error);
  }
}

function setButtonLoading(buttonId, loadingText, isLoading) {
  const button = document.getElementById(buttonId);
  if (!button) return;
  if (isLoading) {
    button.dataset.originalHtml = button.innerHTML;
    button.disabled = true;
    button.innerHTML = `<i class="fas fa-spinner fa-spin"></i> ${loadingText}`;
    return;
  }

  button.disabled = false;
  if (button.dataset.originalHtml) {
    button.innerHTML = button.dataset.originalHtml;
  }
}

window.loadNotificationSettingsOnDemand = async function loadNotificationSettingsOnDemand() {
  if (hasLoadedNotificationSettings) return;
  setButtonLoading('loadNotificationSettingsBtn', 'Loading...', true);
  try {
    await loadNotificationSettings();
  } finally {
    setButtonLoading('loadNotificationSettingsBtn', '', false);
  }
};

window.loadRecentMatchesOnDemand = async function loadRecentMatchesOnDemand() {
  if (hasLoadedRecentMatches) return;
  setButtonLoading('loadRecentMatchesBtn', 'Loading...', true);
  try {
    await loadRecentMatches();
  } finally {
    setButtonLoading('loadRecentMatchesBtn', '', false);
  }
};

window.loadGamemodeActivityOnDemand = async function loadGamemodeActivityOnDemand() {
  setButtonLoading('loadGamemodeActivityBtn', 'Loading...', true);
  try {
    if (hasLoadedGamemodeActivity) {
      await loadGamemodeStats();
      return;
    }
    await loadGamemodeStats();
    hasLoadedGamemodeActivity = true;
    startGamemodeStatsPolling();
  } finally {
    setButtonLoading('loadGamemodeActivityBtn', '', false);
  }
};

window.loadQueueCooldownsOnDemand = async function loadQueueCooldownsOnDemand() {
  if (hasLoadedQueueCooldowns) return;
  setButtonLoading('loadQueueCooldownsBtn', 'Loading...', true);
  try {
    await loadQueueCooldowns();
    await loadUserCooldowns();
    hasLoadedQueueCooldowns = true;
  } finally {
    setButtonLoading('loadQueueCooldownsBtn', '', false);
  }
};

/**
 * Open tester availability settings modal
 * Made globally accessible for onclick handlers
 */
window.openTesterAvailabilitySettings = async function openTesterAvailabilitySettings() {
  try {
    const profile = await getCachedProfile();
    const selectedGamemodes = profile.notificationSettings?.testerAvailabilityGamemodes || [];

    const gamemodeOptions = CONFIG.GAMEMODES
      .filter(gm => gm.id !== 'overall')
      .map(gm => {
        const isSelected = selectedGamemodes.includes(gm.id);
        return `
          <div class="form-group" style="display: flex; align-items: center; padding: 0.5rem; border: 1px solid var(--border-color); border-radius: 4px; margin-bottom: 0.5rem;">
            <label style="display: flex; align-items: center; cursor: pointer; flex: 1;">
              <input type="checkbox" class="tester-gamemode-notify-checkbox" value="${gm.id}" ${isSelected ? 'checked' : ''} style="margin-right: 0.5rem;">
              <img src="${gm.icon}" alt="${gm.name}" style="width: 24px; height: 24px; margin-right: 0.5rem; border-radius: 4px;">
              <span>${gm.name}</span>
            </label>
          </div>
        `;
      }).join('');

    const result = await Swal.fire({
      title: 'Tester Availability Notifications',
      html: `
        <p class="text-muted mb-3">Select gamemodes where you want to be notified when a tier tester becomes available:</p>
        <div style="max-height: 400px; overflow-y: auto;">
          ${gamemodeOptions}
        </div>
      `,
      showCancelButton: true,
      confirmButtonText: 'Save',
      preConfirm: () => {
        const checkboxes = document.querySelectorAll('.tester-gamemode-notify-checkbox');
        const selected = Array.from(checkboxes)
          .filter(cb => cb.checked)
          .map(cb => cb.value);
        return selected;
      }
    });

    if (result.isConfirmed) {
      const currentProfile = await getCachedProfile();
      const notifySettings = currentProfile.notificationSettings || {};
      notifySettings.testerAvailabilityGamemodes = result.value;
      
      await apiService.updateProfile({ notificationSettings: notifySettings });
      AppState.setProfile({
        ...currentProfile,
        notificationSettings: notifySettings
      });
      
      // Request browser notification permission if not already granted
      await ensureBrowserNotificationPermission();
      
      const notifyTesterAvailable = document.getElementById('notifyTesterAvailable');
      // Send test notification if enabled and at least one gamemode is selected
      if (notifyTesterAvailable && notifyTesterAvailable.checked && result.value.length > 0) {
        try {
          const testGamemode = CONFIG.GAMEMODES.find(gm => gm.id === result.value[0] && gm.id !== 'overall');
          const gamemodeName = testGamemode ? testGamemode.name : result.value[0];
          const message = `Tester availability notifications configured! You'll be notified when a tier tester becomes available for ${gamemodeName}.`;
          
          await apiService.sendTestNotification('tester_available', message);
          
          // Trigger the notification display
          showNotification({
            type: 'tester_available',
            title: 'Test Notification',
            message: message
          });
        } catch (notifError) {
          console.error('Error sending test notification:', notifError);
          // Don't fail the save if notification fails
        }
      }
      
      Swal.fire({
        icon: 'success',
        title: 'Settings Saved',
        text: result.value.length > 0 
          ? 'Tester availability notifications updated. A test notification has been sent.'
          : 'Tester availability notifications updated.',
        timer: 2000,
        showConfirmButton: false
      });
    }
  } catch (error) {
    Swal.fire({
      icon: 'error',
      title: 'Failed to Save',
      text: error.message
    });
  }
}

/**
 * Save notification settings
 * Made globally accessible for onclick handlers
 */
window.saveNotificationSettings = async function saveNotificationSettings() {
  const notifyMatchCreated = document.getElementById('notifyMatchCreated');
  const notifyMatchFinalized = document.getElementById('notifyMatchFinalized');
  const notifyTesterAvailable = document.getElementById('notifyTesterAvailable');
  
  if (!notifyMatchCreated || !notifyMatchFinalized || !notifyTesterAvailable) return;

  if (!hasLoadedNotificationSettings) {
    await window.loadNotificationSettingsOnDemand();
    Swal.fire({
      icon: 'info',
      title: 'Settings Loaded',
      text: 'Review your notification toggles, then click Save again.'
    });
    return;
  }

  try {
    const profile = await getCachedProfile();
    const notifySettings = profile.notificationSettings || {};
    notifySettings.notifyMatchCreated = notifyMatchCreated.checked;
    notifySettings.notifyMatchFinalized = notifyMatchFinalized.checked;
    notifySettings.notifyTesterAvailable = notifyTesterAvailable.checked;

    await apiService.updateProfile({ notificationSettings: notifySettings });
    AppState.setProfile({
      ...profile,
      notificationSettings: notifySettings
    });
    
    // Request browser notification permission if not already granted
    await ensureBrowserNotificationPermission();
    
    // Send test notification via backend
    try {
      const testNotif = await apiService.sendTestNotification('test', 'Your notification settings have been saved successfully! If you see this, notifications are working.');
      
      // Trigger the notification display
      showNotification({
        type: 'test',
        title: 'Test Notification',
        message: 'Your notification settings have been saved successfully! If you see this, notifications are working.'
      });
    } catch (notifError) {
      console.error('Error sending test notification:', notifError);
      // Don't fail the save if notification fails
    }
    
    Swal.fire({
      icon: 'success',
      title: 'Settings Saved',
      text: 'Notification preferences updated. A test notification has been sent.',
      timer: 2000,
      showConfirmButton: false
    });
  } catch (error) {
    Swal.fire({
      icon: 'error',
      title: 'Failed to Save',
      text: error.message
    });
  }
}

/**
 * Set up real-time notification listener
 */

// Real-time SSE connection for dashboard
function startDashboardSSE() {
  const userId = AppState.currentUser?.uid || AppState.getProfile?.()?.uid;
  if (!userId) return;
  const token = AppState.getAuthToken?.() || (firebase && firebase.auth && firebase.auth().currentUser ? firebase.auth().currentUser.getIdToken() : null);
  const sseUrl = `/api/user/${userId}/stream`;
  const evtSource = new EventSource(sseUrl, { withCredentials: true });

  evtSource.onopen = () => {
    console.log('Dashboard SSE connected');
  };
  evtSource.onerror = (e) => {
    console.warn('Dashboard SSE error', e);
    // Optionally, try to reconnect after a delay
  };

  evtSource.addEventListener('profile', (e) => {
    const data = JSON.parse(e.data);
    if (data.profile) {
      AppState.setProfile(data.profile);
      loadUserProfile();
    }
  });
  evtSource.addEventListener('queue', (e) => {
    const data = JSON.parse(e.data);
    // Update queue UI instantly
    if (data.entries) {
      updateQueueUI(data.entries);
    }
  });
  evtSource.addEventListener('matches', (e) => {
    const data = JSON.parse(e.data);
    if (data.matches) {
      updateMatchesUI(data.matches);
    }
  });
  evtSource.addEventListener('notifications', (e) => {
    const data = JSON.parse(e.data);
    if (data.notifications) {
      handleRealtimeNotifications(data.notifications);
    }
  });
  evtSource.addEventListener('onboarding', (e) => {
    const data = JSON.parse(e.data);
    if (data.completed) {
      showThemedPopup('Onboarding Complete!', 'You have completed onboarding.');
    }
  });
  evtSource.addEventListener('ping', () => {}); // Heartbeat
}

function updateQueueUI(entries) {
  // TODO: Update queue UI with new entries (match entry, queue status, etc.)
  // Example: refresh queue table, show popups, etc.
}

function updateMatchesUI(matches) {
  // TODO: Update match UI (cooldowns, timers, chat, etc.)
  // Example: update match cards, timers, etc.
}

function handleRealtimeNotifications(notifications) {
  // Show new notifications as themed popups
  Object.values(notifications).forEach((n) => {
    if (!n || !n.id) return;
    if (seenNotificationIds.has(n.id)) return;
    seenNotificationIds.add(n.id);
    if (seenNotificationIds.size > MAX_SEEN_NOTIFICATIONS) {
      const iterator = seenNotificationIds.values();
      seenNotificationIds.delete(iterator.next().value);
    }
    showThemedPopup(n.title || 'Notification', n.message || 'You have a new update.');
  });
}

function showThemedPopup(title, message) {
  // Use SweetAlert2 with custom theme for consistency
  if (typeof Swal !== 'undefined') {
    Swal.fire({
      icon: 'info',
      title: `<span style="color:var(--primary-color)">${title}</span>`,
      html: `<div style="color:var(--text-color)">${message}</div>`,
      background: 'var(--background-color, #181a1b)',
      color: 'var(--text-color, #e0e0e0)',
      showConfirmButton: false,
      timer: 4000,
      toast: true,
      position: 'top-end',
      customClass: {
        popup: 'mclb-themed-popup',
        title: 'mclb-themed-popup-title',
        content: 'mclb-themed-popup-content'
      }
    });
  } else {
    alert(title + '\n' + message);
  }
}

/**
 * Show a browser notification
 */
async function ensureBrowserNotificationPermission() {
  if (typeof window === 'undefined' || !('Notification' in window)) {
    return 'unsupported';
  }
  if (Notification.permission === 'default') {
    try {
      return await Notification.requestPermission();
    } catch (_) {
      return Notification.permission || 'default';
    }
  }
  return Notification.permission;
}

async function showNotification(notification) {
  const permission = await ensureBrowserNotificationPermission();
  if (permission === 'granted') {
    try {
      const browserNotif = new Notification(notification.title || 'Notification', {
        body: notification.message || 'You have a new update.',
        icon: '/assets/vanilla.svg',
        tag: notification.matchId || notification.id || `mclb-${Date.now()}`
      });
      browserNotif.onclick = () => {
        try {
          window.focus();
        } catch (_) {}
        if (notification.matchId) {
          window.location.href = 'testing.html';
        }
      };
    } catch (error) {
      console.warn('Browser notification failed:', error);
    }
  }

  // Also show in-page notification if possible
  if (typeof Swal !== 'undefined') {
    Swal.fire({
      icon: 'info',
      title: notification.title || 'Notification',
      text: notification.message || 'You have a new update.',
      timer: 5000,
      showConfirmButton: false,
      toast: true,
      position: 'top-end'
    });
  }
}

function startGamemodeStatsPolling() {
  if (!hasLoadedGamemodeActivity) return;
  if (gamemodeStatsInterval) clearInterval(gamemodeStatsInterval);

  gamemodeStatsInterval = setInterval(async () => {
    if (document.visibilityState !== 'visible') return;
    if (Date.now() < gamemodeStatsBackoffUntilMs) return;
    await loadGamemodeStats();
  }, CONFIG.QUEUE_POLL_INTERVAL || 10000);
}

async function loadGamemodeStats() {
  const listEl = document.getElementById('gamemodeStatsList');
  const updatedEl = document.getElementById('gamemodeStatsUpdated');
  const regionFilter = document.getElementById('regionFilter');
  if (!listEl) return;

  try {
    const region = regionFilter ? regionFilter.value : '';
    const resp = await apiService.getDashboardGamemodeStats(region);
    if (!resp?.success || !resp?.statsByGamemode) {
      throw new Error(resp?.message || 'Failed to load gamemode stats');
    }

    renderGamemodeStats(resp.statsByGamemode);
    hasLoadedGamemodeActivity = true;

    if (updatedEl) {
      const t = resp.generatedAt ? new Date(resp.generatedAt) : new Date();
      updatedEl.textContent = `Updated ${t.toLocaleTimeString()}`;
    }
  } catch (error) {
    // If we get rate-limited, back off for 60 seconds
    if (error?.message?.includes('429') || error?.status === 429) {
      gamemodeStatsBackoffUntilMs = Date.now() + 60000;
      return;
    }

    console.error('Error loading gamemode stats:', error);
    listEl.innerHTML = `
      <div class="alert alert-warning">
        <i class="fas fa-exclamation-triangle"></i> Unable to load gamemode activity right now.
      </div>
    `;
  }
}

/**
 * Handle region filter change
 */
function handleRegionFilterChange() {
  if (!hasLoadedGamemodeActivity) {
    window.loadGamemodeActivityOnDemand();
    return;
  }
  loadGamemodeStats();
}

// Mobile animation state for gamemode stats
let mobileStatsAnimationInterval = null;
let currentMobileColumn = 0;

function renderGamemodeStats(statsByGamemode) {
  const listEl = document.getElementById('gamemodeStatsList');
  if (!listEl) return;

  const gamemodes = (CONFIG?.GAMEMODES || []).filter(g => g.id && g.id !== 'overall');
  if (gamemodes.length === 0) {
    listEl.innerHTML = '<div class="text-muted">No gamemodes configured.</div>';
    return;
  }

  const rowsHtml = gamemodes.map(gm => {
    const s = statsByGamemode[gm.id] || { testersAvailable: 0, playersQueued: 0, activeMatches: 0 };
    return `
      <div class="gamemode-stats-row">
        <div class="gamemode-stats-left">
          <img class="gamemode-stats-icon" src="${gm.icon}" alt="${escapeHtml(gm.name)} icon">
          <span class="gamemode-stats-name">${escapeHtml(gm.name)}</span>
        </div>
        <div class="gamemode-stats-metrics">
          <div class="gamemode-stats-metric" data-column="0">
            <span class="gamemode-stats-metric-label">Testers</span>
            <span class="gamemode-stats-metric-value">${Number(s.testersAvailable || 0)}</span>
          </div>
          <div class="gamemode-stats-metric" data-column="1">
            <span class="gamemode-stats-metric-label">Queued</span>
            <span class="gamemode-stats-metric-value">${Number(s.playersQueued || 0)}</span>
          </div>
          <div class="gamemode-stats-metric" data-column="2">
            <span class="gamemode-stats-metric-label">Matches</span>
            <span class="gamemode-stats-metric-value">${Number(s.activeMatches || 0)}</span>
          </div>
        </div>
      </div>
    `;
  }).join('');

  listEl.innerHTML = `
    <div class="gamemode-stats-grid">
      ${rowsHtml}
    </div>
  `;
  
  // Initialize mobile animation
  initMobileStatsAnimation();
}

/**
 * Initialize mobile animation for gamemode stats
 */
function initMobileStatsAnimation() {
  // Clear any existing interval
  if (mobileStatsAnimationInterval) {
    clearInterval(mobileStatsAnimationInterval);
    mobileStatsAnimationInterval = null;
  }
  
  // Only animate on mobile (screen width < 768px)
  const isMobile = window.innerWidth < 768;
  
  if (!isMobile) {
    // Show all columns on desktop
    const allMetrics = document.querySelectorAll('.gamemode-stats-metric');
    allMetrics.forEach(metric => {
      metric.style.display = 'flex';
    });
    return;
  }
  
  // Start mobile animation
  currentMobileColumn = 0;
  showMobileColumn(currentMobileColumn);
  
  // Rotate through columns every 3 seconds
  mobileStatsAnimationInterval = setInterval(() => {
    currentMobileColumn = (currentMobileColumn + 1) % 3;
    showMobileColumn(currentMobileColumn);
  }, 3000);
}

/**
 * Show specific column on mobile
 */
function showMobileColumn(columnIndex) {
  const allMetrics = document.querySelectorAll('.gamemode-stats-metric');
  
  allMetrics.forEach(metric => {
    const column = parseInt(metric.getAttribute('data-column'));
    if (column === columnIndex) {
      metric.style.display = 'flex';
      metric.style.animation = 'fadeIn 0.5s ease-in-out';
    } else {
      metric.style.display = 'none';
    }
  });
}

// Handle window resize to restart animation
let resizeTimeout;
window.addEventListener('resize', () => {
  clearTimeout(resizeTimeout);
  resizeTimeout = setTimeout(() => {
    initMobileStatsAnimation();
  }, 250);
});

/**
 * Load user profile
 */
async function loadUserProfile() {
  try {
    const profile = await getCachedProfile();

    AppState.setProfile(profile);
    if (profile?.region) {
      const preferredRegionCheckbox = document.querySelector(`.player-region-checkbox[value="${profile.region}"]`);
      if (preferredRegionCheckbox) {
        preferredRegionCheckbox.checked = true;
      }
    }
  } catch (error) {
    console.error('Error loading profile:', error);
  }
}

// Cooldown tracking for buttons
let joinQueueCooldownUntil = 0;

/**
 * Handle join queue
 */
async function handleJoinQueue(event) {
  event.preventDefault();
  const tierTester = isTierTesterUser();

  const now = Date.now();
  if (now < joinQueueCooldownUntil) {
    const remaining = Math.ceil((joinQueueCooldownUntil - now) / 1000);
    Swal.fire({
      icon: 'warning',
      title: 'Cooldown Active',
      text: `Please wait ${remaining} second${remaining !== 1 ? 's' : ''} before joining the queue again.`,
      timer: 2000,
      showConfirmButton: false
    });
    return;
  }

  const gamemodes = getSelectedPlayerQueueGamemodes();
  const regions = getSelectedPlayerQueueRegions();
  const serverIP = (document.getElementById('serverIP').value || '').trim();
  const joinBtn = document.getElementById('joinQueueBtn');

  if (gamemodes.length === 0 || regions.length === 0 || !serverIP) {
    Swal.fire({
      icon: 'warning',
      title: 'Missing Fields',
      text: 'Please select at least one gamemode, one region, and a server before joining queue.'
    });
    return;
  }

  // Check if Minecraft username is linked
  if (!AppState.userProfile?.minecraftUsername) {
    Swal.fire({
      icon: 'warning',
      title: 'Minecraft Username Required',
      text: 'Please link your Minecraft username in Account settings first.',
      confirmButtonText: 'Go to Account',
      showCancelButton: true
    }).then((result) => {
      if (result.isConfirmed) {
        window.location.href = 'account.html';
      }
    });
    return;
  }

  if (!tierTester) {
    const cooldownCheck = await getBlockedQueueCooldown(gamemodes);
    if (cooldownCheck) {
      const timeLeft = formatTimeLeft(cooldownCheck.timeLeft);
      const reason = cooldownCheck.reason || 'You recently participated in a match in this gamemode.';
      Swal.fire({
        icon: 'warning',
        title: 'Queue Cooldown Active',
        html: `<p>${reason}</p><br>You can join the <strong>${cooldownCheck.gamemode.toUpperCase()}</strong> queue again in:<br><br><div style="font-size: 1.2em; font-weight: bold; color: var(--accent-color);">${timeLeft}</div>`,
        confirmButtonText: 'OK'
      });
      return;
    }
  }

  joinBtn.disabled = true;
  joinBtn.innerHTML = `<i class="fas fa-spinner fa-spin"></i> ${tierTester ? 'Joining as tester...' : 'Joining...'}`;

  try {
    if (tierTester) {
      await apiService.setTesterAvailability(true, gamemodes, regions, serverIP);

      const stayInQueueCheckbox = document.getElementById('stayInQueueAfterMatch');
      if (stayInQueueCheckbox) {
        await apiService.updateProfile({
          stayInQueueAfterMatch: stayInQueueCheckbox.checked
        });
      }

      localStorage.setItem('queueJoinTime', Date.now());
      localStorage.setItem('queueGamemode', JSON.stringify(gamemodes));
      localStorage.setItem('queueRegion', JSON.stringify(regions));
      window.location.reload();
      return;
    }

    const response = await apiService.joinQueue(gamemodes, regions, serverIP);
    

    if (response.matched) {
      // Immediate match found.
      window.location.href = `testing.html?matchId=${response.matchId}`;
      return;
    } else {
      // Added to the unified queue and waiting for a compatible match.
      // Store queue join time for auto-kick functionality
      localStorage.setItem('queueJoinTime', Date.now());
      localStorage.setItem('queueGamemode', JSON.stringify(gamemodes));
      localStorage.setItem('queueRegion', JSON.stringify(regions));
      window.location.reload();
      return;
    }
  } catch (error) {
    // Check if this is a skill level error
    if (error.message && error.message.includes('skill level')) {
      Swal.fire({
        icon: 'warning',
        title: 'Skill Level Required',
        text: error.message,
        confirmButtonText: 'Go to Account Settings',
        showCancelButton: true,
        cancelButtonText: 'Cancel'
      }).then((result) => {
        if (result.isConfirmed) {
          window.location.href = 'account.html#skill-levels';
        }
      });
    } else if (error?.code === 'GAMEMODE_RETIRED' || error?.data?.code === 'GAMEMODE_RETIRED') {
      Swal.fire({
        icon: 'warning',
        title: 'Gamemode Retired',
        text: error.message || 'You have retired from this gamemode and cannot join its queue.'
      });
    } else if (error.message && error.message.includes('not whitelisted')) {
      // Server IP not whitelisted error
      Swal.fire({
        icon: 'warning',
        title: 'Server Not Whitelisted',
        html: `
          <p>${error.message}</p>
          <p style="margin-top: 1rem; color: var(--text-muted); font-size: 0.9rem;">
            Please use the "Select Server" button to choose from approved servers.
          </p>
        `,
        confirmButtonText: 'OK'
      });
    } else {
      Swal.fire({
        icon: 'error',
        title: 'Failed to Join Queue',
        text: error.message
      });
    }
  } finally {
    joinBtn.disabled = false;
    joinBtn.innerHTML = `<i class="fas fa-play"></i> ${getJoinQueueButtonLabel()}`;
    updateJoinQueueButtonState();
  }
}

/**
 * Update join queue button state based on cooldown
 */
function updateJoinQueueButtonState() {
  const joinBtn = document.getElementById('joinQueueBtn');
  if (!joinBtn) return;

  const now = Date.now();
  if (now < joinQueueCooldownUntil) {
    const remaining = Math.ceil((joinQueueCooldownUntil - now) / 1000);
    joinBtn.disabled = true;
    joinBtn.innerHTML = `<i class="fas fa-clock"></i> Wait ${remaining}s`;
  } else {
    joinBtn.disabled = false;
    joinBtn.innerHTML = `<i class="fas fa-play"></i> ${getJoinQueueButtonLabel()}`;
  }
}

// Update button state every second during cooldown
if (joinQueueButtonInterval) clearInterval(joinQueueButtonInterval);
joinQueueButtonInterval = setInterval(updateJoinQueueButtonState, 1000);

/**
 * Handle leave queue
 */
async function handleLeaveQueue() {
  try {
    await apiService.leaveQueue();
    window.location.reload();
    return;
  } catch (error) {
    Swal.fire({
      icon: 'error',
      title: 'Failed to Leave Queue',
      text: error.message
    });
  }
}

// Queue waiting timer
let queueWaitingInterval = null;

/**
 * Check queue status
 */
async function checkQueueStatus() {
  try {
    const status = await apiService.getQueueStatus();

    if (status.inQueue) {
      document.getElementById('queueForm').style.display = 'none';
      document.getElementById('queueStatus').style.display = 'block';
      document.getElementById('queueGamemode').textContent = formatQueueSelectionText(status.queueEntry.gamemodes, status.queueEntry.gamemode, (value) => value.toUpperCase());
      document.getElementById('queueRegion').textContent = formatQueueSelectionText(status.queueEntry.regions, status.queueEntry.region);

      // Start waiting timer
      startQueueWaitingTimer(status.queueEntry);

      // Update queue statistics
      await updateQueueStatistics(status);

      // Check for auto-kick after 5 minutes if no testers available
      checkQueueTimeout(status.queueEntry);
    } else {
      document.getElementById('queueForm').style.display = 'block';
      document.getElementById('queueStatus').style.display = 'none';

      // Clear waiting timer
      if (queueWaitingInterval) {
        clearInterval(queueWaitingInterval);
        queueWaitingInterval = null;
      }

      // Clear stored queue data when not in queue
      localStorage.removeItem('queueJoinTime');
      localStorage.removeItem('queueGamemode');
      localStorage.removeItem('queueRegion');
    }
  } catch (error) {
    console.error('Error checking queue status:', error);
  }
}

/**
 * Start timer to show waiting time
 */
function startQueueWaitingTimer(queueEntry) {
  // Clear existing timer
  if (queueWaitingInterval) {
    clearInterval(queueWaitingInterval);
  }

  const startTime = new Date(queueEntry.joinedAt || Date.now());
  const waitingTimeElement = document.getElementById('queueWaitingTime');

  queueWaitingInterval = setInterval(() => {
    const now = new Date();
    const elapsed = now - startTime;
    const minutes = Math.floor(elapsed / (1000 * 60));
    const seconds = Math.floor((elapsed % (1000 * 60)) / 1000);

    if (minutes > 0) {
      waitingTimeElement.textContent = `Waiting ${minutes}m ${seconds}s`;
    } else {
      waitingTimeElement.textContent = `Waiting ${seconds}s`;
    }
  }, 1000);
}

/**
 * Update queue statistics display
 */
async function updateQueueStatistics(queueState) {
  try {
    const queueEntry = queueState?.queueEntry || queueState;
    const queueSummary = queueState?.queueSummary || null;

    if (queueSummary) {
      const playersInQueue = Number(queueSummary.compatiblePlayers || 0);
      const availableTesters = Number(queueSummary.compatibleTesters || 0);
      const rolePreference = queueSummary.rolePreference === 'tester' ? 'tester' : 'player';
      const position = Math.max(1, Number(queueSummary.yourPosition || 1));
      const etaMinutes = Number.isFinite(Number(queueSummary.estimatedWaitMinutes))
        ? Math.max(1, Number(queueSummary.estimatedWaitMinutes))
        : null;

      document.getElementById('queuePlayersCount').textContent = playersInQueue;
      document.getElementById('queueAvailableTesters').textContent = availableTesters;
      document.getElementById('queuePosition').textContent = position;

      const statusTextEl = document.getElementById('queueStatusText');
      const etaEl = document.getElementById('queueEta');
      const gamemodes = Array.isArray(queueEntry?.gamemodes) && queueEntry.gamemodes.length > 0
        ? queueEntry.gamemodes
        : (queueEntry?.gamemode ? [queueEntry.gamemode] : []);
      const gamemodeName = formatQueueSelectionText(gamemodes, null, (value) => {
        const gamemode = CONFIG.GAMEMODES.find((gm) => gm.id === value);
        return gamemode?.name || value.toUpperCase();
      });

      if (rolePreference === 'tester') {
        if (playersInQueue > 0) {
          if (statusTextEl) {
            statusTextEl.innerHTML = `Compatible players are queued for ${gamemodeName}`;
            statusTextEl.style.color = 'var(--success-color)';
          }
          if (etaEl) {
            etaEl.textContent = etaMinutes ? `~${etaMinutes} min` : 'Calculating...';
          }
        } else {
          if (statusTextEl) {
            statusTextEl.innerHTML = `Waiting for a compatible player in ${gamemodeName}...`;
            statusTextEl.style.color = 'var(--warning-color)';
          }
          if (etaEl) {
            etaEl.textContent = 'No players queued';
          }
        }
      } else if (availableTesters > 0) {
        if (statusTextEl) {
          statusTextEl.innerHTML = `Compatible tier testers are queued for ${gamemodeName}`;
          statusTextEl.style.color = 'var(--success-color)';
        }
        if (etaEl) {
          etaEl.textContent = etaMinutes ? `~${etaMinutes} min` : 'Calculating...';
        }
      } else {
        if (statusTextEl) {
          statusTextEl.innerHTML = `Waiting for a compatible tier tester in ${gamemodeName}...`;
          statusTextEl.style.color = 'var(--warning-color)';
        }
        if (etaEl) {
          etaEl.textContent = 'No tier testers queued';
        }
      }

      return;
    }

    // Get queue stats
    const queueStats = await apiService.getQueueStats();
    const gamemodes = Array.isArray(queueEntry?.gamemodes) && queueEntry.gamemodes.length > 0
      ? queueEntry.gamemodes
      : (queueEntry?.gamemode ? [queueEntry.gamemode] : []);
    const regions = Array.isArray(queueEntry?.regions) && queueEntry.regions.length > 0
      ? queueEntry.regions
      : (queueEntry?.region ? [queueEntry.region] : []);

    if (!gamemodes.length || !regions.length) {
      document.getElementById('queuePlayersCount').textContent = '-';
      document.getElementById('queueAvailableTesters').textContent = '-';
      document.getElementById('queuePosition').textContent = '-';
      const etaElMissing = document.getElementById('queueEta');
      if (etaElMissing) etaElMissing.textContent = '-';
      return;
    }

    const { playersInQueue, availableTesters } = calculateQueueTotals(queueStats, queueEntry);
    document.getElementById('queuePlayersCount').textContent = playersInQueue;

    document.getElementById('queueAvailableTesters').textContent = availableTesters;

    // Calculate approximate queue position (simplified)
    const position = Math.max(1, Math.floor(playersInQueue / Math.max(1, availableTesters)));
    document.getElementById('queuePosition').textContent = position;

    // Update status text based on tester availability
    const statusTextEl = document.getElementById('queueStatusText');
    const etaEl = document.getElementById('queueEta');
    
    const gamemodeName = formatQueueSelectionText(gamemodes, null, (value) => {
      const gamemode = CONFIG.GAMEMODES.find((gm) => gm.id === value);
      return gamemode?.name || value.toUpperCase();
    });
    
    if (availableTesters > 0) {
      if (statusTextEl) {
        statusTextEl.innerHTML = `Compatible tier testers are queued for ${gamemodeName}`;
        statusTextEl.style.color = 'var(--success-color)';
      }
      if (etaEl) {
        const etaMinutes = Math.max(1, Math.ceil(position / Math.max(1, availableTesters)) * 2);
        etaEl.textContent = `~${etaMinutes} min`;
      }
    } else {
      if (statusTextEl) {
        statusTextEl.innerHTML = `Waiting for a compatible tier tester in ${gamemodeName}...`;
        statusTextEl.style.color = 'var(--warning-color)';
      }
      if (etaEl) {
        etaEl.textContent = 'No tier testers queued';
      }
    }

  } catch (error) {
    console.error('Error updating queue statistics:', error);
    // Set fallback values
    document.getElementById('queuePlayersCount').textContent = '-';
    document.getElementById('queueAvailableTesters').textContent = '-';
    document.getElementById('queuePosition').textContent = '-';
    const etaEl = document.getElementById('queueEta');
    if (etaEl) etaEl.textContent = '-';
  }
}

/**
 * Check for queue timeout and auto-kick if no testers available for 5 minutes
 */
async function checkQueueTimeout(queueEntry) {
  const joinTime = localStorage.getItem('queueJoinTime');
  if (!joinTime || !queueEntry) return;

  const elapsed = Date.now() - parseInt(joinTime);
  const fiveMinutes = 5 * 60 * 1000; // 5 minutes in milliseconds

  if (elapsed >= fiveMinutes) {
    try {
      const stats = await apiService.getQueueStats();
      const { availableTesters } = calculateQueueTotals(stats, queueEntry);

      if (availableTesters === 0) {
        // No testers queued and been waiting 5+ minutes - auto leave queue
        console.log('Auto-kicking from queue: no tier testers queued for 5+ minutes');

        await apiService.leaveQueue();
        const isRegularPlayer = !(typeof AppState !== 'undefined' && AppState.isTierTester && AppState.isTierTester());

        // Clear stored queue data
        localStorage.removeItem('queueJoinTime');
        localStorage.removeItem('queueGamemode');
        localStorage.removeItem('queueRegion');

        Swal.fire({
          icon: 'info',
          title: 'Auto-Left Queue',
          text: 'You were automatically removed from the queue because no compatible tier testers were queued for 5 minutes.',
          confirmButtonText: 'OK'
        });

        // Update UI
        document.getElementById('queueForm').style.display = 'block';
        document.getElementById('queueStatus').style.display = 'none';
        if (isRegularPlayer) {
          window.location.reload();
        }
      }
    } catch (error) {
      console.error('Error checking queue timeout:', error);
    }
  }
}

/**
 * Check for active match
 */
async function checkActiveMatch() {
  try {
    const response = await apiService.getActiveMatch();
    
    if (response.hasMatch) {
      activeMatch = response.match;
      document.getElementById('activeMatchCard').style.display = 'block';
      
      const isPlayer = activeMatch.playerId === AppState.getUserId();
      const opponent = isPlayer ? activeMatch.testerUsername : activeMatch.playerUsername;

      // Check if tester has joined
      const testerJoined = activeMatch.pagestats && activeMatch.pagestats.testerJoined;
      let countdownHtml = '';

      if (isPlayer && !testerJoined && activeMatch.testerJoinTimeout) {
        // Show countdown for player when tester hasn't joined yet
        const startedAt = new Date(activeMatch.testerJoinTimeout.startedAt);
        const timeoutMinutes = activeMatch.testerJoinTimeout.timeoutMinutes || 3;
        const timeoutMs = timeoutMinutes * 60 * 1000;
        const endTime = new Date(startedAt.getTime() + timeoutMs);
        const now = new Date();
        const remainingMs = endTime - now;

        if (remainingMs > 0) {
          const remainingSeconds = Math.ceil(remainingMs / 1000);
          const minutes = Math.floor(remainingSeconds / 60);
          const seconds = remainingSeconds % 60;

          countdownHtml = `
            <div class="alert alert-warning mt-2">
              <h5><i class="fas fa-clock"></i> Waiting for Tier Tester to Join</h5>
              <p class="mb-1">Time remaining: <span id="testerCountdown">${minutes}:${seconds.toString().padStart(2, '0')}</span></p>
              <div class="progress">
                <div class="progress-bar progress-bar-striped progress-bar-animated bg-warning"
                     style="width: ${(remainingMs / timeoutMs) * 100}%"></div>
              </div>
              <small class="text-muted">If the tester doesn't join within 3 minutes, the match will be cancelled.</small>
            </div>
          `;

          // Start countdown timer
          startTesterCountdown(endTime);
        } else {
          countdownHtml = `
            <div class="alert alert-danger mt-2">
              <h5><i class="fas fa-exclamation-triangle"></i> Tester Failed to Join</h5>
              <p>The tier tester did not join within the time limit. This match will be cancelled.</p>
            </div>
          `;
        }
      }
      
      document.getElementById('activeMatchInfo').innerHTML = `
        <div class="alert alert-success">
          <h4><i class="fas fa-gamepad"></i> Match Found!</h4>
          <p><strong>Gamemode:</strong> ${activeMatch.gamemode.toUpperCase()}</p>
          <p><strong>Opponent:</strong> ${escapeHtml(opponent)}</p>
          <p><strong>Your Role:</strong> ${isPlayer ? 'Player' : 'Tier Tester'}</p>
          <p><strong>Region:</strong> ${activeMatch.region}</p>
          <p><strong>Server IP:</strong> ${escapeHtml(activeMatch.serverIP)}</p>
          ${activeMatch.roleAssignment?.explanation ? `<p><strong>Role Assignment:</strong> ${escapeHtml(isPlayer ? (activeMatch.roleAssignment.playerReason || activeMatch.roleAssignment.explanation) : (activeMatch.roleAssignment.testerReason || activeMatch.roleAssignment.explanation))}</p>` : ''}
        </div>
        ${countdownHtml}
      `;
      
      openTestingPage();
    } else {
      activeMatch = null;
      document.getElementById('activeMatchCard').style.display = 'none';
    }
  } catch (error) {
    console.error('Error checking active match:', error);
  }
}

function startActiveMatchPolling() {
  if (activeMatchPollInterval) {
    clearInterval(activeMatchPollInterval);
  }

  activeMatchPollInterval = setInterval(() => {
    if (document.visibilityState !== 'visible') {
      return;
    }
    checkActiveMatch();
  }, DASHBOARD_ACTIVE_MATCH_POLL_MS);
}

/**
 * Start countdown timer for tester join timeout
 */
let testerCountdownInterval = null;

function startTesterCountdown(endTime) {
  // Clear any existing countdown
  if (testerCountdownInterval) {
    clearInterval(testerCountdownInterval);
  }

  testerCountdownInterval = setInterval(() => {
    const now = new Date();
    const remainingMs = endTime - now;

    if (remainingMs <= 0) {
      clearInterval(testerCountdownInterval);
      testerCountdownInterval = null;
      // Refresh the active match display
      checkActiveMatch();
      return;
    }

    const remainingSeconds = Math.ceil(remainingMs / 1000);
    const minutes = Math.floor(remainingSeconds / 60);
    const seconds = remainingSeconds % 60;

    const countdownElement = document.getElementById('testerCountdown');
    if (countdownElement) {
      countdownElement.textContent = `${minutes}:${seconds.toString().padStart(2, '0')}`;
    }
  }, 1000);
}

/**
 * Open testing page
 */
function openTestingPage() {
  if (!activeMatch) return;
  
  sessionStorage.setItem('testingPageOpen', 'true');
  const url = `testing.html?matchId=${activeMatch.matchId}`;
  window.location.href = url;
}

/**
 * Get time ago string from date
 */
function getTimeAgo(date) {
  const now = new Date();
  const diffMs = now - date;
  const diffSeconds = Math.floor(diffMs / 1000);
  const diffMinutes = Math.floor(diffSeconds / 60);
  const diffHours = Math.floor(diffMinutes / 60);
  const diffDays = Math.floor(diffHours / 24);

  if (diffSeconds < 60) {
    return 'Just now';
  } else if (diffMinutes < 60) {
    return `${diffMinutes}m ago`;
  } else if (diffHours < 24) {
    return `${diffHours}h ago`;
  } else {
    return `${diffDays}d ago`;
  }
}
/**
 * Copy match ID to clipboard
 */
async function copyMatchId(matchId, buttonEl = null) {
  if (!matchId) return;
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(matchId);
    } else {
      const textarea = document.createElement('textarea');
      textarea.value = matchId;
      textarea.setAttribute('readonly', '');
      textarea.style.position = 'absolute';
      textarea.style.left = '-9999px';
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand('copy');
      document.body.removeChild(textarea);
    }

    if (buttonEl) {
      const original = buttonEl.innerHTML;
      buttonEl.innerHTML = '<i class="fas fa-check"></i>';
      setTimeout(() => {
        buttonEl.innerHTML = original;
      }, 900);
    }
  } catch (error) {
    console.error('Failed to copy match ID:', error);
  }
}

function openMatchWithFallback(matchId, preferNewTab = true) {
  if (!matchId) return false;
  const url = `testing.html?matchId=${encodeURIComponent(matchId)}`;

  if (!preferNewTab) {
    window.location.href = url;
    return true;
  }

  let opened = null;
  try {
    opened = window.open(url, '_blank', 'noopener,noreferrer');
  } catch (_) {
    opened = null;
  }

  // Some browsers/extensions block popups and return null/undefined.
  if (!opened || opened.closed || typeof opened.closed === 'undefined') {
    window.location.href = url;
    return false;
  }

  return true;
}

/**
 * Load and display recent matches
 */
async function loadRecentMatches() {
  try {
    const response = await apiService.getRecentMatches(5);
    const container = document.getElementById('recentMatchesContainer');

    if (!response.matches || response.matches.length === 0) {
      container.innerHTML = `
        <div class="text-center text-muted py-4">
          <i class="fas fa-history fa-2x mb-2"></i>
          <p>No recent matches found</p>
        </div>
      `;
      hasLoadedRecentMatches = true;
      return;
    }

    const matchesHtml = response.matches.map(match => {
      const date = new Date(match.finalizedAt || match.createdAt);
      const timeAgo = getTimeAgo(date);
      const matchIdRaw = match.matchId || match.id || '';
      const matchIdSafe = escapeHtml(matchIdRaw);

      // Determine match result and reason
      let resultClass = 'text-muted';
      let resultText = 'Unknown';
      let reasonText = '';

      if (match.finalizationData) {
        const { type, reason, abortedBy } = match.finalizationData;

        if (match.userScore > match.opponentScore) {
          resultClass = 'text-success';
          resultText = 'Won';
        } else if (match.userScore < match.opponentScore) {
          resultClass = 'text-danger';
          resultText = 'Lost';
        } else {
          resultClass = 'text-warning';
          resultText = 'Draw';
        }

        // Add reason for ended matches
        if (type === 'forfeit') {
          reasonText = ` - ${reason}`;
        } else if (type === 'no_show') {
          reasonText = ` - ${reason}`;
        }
      }

      const gamemodeIcon = CONFIG.GAMEMODES.find(gm => gm.id === match.gamemode)?.icon || 'assets/vanilla.svg';

      return `
        <div class="match-history-item" style="display: flex; align-items: center; justify-content: space-between; padding: 0.75rem; border-radius: 0.5rem; background: rgba(255,255,255,0.05); margin-bottom: 0.5rem;">
          <div style="display: flex; align-items: center; gap: 0.75rem;">
            <img src="${gamemodeIcon}" alt="${match.gamemode}" style="width: 32px; height: 32px; border-radius: 4px;">
            <div>
              <div style="font-weight: 600; margin-bottom: 0.25rem;">
                vs ${escapeHtml(match.opponentName)}
                <span class="badge badge-sm ${match.userRole === 'tester' ? 'badge-primary' : 'badge-secondary'}" style="font-size: 0.7rem;">
                  ${match.userRole === 'tester' ? 'Tester' : 'Player'}
                </span>
              </div>
              <div style="font-size: 0.85rem; color: var(--text-secondary);">
                ${match.gamemode.toUpperCase()} - ${timeAgo}${reasonText}
              </div>
              <div style="font-size: 0.72rem; color: var(--text-muted); display: flex; align-items: center; gap: 0.4rem; flex-wrap: wrap; margin-top: 0.2rem;">
                <span>Match ID: <code style="font-size: 0.7rem; background: rgba(255,255,255,0.08); padding: 0.1rem 0.35rem; border-radius: 4px;">${matchIdSafe || 'N/A'}</code></span>
                ${matchIdRaw ? `<button type="button" class="btn btn-secondary btn-sm" style="padding: 0.15rem 0.35rem; font-size: 0.7rem; line-height: 1;" data-match-id="${matchIdSafe}" onclick="copyMatchId(this.dataset.matchId, this)" title="Copy Match ID"><i class="fas fa-copy"></i></button>` : ''}
                ${matchIdRaw ? `<button type="button" class="btn btn-secondary btn-sm" style="padding: 0.15rem 0.35rem; font-size: 0.7rem; line-height: 1;" data-match-id="${matchIdSafe}" onclick="openMatchWithFallback(this.dataset.matchId, true)">Open Match</button>` : ''}
              </div>
            </div>
          </div>
          <div style="text-align: right;">
            <div style="font-weight: 600; font-size: 1.1rem;">
              ${match.userScore || 0} - ${match.opponentScore || 0}
            </div>
            <div class="${resultClass}" style="font-size: 0.85rem; font-weight: 500;">
              ${resultText}
            </div>
          </div>
        </div>
      `;
    }).join('');

    container.innerHTML = matchesHtml;
    hasLoadedRecentMatches = true;
  } catch (error) {
    console.error('Error loading recent matches:', error);
    const container = document.getElementById('recentMatchesContainer');
    container.innerHTML = `
      <div class="text-center text-muted py-4">
        <i class="fas fa-exclamation-triangle fa-2x mb-2"></i>
        <p>Failed to load recent matches</p>
      </div>
    `;
  }
}

/**
 * Load and display queue cooldowns
 */
async function loadQueueCooldowns() {
  try {
    const profile = await getCachedProfile();
    const cooldownsContainer = document.getElementById('queueCooldowns');
    const lastQueueJoins = profile.lastQueueJoins || {};
    const lastTestCompletions = profile.lastTestCompletions || {};
    const now = new Date();
    const cooldownMs = 30 * 60 * 1000; // 30 minutes

    const activeCooldowns = [];

    CONFIG.GAMEMODES.forEach(gamemode => {
      if (gamemode.id === 'overall') return;

      // Check testing cooldowns (higher priority)
      const lastTestCompletion = lastTestCompletions[gamemode.id];
      if (lastTestCompletion) {
        const lastTestTime = new Date(lastTestCompletion);
        const timePassed = now - lastTestTime;

        if (timePassed < cooldownMs) {
          const timeLeft = cooldownMs - timePassed;
          activeCooldowns.push({
            gamemode: gamemode.id,
            name: gamemode.name,
            timeLeft,
            type: 'testing',
            reason: 'Recently tested'
          });
          return; // Skip queue cooldown check if testing cooldown is active
        }
      }

      // Check regular queue cooldowns
      const lastJoin = lastQueueJoins[gamemode.id];
      if (lastJoin) {
        const lastJoinTime = new Date(lastJoin);
        const timePassed = now - lastJoinTime;

        if (timePassed < cooldownMs) {
          const timeLeft = cooldownMs - timePassed;
          activeCooldowns.push({
            gamemode: gamemode.id,
            name: gamemode.name,
            timeLeft,
            type: 'queue',
            reason: 'Recent match'
          });
        }
      }
    });

    if (activeCooldowns.length === 0) {
      cooldownsContainer.innerHTML = '';
      return;
    }

    cooldownsContainer.innerHTML = `
      <div class="alert alert-info" style="padding: 1rem;">
        <h6 style="margin-bottom: 0.75rem;"><i class="fas fa-clock"></i> Queue Cooldowns</h6>
        <div id="cooldownTimers">
          ${activeCooldowns.map(cooldown => `
            <div class="cooldown-item" style="margin-bottom: 1rem; padding: 0.75rem; border-radius: 0.5rem; background: rgba(255,255,255,0.05);">
              <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.25rem;">
                <span style="font-weight: 600;">${cooldown.name}:</span>
                <span class="cooldown-timer" data-gamemode="${cooldown.gamemode}" data-timeleft="${cooldown.timeLeft}" style="font-weight: bold; color: ${cooldown.type === 'testing' ? '#ff9800' : 'var(--accent-color)'};">
                  ${formatTimeLeft(cooldown.timeLeft)}
                </span>
              </div>
              <div style="font-size: 0.85rem; color: var(--text-secondary);">
                <i class="fas fa-${cooldown.type === 'testing' ? 'user-check' : 'clock'}"></i>
                ${cooldown.reason || 'Cooldown active'}
              </div>
            </div>
          `).join('')}
        </div>
      </div>
    `;

    // Start updating timers
    updateCooldownTimers();

  } catch (error) {
    console.error('Error loading queue cooldowns:', error);
  }
}

/**
 * Update cooldown timers every second
 */
function updateCooldownTimers() {
  const timers = document.querySelectorAll('.cooldown-timer');

  if (timers.length === 0) return;

  const interval = setInterval(() => {
    let allExpired = true;

    timers.forEach(timer => {
      const timeLeft = parseInt(timer.dataset.timeleft);
      const newTimeLeft = timeLeft - 1000;

      if (newTimeLeft > 0) {
        timer.textContent = formatTimeLeft(newTimeLeft);
        timer.dataset.timeleft = newTimeLeft;
        allExpired = false;
      } else {
        timer.textContent = '00:00:00';
        timer.style.color = 'var(--success-color)';
      }
    });

    if (allExpired) {
      clearInterval(interval);
      // Refresh cooldowns after a short delay
      setTimeout(() => loadQueueCooldowns(), 2000);
    }
  }, 1000);
}

/**
 * Handle set unavailable
 */
async function handleSetUnavailable() {
  try {
    await apiService.setTesterAvailability(false, [], []);

    await Swal.fire({
      icon: 'success',
      title: 'Removed from Queue',
      timer: 1500,
      showConfirmButton: false
    });
    window.location.reload();
    return;
  } catch (error) {
    Swal.fire({
      icon: 'error',
      title: 'Failed',
      text: error.message
    });
  }
}

// Add player functionality removed - players are now created automatically through account linking

/**
 * Check queue cooldown for gamemode
 */
async function checkQueueCooldown(gamemode) {
  try {
    const profile = await getCachedProfile();

    // Check for testing cooldown first (30 minutes after being tested)
    const lastTestCompletion = profile.lastTestCompletions?.[gamemode];
    if (lastTestCompletion) {
      const lastTestTime = new Date(lastTestCompletion);
      const now = new Date();
      const testCooldownMs = 30 * 60 * 1000; // 30 minutes in milliseconds

      if (now - lastTestTime < testCooldownMs) {
        const timeLeft = testCooldownMs - (now - lastTestTime);
        return {
          allowed: false,
          timeLeft,
          reason: 'You were recently tested in this gamemode. Please wait before queuing again.'
        };
      }
    }

    // Check regular queue cooldown (existing logic)
    const lastQueueJoin = profile.lastQueueJoins?.[gamemode];
    if (!lastQueueJoin) {
      return { allowed: true };
    }

    const lastJoinTime = new Date(lastQueueJoin);
    const now = new Date();
    const cooldownMs = 30 * 60 * 1000; // 30 minutes in milliseconds

    if (now - lastJoinTime >= cooldownMs) {
      return { allowed: true };
    }

    const timeLeft = cooldownMs - (now - lastJoinTime);
    return { allowed: false, timeLeft, reason: 'You recently joined a match in this gamemode.' };
  } catch (error) {
    console.error('Error checking queue cooldown:', error);
    // Allow joining if we can't check cooldown
    return { allowed: true };
  }
}

/**
 * Format time left in countdown format
 */
function formatTimeLeft(timeLeftMs) {
  const hours = Math.floor(timeLeftMs / (1000 * 60 * 60));
  const minutes = Math.floor((timeLeftMs % (1000 * 60 * 60)) / (1000 * 60));
  const seconds = Math.floor((timeLeftMs % (1000 * 60)) / 1000);

  return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
}

/**
 * Escape HTML
 */
function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// ===== Dashboard Stats =====

/**
 * Load dashboard statistics
 */
async function loadDashboardStats() {
  try {
    const stats = await apiService.getDashboardStats();
    updateDashboardStats(stats);
  } catch (error) {
    console.error('Error loading dashboard stats:', error);
    // Don't show error to user, just keep default values
  }
}

/**
 * Update dashboard stats display
 */
async function updateDashboardStats(stats) {
  // Dashboard stats removed from UI; keep function safe for future reuse
  const activeMatchesEl = document.getElementById('activeMatchesCount');
  const totalQueuedEl = document.getElementById('totalQueuedPlayers');
  const totalAvailableEl = document.getElementById('totalAvailableTierTesters');

  if (activeMatchesEl) activeMatchesEl.textContent = stats.activeMatchesCount || 0;
  if (totalQueuedEl) totalQueuedEl.textContent = stats.totalQueuedPlayers || 0;
  if (totalAvailableEl) totalAvailableEl.textContent = stats.totalAvailableTierTesters || 0;

  // Update gamemode breakdown
  const gamemodeStatsContainer = document.getElementById('gamemodeStats');
  if (!gamemodeStatsContainer) return;
  const gamemodes = CONFIG.GAMEMODES.filter(gm => gm.id !== 'overall');

  // Fetch player data to show ratings
  let playerData = null;
  try {
    const players = await apiService.getPlayers();
    playerData = players.players.find(p => p.userId === AppState.getUserId());
  } catch (error) {
    console.error('Error fetching player data for gamemode breakdown:', error);
  }

  let gamemodeStatsHtml = '';

  gamemodes.forEach(gamemode => {
    const playersQueued = stats.playersQueued[gamemode.id] || 0;
    const testersAvailable = stats.testersAvailable[gamemode.id] || 0;
    const playerRating = playerData?.gamemodeRatings?.[gamemode.id] || 0;

    // Determine status text and color
    let statusText = '';
    let statusColor = '';
    
    if (testersAvailable > 0) {
      statusText = `Tier tester queued for ${gamemode.name}`;
      statusColor = 'var(--success-color)';
    } else if (playersQueued > 0) {
      statusText = `Waiting for a tier tester in ${gamemode.name}...`;
      statusColor = 'var(--warning-color)';
    } else {
      statusText = `No Queue Activity`;
      statusColor = 'var(--text-muted)';
    }

    gamemodeStatsHtml += `
      <div class="gamemode-stat-item">
        <div class="gamemode-stat-name">
          <img src="${gamemode.icon}" alt="${gamemode.name}" style="width: 20px; height: 20px; margin-right: 0.5rem;">
          ${gamemode.name} <span class="gamemode-player-rating">(${playerRating} Elo)</span>
        </div>
        <div class="gamemode-stat-status" style="color: ${statusColor}; font-size: 0.875rem; margin: 0.25rem 0;">
          ${statusText}
        </div>
        <div class="gamemode-stat-numbers">
          <div class="gamemode-stat-players">
            <div class="gamemode-stat-label">Queued</div>
            <div class="gamemode-stat-value">${playersQueued}</div>
          </div>
          <div class="gamemode-stat-testers">
            <div class="gamemode-stat-label">Tier Testers</div>
            <div class="gamemode-stat-value">${testersAvailable}</div>
          </div>
        </div>
      </div>
    `;
  });

  gamemodeStatsContainer.innerHTML = gamemodeStatsHtml;
}

/**
 * Toggle testing info section
 */
function toggleTestingInfo() {
  const content = document.getElementById('testingInfoContent');
  const toggle = document.getElementById('testingInfoToggle');

  if (content && toggle) {
    if (content.style.display === 'none' || content.style.display === '') {
      content.style.display = 'block';
      toggle.style.transform = 'rotate(180deg)';
    } else {
      content.style.display = 'none';
      toggle.style.transform = 'rotate(0deg)';
    }
  }
}

// ===== Queue Cooldown Management =====

let cooldownIntervals = {};
let userCooldowns = [];

/**
 * Start cooldown timer updates
 */
function startCooldownTimers() {
  // Update cooldowns every second (only one interval)
  if (cooldownDisplayInterval) clearInterval(cooldownDisplayInterval);
  cooldownDisplayInterval = setInterval(updateCooldownDisplays, 1000);
  // Initial update
  updateCooldownDisplays();
}

/**
 * Update all cooldown displays
 */
function updateCooldownDisplays() {
  const cooldownsContainer = document.getElementById('queueCooldowns');
  if (!cooldownsContainer) return;

  // Update remaining times for active cooldowns
  userCooldowns.forEach(cooldown => {
    cooldown.remainingMs = Math.max(0, (cooldown.remainingMs || 0) - 1000);
  });

  // Remove expired cooldowns
  userCooldowns = userCooldowns.filter(cooldown => cooldown.remainingMs > 0);

  if (userCooldowns.length === 0) {
    cooldownsContainer.innerHTML = '';
    return;
  }

  cooldownsContainer.innerHTML = `
    <div class="cooldown-dashboard-panel">
      <div class="cooldown-dashboard-header">
        <div>
          <h5 class="cooldown-title"><i class="fas fa-clock"></i> Queue Cooldowns</h5>
          <p class="cooldown-subtitle">Each timer shows the exact event that triggered the cooldown.</p>
        </div>
      </div>
      <div class="cooldown-list">
        ${userCooldowns.map(cooldown => `
          <div class="cooldown-item cooldown-item-detailed">
            <div class="cooldown-info cooldown-info-detailed">
              <div>
                <span class="cooldown-gamemode">${cooldown.gamemode.toUpperCase()}</span>
                <div class="cooldown-trigger-label">${escapeHtml(cooldown.eventLabel || 'Cooldown active')}</div>
              </div>
              <span class="cooldown-timer" id="cooldown-${cooldown.gamemode}">${formatTimeRemaining(cooldown.remainingMs)}</span>
            </div>
            <div class="cooldown-progress">
              <div class="cooldown-progress-bar" style="width: ${(cooldown.remainingMs / (30 * 60 * 1000)) * 100}%"></div>
            </div>
            <div class="cooldown-meta-row">
              <span><strong>Triggered:</strong> ${cooldown.startedAt ? new Date(cooldown.startedAt).toLocaleString() : 'Unknown'}</span>
              <span><strong>Expires:</strong> ${cooldown.expiresAt ? new Date(cooldown.expiresAt).toLocaleString() : 'Unknown'}</span>
            </div>
            <div class="cooldown-reason-text">${escapeHtml(cooldown.reason || 'Cooldown active')}</div>
          </div>
        `).join('')}
      </div>
      <div class="cooldown-note">
        <small>You cannot queue as the player for these gamemodes until the cooldown expires.</small>
      </div>
    </div>
  `;
}

/**
 * Load user cooldowns from backend
 */
async function loadUserCooldowns() {
  try {
    const response = await apiService.getUserCooldowns();
    if (response.success) {
      userCooldowns = response.cooldowns || [];
      updateCooldownDisplays();
    }
  } catch (error) {
    console.error('Error loading user cooldowns:', error);
    userCooldowns = [];
  }
}

/**
 * Check if a gamemode is on cooldown
 */
function isGamemodeOnCooldown(gamemode) {
  return userCooldowns.some(cooldown =>
    cooldown.gamemode === gamemode && cooldown.remainingMs > 0
  );
}

/**
 * Get remaining cooldown time for a gamemode
 */
function getCooldownTimeRemaining(gamemode) {
  const cooldown = userCooldowns.find(c => c.gamemode === gamemode);
  return cooldown ? cooldown.remainingMs : 0;
}

/**
 * Format time remaining as MM:SS
 */
function formatTimeRemaining(ms) {
  if (ms <= 0) return '00:00:00';

  const totalSeconds = Math.floor(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
}

/**
 * Check for user warnings and display them
 */
async function checkUserWarnings() {
  try {
    const profile = AppState.getProfile();
    if (!profile || !profile.warnings) return;

    const unacknowledgedWarnings = profile.warnings.filter(w => !w.acknowledged);

    if (unacknowledgedWarnings.length > 0) {
      showWarningBanner(unacknowledgedWarnings);
    }
  } catch (error) {
    console.error('Error checking user warnings:', error);
  }
}

/**
 * Show warning banner at top of dashboard
 */
function showWarningBanner(warnings) {
  // Remove existing warning banner
  const existingBanner = document.getElementById('warningBanner');
  if (existingBanner) {
    existingBanner.remove();
  }

  // Create warning banner
  const banner = document.createElement('div');
  banner.id = 'warningBanner';
  banner.style.cssText = `
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    background: linear-gradient(135deg, #ff6b35, #f7931e);
    color: white;
    padding: 1rem;
    z-index: 1000;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
    border-bottom: 3px solid #e55a2b;
  `;

  // Add margin to body to account for fixed banner
  document.body.style.marginTop = '80px';

  const warning = warnings[0]; // Show first unacknowledged warning
  const warnedAt = new Date(warning.warnedAt).toLocaleDateString();

  banner.innerHTML = `
    <div style="max-width: 1200px; margin: 0 auto; display: flex; align-items: center; justify-content: space-between;">
      <div style="display: flex; align-items: center; flex-grow: 1;">
        <i class="fas fa-exclamation-triangle" style="font-size: 1.5rem; margin-right: 1rem;"></i>
        <div>
          <strong style="font-size: 1.1rem;">Warning</strong><br>
          <span style="opacity: 0.9;">${warning.reason}</span>
          <br>
          <small style="opacity: 0.7;">Issued on ${warnedAt}</small>
        </div>
      </div>
      <button onclick="acknowledgeWarning('${warning.id}')" style="
        background: rgba(255, 255, 255, 0.2);
        color: white;
        border: 1px solid rgba(255, 255, 255, 0.3);
        padding: 0.5rem 1rem;
        border-radius: 4px;
        cursor: pointer;
        font-weight: 500;
        transition: background-color 0.2s;
      " onmouseover="this.style.background='rgba(255, 255, 255, 0.3)'" onmouseout="this.style.background='rgba(255, 255, 255, 0.2)'">
        <i class="fas fa-check"></i> I Understand
      </button>
    </div>
  `;

  document.body.insertBefore(banner, document.body.firstChild);
}

/**
 * Acknowledge a warning
 */
async function acknowledgeWarning(warningId) {
  try {
    await apiService.acknowledgeWarning(warningId);

    // Remove banner
    const banner = document.getElementById('warningBanner');
    if (banner) {
      banner.remove();
      document.body.style.marginTop = '0';
    }

    // Update local profile
    const profile = AppState.getProfile();
    if (profile && profile.warnings) {
      profile.warnings = profile.warnings.map(w =>
        w.id === warningId ? { ...w, acknowledged: true, acknowledgedAt: new Date().toISOString() } : w
      );
      AppState.setProfile(profile);
    }

    // Check if there are more warnings to show
    const unacknowledgedWarnings = profile.warnings.filter(w => !w.acknowledged);
    if (unacknowledgedWarnings.length > 0) {
      showWarningBanner(unacknowledgedWarnings);
    }

        } catch (error) {
    console.error('Error acknowledging warning:', error);
    // Show error message
    const banner = document.getElementById('warningBanner');
    if (banner) {
      const button = banner.querySelector('button');
      if (button) {
        button.innerHTML = '<i class="fas fa-exclamation-triangle"></i> Error - Try Again';
        button.style.background = 'rgba(220, 53, 69, 0.8)';
        setTimeout(() => {
          button.innerHTML = '<i class="fas fa-check"></i> I Understand';
          button.style.background = 'rgba(255, 255, 255, 0.2)';
        }, 3000);
      }
    }
  }
}

// Make acknowledgeWarning globally available
if (typeof window !== 'undefined') {
  window.acknowledgeWarning = acknowledgeWarning;
  }

/**
 * Setup HT3+ testing interface
 */
function removed_setupHT3TestingInterface() {
  const gamemodeTabs = document.getElementById('ht3GamemodeTabs');

  // Create gamemode tabs for HT3+ testing
  const gamemodes = [
    { id: 'vanilla', name: 'Vanilla', icon: 'assets/gamemodes/vanilla.png' },
    { id: 'uhc', name: 'UHC', icon: 'assets/gamemodes/uhc.png' },
    { id: 'pot', name: 'Pot', icon: 'assets/gamemodes/pot.png' },
    { id: 'nethop', name: 'Nether Hop', icon: 'assets/gamemodes/nethop.png' },
    { id: 'smp', name: 'SMP', icon: 'assets/gamemodes/smp.png' },
    { id: 'sword', name: 'Sword', icon: 'assets/gamemodes/sword.png' },
    { id: 'axe', name: 'Axe', icon: 'assets/gamemodes/axe.png' },
    { id: 'mace', name: 'Mace', icon: 'assets/gamemodes/mace.png' }
  ];

  gamemodeTabs.innerHTML = gamemodes.map(gamemode => `
    <button class="gamemode-tab-btn" onclick="selectHT3Gamemode('${gamemode.id}')">
      <img src="${gamemode.icon}" alt="${gamemode.name}" class="gamemode-icon" onerror="this.style.display='none'">
      <span>${gamemode.name}</span>
    </button>
  `).join('');
}

/**
 * Select gamemode for HT3+ testing
 */
function selectHT3Gamemode(gamemode) {
  // Update tab selection
  const tabs = document.querySelectorAll('#ht3GamemodeTabs .gamemode-tab-btn');
  tabs.forEach(tab => tab.classList.remove('active'));
  event.target.closest('.gamemode-tab-btn').classList.add('active');

  // Load available players for this gamemode
  loadAvailablePlayersForTesting(gamemode);
}

/**
 * Load players available for HT3+ testing
 */
async function loadAvailablePlayersForTesting(gamemode) {
  const availablePlayersSection = document.getElementById('availablePlayersSection');
  const availablePlayersList = document.getElementById('availablePlayersList');
  const noAvailablePlayers = document.getElementById('noAvailablePlayers');

  if (!availablePlayersSection || !availablePlayersList || !noAvailablePlayers) {
    return;
  }

  availablePlayersSection.style.display = 'block';
  availablePlayersList.innerHTML = '<p class="text-muted mt-2">HT3 browser queue access has been removed. Use backend/admin tools for this flow.</p>';
  noAvailablePlayers.style.display = 'none';
}

/**
 * Start HT3 test with a player
 */
async function startHT3Test(playerId, gamemode, playerUsername) {
  try {
    await Swal.fire({
      icon: 'info',
      title: 'HT3 Browser Flow Disabled',
      text: 'Direct HT3 queue access from the browser was removed for security. Use the backend or admin tools for this flow.'
    });
    return;

    const firstTo = CONFIG?.FIRST_TO?.[gamemode] || 3;
    // Confirm the test
    const result = await Swal.fire({
      title: 'Start HT3 Test',
      html: `
        <p>You are about to test <strong>${escapeHtml(playerUsername)}</strong> for HT3 in <strong>${gamemode.toUpperCase()}</strong>.</p>
        <p>This will be a <strong>First to ${firstTo}</strong> match.</p>
        <p>Make sure you have:</p>
        <ul style="text-align: left;">
          <li>8 Totems of Undying</li>
          <li>Proper kit setup</li>
          <li>Server information ready</li>
        </ul>
      `,
      icon: 'question',
      showCancelButton: true,
      confirmButtonText: 'Start Test',
      cancelButtonText: 'Cancel'
    });

    if (!result.isConfirmed) return;

    // Get server info from user
    const { value: serverInfo } = await Swal.fire({
      title: 'Server Information',
      html: `
        <div class="form-group" style="margin-bottom: 1rem;">
          <label style="display: block; margin-bottom: 0.5rem;">Server IP:</label>
          <input type="text" id="serverIP" class="form-input" placeholder="mc.server.com" style="width: 100%; padding: 0.5rem;" required>
        </div>
        <div class="form-group">
          <label style="display: block; margin-bottom: 0.5rem;">Region:</label>
          <select id="serverRegion" class="form-select" style="width: 100%; padding: 0.5rem;" required>
            <option value="">Select region...</option>
            <option value="NA">NA - North America</option>
            <option value="EU">EU - Europe</option>
            <option value="AS">AS - Asia</option>
            <option value="SA">SA - South America</option>
            <option value="AU">AU - Australia</option>
          </select>
        </div>
      `,
      showCancelButton: true,
      confirmButtonText: 'Create Match',
      preConfirm: () => {
        const serverIP = document.getElementById('serverIP').value.trim();
        const region = document.getElementById('serverRegion').value;

        if (!serverIP || !region) {
          Swal.showValidationMessage('Please fill in all server information');
          return false;
        }

        return { serverIP, region };
      }
    });

    if (!serverInfo) return;

    // Create HT3 test match
    const matchData = {
      gamemode,
      region: serverInfo.region,
      serverIP: serverInfo.serverIP,
      ht3PlayerId: playerId,
      ht3PlayerUsername: playerUsername,
      matchType: 'ht3_test'
    };

    const response = await apiService.createHT3TestMatch(matchData);

    if (response.success) {
      window.location.href = `testing.html?matchId=${response.matchId}`;
      return;
    }

  } catch (error) {
    console.error('Error starting HT3 test:', error);
    Swal.fire({
      icon: 'error',
      title: 'Failed to Start Test',
      text: error.message
    });
  }
}

/**
 * Load player progression tracking
 */
async function loadPlayerProgression() {
  try {
    const profile = await apiService.getProfile();
    if (!profile) return;

    const players = await apiService.getPlayers();
    const playerData = players.players.find(p => p.userId === AppState.getUserId());

    if (!playerData) return;

    // Show progression card
    document.getElementById('progressionCard').style.display = 'block';

    const progressionContent = document.getElementById('progressionContent');
    let progressionHtml = '';

    // Check each gamemode - only show progression for LT3+ players
    const gamemodes = ['vanilla', 'uhc', 'pot', 'nethop', 'smp', 'sword', 'axe', 'mace'];

    gamemodes.forEach(gamemode => {
      const playerTier = playerData.gamemodeTiers?.[gamemode];
      const evaluationStatus = playerData.evaluationStatus?.[gamemode];
      const ht3Status = playerData.ht3Status?.[gamemode];

      // Only show progression for players who are LT3 or better in this gamemode
      // Tiers under LT3 are not phased according to the requirements
      const isLT3OrBetter = playerTier && !['LT5', 'HT5', 'LT4', 'HT4'].includes(playerTier);

      if (isLT3OrBetter) {
        progressionHtml += generateGamemodeProgression(gamemode, playerTier, evaluationStatus, ht3Status);
      }
    });

    if (!progressionHtml) {
      progressionHtml = `
        <div class="alert alert-info">
          <i class="fas fa-info-circle"></i>
          <strong>No testing progression yet.</strong> Join the queue to start your testing journey!
        </div>
      `;
    }

    progressionContent.innerHTML = progressionHtml;

  } catch (error) {
    console.error('Error loading player progression:', error);
  }
}

/**
 * Generate progression HTML for a specific gamemode
 */
function generateGamemodeProgression(gamemode, playerTier, evaluationStatus, ht3Status) {
  const gamemodeName = gamemode.toUpperCase();

  let progressionHtml = `
    <div class="mb-4">
      <h5 style="color: var(--accent-color); border-bottom: 1px solid var(--border-color); padding-bottom: 0.5rem;">
        <i class="fas fa-gamepad"></i> ${gamemodeName}
      </h5>
  `;

  if (!playerTier) {
    // Show rating status
    progressionHtml += `
      <div class="alert alert-secondary">
        <strong>Status:</strong> Unranked - Join the queue to begin testing!
      </div>
    `;
  } else if (playerTier === 'LT3' && evaluationStatus === 'passed') {
    // Passed evaluation, eligible for HT3 testing
    progressionHtml += `
      <div class="alert alert-success">
        <strong>Current Tier:</strong> LT3 (Evaluation Passed)
        <br><strong>Next Step:</strong> Wait for HT3+ tester to create test match
      </div>
      <div class="progression-steps">
        <div class="step completed">
          <i class="fas fa-check"></i> Evaluation Completed
        </div>
        <div class="step current">
          <i class="fas fa-clock"></i> Awaiting HT3 Test
        </div>
        <div class="step">
          <i class="fas fa-trophy"></i> HT3 Achievement
        </div>
      </div>
    `;
  } else if (playerTier === 'LT3' && ht3Status === 'failed_attempt') {
    // Failed HT3 test, can try again
    progressionHtml += `
      <div class="alert alert-warning">
        <strong>Current Tier:</strong> LT3
        <br><strong>Status:</strong> Previous HT3 test failed - eligible for retry
      </div>
      <div class="progression-steps">
        <div class="step completed">
          <i class="fas fa-check"></i> Evaluation Completed
        </div>
        <div class="step current">
          <i class="fas fa-redo"></i> HT3 Test Available
        </div>
        <div class="step">
          <i class="fas fa-trophy"></i> HT3 Achievement
        </div>
      </div>
    `;
  } else if (playerTier === 'HT3') {
    // Achieved HT3
    progressionHtml += `
      <div class="alert alert-success">
        <strong>Current Tier:</strong> HT3 <i class="fas fa-crown" style="color: gold;"></i>
        <br><strong>Excellent rating!</strong> You have achieved a high Elo rating!
      </div>
      <div class="progression-steps">
        <div class="step completed">
          <i class="fas fa-check"></i> Evaluation Completed
        </div>
        <div class="step completed">
          <i class="fas fa-check"></i> HT3 Test Passed
        </div>
        <div class="step completed">
          <i class="fas fa-trophy"></i> HT3 Achieved
        </div>
      </div>
    `;
  } else {
    // Show rating information
    progressionHtml += `
      <div class="alert alert-info">
        <strong>Current Rating:</strong> ${playerRating} Elo
        <br><strong>Keep improving!</strong> Your rating updates automatically after each match.
      </div>
    `;
  }

  progressionHtml += '</div>';
  return progressionHtml;
}


/**
 * Toggle testing information visibility
 */
function toggleTestingInfo() {
  const body = document.getElementById('testingInfoBody');
  const toggle = document.getElementById('testingInfoToggle');

  if (body && toggle) {
    const isCollapsed = body.style.display === 'none';
    body.style.display = isCollapsed ? 'block' : 'none';
    toggle.style.transform = isCollapsed ? 'rotate(0deg)' : 'rotate(180deg)';

    // Store preference in localStorage
    localStorage.setItem('testingInfoCollapsed', !isCollapsed);
  }
}

// Initialize testing info state on page load
document.addEventListener('DOMContentLoaded', () => {
  const shouldCollapse = localStorage.getItem('testingInfoCollapsed') === 'true';
  if (shouldCollapse) {
    const body = document.getElementById('testingInfoBody');
    const toggle = document.getElementById('testingInfoToggle');
    if (body && toggle) {
      body.style.display = 'none';
      toggle.style.transform = 'rotate(180deg)';
    }
  }
});

// Cleanup on page unload
window.addEventListener('beforeunload', () => {
  if (queueCheckInterval) {
    clearInterval(queueCheckInterval);
  }
  if (mobileStatsAnimationInterval) {
    clearInterval(mobileStatsAnimationInterval);
  }
  if (dashboardRefreshInterval) {
    clearInterval(dashboardRefreshInterval);
  }
  if (cooldownDisplayInterval) {
    clearInterval(cooldownDisplayInterval);
  }
  if (joinQueueButtonInterval) {
    clearInterval(joinQueueButtonInterval);
  }
  if (notificationPollInterval) {
    clearInterval(notificationPollInterval);
  }
  if (activeMatchPollInterval) {
    clearInterval(activeMatchPollInterval);
  }
  if (gamemodeStatsInterval) {
    clearInterval(gamemodeStatsInterval);
  }
  sessionStorage.removeItem('testingPageOpen');
});

/**
 * Show tier tester application banner if user is eligible
 */
async function showTierTesterBanner() {
  const profile = AppState.getProfile();
  if (!profile) return;

  const plusBanner = document.getElementById('plusBanner');
  const tierBanner = document.getElementById('tierTesterBanner');

  // Always show both banners, just update their content
  if (plusBanner) plusBanner.style.display = 'block';
  if (tierBanner) tierBanner.style.display = 'block';

  // Update Plus banner content
  const plus = profile.plus || {};
  const hasPlus = plus.active === true && plus.blocked !== true;
  
  const plusTitle = document.getElementById('plusBannerTitle');
  const plusText = document.getElementById('plusBannerText');
  const plusButton = document.getElementById('plusBannerButton');

  if (hasPlus && plusTitle && plusText && plusButton) {
    // User has Plus - show thank you message
    const endDate = plus.endDate ? new Date(plus.endDate).toLocaleDateString() : 'N/A';
    plusTitle.innerHTML = '<i class="fas fa-crown"></i><span>Thank You for Supporting Plus!</span>';
    plusText.textContent = `Your Plus membership is active until ${endDate}. You have priority queue access and exclusive features.`;
    plusButton.innerHTML = '<a href="account.html" class="btn btn-warning" style="background: linear-gradient(135deg, #f2c94c, #d9a441); color: #1b1f24; font-weight: 600;"><i class="fas fa-cog"></i> Manage in Settings</a>';
  } else if (plusTitle && plusText && plusButton) {
    // User doesn't have Plus - show upgrade message
    plusTitle.innerHTML = '<i class="fas fa-crown"></i><span>Upgrade to Plus</span>';
    plusText.textContent = 'Get priority queue access, exclusive features, and support the platform';
    plusButton.innerHTML = '<a href="plus.html" class="btn btn-warning" style="background: linear-gradient(135deg, #f2c94c, #d9a441); color: #1b1f24; font-weight: 600;"><i class="fas fa-star"></i> Learn More</a>';
  }

  // Update Tier Tester banner content
  const isTierTester = profile.tester === true;
  const isBlacklisted = profile.blacklisted === true;
  
  const tierTitle = document.getElementById('tierTesterBannerTitle');
  const tierText = document.getElementById('tierTesterBannerText');
  const tierButton = document.getElementById('tierTesterBannerButton');

  if (isTierTester && tierTitle && tierText && tierButton) {
    // User is a tier tester - show thank you message
    const testerSince = profile.testerSince ? new Date(profile.testerSince) : new Date();
    const daysSince = Math.floor((Date.now() - testerSince.getTime()) / (1000 * 60 * 60 * 24));
    const duration = daysSince < 30 ? `${daysSince} days` : `${Math.floor(daysSince / 30)} months`;
    
    tierTitle.innerHTML = '<i class="fas fa-user-shield"></i><span>Thank You for Being a Tier Tester!</span>';
    tierText.textContent = `You've been a tier tester for ${duration}. Thank you for helping evaluate players!`;
    tierButton.innerHTML = '<button class="btn btn-success" style="background: linear-gradient(135deg, var(--success-color), #059669); font-weight: 600;" onclick="scrollToTesterDashboard()"><i class="fas fa-arrow-down"></i> Go to Tier Tester Section</button>';
  } else if (!isBlacklisted && tierTitle && tierText && tierButton) {
    // User is not a tier tester - check if applications are open
    try {
      const resp = await apiService.getTierTesterApplicationsOpen();
      const open = resp && resp.open === true;
      
      if (open) {
        // Applications are open
        tierTitle.innerHTML = '<i class="fas fa-user-shield"></i><span>Think You Have What It Takes?</span>';
        tierText.textContent = 'Help evaluate players and earn exclusive perks as an official tier tester';
        tierButton.innerHTML = '<a href="tier-tester-application.html" class="btn btn-success" style="background: linear-gradient(135deg, var(--success-color), #059669); font-weight: 600;"><i class="fas fa-clipboard-check"></i> Apply Now</a>';
      } else {
        // Applications are closed
        tierTitle.innerHTML = '<i class="fas fa-user-shield"></i><span>Tier Tester Applications</span>';
        tierText.textContent = 'Tier tester applications are currently closed. Check back later for opportunities to join our testing team!';
        tierButton.innerHTML = '<button class="btn btn-secondary" style="font-weight: 600;" disabled><i class="fas fa-lock"></i> Applications Closed</button>';
      }
    } catch (error) {
      console.error('Error checking tier tester applications:', error);
      // Default to closed if error
      tierTitle.innerHTML = '<i class="fas fa-user-shield"></i><span>Tier Tester Applications</span>';
      tierText.textContent = 'Unable to check application status. Please try again later.';
      tierButton.innerHTML = '<button class="btn btn-secondary" style="font-weight: 600;" disabled><i class="fas fa-exclamation-triangle"></i> Status Unknown</button>';
    }
  } else if (isBlacklisted && tierBanner) {
    // User is blacklisted - hide the tier tester banner
    tierBanner.style.display = 'none';
  }
}

/**
 * Show server selection popup
 */
async function showServerSelectionPopup(targetInputId = 'serverIP') {
  try {
    // Fetch whitelisted servers
    const response = await apiService.getWhitelistedServers();

    if (!response.success || !response.servers || response.servers.length === 0) {
      Swal.fire({
        icon: 'info',
        title: 'No Servers Available',
        text: 'No whitelisted servers are currently available. Please enter a server IP manually.'
      });
      return;
    }

    // Create server selection HTML
    const serversHtml = response.servers.map(server => `
      <div style="padding: 1rem; margin-bottom: 0.5rem; background: rgba(52, 152, 219, 0.05); border: 1px solid rgba(52, 152, 219, 0.2); border-radius: 8px; cursor: pointer; transition: all 0.2s ease;" 
         onclick="selectServer('${escapeHtml(server.ip)}', '${escapeHtml(targetInputId)}')"
           onmouseover="this.style.background='rgba(52, 152, 219, 0.1)'; this.style.borderColor='rgba(52, 152, 219, 0.3)'"
           onmouseout="this.style.background='rgba(52, 152, 219, 0.05)'; this.style.borderColor='rgba(52, 152, 219, 0.2)'">
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <div>
            <strong style="color: var(--text-color); font-size: 1rem;">${escapeHtml(server.name)}</strong>
            <div style="color: var(--text-muted); font-size: 0.875rem; margin-top: 0.25rem;">
              <code style="background: rgba(52, 152, 219, 0.1); padding: 0.25rem 0.5rem; border-radius: 4px;">${escapeHtml(server.ip)}</code>
            </div>
          </div>
          <i class="fas fa-chevron-right" style="color: var(--primary-color);"></i>
        </div>
      </div>
    `).join('');

    Swal.fire({
      title: '<i class="fas fa-server"></i> Select a Server',
      html: `
        <div style="text-align: left; max-height: 400px; overflow-y: auto;">
          ${serversHtml}
        </div>
      `,
      showCancelButton: true,
      showConfirmButton: false,
      cancelButtonText: 'Cancel',
      width: '600px',
      customClass: {
        popup: 'server-selection-popup'
      }
    });
  } catch (error) {
    console.error('Error loading servers:', error);
    Swal.fire({
      icon: 'error',
      title: 'Error',
      text: 'Failed to load whitelisted servers. Please try again.'
    });
  }
}

/**
 * Select a server from the popup
 */
function selectServer(serverIp, targetInputId = 'serverIP') {
  const serverIpInput = document.getElementById(targetInputId);
  if (serverIpInput) {
    serverIpInput.value = serverIp;
  }
  Swal.close();
}

// Make functions globally available
window.showServerSelectionPopup = showServerSelectionPopup;
window.selectServer = selectServer;

window.addEventListener('beforeunload', () => {
  if (notificationPollInterval) {
    clearInterval(notificationPollInterval);
    notificationPollInterval = null;
  }
});

// Initialize on page load
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initDashboard);
} else {
  initDashboard();
}
