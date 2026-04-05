// MC Leaderboards - Testing Interface

let matchId = null;
let match = null;
let isTester = false;
let isPlayer = false;
let isSpectator = false;
let lastMessageTime = 0;
let buttonHandlerCounter = 0;
let countdownInterval = null;
let countdownStartTime = null;
let matchPollingInterval = null;
let chatPollingInterval = null;
let lastChatSignature = '';
const chatMessageCache = new Map();

function getRoleLabelForMatchUser(userId) {
  if (!match || !userId) return 'Participant';
  if (match.playerId === userId) return 'Player';
  if (match.testerId === userId) return 'Tier Tester';
  return 'Participant';
}

function getCurrentUserRoleLabel() {
  if (isPlayer) return 'Player';
  if (isTester) return 'Tier Tester';
  return 'Spectator';
}

function getCurrentUserRoleReason() {
  if (!match?.roleAssignment) return 'Roles are assigned automatically when a compatible pair is found.';
  if (isPlayer) return match.roleAssignment.playerReason || match.roleAssignment.explanation || 'You were assigned as the player for this match.';
  if (isTester) return match.roleAssignment.testerReason || match.roleAssignment.explanation || 'You were assigned as the tier tester for this match.';
  return match.roleAssignment.explanation || 'Roles are assigned automatically when a compatible pair is found.';
}

function getRoleAssignmentDebugLabel() {
  if (!AppState.isAdmin() || !match?.roleAssignment) return '';
  if (match.roleAssignment.type === 'dual_tier_tester_cooldown_priority') {
    return 'Admin Debug: cooldown forced the tester/player role split.';
  }
  if (match.roleAssignment.type === 'dual_tier_tester_random' || match.roleAssignment.randomized === true) {
    return 'Admin Debug: both users were tester-eligible, so roles were randomized.';
  }
  if (match.roleAssignment.type === 'admin_force_test') {
    if (match.roleAssignment.serverSelectionSource === 'player_queue') {
      return 'Admin Debug: player queue server IP overrode the requested server.';
    }
    return 'Admin Debug: this match was force-created by an admin.';
  }
  return '';
}

function updateRoleAssignmentDisplays() {
  const roleNotice = document.getElementById('roleAssignmentNotice');
  const waitingRoleNotice = document.getElementById('waitingRoleAssignmentNotice');
  const roleNoticeDebug = document.getElementById('roleAssignmentDebugNotice');
  const waitingRoleNoticeDebug = document.getElementById('waitingRoleAssignmentDebugNotice');
  const yourRoleEl = document.getElementById('matchYourRole');
  const currentRole = getCurrentUserRoleLabel();
  const reasonText = getCurrentUserRoleReason();
  const debugLabel = getRoleAssignmentDebugLabel();

  if (yourRoleEl) {
    yourRoleEl.textContent = currentRole;
  }

  if (roleNotice) {
    roleNotice.innerHTML = `<i class="fas fa-balance-scale"></i> ${escapeHtml(reasonText)}`;
  }

  if (waitingRoleNotice) {
    waitingRoleNotice.innerHTML = `<i class="fas fa-balance-scale"></i> ${escapeHtml(reasonText)}`;
  }

  if (roleNoticeDebug) {
    roleNoticeDebug.textContent = debugLabel;
    roleNoticeDebug.classList.toggle('d-none', !debugLabel);
  }

  if (waitingRoleNoticeDebug) {
    waitingRoleNoticeDebug.textContent = debugLabel;
    waitingRoleNoticeDebug.classList.toggle('d-none', !debugLabel);
  }
}

function getMatchFirstTo() {
  if (!match) return 3;
  if (Number.isFinite(match.firstTo) && match.firstTo > 0) {
    return match.firstTo;
  }
  const fromConfig = CONFIG?.FIRST_TO?.[match.gamemode];
  return Number.isFinite(fromConfig) && fromConfig > 0 ? fromConfig : 3;
}

function shouldShowTotemDrain() {
  return String(match?.gamemode || '').trim().toLowerCase() === 'vanilla';
}

/**
 * Initialize testing page
 */
async function initTesting() {
  // Authentication is already verified by auth-guard.js
  if (!AppState.isAuthenticated()) {
    return; // Will be handled by auth guard
  }

  // Get match ID from URL
  const urlParams = new URLSearchParams(window.location.search);
  matchId = urlParams.get('matchId');

  if (!matchId) {
    showCustomModal('No Match ID', 'Invalid match link. Please check the URL and try again.', null);
    return;
  }

  if (window.mclbLoadingOverlay) {
    window.mclbLoadingOverlay.updateStatus('Loading match data...', 85);
  }

  await loadMatch();
  startSecureMatchPolling();

  if (window.mclbLoadingOverlay) {
    window.mclbLoadingOverlay.updateStatus('Match ready!', 100);
  }
}

/**
 * Load match data
 */
async function loadMatch() {
  try {
    if (window.mclbLoadingOverlay) {
      window.mclbLoadingOverlay.updateStatus('Fetching match...', 88);
    }
    match = await apiService.getMatch(matchId);
    
    // Determine user role
    isPlayer = match.playerId === AppState.getUserId();
    isTester = match.testerId === AppState.getUserId();
    isSpectator = !isPlayer && !isTester;
    
    if (!isSpectator) {
      if (window.mclbLoadingOverlay) {
        window.mclbLoadingOverlay.updateStatus('Joining match...', 92);
      }
      await apiService.joinMatch(matchId);
      await apiService.updatePageStats(matchId, isPlayer);
    }

    if (window.mclbLoadingOverlay) {
      window.mclbLoadingOverlay.updateStatus('Starting secure live sync...', 96);
    }

    // Render match
    renderMatch();
    await loadChatMessages();

    // Check if both players joined (updated through secure polling)
    updateBothJoinedStatus();
  } catch (error) {
    console.error('Error loading match:', error);
    showCustomModal('Error', error.message || 'Failed to load match. Please refresh the page and try again.', null);
  }
}

/**
 * Render match information
 */
function renderMatch() {
  document.getElementById('loadingState').classList.add('d-none');
  
  const opponent = isPlayer
    ? `${match.testerUsername} (${getRoleLabelForMatchUser(match.testerId)})`
    : isTester
      ? `${match.playerUsername} (${getRoleLabelForMatchUser(match.playerId)})`
      : `${match.playerUsername} (${getRoleLabelForMatchUser(match.playerId)}) vs ${match.testerUsername} (${getRoleLabelForMatchUser(match.testerId)})`;
  
  document.getElementById('matchGamemode').textContent = match.gamemode.toUpperCase();
  document.getElementById('displayGamemode').textContent = match.gamemode.toUpperCase();
  document.getElementById('matchOpponent').textContent = opponent;
  document.getElementById('matchRegion').textContent = match.region;
  document.getElementById('matchServerIP').textContent = match.serverIP;
  const showTotemDrain = shouldShowTotemDrain();
  const matchTotemDrainItem = document.getElementById('matchTotemDrainItem');
  const matchFormatTotemDrain = document.getElementById('matchFormatTotemDrain');
  const totemDrain = Number.isFinite(Number(match.totemDrain)) ? Number(match.totemDrain) : 14;
  if (matchTotemDrainItem) {
    matchTotemDrainItem.classList.toggle('d-none', !showTotemDrain);
  }
  if (matchFormatTotemDrain) {
    matchFormatTotemDrain.classList.toggle('d-none', !showTotemDrain);
  }
  if (showTotemDrain) {
    document.getElementById('matchTotemDrain').textContent = `${totemDrain} Totems`;
    document.getElementById('displayTotemDrain').textContent = `${totemDrain} Totems`;
  }
  updateRoleAssignmentDisplays();
  
  // Display firstTo value
  const firstTo = getMatchFirstTo();
  document.getElementById('displayFirstTo').textContent = firstTo;
  const playerForfeitScore = document.getElementById('playerForfeitScore');
  const testerForfeitScore = document.getElementById('testerForfeitScore');
  const exampleFirstTo = document.getElementById('exampleFirstTo');
  const examplePlayerWinScore = document.getElementById('examplePlayerWinScore');
  const examplePlayerLoseScore = document.getElementById('examplePlayerLoseScore');
  if (playerForfeitScore) playerForfeitScore.textContent = `${firstTo}-0`;
  if (testerForfeitScore) testerForfeitScore.textContent = `${firstTo}-0`;
  if (exampleFirstTo) exampleFirstTo.textContent = `${firstTo}`;
  if (examplePlayerWinScore) examplePlayerWinScore.textContent = `${firstTo}-0`;
  if (examplePlayerLoseScore) examplePlayerLoseScore.textContent = `0-${firstTo}`;
  
  if (isTester) {
    document.getElementById('testerActions').style.display = 'block';
  }

  const spectatorNotice = document.getElementById('spectatorNotice');
  const spectatorChatNotice = document.getElementById('spectatorChatNotice');
  const chatForm = document.getElementById('chatForm');
  const messageInput = document.getElementById('messageInput');
  const sendBtn = document.getElementById('sendBtn');
  const abortBtn = document.getElementById('abortMatchBtn');
  const drawVotePanel = document.getElementById('drawVotePanel');
  const matchStartRoleNotice = document.getElementById('matchStartRoleNotice');
  const markStartedBtn = document.getElementById('markStartedBtn');

  if (isSpectator) {
    spectatorNotice.classList.remove('d-none');
    spectatorChatNotice.classList.remove('d-none');
    chatForm.style.display = 'none';
    messageInput.disabled = true;
    sendBtn.disabled = true;
    if (abortBtn) abortBtn.style.display = 'none';
    if (drawVotePanel) drawVotePanel.classList.add('d-none');
    if (matchStartRoleNotice) {
      matchStartRoleNotice.textContent = 'Spectators can watch the join and start state, but only the tier tester can start the match.';
    }
    if (markStartedBtn) markStartedBtn.style.display = 'none';
  } else {
    spectatorNotice.classList.add('d-none');
    spectatorChatNotice.classList.add('d-none');
    chatForm.style.display = '';
    messageInput.disabled = false;
    sendBtn.disabled = false;
    if (drawVotePanel) drawVotePanel.classList.remove('d-none');
    if (matchStartRoleNotice) {
      matchStartRoleNotice.textContent = isTester
        ? 'You are the tier tester for this match. Confirm the start once both sides are ready.'
        : 'Waiting for the tier tester to confirm the match start. This updates automatically.';
    }
    if (markStartedBtn) markStartedBtn.style.display = isTester ? '' : 'none';
  }

  updateDrawVoteStatus();
}

/**
 * Start countdown timer
 */
function startCountdownTimer() {
  if (!match || !match.joinTimeout) return;

  const expiresAt = new Date(match.joinTimeout.expiresAt).getTime();
  countdownStartTime = new Date(match.joinTimeout.startedAt).getTime();
  const totalDuration = 3 * 60 * 1000; // 3 minutes in ms

  // Clear any existing interval
  if (countdownInterval) {
    clearInterval(countdownInterval);
  }

  // Update countdown every second
  countdownInterval = setInterval(() => {
    const now = Date.now();
    const remaining = Math.max(0, expiresAt - now);
    
    if (remaining <= 0) {
      clearInterval(countdownInterval);
      document.getElementById('countdownDisplay').textContent = '0:00';
      document.getElementById('countdownProgress').style.width = '0%';
      return;
    }

    // Calculate minutes and seconds
    const minutes = Math.floor(remaining / 60000);
    const seconds = Math.floor((remaining % 60000) / 1000);
    
    // Update display
    document.getElementById('countdownDisplay').textContent = `${minutes}:${seconds.toString().padStart(2, '0')}`;
    
    // Update progress bar
    const elapsed = now - countdownStartTime;
    const progress = Math.max(0, Math.min(100, ((totalDuration - elapsed) / totalDuration) * 100));
    document.getElementById('countdownProgress').style.width = `${progress}%`;
    
    // Change color based on time remaining
    const progressBar = document.getElementById('countdownProgress');
    if (remaining < 60000) { // Less than 1 minute
      progressBar.style.background = '#ef4444';
      document.getElementById('countdownDisplay').style.color = '#ef4444';
    } else if (remaining < 120000) { // Less than 2 minutes
      progressBar.style.background = '#f59e0b';
      document.getElementById('countdownDisplay').style.color = '#f59e0b';
    } else {
      progressBar.style.background = 'var(--warning-color)';
      document.getElementById('countdownDisplay').style.color = 'var(--warning-color)';
    }
  }, 1000);
}

/**
 * Start match start countdown (5 minutes after both players join)
 */
function startMatchStartCountdown() {
  if (!match || !match.countdownStartedAt) return;

  const startedAt = new Date(match.countdownStartedAt).getTime();
  const totalDuration = 5 * 60 * 1000; // 5 minutes in ms
  const expiresAt = startedAt + totalDuration;

  // Update countdown every second
  window.matchStartCountdown = setInterval(() => {
    const now = Date.now();
    const remaining = Math.max(0, expiresAt - now);
    
    if (remaining <= 0) {
      clearInterval(window.matchStartCountdown);
      window.matchStartCountdown = null;
      document.getElementById('matchStartedCountdown').textContent = '0:00';
      document.getElementById('matchStartedProgress').style.width = '0%';
      refreshMatchState({ silent: false });
      return;
    }

    // Calculate minutes and seconds
    const minutes = Math.floor(remaining / 60000);
    const seconds = Math.floor((remaining % 60000) / 1000);
    
    // Update display
    document.getElementById('matchStartedCountdown').textContent = `${minutes}:${seconds.toString().padStart(2, '0')}`;
    
    // Update progress bar
    const elapsed = now - startedAt;
    const progress = Math.max(0, Math.min(100, ((totalDuration - elapsed) / totalDuration) * 100));
    document.getElementById('matchStartedProgress').style.width = `${progress}%`;
    
    // Change color based on time remaining
    const progressBar = document.getElementById('matchStartedProgress');
    if (remaining < 60000) { // Less than 1 minute
      progressBar.style.background = '#ef4444';
      document.getElementById('matchStartedCountdown').style.color = '#ef4444';
    } else if (remaining < 120000) { // Less than 2 minutes
      progressBar.style.background = '#f59e0b';
      document.getElementById('matchStartedCountdown').style.color = '#f59e0b';
    } else {
      progressBar.style.background = 'var(--warning-color)';
      document.getElementById('matchStartedCountdown').style.color = 'var(--warning-color)';
    }
  }, 1000);
}

/**
 * Mark match as started
 */
async function handleMarkMatchStarted() {
  if (isSpectator) {
    showCustomModal('Spectator Mode', 'Spectators cannot perform match actions.', null);
    return;
  }

  if (!isTester) {
    showCustomModal('Tester Only', 'Only the tier tester can mark the match as started.', null);
    return;
  }

  try {
    // Disable button during API call
    const btn = document.getElementById('markStartedBtn');
    btn.disabled = true;
    btn.textContent = 'Starting match...';
    
    const result = await apiService.markMatchStarted(matchId);
    
    if (result.success) {
      // Update local match state
      match.matchStarted = true;
      match.matchStartedAt = new Date().toISOString();
      match.countdownStartedAt = null;
      
      // Update UI
      document.getElementById('matchStartedSection').style.display = 'none';
      document.getElementById('matchAlreadyStartedSection').style.display = 'block';
      
      // Clear countdown
      if (window.matchStartCountdown) {
        clearInterval(window.matchStartCountdown);
        window.matchStartCountdown = null;
      }
      
      showCustomModal('Match Started', 'The tier tester marked the match as started. Begin playing!', null);
    } else {
      btn.disabled = false;
      btn.textContent = '<i class="fas fa-play"></i> Mark Match as Started';
      showCustomModal('Error', result.message || 'Failed to mark match as started', null);
    }
  } catch (error) {
    console.error('Error marking match as started:', error);
    const btn = document.getElementById('markStartedBtn');
    btn.disabled = false;
    btn.textContent = '<i class="fas fa-play"></i> Mark Match as Started';
    showCustomModal('Error', error.message || 'Error marking match as started', null);
  }
}

/**
 * Update join status indicators
 */
function updateJoinStatusIndicators() {
  if (!match || !match.pagestats) return;

  const pagestats = match.pagestats;

  // Update player status
  if (pagestats.playerJoined) {
    document.getElementById('playerJoinIcon').innerHTML = '<i class="fas fa-check-circle" style="color: var(--success-color);"></i>';
    document.getElementById('playerJoinStatus').innerHTML = '<span style="color: var(--success-color); font-weight: 600;">Joined!</span>';
  } else {
    document.getElementById('playerJoinIcon').innerHTML = '<i class="fas fa-user" style="color: var(--text-muted);"></i>';
    document.getElementById('playerJoinStatus').innerHTML = '<span style="color: var(--text-muted);">Waiting...</span>';
  }

  // Update tester status
  if (pagestats.testerJoined) {
    document.getElementById('testerJoinIcon').innerHTML = '<i class="fas fa-check-circle" style="color: var(--success-color);"></i>';
    document.getElementById('testerJoinStatus').innerHTML = '<span style="color: var(--success-color); font-weight: 600;">Joined!</span>';
  } else {
    document.getElementById('testerJoinIcon').innerHTML = '<i class="fas fa-user-shield" style="color: var(--text-muted);"></i>';
    document.getElementById('testerJoinStatus').innerHTML = '<span style="color: var(--text-muted);">Waiting...</span>';
  }

  // Update names
  document.getElementById('playerJoinName').textContent = `${match.playerUsername || 'Player'} (Player)`;
  document.getElementById('testerJoinName').textContent = `${match.testerUsername || 'Tier Tester'} (Tier Tester)`;
  updateRoleAssignmentDisplays();
}

/**
 * Update the display based on whether both players have joined
 */
function updateBothJoinedStatus() {
  if (!match) return;

  const pagestats = match.pagestats || { playerJoined: false, testerJoined: false };

  // Update join status indicators
  updateJoinStatusIndicators();

  if (pagestats.playerJoined && pagestats.testerJoined) {
    // Both players joined - stop countdown and show match content
    if (countdownInterval) {
      clearInterval(countdownInterval);
      countdownInterval = null;
    }
    document.getElementById('waitingState').classList.add('d-none');
    document.getElementById('matchContent').classList.remove('d-none');
    
    // Clear any countdown timer
    if (window.playerJoinCountdown) {
      clearInterval(window.playerJoinCountdown);
      window.playerJoinCountdown = null;
    }

    // Show match started section if not already started
    if (match.matchStarted) {
      // Match has already been marked as started
      document.getElementById('matchStartedSection').style.display = 'none';
      document.getElementById('matchAlreadyStartedSection').style.display = 'block';
      if (window.matchStartCountdown) {
        clearInterval(window.matchStartCountdown);
        window.matchStartCountdown = null;
      }
    } else if (match.countdownStartedAt) {
      // Match not started yet, show countdown
      document.getElementById('matchStartedSection').style.display = 'block';
      document.getElementById('matchAlreadyStartedSection').style.display = 'none';

      const lead = document.getElementById('matchStartedLead');
      if (lead) {
        lead.innerHTML = isTester
          ? 'Both sides have joined. You have <strong>5 minutes</strong> to mark the match as started.'
          : 'Both sides have joined. The tier tester has <strong>5 minutes</strong> to mark the match as started.';
      }
      
      if (!window.matchStartCountdown) {
        startMatchStartCountdown();
      }
    }
  } else {
    // Show waiting state with countdown (spectators still see match details)
    document.getElementById('waitingState').classList.remove('d-none');
    if (!isSpectator) {
      document.getElementById('matchContent').classList.add('d-none');
    } else {
      document.getElementById('matchContent').classList.remove('d-none');
    }
    document.getElementById('matchStartedSection').style.display = 'none';
    document.getElementById('matchAlreadyStartedSection').style.display = 'none';
    
    // Start countdown timer if not already started
    if (!countdownInterval && match.joinTimeout) {
      startCountdownTimer();
    }

    // Show countdown timer for testers waiting for players
    if (isTester && !pagestats.playerJoined && match.playerJoinTimeout) {
      showPlayerJoinCountdown();
    }
  }
}

async function refreshMatchState({ silent = true } = {}) {
  try {
    const updatedMatch = await apiService.getMatch(matchId);
    if (!updatedMatch) return;

    const previousStatus = match?.status;
    const previousFinalized = match?.finalized === true;
    match = updatedMatch;

    renderMatch();
    updateBothJoinedStatus();

    if ((updatedMatch.status === 'ended' || updatedMatch.finalized === true) && (previousStatus !== 'ended' || !previousFinalized)) {
      handleMatchEnded();
    }

    const abortBtn = document.getElementById('abortMatchBtn');
    if (abortBtn && updatedMatch.finalized === true) {
      abortBtn.style.display = 'none';
    }
  } catch (error) {
    if (!silent) {
      console.error('Error refreshing match state:', error);
    }
  }
}

function renderChatMessages(messages = []) {
  const chatDiv = document.getElementById('chatMessages');
  if (!chatDiv) return;

  const normalizedMessages = Array.isArray(messages) ? messages : [];
  const nextSignature = normalizedMessages
    .map((message) => `${message.messageId || ''}:${message.timestamp || 0}:${message.text || ''}`)
    .join('|');
  if (nextSignature === lastChatSignature) {
    return;
  }

  const nearBottom = (chatDiv.scrollHeight - chatDiv.scrollTop - chatDiv.clientHeight) < 48;
  lastChatSignature = nextSignature;
  chatMessageCache.clear();

  chatDiv.innerHTML = normalizedMessages.map((message) => {
    const rawMessageId = message.messageId || '';
    const messageId = escapeHtml(rawMessageId);
    chatMessageCache.set(rawMessageId, {
      username: message.username || 'Unknown',
      text: message.text || ''
    });

    const isOwnMessage = message.userId === AppState.getUserId();
    const alignment = isOwnMessage ? 'text-right' : 'text-left';

    return `
      <div class="mb-2" id="msg-${messageId}">
        <div class="${alignment}">
          <strong style="color: var(--accent-color);">${escapeHtml(message.username || 'Unknown')}:</strong>
          <span style="color: var(--text-primary);">${escapeHtml(message.text || '')}</span>
          ${isTester ? `<button class="btn btn-sm btn-danger ml-2" onclick="deleteMessage('${messageId}')" style="padding: 0.25rem 0.5rem;"><i class="fas fa-trash"></i></button>` : ''}
          ${(!isOwnMessage && !isSpectator) ? `<button class="btn btn-sm btn-warning ml-2" onclick="reportChatMessage('${messageId}')" style="padding: 0.25rem 0.5rem;"><i class="fas fa-flag"></i></button>` : ''}
          <br>
          <small class="text-muted">${message.timestamp ? new Date(message.timestamp).toLocaleTimeString() : ''}</small>
        </div>
      </div>
    `;
  }).join('');

  if (nearBottom || normalizedMessages.length <= 1) {
    chatDiv.scrollTop = chatDiv.scrollHeight;
  }
}

async function loadChatMessages() {
  try {
    const response = await apiService.getChatMessages(matchId);
    renderChatMessages(response?.messages || []);
  } catch (error) {
    console.error('Error loading chat messages:', error);
  }
}

function startSecureMatchPolling() {
  stopSecureMatchPolling();
  refreshMatchState({ silent: false });
  loadChatMessages();

  matchPollingInterval = setInterval(() => {
    if (document.visibilityState === 'visible') {
      refreshMatchState();
    }
  }, 2500);

  chatPollingInterval = setInterval(() => {
    if (document.visibilityState === 'visible') {
      loadChatMessages();
    }
  }, 2500);
}

function stopSecureMatchPolling() {
  if (matchPollingInterval) {
    clearInterval(matchPollingInterval);
    matchPollingInterval = null;
  }

  if (chatPollingInterval) {
    clearInterval(chatPollingInterval);
    chatPollingInterval = null;
  }
}

/**
 * Handle send message
 */
async function handleSendMessage(event) {
  event.preventDefault();

  if (isSpectator) {
    Swal.fire({
      icon: 'info',
      title: 'Spectator Mode',
      text: 'Only participants can send chat messages.'
    });
    return;
  }
  
  const messageInput = document.getElementById('messageInput');
  const text = messageInput.value.trim();
  
  if (!text) return;
  
  // Cooldown check
  const now = Date.now();
  if (now - lastMessageTime < CONFIG.CHAT_COOLDOWN) {
    Swal.fire({
      icon: 'warning',
      title: 'Cooldown',
      text: `Please wait ${(CONFIG.CHAT_COOLDOWN / 1000).toFixed(1)} seconds between messages.`,
      timer: 2000,
      showConfirmButton: false
    });
    return;
  }
  
  const sendBtn = document.getElementById('sendBtn');
  sendBtn.disabled = true;
  
  try {
    await apiService.sendChatMessage(matchId, text);
    messageInput.value = '';
    lastMessageTime = now;
    await loadChatMessages();
  } catch (error) {
    Swal.fire({
      icon: 'error',
      title: 'Failed to Send',
      text: error.message
    });
  } finally {
    sendBtn.disabled = false;
    messageInput.focus();
  }
}

/**
 * Delete message (tester only)
 */
async function deleteMessage(messageId) {
  try {
    await apiService.deleteChatMessage(matchId, messageId);
    await loadChatMessages();
  } catch (error) {
    Swal.fire({
      icon: 'error',
      title: 'Failed to Delete',
      text: error.message
    });
  }
}

/**
 * Report chat message
 */
async function reportChatMessage(messageId) {
  if (isSpectator) {
    Swal.fire('Spectator Mode', 'Spectators cannot submit chat reports.', 'info');
    return;
  }

  const cached = chatMessageCache.get(messageId) || {};
  const messageUsername = cached.username || 'Unknown';
  const messageText = cached.text || '';

  const result = await Swal.fire({
    title: `Report message from ${escapeHtml(messageUsername || 'Unknown')}`,
    html: `
      <div style="text-align: left; margin-bottom: 0.75rem;">
        <strong>Message:</strong>
        <div style="margin-top: 0.35rem; padding: 0.6rem; border-radius: 8px; background: var(--tertiary-bg); border: 1px solid var(--border-color);">
          ${escapeHtml(messageText || '')}
        </div>
      </div>
      <textarea id="chatReportDescription" class="swal2-textarea" placeholder="Why are you reporting this message?" style="display:block; width:100%;"></textarea>
      <input id="chatReportEvidence" class="swal2-input" placeholder="Optional evidence link (https://...)" />
    `,
    showCancelButton: true,
    confirmButtonText: 'Submit Report',
    cancelButtonText: 'Cancel',
    buttonsStyling: false,
    customClass: {
      popup: 'mclb-swal-popup',
      title: 'mclb-swal-title',
      htmlContainer: 'mclb-swal-html',
      confirmButton: 'mclb-swal-confirm',
      cancelButton: 'mclb-swal-cancel'
    },
    preConfirm: () => {
      const description = (document.getElementById('chatReportDescription')?.value || '').trim();
      const evidence = (document.getElementById('chatReportEvidence')?.value || '').trim();
      if (!description) {
        Swal.showValidationMessage('Please include a short reason.');
        return null;
      }
      return {
        description,
        evidenceLinks: evidence ? [evidence] : []
      };
    }
  });

  if (!result.isConfirmed || !result.value) return;

  try {
    const response = await apiService.reportChatMessage(matchId, messageId, {
      reason: 'chat_abuse',
      description: result.value.description,
      evidenceLinks: result.value.evidenceLinks,
      hasEvidence: result.value.evidenceLinks.length > 0
    });

    if (!response?.success) {
      throw new Error(response?.message || 'Failed to submit message report');
    }

    Swal.fire({
      icon: 'success',
      title: 'Message Reported',
      text: 'The full conversation was attached for admin review.',
      timer: 1800,
      showConfirmButton: false,
      customClass: {
        popup: 'mclb-swal-popup',
        title: 'mclb-swal-title',
        htmlContainer: 'mclb-swal-html'
      }
    });
  } catch (error) {
    Swal.fire({
      icon: 'error',
      title: 'Failed to Report',
      text: error.message || 'Could not submit message report.',
      buttonsStyling: false,
      customClass: {
        popup: 'mclb-swal-popup',
        title: 'mclb-swal-title',
        htmlContainer: 'mclb-swal-html',
        confirmButton: 'mclb-swal-confirm'
      }
    });
  }
}

/**
 * Update presence
 */
async function updatePresence(onPage) {
  try {
    // Presence updates must go through backend for security (no direct client writes)
    await apiService.updatePresence(matchId, onPage === true);
  } catch (error) {
    console.error('Error updating presence:', error);
  }
}

/**
 * Show add player modal
 */
// Add player functionality removed
function showAddPlayerModal() {
  Swal.fire({
    title: 'Add Player to Leaderboard',
    html: `
      <div class="form-group">
        <label>Minecraft Username</label>
        <input type="text" id="newPlayerUsername" class="form-input" placeholder="PlayerName" style="width: 100%; padding: 0.5rem; margin-top: 0.5rem;">
      </div>
      <div class="form-group" style="margin-top: 1rem;">
        <label>Region (Optional)</label>
        <select id="newPlayerRegion" class="form-select" style="width: 100%; padding: 0.5rem; margin-top: 0.5rem;">
          <option value="">Select region...</option>
          <option value="NA" style="background-color: #412328; color: white;">NA - North America</option>
          <option value="EU" style="background-color: #203e20; color: white;">EU - Europe</option>
          <option value="AS" style="background-color: #402c3f; color: white;">AS - Asia</option>
          <option value="SA" style="background-color: #1f3845; color: white;">SA - South America</option>
          <option value="AU" style="background-color: #382e27; color: white;">AU - Australia</option>
        </select>
      </div>
    `,
    showCancelButton: true,
    confirmButtonText: 'Add Player',
    preConfirm: () => {
      const username = document.getElementById('newPlayerUsername').value.trim();
      const region = document.getElementById('newPlayerRegion').value;

      if (!username) {
        Swal.showValidationMessage('Please enter a username');
        return false;
      }

      return { username, region: region || null };
    }
  }).then(async (result) => {
    if (result.isConfirmed) {
      await handleAddPlayerFromTesting(result.value.username, result.value.region);
    }
  });
}

/**
 * Handle add player from testing page
 */
async function handleAddPlayerFromTesting(username, region) {
  try {
    await apiService.createPlayer(username, region);
    Swal.fire({
      icon: 'success',
      title: 'Player Added!',
      text: `${username} has been added to the leaderboard.`,
      timer: 2000,
      showConfirmButton: false
    });
  } catch (error) {
    Swal.fire({
      icon: 'error',
      title: 'Failed to Add Player',
      text: error.message
    });
  }
}

/**
 * Handle finalize match
 */
async function handleFinalizeMatch() {
  if (!isTester || isSpectator) {
    return;
  }

  // Show score submission form for all match types
  showScoreSubmissionForm();
}


/**
 * Get match result submission information
 */
function getMatchResultInfo() {
  // In Elo system, testers just submit the score - no tier assignment needed
  return {
    requiresScoreSubmission: true,
    message: 'Submit the match score to calculate Elo rating changes.'
  };
}

/**
 * Show score submission form
 */
function showScoreSubmissionForm() {
  const firstTo = getMatchFirstTo();
  const winnerRequirement = `Winner must reach ${firstTo}`;
  const testerDisplayName = AppState.getProfile()?.minecraftUsername
    || match?.testerUsername
    || AppState.currentUser?.displayName
    || 'Tester';
  const modalBody = `
    <div class="finalize-modal-shell">
      <div class="finalize-modal-intro">
        <div class="finalize-modal-panel">
          <div class="finalize-modal-kicker">Match Review</div>
          <h3 class="finalize-modal-title">Confirm the completed score</h3>
          <p class="finalize-modal-copy">Enter the final result exactly as played. A tied result cannot be submitted, and the winner must hit the configured target.</p>
        </div>
        <div class="finalize-modal-panel finalize-meta-list">
          <div class="finalize-meta-row">
            <span class="finalize-meta-label">Player</span>
            <span class="finalize-meta-value">${escapeHtml(match.playerUsername)}</span>
          </div>
          <div class="finalize-meta-row">
            <span class="finalize-meta-label">Gamemode</span>
            <span class="finalize-meta-value">${escapeHtml(match.gamemode.toUpperCase())}</span>
          </div>
          <div class="finalize-meta-row">
            <span class="finalize-meta-label">Format</span>
            <span class="finalize-meta-value">First to ${firstTo}</span>
          </div>
        </div>
      </div>

      <div class="finalize-scoreboard">
        <div class="finalize-scoreboard-header">
          <div>
            <h4 class="finalize-scoreboard-title">Score Entry</h4>
            <p class="finalize-scoreboard-subtitle">Record both sides of the finished set.</p>
          </div>
          <div class="finalize-target-chip">${winnerRequirement}</div>
        </div>

        <div class="finalize-score-grid">
          <div class="finalize-score-card">
            <label for="playerScore" class="finalize-score-label">Player Score</label>
            <span class="finalize-score-name">${escapeHtml(match.playerUsername)}</span>
            <input type="number" id="playerScore" class="finalize-score-input" min="0" max="${firstTo}" value="0" inputmode="numeric">
          </div>
          <div class="finalize-score-divider">VS</div>
          <div class="finalize-score-card">
            <label for="testerScore" class="finalize-score-label">Your Score</label>
            <span class="finalize-score-name">${escapeHtml(testerDisplayName)}</span>
            <input type="number" id="testerScore" class="finalize-score-input" min="0" max="${firstTo}" value="0" inputmode="numeric">
          </div>
        </div>

        <div class="finalize-helper-row">
          <div class="finalize-helper-chip">No ties</div>
          <div class="finalize-helper-chip">Winner must reach ${firstTo}</div>
          <div class="finalize-helper-chip">Submit only once the match is complete</div>
        </div>
      </div>

      <div id="finalizeValidationMessage" class="finalize-validation"></div>
    </div>
  `;

  const modalFooter = `
    ${createModalButton('Cancel', 'custom-modal-btn-secondary', closeCustomModal)}
    ${createModalButton('Submit Score', 'custom-modal-btn-primary', () => {
      const playerScore = parseInt(document.getElementById('playerScore').value) || 0;
      const testerScore = parseInt(document.getElementById('testerScore').value) || 0;
      const validationEl = document.getElementById('finalizeValidationMessage');

      if (validationEl) {
        validationEl.textContent = '';
      }

      const showValidationError = (message) => {
        if (validationEl) {
          validationEl.textContent = message;
        }
      };

      if (playerScore + testerScore === 0) {
        showValidationError('At least one round must be played.');
        return;
      }

      if (playerScore === testerScore) {
        showValidationError('Ties are not allowed.');
        return;
      }

      if (playerScore !== firstTo && testerScore !== firstTo) {
        showValidationError(`Winner must reach ${firstTo}.`);
        return;
      }

      closeCustomModal();
      finalizeMatch({ playerScore, testerScore });
    })}
  `;

  showCustomModal('Submit Match Score', modalBody, modalFooter);
}

// Elo-based score submission system

/**
 * Finalize match
 */
async function finalizeMatch(data) {
  try {
    const result = await apiService.finalizeMatch(matchId, data);

    // Match results will be shown by handleMatchEnded() when realtime update comes through
    // Update local match status optimistically
    match.status = 'ended';
    match.finalized = true;
    match.finalizationData = data; // Store locally for immediate display

    // Show results immediately (will be replaced by realtime update if needed)
    showMatchResults(data, result);

  } catch (error) {
    Swal.fire({
      icon: 'error',
      title: 'Failed to Finalize',
      text: error.message
    });
  }
}

// Track finalized matches to prevent duplicate popups
const finalizedMatches = new Set();

/**
 * Show rank up animation with confetti
 */
// Make rank-up animation function globally available for testing
window.showRankUpAnimation = showRankUpAnimation;

function showRankUpAnimation(titleChanges, callback) {
  // Hide testing interface
  const mainContent = document.querySelector('main.container');
  if (mainContent) {
    mainContent.style.display = 'none';
  }

  const testingContainer = document.getElementById('testingContainer');
  if (testingContainer) {
    testingContainer.style.display = 'none';
  }

  // Create rank up overlay
  const overlay = document.createElement('div');
  overlay.id = 'rankUpOverlay';
  overlay.style.cssText = `
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    z-index: 10000;
    display: flex;
    align-items: center;
    justify-content: center;
    animation: fadeIn 1s ease-in-out;
  `;

  // Add light streams container and confetti container
  overlay.innerHTML = `
    <div id="light-streams-container" style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; pointer-events: none;"></div>
    <div id="confetti-container" style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; pointer-events: none;"></div>
    <div class="rank-up-content" style="
      text-align: center;
      color: white;
      z-index: 10001;
      position: relative;
      opacity: 0;
      transform: scale(0.5);
      transition: all 0.5s ease-out;
    ">
      <div class="rank-up-icon" style="
        font-size: 5rem;
        margin-bottom: 1rem;
        opacity: 0;
        transform: scale(0.3);
        transition: all 0.8s ease-out;
      "></div>
      <h1 style="
        font-size: 3rem;
        margin-bottom: 1rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.5);
        opacity: 0;
        transform: translateY(50px);
        transition: all 0.6s ease-out 1.5s;
      ">BADGE UNLOCKED!</h1>
      <div class="title-changes" style="
        opacity: 0;
        transform: translateY(30px);
        transition: all 0.6s ease-out 2s;
      "></div>
      <button class="continue-btn" style="
        background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
        border: none;
        color: white;
        padding: 1rem 2rem;
        font-size: 1.2rem;
        border-radius: 50px;
        cursor: pointer;
        margin-top: 2rem;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
        transition: transform 0.2s;
        opacity: 0;
        transform: translateY(20px);
        transition: all 0.6s ease-out 2.5s;
      " onmouseover="this.style.transform='scale(1.05)'" onmouseout="this.style.transform='scale(1)'">Continue to Results</button>
    </div>
  `;

  document.body.appendChild(overlay);

  // Create light streams effect
  createLightStreams();

  // Get elements for animation
  const rankUpContent = overlay.querySelector('.rank-up-content');
  const rankUpIcon = overlay.querySelector('.rank-up-icon');
  const badgeTitle = overlay.querySelector('h1');
  const titleChangesDiv = overlay.querySelector('.title-changes');
  const continueBtn = overlay.querySelector('.continue-btn');

  // Determine which badge to show (prefer player badge, fallback to tester)
  let badgeIcon = '';
  let badgeName = '';
  let badgeColor = '';

  if (titleChanges.player) {
    badgeIcon = titleChanges.player.newTitle.icon;
    badgeName = titleChanges.player.newTitle.title;
    badgeColor = titleChanges.player.newTitle.color;
  } else if (titleChanges.tester) {
    badgeIcon = titleChanges.tester.newTitle.icon;
    badgeName = titleChanges.tester.newTitle.title;
    badgeColor = titleChanges.tester.newTitle.color;
  }

  // Animation sequence
  setTimeout(() => {
    // Phase 1: Badge appears and grows with shaking (0-1s)
    rankUpIcon.innerHTML = `<img src="${badgeIcon}" alt="Badge" style="width: 50px; height: 50px; filter: brightness(2);">`;
    rankUpIcon.style.opacity = '1';
    rankUpIcon.style.animation = 'badgeAppear 1s ease-out';

    // Flash effect
    setTimeout(() => {
      overlay.style.background = 'white';
      setTimeout(() => {
        overlay.style.background = 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)';
      }, 100);
    }, 800);

  }, 500);

  setTimeout(() => {
    // Phase 2: Content expands and shows (1-2s)
    rankUpContent.style.opacity = '1';
    rankUpContent.style.transform = 'scale(1)';
    rankUpIcon.style.transform = 'scale(1)';
    rankUpIcon.style.animation = 'badgeFloat 2s ease-in-out infinite';

  }, 1500);

  setTimeout(() => {
    // Phase 3: Show title and details (2-3s)
    badgeTitle.style.opacity = '1';
    badgeTitle.style.transform = 'translateY(0)';

    // Populate title changes
    if (titleChanges.player) {
      const playerDiv = document.createElement('div');
      playerDiv.style.cssText = 'margin: 1rem 0;';
      playerDiv.innerHTML = `
        <h3 style="color: #FFD700; margin-bottom: 0.5rem;">${escapeHtml(match.playerUsername)}</h3>
        <div style="display: flex; align-items: center; justify-content: center; gap: 1rem; margin-bottom: 0.5rem;">
          <img src="${titleChanges.player.oldTitle.icon}" alt="${titleChanges.player.oldTitle.title}" style="width: 48px; height: 48px; opacity: 0.5;">
          <span style="font-size: 2rem; color: #FFD700;">→</span>
          <img src="${titleChanges.player.newTitle.icon}" alt="${titleChanges.player.newTitle.title}" style="width: 64px; height: 64px; animation: pulse 1s infinite;">
        </div>
        <div style="font-size: 1.5rem; color: ${badgeColor}; font-weight: bold;">
          ${badgeName}
        </div>
      `;
      titleChangesDiv.appendChild(playerDiv);
    }

    if (titleChanges.tester) {
      const testerDiv = document.createElement('div');
      testerDiv.style.cssText = 'margin: 1rem 0;';
      testerDiv.innerHTML = `
        <h3 style="color: #FFD700; margin-bottom: 0.5rem;">${escapeHtml(match.testerUsername)}</h3>
        <div style="display: flex; align-items: center; justify-content: center; gap: 1rem; margin-bottom: 0.5rem;">
          <img src="${titleChanges.tester.oldTitle.icon}" alt="${titleChanges.tester.oldTitle.title}" style="width: 48px; height: 48px; opacity: 0.5;">
          <span style="font-size: 2rem; color: #FFD700;">→</span>
          <img src="${titleChanges.tester.newTitle.icon}" alt="${titleChanges.tester.newTitle.title}" style="width: 64px; height: 64px; animation: pulse 1s infinite;">
        </div>
        <div style="font-size: 1.5rem; color: ${titleChanges.tester.newTitle.color}; font-weight: bold;">
          ${titleChanges.tester.newTitle.title}
        </div>
      `;
      titleChangesDiv.appendChild(testerDiv);
    }

    titleChangesDiv.style.opacity = '1';
    titleChangesDiv.style.transform = 'translateY(0)';

  }, 2000);

  setTimeout(() => {
    // Phase 4: Show continue button (2.5s+)
    continueBtn.style.opacity = '1';
    continueBtn.style.transform = 'translateY(0)';

    // Add confetti animation
    createConfetti();

  }, 2500);

  // Continue button handler
  continueBtn.onclick = () => {
    overlay.remove();
    if (callback) callback();
  };

  // Auto-continue after 8 seconds (longer for more dramatic effect)
  setTimeout(() => {
    if (document.body.contains(overlay)) {
      overlay.remove();
      if (callback) callback();
    }
  }, 8000);
}

/**
 * Create light streams animation
 */
function createLightStreams() {
  const container = document.getElementById('light-streams-container');
  if (!container) return;

  for (let i = 0; i < 12; i++) {
    const stream = document.createElement('div');
    stream.style.cssText = `
      position: absolute;
      width: 2px;
      height: 100px;
      background: linear-gradient(to bottom, rgba(255, 255, 255, 0.8), rgba(255, 255, 255, 0));
      top: 50%;
      left: 50%;
      transform-origin: 50% 100%;
      animation: lightStream 2s ease-out ${i * 0.1}s both;
      opacity: 0;
    `;
    container.appendChild(stream);

    // Remove light streams after animation
    setTimeout(() => {
      if (stream.parentNode) {
        stream.remove();
      }
    }, 3000);
  }
}

/**
 * Create confetti animation
 */
function createConfetti() {
  const container = document.getElementById('confetti-container');
  if (!container) return;

  for (let i = 0; i < 100; i++) {
    const confetti = document.createElement('div');
    confetti.style.cssText = `
      position: absolute;
      width: 10px;
      height: 10px;
      background: ${['#FFD700', '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57'][Math.floor(Math.random() * 6)]};
      left: ${Math.random() * 100}%;
      top: -10px;
      animation: confettiFall ${2 + Math.random() * 3}s linear ${Math.random() * 2}s both;
      transform: rotate(${Math.random() * 360}deg);
    `;
    container.appendChild(confetti);

    // Remove confetti after animation
    setTimeout(() => {
      if (confetti.parentNode) {
        confetti.remove();
      }
    }, 5000);
  }
}

/**
 * Show match results info page
 */
function showMatchResults(finalizationData, apiResult) {
  // Check for title changes and show rank up animation first
  if (finalizationData.titleChanges && (finalizationData.titleChanges.player || finalizationData.titleChanges.tester)) {
    return showRankUpAnimation(finalizationData.titleChanges, () => showMatchResultsOverlay(finalizationData, apiResult));
  }

  // No title changes, show results directly
  showMatchResultsOverlay(finalizationData, apiResult);
}

/**
 * Show match results overlay (renamed from original showMatchResults)
 */
function showMatchResultsOverlay(finalizationData, apiResult) {
  // Prevent duplicate results pages for the same match
  if (finalizedMatches.has(matchId)) {
    return;
  }

  // Ensure match data is available
  if (!match) {
    console.error('Match data not available for results display');
    return;
  }

  finalizedMatches.add(matchId);
  const { playerScore, testerScore, ratingChanges, titleChanges } = finalizationData;
  const isDrawWithoutScoring = ['draw_vote', 'draw_timeout'].includes(finalizationData?.type);
  const drawSubtitle = finalizationData?.type === 'draw_timeout'
    ? 'The 5 minute start window expired, so the match was finalized as a draw without rating changes.'
    : 'Both participants agreed to end match without scoring.';

  // Note: last tested timestamp is now handled by backend during match finalization

  // Hide the testing interface and show results
  const mainContent = document.querySelector('main.container');
  if (mainContent) {
    mainContent.style.display = 'none';
  }

  // Create centered modal backdrop
  const backdrop = document.createElement('div');
  backdrop.id = 'matchResultsBackdrop';
  backdrop.style.cssText = `
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background: rgba(0, 0, 0, 0.8);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 10000;
    animation: fadeIn 0.5s ease-out;
  `;

  // Add confetti container
  const confettiContainer = document.createElement('div');
  confettiContainer.id = 'matchResultsConfetti';
  confettiContainer.style.cssText = `
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    pointer-events: none;
    z-index: 10001;
  `;
  backdrop.appendChild(confettiContainer);

  document.body.appendChild(backdrop);

  // Create animated modal content
  const modalContent = document.createElement('div');
  modalContent.className = 'match-results-modal-animated';
  modalContent.style.cssText = `
    background: var(--secondary-bg);
    border-radius: 1rem;
    padding: 3rem;
    box-shadow: 0 25px 50px rgba(0, 0, 0, 0.5);
    border: 1px solid var(--border-color);
    max-width: 900px;
    width: 90%;
    max-height: 80vh;
    overflow-y: auto;
    animation: modalSlideIn 0.6s ease-out;
    position: relative;
    z-index: 10002;
  `;

  backdrop.appendChild(modalContent);

  let resultsHtml = `
        <div class="match-results-header">
          <div class="success-icon">
            <i class="fas ${isDrawWithoutScoring ? 'fa-handshake' : 'fa-trophy'}"></i>
          </div>
          <h2>${isDrawWithoutScoring ? 'Match Ended As Draw' : 'Match Finalized!'}</h2>
          <p class="match-results-subtitle">${isDrawWithoutScoring ? drawSubtitle : 'Your ratings have been updated based on the match results'}</p>
        </div>

      <div class="match-results-details">
        <div class="results-grid">
          <div class="result-card">
            <h3>Match Details</h3>
            <div class="result-info">
              <div class="info-row">
                <span class="info-label">Gamemode:</span>
                <span class="info-value">${match.gamemode.toUpperCase()}</span>
              </div>
              <div class="info-row">
                <span class="info-label">Player:</span>
                <span class="info-value">${escapeHtml(match.playerUsername)}</span>
              </div>
              <div class="info-row">
                <span class="info-label">Tester:</span>
                <span class="info-value">${escapeHtml(match.testerUsername)}</span>
              </div>
              <div class="info-row">
                <span class="info-label">Region:</span>
                <span class="info-value">${match.region}</span>
              </div>
              <div class="info-row">
                <span class="info-label">Server:</span>
                <span class="info-value">${escapeHtml(match.serverIP)}</span>
              </div>
              <div class="info-row">
                <span class="info-label">Final Score:</span>
                <span class="info-value score-highlight">${playerScore || 0} - ${testerScore || 0}</span>
              </div>
            </div>
          </div>

          <div class="result-card">
            <h3>Rating Changes</h3>
            <div class="rating-changes">
              <div class="rating-change-item" id="playerRatingItem">
                <div class="player-info">
                  <span class="player-name">${escapeHtml(match.playerUsername)}</span>
                  <span class="player-role">(Player)</span>
                </div>
                <div class="rating-change">
                  <span class="rating-value" id="playerRatingValue">${ratingChanges?.playerOldRating || ratingChanges?.playerNewRating || 'N/A'}</span>
                  <span class="rating-diff ${ratingChanges?.playerRatingChange >= 0 ? 'positive' : 'negative'}" id="playerRatingDiff">
                    ${ratingChanges?.playerRatingChange >= 0 ? '+' : ''}${ratingChanges?.playerRatingChange || 0}
                  </span>
                </div>
              </div>
              <div class="rating-change-item" id="testerRatingItem">
                <div class="player-info">
                  <span class="player-name">${escapeHtml(match.testerUsername)}</span>
                  <span class="player-role">(Tester)</span>
                </div>
                <div class="rating-change">
                  <span class="rating-value" id="testerRatingValue">${ratingChanges?.testerOldRating || ratingChanges?.testerNewRating || 'N/A'}</span>
                  <span class="rating-diff ${ratingChanges?.testerRatingChange >= 0 ? 'positive' : 'negative'}" id="testerRatingDiff">
                    ${ratingChanges?.testerRatingChange >= 0 ? '+' : ''}${ratingChanges?.testerRatingChange || 0}
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

        <div class="match-results-actions">
          <button class="btn btn-secondary stay-on-page-btn" onclick="closeMatchResults()">
            <i class="fas fa-times"></i> Stay on Testing Page
          </button>
          <button class="btn btn-primary return-dashboard-btn" onclick="returnToDashboard()">
            <i class="fas fa-home"></i> Back to Dashboard
          </button>
        </div>
      </div>
    </div>
  `;

  modalContent.innerHTML = resultsHtml;

  // Start rating animations after a delay
  setTimeout(() => {
    animateRatingChanges(ratingChanges);
    createMatchResultsConfetti();
  }, 1000);

  // Add close functionality
  backdrop.addEventListener('click', (e) => {
    if (e.target === backdrop) {
      closeMatchResults();
    }
  });

  // Handle actions
  const stayBtn = modalContent.querySelector('.stay-on-page-btn');
  const dashboardBtn = modalContent.querySelector('.return-dashboard-btn');

  if (stayBtn) {
    stayBtn.onclick = () => {
      backdrop.remove();
      closeMatchResults();
    };
  }

  if (dashboardBtn) {
    dashboardBtn.onclick = () => {
      backdrop.remove();
      returnToDashboard();
    };
  }
}

/**
 * Animate rating changes with counting effect
 */
function animateRatingChanges(ratingChanges) {
  if (!ratingChanges) return;

  // Animate player rating
  if (ratingChanges.playerOldRating !== undefined && ratingChanges.playerNewRating !== undefined) {
    const playerRatingValue = document.getElementById('playerRatingValue');
    const playerRatingDiff = document.getElementById('playerRatingDiff');

    if (playerRatingValue && playerRatingDiff) {
      animateNumberChange(playerRatingValue, ratingChanges.playerOldRating, ratingChanges.playerNewRating, 1000);
      playerRatingDiff.style.opacity = '0';
      setTimeout(() => {
        playerRatingDiff.style.transition = 'opacity 0.5s ease-out';
        playerRatingDiff.style.opacity = '1';
      }, 1200);
    }
  }

  // Animate tester rating
  if (ratingChanges.testerOldRating !== undefined && ratingChanges.testerNewRating !== undefined) {
    const testerRatingValue = document.getElementById('testerRatingValue');
    const testerRatingDiff = document.getElementById('testerRatingDiff');

    if (testerRatingValue && testerRatingDiff) {
      animateNumberChange(testerRatingValue, ratingChanges.testerOldRating, ratingChanges.testerNewRating, 1000);
      testerRatingDiff.style.opacity = '0';
      setTimeout(() => {
        testerRatingDiff.style.transition = 'opacity 0.5s ease-out';
        testerRatingDiff.style.opacity = '1';
      }, 1200);
    }
  }
}

/**
 * Animate number change with counting effect
 */
function animateNumberChange(element, startValue, endValue, duration) {
  const startTime = performance.now();
  const difference = endValue - startValue;

  function updateNumber(currentTime) {
    const elapsed = currentTime - startTime;
    const progress = Math.min(elapsed / duration, 1);

    // Easing function for smooth animation
    const easeOut = 1 - Math.pow(1 - progress, 3);
    const currentValue = Math.round(startValue + (difference * easeOut));

    element.textContent = currentValue;

    if (progress < 1) {
      requestAnimationFrame(updateNumber);
    }
  }

  requestAnimationFrame(updateNumber);
}

/**
 * Create confetti for match results
 */
function createMatchResultsConfetti() {
  const container = document.getElementById('matchResultsConfetti');
  if (!container) return;

  for (let i = 0; i < 150; i++) {
    const confetti = document.createElement('div');
    confetti.style.cssText = `
      position: absolute;
      width: 10px;
      height: 10px;
      background: ${['#FFD700', '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57', '#FF9F43'][Math.floor(Math.random() * 7)]};
      left: ${Math.random() * 100}%;
      top: -10px;
      animation: matchResultsConfettiFall ${2 + Math.random() * 4}s linear ${Math.random() * 2}s both;
      transform: rotate(${Math.random() * 360}deg);
      border-radius: ${Math.random() > 0.5 ? '50%' : '0'};
    `;
    container.appendChild(confetti);

    // Remove confetti after animation
    setTimeout(() => {
      if (confetti.parentNode) {
        confetti.remove();
      }
    }, 6000);
  }
}

/**
 * Handle timeout auto-end when countdown reaches zero
 */
async function handleTimeoutAutoEnd() {
  try {
    const countdownElement = document.getElementById('playerJoinCountdown');
    if (!countdownElement || !match || !matchId) return;

    // Show expired message
    countdownElement.innerHTML = `
      <div class="countdown-expired">
        <i class="fas fa-exclamation-triangle text-danger"></i>
        <span class="text-danger">Time expired - finalizing match...</span>
      </div>
    `;

    const userId = AppState.getUserId();
    const isPlayer = match.playerId === userId;

    // Determine who should win based on who is still waiting
    let playerScore, testerScore, reason;

    if (isPlayer) {
      // Player is waiting for tester - player wins
      playerScore = 3;
      testerScore = 0;
      reason = 'Tester did not join within time limit';
    } else {
      // Tester is waiting for player - tester wins
      playerScore = 0;
      testerScore = 3;
      reason = 'Player did not join within time limit';
    }

    // Finalize the match with appropriate scores
    const response = await apiService.post(`/match/${matchId}/finalize`, {
      playerScore: playerScore,
      testerScore: testerScore
    });

    if (response.success) {
      console.log(`Match auto-finalized: ${playerScore}-${testerScore} (${reason})`);

      // Show success message
      countdownElement.innerHTML = `
        <div class="countdown-finalized">
          <i class="fas fa-check-circle text-success"></i>
          <span class="text-success">Match finalized: ${playerScore}-${testerScore}</span>
        </div>
      `;

      // Trigger match results display - the realtime listener should handle this
      // But we'll also trigger it manually as backup
      setTimeout(() => {
        // Refresh match data to trigger results display
        if (typeof checkActiveMatch === 'function') {
          checkActiveMatch();
        }
      }, 1500);
    } else {
      throw new Error(response.message || 'Failed to finalize match');
    }

  } catch (error) {
    console.error('Error auto-finalizing match on timeout:', error);

    const countdownElement = document.getElementById('playerJoinCountdown');
    if (countdownElement) {
      countdownElement.innerHTML = `
        <div class="countdown-error">
          <i class="fas fa-exclamation-triangle text-warning"></i>
          <span class="text-warning">Auto-finalization failed - please refresh</span>
        </div>
      `;
    }
  }
}

/**
 * Handle abort match
 */
async function handleAbortMatch() {
  if (isSpectator) {
    Swal.fire({
      icon: 'info',
      title: 'Spectator Mode',
      text: 'Spectators cannot abort matches.'
    });
    return;
  }

  // Check if match is already finalized
  if (match && match.finalized === true) {
    Swal.fire({
      icon: 'warning',
      title: 'Match Already Finalized',
      text: 'This match has already been finalized and cannot be aborted.',
      confirmButtonText: 'OK'
    });
    return;
  }
  
  const result = await Swal.fire({
    icon: 'warning',
    title: 'Abort Match?',
    text: 'Are you sure you want to abort this match?',
    showCancelButton: true,
    confirmButtonText: 'Yes, Abort',
    cancelButtonText: 'Cancel'
  });
  
  if (result.isConfirmed) {
    try {
      await apiService.abortMatch(matchId);
      // Match is now finalized with scores, so results will be shown automatically
      // via the secure match polling loop.
    } catch (error) {
      Swal.fire({
        icon: 'error',
        title: 'Failed to Abort',
        text: error.message
      });
    }
  }
}

function updateDrawVoteStatus() {
  const statusEl = document.getElementById('drawVoteStatus');
  const btn = document.getElementById('drawVoteBtn');
  if (!statusEl || !btn || !match) return;

  const playerVote = match.drawVotes?.[match.playerId]?.agree === true;
  const testerVote = match.drawVotes?.[match.testerId]?.agree === true;
  const myVote = match.drawVotes?.[AppState.getUserId()]?.agree === true;

  statusEl.innerHTML = `
    ${escapeHtml(match.playerUsername)}: <strong>${playerVote ? 'Agreed' : 'Pending'}</strong> |
    ${escapeHtml(match.testerUsername)}: <strong>${testerVote ? 'Agreed' : 'Pending'}</strong>
  `;

  btn.disabled = isSpectator || myVote || match.finalized === true || match.status === 'ended';
  btn.innerHTML = myVote
    ? '<i class="fas fa-check"></i> You Agreed'
    : '<i class="fas fa-handshake"></i> Agree to End Match Without Scoring';
}

async function handleDrawVote() {
  if (isSpectator) {
    return;
  }

  try {
    const result = await apiService.voteDraw(matchId, true);
    if (result?.votes) {
      match.drawVotes = {
        ...(match.drawVotes || {}),
        [match.playerId]: { agree: result.votes.playerAgreed === true },
        [match.testerId]: { agree: result.votes.testerAgreed === true }
      };
    }
    updateDrawVoteStatus();
  } catch (error) {
    Swal.fire({
      icon: 'error',
      title: 'Draw Vote Failed',
      text: error.message || 'Could not submit draw vote.'
    });
  }
}

/**
 * Handle match ended
 */
function handleMatchEnded() {
  // Show match results overlay when match ends (detected via secure polling)
  // Only show if not already shown by finalizeMatch()
  if (match && match.finalizationData && !finalizedMatches.has(matchId)) {
    showMatchResults(match.finalizationData, null);
  }
}

/**
 * Escape HTML
 */
function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// Cleanup on page unload
window.addEventListener('beforeunload', () => {
  stopSecureMatchPolling();
});

/**
 * Show custom modal
 */
function showCustomModal(title, body, footer) {
  // Remove existing modal if any
  const existingModal = document.getElementById('custom-modal-overlay');
  if (existingModal) {
    existingModal.remove();
  }

  // Create modal HTML
  const modalHTML = `
    <div id="custom-modal-overlay" class="custom-modal-overlay">
      <div class="custom-modal">
        <div class="custom-modal-header">
          <h3>${escapeHtml(title)}</h3>
          <button class="custom-modal-close" onclick="closeCustomModal()">&times;</button>
        </div>
        <div class="custom-modal-body">
          ${body}
        </div>
        ${footer ? `<div class="custom-modal-footer">${footer}</div>` : ''}
      </div>
    </div>
  `;

  // Add to page
  document.body.insertAdjacentHTML('beforeend', modalHTML);
}

/**
 * Close custom modal
 */
function closeCustomModal() {
  const modal = document.getElementById('custom-modal-overlay');
  if (modal) {
    modal.remove();
  }
}

/**
 * Toggle settings panel visibility
 */
function toggleSettings() {
  const settingsPanel = document.getElementById('settingsPanel');
  if (settingsPanel) {
    settingsPanel.classList.toggle('open');
  }
}

/**
 * Create modal button
 */
function createModalButton(text, className, onClick) {
  buttonHandlerCounter++;
  const buttonId = 'modalBtn_' + buttonHandlerCounter;

  // Store the click handler globally
  window[buttonId] = onClick;

  return `<button class="custom-modal-btn ${className}" onclick="window['${buttonId}']()">${escapeHtml(text)}</button>`;
}

/**
 * Close match results and stay on testing page
 */
function closeMatchResults() {
  const resultsContainer = document.getElementById('matchResultsContainer');
  const mainContent = document.querySelector('main.container');

  resultsContainer.style.display = 'none';
  if (mainContent) {
    mainContent.style.display = 'block';
  }
}

/**
 * Return to dashboard after viewing match results
 */
function returnToDashboard() {
  window.location.href = 'dashboard.html';
}

/**
 * Show countdown timer for player join timeout
 */
function showPlayerJoinCountdown() {
  if (!match.playerJoinTimeout || !match.playerJoinTimeout.autoEndEnabled) return;

  const startedAt = new Date(match.playerJoinTimeout.startedAt);
  const timeoutMs = match.playerJoinTimeout.timeoutMinutes * 60 * 1000;
  const endTime = startedAt.getTime() + timeoutMs;

  // Clear any existing countdown
  if (window.playerJoinCountdown) {
    clearInterval(window.playerJoinCountdown);
  }

  const countdownElement = document.getElementById('playerJoinCountdown');
  if (!countdownElement) return;

  window.playerJoinCountdown = setInterval(() => {
    const now = Date.now();
    const remaining = Math.max(0, endTime - now);

    if (remaining <= 0) {
      // Time's up - server will handle auto-end
      clearInterval(window.playerJoinCountdown);
      countdownElement.innerHTML = `
        <div class="countdown-expired">
          <i class="fas fa-exclamation-triangle text-danger"></i>
          <span class="text-danger">Time expired - match auto-ending...</span>
        </div>
      `;
      return;
    }

    const minutes = Math.floor(remaining / (60 * 1000));
    const seconds = Math.floor((remaining % (60 * 1000)) / 1000);

    const userId = AppState.getUserId();
    const isPlayer = match.playerId === userId;
    const waitingFor = isPlayer ? 'tier tester' : 'player';
    const firstTo = getMatchFirstTo();
    const winner = isPlayer ? `You (${firstTo}-0)` : `Tier tester (${firstTo}-0)`;

    countdownElement.innerHTML = `
      <div class="countdown-warning">
        <i class="fas fa-clock text-warning"></i>
        <div class="countdown-text">
          Waiting for ${waitingFor} to join: ${minutes}:${seconds.toString().padStart(2, '0')}
        </div>
        <div class="countdown-subtext small text-muted">
          Match auto-ends in ${minutes}:${seconds.toString().padStart(2, '0')} if ${waitingFor} doesn't join
        </div>
        <div class="countdown-outcome small text-info">
          <i class="fas fa-trophy"></i> ${winner} will win if ${waitingFor} doesn't join in time
        </div>
      </div>
    `;

    // Show warning in last minute
    if (remaining <= 60000) {
      countdownElement.classList.add('urgent');
    }
  }, 1000);
}

/**
 * Auto-end match when player doesn't join within timeout
 */
// This function is no longer used - server handles all auto-end logic
async function autoEndMatchForNoShow() {
  // Server now handles all auto-end logic automatically
  console.log('Auto-end request received - server will handle this automatically');
}

/**
 * Report player for not showing up to match
 */
async function reportPlayerForNoShow() {
  try {
    // This would typically send a report to the backend
    // For now, just log it
    console.log('Reporting player', match.playerUsername, 'for not joining match', matchId);

    // In a real implementation, you'd send this to the backend
    // await apiService.reportPlayer(match.playerId, 'no_show', `Failed to join match ${matchId} within 3 minutes`);

  } catch (error) {
    console.error('Error reporting player:', error);
  }
}

/**
 * Report tester for not showing up to match
 */
async function reportTesterForNoShow() {
  try {
    // This would typically send a report to the backend
    // For now, just log it
    console.log('Reporting tester', match.testerUsername, 'for not joining match', matchId);

    // In a real implementation, you'd send this to the backend
    // await apiService.reportPlayer(match.testerId, 'no_show', `Failed to join match ${matchId} within 3 minutes`);

  } catch (error) {
    console.error('Error reporting tester:', error);
  }
}

// Make functions globally available
window.closeCustomModal = closeCustomModal;
window.createModalButton = createModalButton;
window.showCustomModal = showCustomModal;
window.toggleSettings = toggleSettings;
window.closeMatchResults = closeMatchResults;
window.returnToDashboard = returnToDashboard;

// Initialize on page load
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initTesting);
} else {
  initTesting();
}

