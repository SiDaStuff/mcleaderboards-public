// MC Leaderboards - Leaderboard Display Logic

let currentGamemode = 'overall';
let allPlayers = [];
let filteredPlayers = [];
let blacklist = [];
let lastPlayersLoadTime = 0;
const PLAYERS_CACHE_TTL = 50; // 50ms minimal cache
let searchDebounceTimer = null;

// Autocomplete state
let autocompleteDebounceTimer = null;

/**
 * Handle search input for autocomplete suggestions
 */
async function handleSearchInput() {
  clearTimeout(autocompleteDebounceTimer);
  const searchInput = document.getElementById('searchInput');
  if (!searchInput) return;
  
  const query = searchInput.value.trim();
  
  // Show autocomplete if query is long enough
  if (query.length >= 2) {
    autocompleteDebounceTimer = setTimeout(async () => {
      try {
        const response = await apiService.getPlayerSuggestions(query, 15);
        showAutocompleteSuggestions(response.suggestions || []);
      } catch (error) {
        console.error('Error fetching autocomplete suggestions:', error);
      }
    }, 300); // Debounce for 300ms
  } else {
    hideAutocompleteSuggestions();
  }
}

/**
 * Display autocomplete suggestions dropdown
 */
function showAutocompleteSuggestions(suggestions) {
  const searchInput = document.getElementById('searchInput');
  if (!searchInput) return;
  
  // Remove existing dropdown
  hideAutocompleteSuggestions();
  
  if (suggestions.length === 0) {
    return;
  }
  
  // Create dropdown container
  const dropdown = document.createElement('div');
  dropdown.id = 'autocompleteDropdown';
  dropdown.className = 'autocomplete-dropdown';
  dropdown.style.cssText = `
    top: ${searchInput.offsetTop + searchInput.offsetHeight + 4}px;
    left: ${searchInput.offsetLeft}px;
    width: ${searchInput.offsetWidth}px;
    max-height: 300px;
    overflow-y: auto;
  `;
  
  // Add suggestions
  suggestions.forEach((suggestion, index) => {
    const item = document.createElement('div');
    item.className = 'autocomplete-item';
    
    item.innerHTML = `
      <div>${escapeHtml(suggestion.username)}</div>
      <div>${suggestion.overallRating} Elo${suggestion.region ? ' &bull; ' + escapeHtml(suggestion.region) : ''}</div>
    `;
    
    item.onclick = async () => {
      searchInput.value = suggestion.username;
      hideAutocompleteSuggestions();
      handleSearch();
    };
    
    dropdown.appendChild(item);
  });
  
  // Insert dropdown after search input
  searchInput.parentElement.style.position = 'relative';
  searchInput.parentElement.appendChild(dropdown);
}

/**
 * Hide autocomplete dropdown
 */
function hideAutocompleteSuggestions() {
  const dropdown = document.getElementById('autocompleteDropdown');
  if (dropdown) {
    dropdown.remove();
  }
}

// Infinite scroll state
let currentOffset = 0;
const LOAD_COUNT = 10; // Load 10 players per batch
const LOAD_BATCH_COOLDOWN_MS = 0; // No cooldown - load instantly
let isLoadingMore = false;
let hasMorePlayers = true;
let totalPlayers = 0;
let batchCooldownTimer = null;

/**
 * Show provisional rating tooltip
 */
function showProvisionalTooltip(gamemode) {
  Swal.fire({
    title: 'Provisional Rating',
    html: `
      <div style="text-align: left;">
        <p><strong>What does this mean?</strong></p>
        <p>This player has played fewer than 10 matches in ${gamemode}, so their rating is considered "provisional".</p>
        <p><strong>Why provisional?</strong></p>
        <ul style="text-align: left;">
          <li>Ratings with fewer matches are less reliable</li>
          <li>The Elo system needs more data to accurately rank players</li>
          <li>Provisional ratings may change more significantly as more matches are played</li>
        </ul>
        <p><strong>When does it become stable?</strong></p>
        <p>After playing 10+ matches in this gamemode, the provisional indicator will disappear.</p>
      </div>
    `,
    icon: 'info',
    confirmButtonText: 'Got it!'
  });
}

function openReportPageForPlayer(playerName) {
  const safePlayer = encodeURIComponent((playerName || '').trim());
  const targetUrl = safePlayer ? `report.html?player=${safePlayer}` : 'report.html';
  window.location.href = targetUrl;
}

/**
 * Initialize leaderboard
 */
async function initLeaderboard() {
  // Update loading status
  if (window.mclbLoadingOverlay) {
    window.mclbLoadingOverlay.updateStatus('Loading leaderboard...', 85);
  }
  
  await Promise.all([
    loadPlayers(),
    loadBlacklist()
  ]);
  
  renderGamemodeTabs();
  renderLeaderboard();

  // Set up infinite scroll
  setupInfiniteScroll();
  
  // Signal that all initial loading is complete
  if (window.mclbLoadingOverlay) {
    window.mclbLoadingOverlay.updateStatus('Leaderboard ready!', 100);
  }

  // Auto-refresh data every 30 seconds (only when page is visible)
  // Use longer interval when page is hidden to save resources
  let refreshInterval = 30000; // 30 seconds when visible
  const hiddenInterval = 120000; // 2 minutes when hidden
  
  const refreshLeaderboard = async () => {
    // Only refresh if page is visible and data is stale
    if (document.visibilityState === 'visible') {
      const now = Date.now();
      // Only reload if data is older than cache TTL
      if (!allPlayers.length || (now - lastPlayersLoadTime) > PLAYERS_CACHE_TTL) {
        try {
          await loadPlayers();
          await loadBlacklist();
          renderLeaderboard();
        } catch (error) {
          // Stop auto-refresh on rate limit errors
          if (error.message && error.message.includes('429')) {
            console.log('Rate limited, stopping auto-refresh');
            return;
          }
          console.error('Error refreshing leaderboard:', error);
        }
      }
    }
  };
  
  // Adjust interval based on page visibility
  document.addEventListener('visibilitychange', () => {
    refreshInterval = document.visibilityState === 'visible' ? 30000 : hiddenInterval;
  });
  
  setInterval(refreshLeaderboard, refreshInterval);
}

/**
 * Setup infinite scroll
 */
function setupInfiniteScroll() {
  let scrollTimeout;
  
  window.addEventListener('scroll', () => {
    // Debounce scroll events
    clearTimeout(scrollTimeout);
    scrollTimeout = setTimeout(() => {
      const scrollPosition = window.innerHeight + window.scrollY;
      const threshold = document.documentElement.scrollHeight - 500; // Load when 500px from bottom
      
    // Setup search input autocomplete
    const searchInput = document.getElementById('searchInput');
    if (searchInput) {
      searchInput.addEventListener('input', handleSearchInput);
      searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          handleSearch();
        }
      });
    }

      if (scrollPosition >= threshold && hasMorePlayers && !isLoadingMore) {
        loadMorePlayers().then(() => {
          // Render is handled inside loadMorePlayers
        }).catch(error => {
          console.error('Error loading more players:', error);
        });
      }
    }, 100); // 100ms debounce
  });
}

/**
 * Load blacklist
 */
async function loadBlacklist() {
  try {
    if (AppState.isAdmin()) {
      const response = await apiService.getBlacklist();
      blacklist = response.blacklist || [];
    }
  } catch (error) {
    console.error('Error loading blacklist:', error);
  }
}

/**
 * Load initial players from API (first batch)
 */
async function loadPlayers() {
  try {
    AppState.setLoading('players', true);
    clearBatchCooldownTimer();
    
    // Reset pagination state
    currentOffset = 0;
    allPlayers = [];
    hasMorePlayers = true;
    
    // Load first batch (this will call renderLeaderboard internally)
    await loadMorePlayers();
    
  } catch (error) {
    console.error('Error loading players:', error);
    showError(error.message || 'Failed to load leaderboard. Please try again.');
    // Set empty arrays on error to prevent infinite loading
    allPlayers = [];
    filteredPlayers = [];
  } finally {
    // Always clear loading state
    AppState.setLoading('players', false);
    // Render is already handled by loadMorePlayers, but ensure it's called on error
    if (allPlayers.length === 0) {
      renderLeaderboard();
    }
  }
}

/**
 * Load more players (for infinite scroll)
 */
async function loadMorePlayers() {
  if (isLoadingMore || !hasMorePlayers) return;
  
  try {
    isLoadingMore = true;
    const requestOffset = currentOffset;

    // Apply 5s cooldown for every additional batch load (not the first load).
    if (currentOffset > 0) {
      await runBatchLoadCooldown();
    }
    renderLoadMoreIndicator('loading');
    
    const response = await apiService.getPlayers(currentGamemode, requestOffset, LOAD_COUNT);
    
    // Ensure we have a valid response
    if (!response || typeof response !== 'object') {
      throw new Error('Invalid response from server');
    }
    
    const newPlayers = response.players || [];
    totalPlayers = response.total || 0;
    hasMorePlayers = response.hasMore || false;
    
    // Mark blacklisted players (they will have special handling from backend)
    const blacklistedUsernames = new Set(
      blacklist.map(entry => entry.username?.toLowerCase()).filter(Boolean)
    );

    // Mark blacklisted players
    newPlayers.forEach(player => {
      if (player.username && blacklistedUsernames.has(player.username.toLowerCase())) {
        player.blacklisted = true;
      }
    });

    // Exclude blacklisted players from the leaderboard datasets entirely
    const visibleNewPlayers = newPlayers.filter(player => !player.blacklisted);

    // Deduplicate to prevent repeated top entries if backend/client pagination overlaps.
    const existingKeys = new Set(
      allPlayers.map(p => String(p.userId || p.id || (p.username || '').toLowerCase()))
    );
    const uniqueVisibleNewPlayers = visibleNewPlayers.filter((player) => {
      const key = String(player.userId || player.id || (player.username || '').toLowerCase());
      if (existingKeys.has(key)) return false;
      existingKeys.add(key);
      return true;
    });

    // Append new non-blacklisted unique players to existing array
    allPlayers.push(...uniqueVisibleNewPlayers);
    lastPlayersLoadTime = Date.now(); // Update cache timestamp
    
    const isFirstLoad = requestOffset === 0;

    // Update offset for next load using server pagination metadata when available
    const responseOffset = Number.isFinite(response?.offset) ? Number(response.offset) : requestOffset;
    const responseLimit = Number.isFinite(response?.limit) ? Number(response.limit) : LOAD_COUNT;
    currentOffset = Math.max(responseOffset + responseLimit, requestOffset + LOAD_COUNT);
    
    allPlayers = sortPlayersForCurrentGamemode(allPlayers, currentGamemode);

    allPlayers.forEach((player, index) => {
      if (!Number.isFinite(player.rank) || player.rank <= 0) {
        player.rank = index + 1;
      }
    });
    filteredPlayers = [...allPlayers];

    if (isFirstLoad) {
      renderLeaderboard();
    } else {
      appendPlayersToLeaderboard(uniqueVisibleNewPlayers);
    }

    if (hasMorePlayers) {
      renderLoadMoreIndicator('idle');
    } else {
      renderLoadMoreIndicator('done');
    }
    
  } catch (error) {
    console.error('Error loading more players:', error);
    hasMorePlayers = false; // Stop trying to load more on error
    renderLoadMoreIndicator('done');
  } finally {
    isLoadingMore = false;
  }
}

function clearBatchCooldownTimer() {
  if (batchCooldownTimer) {
    clearInterval(batchCooldownTimer);
    batchCooldownTimer = null;
  }
}

async function runBatchLoadCooldown() {
  return new Promise((resolve) => {
    const cooldownEnd = Date.now() + LOAD_BATCH_COOLDOWN_MS;
    clearBatchCooldownTimer();
    renderLoadMoreIndicator('cooldown', LOAD_BATCH_COOLDOWN_MS);

    batchCooldownTimer = setInterval(() => {
      const remainingMs = Math.max(0, cooldownEnd - Date.now());
      renderLoadMoreIndicator('cooldown', remainingMs);
      if (remainingMs <= 0) {
        clearBatchCooldownTimer();
        resolve();
      }
    }, 200);
  });
}

function getLoadMoreIndicatorMessage(mode = 'idle', remainingMs = 0) {
  if (mode === 'cooldown') {
    const seconds = Math.max(1, Math.ceil(remainingMs / 1000));
    return `Loading next ${LOAD_COUNT} players in ${seconds}s...`;
  }
  if (mode === 'loading') {
    return `Loading next ${LOAD_COUNT} players...`;
  }
  return `Scroll to load next ${LOAD_COUNT} players`;
}

function renderLoadMoreIndicator(mode = 'idle', remainingMs = 0) {
  const content = document.getElementById('leaderboardContent');
  if (!content) return;

  let container = document.getElementById('loadingMore');
  if (!hasMorePlayers || mode === 'done') {
    if (container) container.remove();
    return;
  }

  const needsSpinner = mode === 'cooldown' || mode === 'loading';
  if (!container) {
    container = document.createElement('div');
    container.id = 'loadingMore';
    container.style.textAlign = 'center';
    container.style.padding = '1.5rem';
    content.appendChild(container);
  }

  // Only rebuild markup when mode changes so spinner animation stays smooth.
  if (container.dataset.mode !== mode) {
    container.dataset.mode = mode;
    if (needsSpinner) {
      container.innerHTML = `
        <div class="spinner"></div>
        <p class="text-muted mt-2 loading-more-message"></p>
      `;
    } else {
      container.innerHTML = '<p class="text-muted loading-more-message"></p>';
    }
  }

  const messageEl = container.querySelector('.loading-more-message');
  if (messageEl) {
    messageEl.textContent = getLoadMoreIndicatorMessage(mode, remainingMs);
  }
}

/**
 * Render gamemode tabs
 */
function renderGamemodeTabs() {
  const tabsContainer = document.getElementById('gamemodeTabs');
  if (!tabsContainer) return;

  tabsContainer.innerHTML = '';

  CONFIG.GAMEMODES.forEach(gamemode => {
    const isActive = currentGamemode === gamemode.id;
    const tabHtml = `
      <button class="gamemode-tab-btn ${isActive ? 'btn-primary' : 'btn-secondary'}"
              onclick="handleTabClick('${gamemode.id}')"
              id="tab-${gamemode.id}">
        <img src="${gamemode.icon}" alt="${gamemode.name}" class="gamemode-tab-icon">
        <span class="gamemode-tab-text">${gamemode.name}</span>
      </button>
    `;
    tabsContainer.innerHTML += tabHtml;
  });
}

/**
 * Handle tab click (async wrapper)
 */
async function handleTabClick(gamemode) {
  await switchGamemode(gamemode);
}

/**
 * Show loading overlay
 */
function showLoadingOverlay() {
  const overlay = document.createElement('div');
  overlay.className = 'loading-overlay';
  overlay.id = 'leaderboardLoadingOverlay';
  overlay.innerHTML = '<div class="spinner"></div>';
  document.body.appendChild(overlay);
}

/**
 * Hide loading overlay
 */
function hideLoadingOverlay() {
  const overlay = document.getElementById('leaderboardLoadingOverlay');
  if (overlay) {
    overlay.remove();
  }
}

/**
 * Switch gamemode
 */
async function switchGamemode(gamemode) {
  currentGamemode = gamemode;
  clearBatchCooldownTimer();
  renderGamemodeTabs();

  // Show loading overlay
  showLoadingOverlay();

  try {
    // Reset pagination state when switching gamemodes
    currentOffset = 0;
    allPlayers = [];
    hasMorePlayers = true;
    
    // Load first batch of players for new gamemode
    await loadPlayers();
    await loadBlacklist();

    filterPlayersByGamemode();
    renderLeaderboard();

    // Update title
    const title = document.getElementById('leaderboardTitle');
    if (title) {
      const gamemodeName = CONFIG.GAMEMODES.find(g => g.id === gamemode)?.name || 'Overall';
      title.textContent = `${gamemodeName} Leaderboard`;
    }
  } finally {
    // Hide loading overlay
    hideLoadingOverlay();
  }
}


/**
 * Calculate overall rating for a player based on their gamemode ratings
 */
function calculateOverallRating(player) {
  if (!player.gamemodeRatings) return 0;
  
  const ratings = [];
  CONFIG.GAMEMODES.forEach(gm => {
    if (gm.id !== 'overall' && player.gamemodeRatings[gm.id]) {
      ratings.push(player.gamemodeRatings[gm.id]);
    }
  });
  
  if (ratings.length === 0) return 0;
  
  // Calculate average of all gamemode ratings
  const sum = ratings.reduce((acc, rating) => acc + rating, 0);
  return Math.round(sum / ratings.length);
}

function hasAllGamemodeRatings(player) {
  const requiredGamemodes = CONFIG.GAMEMODES.filter(gm => gm.id !== 'overall');
  return requiredGamemodes.every(gm => {
    const rating = Number(player.gamemodeRatings?.[gm.id]);
    return Number.isFinite(rating) && rating > 0;
  });
}

function getLeaderboardSortValue(player, gamemode = currentGamemode) {
  if (gamemode === 'overall') {
    return Number(player.overallRating !== undefined ? player.overallRating : calculateOverallRating(player)) || 0;
  }

  return Number(player.gamemodeRatings?.[gamemode]) || 0;
}

function sortPlayersForCurrentGamemode(players, gamemode = currentGamemode) {
  return [...players].sort((leftPlayer, rightPlayer) => {
    const leftRank = Number(leftPlayer.rank);
    const rightRank = Number(rightPlayer.rank);

    if (Number.isFinite(leftRank) && Number.isFinite(rightRank) && leftRank !== rightRank) {
      return leftRank - rightRank;
    }

    const ratingDelta = getLeaderboardSortValue(rightPlayer, gamemode) - getLeaderboardSortValue(leftPlayer, gamemode);
    if (ratingDelta !== 0) {
      return ratingDelta;
    }

    return String(leftPlayer.username || '').localeCompare(String(rightPlayer.username || ''), undefined, { sensitivity: 'base' });
  });
}

/**
 * Filter players by gamemode
 */
function filterPlayersByGamemode() {
  // Exclude blacklisted players from all leaderboard views.
  const visiblePlayers = allPlayers.filter(player => !player.blacklisted);

  if (currentGamemode === 'overall') {
    filteredPlayers = sortPlayersForCurrentGamemode(visiblePlayers
      .map(player => ({
        ...player,
        overallRating: player.overallRating !== undefined ? player.overallRating : calculateOverallRating(player)
      })), 'overall');
  } else {
    filteredPlayers = sortPlayersForCurrentGamemode(visiblePlayers
      .filter(player => {
        const rating = player.gamemodeRatings?.[currentGamemode];
        return rating !== undefined && rating !== null;
      })
      .map(player => ({
        ...player,
        gamemodePoints: player.gamemodePoints?.[currentGamemode] || 0,
        gamemodeTier: player.gamemodeTiers?.[currentGamemode] || null
      })), currentGamemode);
  }
}

/**
 * Handle search input
 */
function handleSearchInput() {
  // Search input is now only for player lookup, not filtering
  // Do nothing here to avoid interfering with leaderboard display
}

/**
 * Handle search
 */
async function handleSearch() {
  const searchInput = document.getElementById('searchInput');
  if (!searchInput) return;

  const query = searchInput.value.trim();
  if (query) {
    // Perform player lookup - search all players including those not in current leaderboard
    try {
      // Use cached data if available and fresh, otherwise fetch
      let allPlayersData = allPlayers;
      const now = Date.now();
      if (!allPlayersData.length || (now - lastPlayersLoadTime) > PLAYERS_CACHE_TTL) {
        const response = await apiService.getPlayers();
        allPlayersData = response.players || [];
        allPlayers = allPlayersData;
        lastPlayersLoadTime = now;
      }

      // Find exact match first, then partial match
      let player = allPlayersData.find(p =>
        p.username?.toLowerCase() === query.toLowerCase()
      );

      if (!player) {
        // Try partial match
        player = allPlayersData.find(p =>
          p.username?.toLowerCase().includes(query.toLowerCase())
        );
      }

      if (player) {
        // Add role information to player if not present
        if (!player.roles && player.createdBy && AppState.currentUser) {
          try {
            const userResponse = await apiService.getProfile();
            if (userResponse.createdBy === player.createdBy) {
              player.roles = {
                admin: userResponse.admin || false,
                tester: userResponse.tester || false
              };
            }
          } catch (error) {
            console.error('Error fetching user roles for search:', error);
          }
        }

        openPlayerModal(player);
        searchInput.value = '';
      } else {
        // No player found
        Swal.fire({
          icon: 'info',
          title: 'Player Not Found',
          text: `No player found with username "${query}"`,
          timer: 2000,
          showConfirmButton: false
        });
      }
    } catch (error) {
      console.error('Error searching for player:', error);
      Swal.fire({
        icon: 'error',
        title: 'Search Error',
        text: 'Failed to search for player. Please try again.',
        timer: 2000,
        showConfirmButton: false
      });
    }
  }
}

/**
 * Apply search filter
 */
function applySearchFilter(query, includeBlacklisted = false) {
  const lowerQuery = query.toLowerCase();
  // Since blacklisted players are now completely filtered out from allPlayers,
  // we only search within the non-blacklisted players
  const searchBase = allPlayers;

  filteredPlayers = searchBase.filter(player => {
    const matches = player.username?.toLowerCase().includes(lowerQuery);
    return matches;
  });
}

// Search highlighting functions removed - search is now pure lookup only

/**
 * Render leaderboard
 */
function renderLeaderboard() {
  const content = document.getElementById('leaderboardContent');
  if (!content) return;

  if (AppState.loading.players) {
    content.innerHTML = '<div class="spinner"></div>';
    return;
  }

  filterPlayersByGamemode();

  if (currentGamemode === 'overall') {
    renderOverallLeaderboard();
  } else {
    renderGamemodeLeaderboard();
  }
}

/**
 * Render overall leaderboard
 */
function renderOverallLeaderboard() {
  const content = document.getElementById('leaderboardContent');
  if (!content) return;

  // Show all loaded players (no limit for infinite scroll)
  const topPlayers = filteredPlayers;

  if (topPlayers.length === 0) {
    content.innerHTML = `
      <div class="empty-state">
        <div class="empty-state-icon">
          <i class="fas fa-users"></i>
        </div>
        <h3>No players found</h3>
        <p class="text-muted">Try adjusting your search.</p>
      </div>
    `;
    return;
  }

  content.innerHTML = topPlayers.map((player, index) => renderOverallPlayerCard(player, index)).join('');
  
  // Add idle load-more indicator at the bottom if there are more players
  renderLoadMoreIndicator('idle');
}

/**
 * Append new players to existing leaderboard (for infinite scroll)
 */
function appendPlayersToLeaderboard(newPlayers) {
  const content = document.getElementById('leaderboardContent');
  if (!content) return;
  
  // Remove loading indicator
  const loadingMore = document.getElementById('loadingMore');
  if (loadingMore) {
    loadingMore.remove();
  }
  
  // Append new player cards
  const startIndex = allPlayers.length - newPlayers.length;
  const newCardsHtml = newPlayers.map((player, index) => {
    if (currentGamemode === 'overall') {
      return renderOverallPlayerCard(player, startIndex + index);
    } else {
      return renderGamemodePlayerCard(player, startIndex + index);
    }
  }).join('');
  
  content.insertAdjacentHTML('beforeend', newCardsHtml);
  
  // Add idle loading indicator again if there are more players
  renderLoadMoreIndicator('idle');
}

/**
 * Check if a gamemode rating is provisional (less than 10 matches)
 */
function isProvisionalRating(player, gamemode) {
  const matchCount = player.gamemodeMatchCount?.[gamemode] || 0;
  return matchCount < 10;
}

/**
 * Render overall player card
 */
function renderOverallPlayerCard(player, index) {
  // Calculate overall rating dynamically if not already calculated
  const overallRating = player.overallRating !== undefined ? player.overallRating : calculateOverallRating(player);
  const showProfileTitlePill = hasAllGamemodeRatings(player);
  const combatTitle = player.achievementTitles?.overall || getCombatTitle(overallRating);
  const medalClass = index === 0 ? 'gold' : index === 1 ? 'silver' : index === 2 ? 'bronze' : '';
  const medalText = index === 0 ? '1' : index === 1 ? '2' : index === 2 ? '3' : '';
  const region = player.region || 'N/A';

  const gamemodeRatingsHtml = CONFIG.GAMEMODES
    .filter(gm => gm.id !== 'overall')
    .map(gm => {
      const rating = Number(player.gamemodeRatings?.[gm.id]);
      const hasRating = Number.isFinite(rating) && rating > 0;
      const provisional = hasRating && isProvisionalRating(player, gm.id);
      return `
        <div class="lb-gm-chip ${hasRating ? '' : 'unrated'}">
          <img src="${gm.icon}" alt="${gm.name}" class="lb-gm-icon">
          <span class="lb-gm-name">${gm.name}</span>
          <span class="lb-gm-value">${hasRating ? rating : 'Unrated'}${provisional ? '<span class="provisional-marker" title="Provisional - fewer than 10 matches">?</span>' : ''}</span>
        </div>
      `;
    })
    .join('');

  const playerJson = JSON.stringify(player).replace(/'/g, "&#39;").replace(/"/g, '&quot;');
  const playerImageUrl = `https://render.crafty.gg/3d/bust/${escapeHtml(player.username || 'Steve')}`;

  const rankClass = player.rank <= 3 ? `rank-${player.rank}` : '';

  return `
    <div class="leaderboard-card leaderboard-item ${rankClass}" onclick='openPlayerModal(${playerJson})' style="cursor: pointer;">
      <div class="lb-rank-col ${rankClass}">
        ${medalText
          ? `<div class="lb-rank-medal ${medalClass}">${medalText}</div>`
          : `<span class="lb-rank-num">#${player.rank}</span>`
        }
      </div>
      <div class="lb-avatar-col">
        <img src="${playerImageUrl}"
             alt="${escapeHtml(player.username || 'Unknown')}"
             onerror="this.src='https://render.crafty.gg/3d/bust/Steve'"
             class="lb-avatar-img"
             loading="lazy">
      </div>
      <div class="lb-info-col">
        <div class="lb-name-row">
          <span class="lb-username">${escapeHtml(player.username || 'Unknown')}</span>
          <div class="leaderboard-role-badges">${getRoleBadges(player)}</div>
        </div>
        ${showProfileTitlePill ? `
        <div class="lb-title-row">
          <img src="${combatTitle.icon}" alt="${combatTitle.title}" class="badge-icon">
          <span class="badge-text-white">${combatTitle.title}</span>
        </div>
        ` : ''}
        <div class="lb-gm-grid">
          ${gamemodeRatingsHtml}
        </div>
      </div>
      <div class="lb-elo-col">
        <span class="lb-elo-value">${overallRating}</span>
        <span class="lb-elo-label">ELO</span>
      </div>
    </div>
  `;
}

/**
 * Render gamemode leaderboard (Elo-based)
 */
function renderGamemodeLeaderboard() {
  const content = document.getElementById('leaderboardContent');
  if (!content) return;

  // Filter players who have ratings in this gamemode.
  // Preserve backend ordering/ranking from /api/players responses.
  const gamemodePlayers = filteredPlayers
    .filter(player => player.gamemodeRatings?.[currentGamemode])
    ;

  if (gamemodePlayers.length === 0) {
    content.innerHTML = `
      <div class="empty-state">
        <div class="empty-state-icon">
          <i class="fas fa-users"></i>
        </div>
        <h3>No players found</h3>
        <p class="text-muted">No players have ratings in this gamemode yet.</p>
      </div>
    `;
    return;
  }

  content.innerHTML = gamemodePlayers.map((player, index) => renderGamemodePlayerCard(player, index)).join('');
  renderLoadMoreIndicator('idle');
}

/**
 * Render gamemode player card
 */
function renderGamemodePlayerCard(player, index) {
  const rank = Number.isFinite(player.rank) ? player.rank : (index + 1);
  const overallRating = player.overallRating !== undefined ? player.overallRating : calculateOverallRating(player);
  const combatTitle = player.achievementTitles?.overall || getCombatTitle(overallRating);
  const medalClass = rank === 1 ? 'gold' : rank === 2 ? 'silver' : rank === 3 ? 'bronze' : '';
  const medalText = rank === 1 ? '1' : rank === 2 ? '2' : rank === 3 ? '3' : '';
  const gamemodeRating = player.gamemodeRatings?.[currentGamemode] || 0;
  const region = player.region || 'N/A';

  const playerJson = JSON.stringify(player).replace(/'/g, "&#39;").replace(/"/g, '&quot;');
  const playerImageUrl = `https://render.crafty.gg/3d/bust/${escapeHtml(player.username || 'Steve')}`;

  const rankClass = rank <= 3 ? `rank-${rank}` : '';

  return `
    <div class="leaderboard-card leaderboard-item ${rankClass}" onclick='openPlayerModal(${playerJson})' style="cursor: pointer;">
      <div class="lb-rank-col ${rankClass}">
        ${medalText
          ? `<div class="lb-rank-medal ${medalClass}">${medalText}</div>`
          : `<span class="lb-rank-num">#${rank}</span>`
        }
      </div>
      <div class="lb-avatar-col">
        <img src="${playerImageUrl}"
             alt="${escapeHtml(player.username || 'Unknown')}"
             onerror="this.src='https://render.crafty.gg/3d/bust/Steve'"
             class="lb-avatar-img"
             loading="lazy">
      </div>
      <div class="lb-info-col">
        <div class="lb-name-row">
          <span class="lb-username">${escapeHtml(player.username || 'Unknown')}</span>
          <div class="leaderboard-role-badges">${getRoleBadges(player)}</div>
        </div>
        <div class="lb-title-row">
          <img src="${combatTitle.icon}" alt="${combatTitle.title}" class="badge-icon">
          <span class="badge-text-white">${combatTitle.title}</span>
        </div>
      </div>
      <div class="lb-elo-col">
        <span class="lb-elo-value">${gamemodeRating}${isProvisionalRating(player, currentGamemode) ? '<span class="provisional-marker" title="Provisional — fewer than 10 matches">?</span>' : ''}</span>
        <span class="lb-elo-label">ELO</span>
      </div>
    </div>
  `;
}

/**
 * Open player modal
 */
async function openPlayerModal(player) {
  const modal = document.getElementById('playerModal');
  const modalTitle = document.getElementById('playerModalTitle');
  const modalBody = document.getElementById('playerModalBody');

  if (!modal || !modalTitle || !modalBody) return;

  // Show loading state immediately
  modalTitle.textContent = 'Loading...';
  modalBody.innerHTML = '<div style="text-align: center; padding: 3rem;"><div class="spinner"></div><p style="margin-top: 1rem; color: var(--text-muted);">Loading player data...</p></div>';
  modal.style.display = 'flex';

  // Handle if player is passed as string (from onclick)
  if (typeof player === 'string') {
    try {
      player = JSON.parse(player);
    } catch (e) {
      console.error('Error parsing player data:', e);
      modalBody.innerHTML = '<div class="alert alert-danger"><i class="fas fa-exclamation-triangle"></i> Error loading player data</div>';
      return;
    }
  }

  modalTitle.textContent = `${player.username || 'Unknown'} - Player Profile`;

  // Get player image URL
  const playerImageUrl = `https://render.crafty.gg/3d/bust/${escapeHtml(player.username || 'Steve')}`;

  // Get all stats - calculate overall rating dynamically
  const overallRating = player.overallRating !== undefined ? player.overallRating : calculateOverallRating(player);
  const showProfileTitlePill = hasAllGamemodeRatings(player);
  const combatTitle = player.achievementTitles?.overall || getCombatTitle(overallRating);
  const region = player.region || 'N/A';

  // Build username gradient style if Plus is active and gradient is configured
  const gradient = player.plus?.active === true ? (player.plus?.gradient || null) : null;
  const safeHex = (c) => (typeof c === 'string' && /^#([0-9a-fA-F]{6}|[0-9a-fA-F]{3})$/.test(c)) ? c : null;
  const safeAngle = (a) => (typeof a === 'number' && isFinite(a)) ? Math.max(0, Math.min(360, a)) : 90;
  const safeStops = (stops) => {
    if (!Array.isArray(stops)) return [];
    const sorted = stops.slice().sort((a, b) => {
      const ap = typeof a?.pos === 'number' ? a.pos : 0;
      const bp = typeof b?.pos === 'number' ? b.pos : 0;
      return ap - bp;
    });
    return sorted
      .map(s => ({ color: safeHex(s?.color), pos: typeof s?.pos === 'number' ? Math.max(0, Math.min(100, s.pos)) : null }))
      .filter(s => !!s.color)
      .map((s, i) => `${s.color}${s.pos !== null ? ` ${s.pos}%` : ''}`);
  };
  const gradientStops = gradient ? safeStops(gradient.stops) : [];
  const gradientAngle = gradient ? safeAngle(gradient.angle) : 90;
  const gradientAnim = gradient && typeof gradient.animation === 'string' ? gradient.animation : 'none';
  const usernameGradientStyle = (gradientStops.length >= 2)
    ? `style="
        background: linear-gradient(${gradientAngle}deg, ${gradientStops.join(', ')});
        -webkit-background-clip: text;
        background-clip: text;
        color: transparent;
        text-shadow: none;
        ${gradientAnim === 'shift' ? 'background-size: 200% 200%; animation: mclbGradientShift 4s ease infinite;' : ''}
        ${gradientAnim === 'pulse' ? 'animation: mclbGradientPulse 2.2s ease-in-out infinite;' : ''}
      "`
    : '';

  // Build ratings display
  // Get user profile to check retirement status via API
  let retiredGamemodes = player.retiredGamemodes || {};
  if (Object.keys(retiredGamemodes).length === 0 && player.userId) {
    try {
      const retirementData = await apiService.getUserRetirementStatus(player.userId);
      if (retirementData?.retiredGamemodes) {
        retiredGamemodes = retirementData.retiredGamemodes;
      }
    } catch (error) {
      console.error('Error checking retirement status:', error);
      // Silently fail - retirement status is optional for display
    }
  }

  const ratingsHtml = CONFIG.GAMEMODES
    .filter(gm => gm.id !== 'overall')
    .map(gm => {
      const rating = player.gamemodeRatings?.[gm.id];
      const peakRating = player.peakRatings?.[gm.id];
      const isRetired = retiredGamemodes[gm.id] || false;
      if (rating) {
        const isProvisional = isProvisionalRating(player, gm.id);
        const badgeTitle = player.achievementTitles?.[gm.id];
        const showPeak = peakRating && peakRating > rating;
        return `
            <div class="player-rating-item ${isRetired ? 'retired-gamemode' : ''}" style="${isRetired ? 'opacity: 0.5; filter: grayscale(100%);' : ''}">
            <img src="${gm.icon}" alt="${gm.name}" style="width: 20px; height: 20px; border-radius: 4px;">
            <span><strong>${gm.name}:</strong> ${rating} Elo ${showPeak ? `<span style="color: var(--text-muted); font-size: 0.85em;">(Peak: ${peakRating})</span>` : ''} ${isRetired ? '<span class="badge badge-warning" style="font-size: 0.7em; margin-left: 4px;">Retired</span>' : ''}</span>
            ${badgeTitle ? `<img src="${badgeTitle.icon}" alt="${badgeTitle.title}" title="${badgeTitle.title}" style="width: 20px; height: 20px; margin-left: 8px; vertical-align: middle; border-radius: 4px;">` : ''}
            ${isProvisional ? `<span class="provisional-marker" onclick="showProvisionalTooltip('${gm.name}')" style="cursor: pointer;">?</span>` : ''}
          </div>
        `;
      }
      return null;
    })
    .filter(Boolean)
    .join('');

  modalBody.innerHTML = `
    <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem;">
      <img src="${playerImageUrl}"
           alt="${escapeHtml(player.username || 'Unknown')}"
           style="width: 64px; height: 64px; border-radius: 10px; border: 1px solid var(--border-color);"
           onerror="this.src='https://render.crafty.gg/3d/bust/Steve'">
      <div>
        <div style="font-size: 1.2rem; font-weight: 700;" ${usernameGradientStyle}>${escapeHtml(player.username || 'Unknown')}</div>
        <div style="display: ${showProfileTitlePill ? 'flex' : 'none'}; align-items: center; gap: 0.4rem; color: var(--text-muted); margin-top: 0.2rem;">
          <img src="${combatTitle.icon}" alt="${combatTitle.title}" style="width: 16px; height: 16px;">
          <span>${combatTitle.title}</span>
        </div>
        <div style="margin-top: 0.35rem;">
          ${player.blacklisted ? '<span class="badge badge-danger">Blacklisted</span>' : ''}
          ${getRoleBadges(player)}
        </div>
      </div>
    </div>

    <div style="display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 0.65rem; margin-bottom: 1rem;">
      <div class="status-card" style="padding: 0.7rem;">
        <div class="text-muted">Overall</div>
        <div style="font-weight: 700;">${overallRating} Elo</div>
      </div>
      <div class="status-card" style="padding: 0.7rem;">
        <div class="text-muted">Rank</div>
        <div style="font-weight: 700;">#${player.rank || 'N/A'}</div>
      </div>
      <div class="status-card" style="padding: 0.7rem;">
        <div class="text-muted">Region</div>
        <div style="font-weight: 700;">${region}</div>
      </div>
    </div>
    <h3 style="margin: 0 0 0.6rem 0; font-size: 1rem;">Gamemode Ratings</h3>
    <div style="display: grid; gap: 0.45rem;">
      ${ratingsHtml || '<div class="text-muted">No ratings assigned yet.</div>'}
    </div>

    <button type="button"
            class="btn btn-danger"
            style="width: 100%; font-weight: 700; padding: 0.85rem 1rem; border-radius: 12px; margin-top: 1rem;"
            data-player="${escapeHtml(player.username || '')}"
            onclick="openReportPageForPlayer(this.dataset.player)">
      <i class="fas fa-flag"></i> Report This Player
    </button>
  `;

  // Add ESC key listener to close modal and focus search input
  const handleEscKey = (event) => {
    if (event.key === 'Escape') {
      closePlayerModal();
      // Focus search input
      const searchInput = document.getElementById('searchInput');
      if (searchInput) {
        searchInput.focus();
      }
      // Remove the event listener
      document.removeEventListener('keydown', handleEscKey);
    }
  };

  document.addEventListener('keydown', handleEscKey);
}

/**
 * Close player modal
 */
function closePlayerModal() {
  const modal = document.getElementById('playerModal');
  if (modal) {
    modal.style.display = 'none';
  }

  // Focus search input when modal closes
  const searchInput = document.getElementById('searchInput');
  if (searchInput) {
    setTimeout(() => searchInput.focus(), 100); // Small delay to ensure modal is hidden
  }
}

/**
 * Get rating icon (using gamemode icons)
 */
function getTierIcon(tier) {
  // This function is deprecated - we now use gamemode icons directly
  return '';
}

/**
 * Generate role badges HTML for a player
 */
function getRoleBadges(player) {
  const badges = [];
  const isAdmin = player.verifiedRoles?.admin === true || player.roles?.admin === true || player.admin === true;
  const isTester = player.verifiedRoles?.tester === true || player.roles?.tester === true || player.tester === true;

  const buildRoleBadge = (label, variant) => `<span class="role-pill-badge role-pill-${variant}" title="${label}" data-role="${label}">${label}</span>`;

  // Check verified roles first (for other players)
  if (isAdmin) {
    badges.push(buildRoleBadge('Admin', 'admin'));
  }

  if (isTester) {
    badges.push(buildRoleBadge('Tier Tester', 'tester'));
  }

  // Plus badge (can stack with other badges)
  if (player.plus?.active === true && player.plus?.showBadge !== false) {
    badges.push(buildRoleBadge('Plus', 'plus'));
  }

  // If no verified roles but this is the current user, check their profile
  if (badges.length === 0 && player.userId === AppState.getUserId()) {
    const profile = AppState.getProfile();
    if (profile?.admin) {
      badges.push(buildRoleBadge('Admin', 'admin'));
    }
    if (profile?.tester) {
      badges.push(buildRoleBadge('Tier Tester', 'tester'));
    }
    if (profile?.plus?.active === true && profile?.plus?.showBadge !== false) {
      badges.push(buildRoleBadge('Plus', 'plus'));
    }
  }

  return badges.join('');
}

/**
 * Get combat title based on rating thresholds
 */
function getCombatTitle(totalPoints) {
  if (typeof utils !== 'undefined' && utils.getCombatTitle) {
    return utils.getCombatTitle(totalPoints);
  }
  for (const title of CONFIG.COMBAT_TITLES) {
    if (Number(totalPoints) >= Number(title.minRating || 0)) {
      return title;
    }
  }
  return CONFIG.COMBAT_TITLES[CONFIG.COMBAT_TITLES.length - 1];
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
  if (!text) return '';
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

/**
 * Show error message
 */
function showError(message) {
  const content = document.getElementById('leaderboardContent');
  if (content) {
    content.innerHTML = `
      <div class="alert alert-error">
        <i class="fas fa-exclamation-circle"></i> ${escapeHtml(message)}
      </div>
    `;
  }
}

// Close modal on outside click
document.addEventListener('click', (e) => {
  const modal = document.getElementById('playerModal');
  if (modal && e.target === modal) {
    closePlayerModal();
  }
});

window.addEventListener('beforeunload', () => {
  clearBatchCooldownTimer();
});

// Initialize on page load
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initLeaderboard);
} else {
  initLeaderboard();
}
