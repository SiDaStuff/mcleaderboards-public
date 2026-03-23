// MC Leaderboards - Global Player Search
// Adds a player search bar to the header on all pages,
// and provides the leaderboard player popup globally.

(function () {
  let searchCooldownUntilMs = 0;
  let searchCooldownReason = '';
  let keyListenerAttached = false;

  function getSearchInput() {
    return document.getElementById('searchInput');
  }

  function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  function calculateOverallRating(player) {
    if (!player?.gamemodeRatings || !Array.isArray(CONFIG?.GAMEMODES)) return 0;
    const ratings = [];
    CONFIG.GAMEMODES.forEach(gm => {
      if (gm.id !== 'overall' && player.gamemodeRatings[gm.id]) {
        ratings.push(player.gamemodeRatings[gm.id]);
      }
    });
    if (ratings.length === 0) return 0;
    const sum = ratings.reduce((acc, rating) => acc + rating, 0);
    return Math.round(sum / ratings.length);
  }

  function getCombatTitle(totalPoints) {
    if (typeof utils !== 'undefined' && utils.getCombatTitle) {
      return utils.getCombatTitle(totalPoints);
    }
    for (const title of CONFIG.COMBAT_TITLES || []) {
      if (Number(totalPoints) >= Number(title.minRating || 0)) {
        return title;
      }
    }
    return (CONFIG.COMBAT_TITLES && CONFIG.COMBAT_TITLES[CONFIG.COMBAT_TITLES.length - 1]) || { title: 'Rookie', icon: 'assets/badgeicons/rookie.svg' };
  }

  function isProvisionalRating(player, gamemode) {
    const matchCount = player.gamemodeMatchCount?.[gamemode] || 0;
    return matchCount < 10;
  }

  function getRoleBadges(player) {
    const badges = [];
    const buildRoleBadge = (label, variant) => `<span class="role-pill-badge role-pill-${variant}" title="${label}" data-role="${label}">${label}</span>`;

    const isAdmin = player.verifiedRoles?.admin === true || player.roles?.admin === true || player.admin === true;
    const isTester = player.verifiedRoles?.tester === true || player.roles?.tester === true || player.tester === true;

    if (isAdmin) {
      badges.push(buildRoleBadge('Admin', 'admin'));
    }
    if (isTester) {
      badges.push(buildRoleBadge('Tier Tester', 'tester'));
    }
    if (player.plus?.active === true && player.plus?.showBadge !== false) {
      badges.push(buildRoleBadge('Plus', 'plus'));
    }

    if (badges.length === 0 && player.userId && typeof AppState !== 'undefined' && player.userId === AppState.getUserId?.()) {
      const profile = AppState.getProfile?.();
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

  function openReportPageForPlayer(playerName) {
    const safePlayer = encodeURIComponent((playerName || '').trim());
    const targetUrl = safePlayer ? `report.html?player=${safePlayer}` : 'report.html';
    window.location.href = targetUrl;
  }

  function ensureGlobalModalExists() {
    if (document.getElementById('playerModal')) return;

    const modal = document.createElement('div');
    modal.id = 'playerModal';
    modal.className = 'modal';
    modal.style.display = 'none';
    modal.innerHTML = `
      <div class="modal-content" style="max-width: 800px;">
        <div class="modal-header">
          <h2 class="modal-title" id="playerModalTitle">Player Details</h2>
          <button class="modal-close" onclick="closePlayerModal()">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="modal-body" id="playerModalBody">
          <div class="spinner"></div>
        </div>
      </div>
    `;
    document.body.appendChild(modal);

    modal.addEventListener('click', (e) => {
      if (e.target === modal && typeof window.closePlayerModal === 'function') {
        window.closePlayerModal();
      }
    });
  }

  if (typeof window.closePlayerModal !== 'function') {
    window.closePlayerModal = function closePlayerModal() {
      const modal = document.getElementById('playerModal');
      if (modal) modal.style.display = 'none';
      const searchInput = getSearchInput();
      if (searchInput) setTimeout(() => searchInput.focus(), 100);
    };
  }

  if (typeof window.openPlayerModal !== 'function') {
    window.openPlayerModal = async function openPlayerModal(player) {
      ensureGlobalModalExists();

      const modal = document.getElementById('playerModal');
      const modalTitle = document.getElementById('playerModalTitle');
      const modalBody = document.getElementById('playerModalBody');

      if (!modal || !modalTitle || !modalBody) return;

      modalTitle.textContent = 'Loading...';
      modalBody.innerHTML = '<div style="text-align: center; padding: 3rem;"><div class="spinner"></div><p style="margin-top: 1rem; color: var(--text-muted);">Loading player data...</p></div>';
      modal.style.display = 'flex';

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

      const playerImageUrl = `https://render.crafty.gg/3d/bust/${escapeHtml(player.username || 'Steve')}`;
      const overallRating = player.overallRating !== undefined ? player.overallRating : calculateOverallRating(player);
      const combatTitle = player.achievementTitles?.overall || getCombatTitle(overallRating);
      const region = player.region || 'N/A';

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
          .map(s => `${s.color}${s.pos !== null ? ` ${s.pos}%` : ''}`);
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

      let retiredGamemodes = player.retiredGamemodes || {};
      if (Object.keys(retiredGamemodes).length === 0 && player.userId) {
        try {
          const retirementData = await apiService.getUserRetirementStatus(player.userId);
          if (retirementData?.retiredGamemodes) {
            retiredGamemodes = retirementData.retiredGamemodes;
          }
        } catch (error) {
          console.error('Error checking retirement status:', error);
        }
      }

      const ratingsHtml = (CONFIG.GAMEMODES || [])
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
                ${isProvisional ? `<span class="provisional-marker" title="Provisional Rating - Less than 10 matches played" style="cursor: help;">?</span>` : ''}
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
            <div style="display: flex; align-items: center; gap: 0.4rem; color: var(--text-muted); margin-top: 0.2rem;">
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
                onclick="(function(btn){ window.location.href = 'report.html?player=' + encodeURIComponent(btn.dataset.player || ''); })(this)">
          <i class="fas fa-flag"></i> Report This Player
        </button>
      `;

      const handleEscKey = (event) => {
        if (event.key === 'Escape') {
          window.closePlayerModal();
          const searchInput = getSearchInput();
          if (searchInput) {
            searchInput.focus();
          }
          document.removeEventListener('keydown', handleEscKey);
        }
      };

      document.addEventListener('keydown', handleEscKey);
    };
  }

  function formatDuration(ms) {
    if (!ms || ms <= 0) return 'a moment';
    const totalSeconds = Math.ceil(ms / 1000);
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    if (minutes > 0) {
      return seconds > 0
        ? `${minutes}m ${seconds}s`
        : `${minutes}m`;
    }
    return `${seconds}s`;
  }

  function showCooldownAlert() {
    const now = Date.now();
    const remainingMs = Math.max(0, searchCooldownUntilMs - now);
    const remainingText = formatDuration(remainingMs);

    const baseMessage = searchCooldownReason || 'You are searching too quickly.';
    const text = `${baseMessage} You can search again in ${remainingText}.`;

    if (typeof Swal === 'function') {
      Swal.fire({
        icon: 'warning',
        title: 'Search Cooldown',
        text,
        timer: 3000,
        showConfirmButton: false
      });
    } else {
      console.warn(text);
    }
  }

  function startSearchCooldown(error) {
    const now = Date.now();
    const retryAt = (error && error.retryAt) || (now + 60000);
    searchCooldownUntilMs = retryAt;
    searchCooldownReason =
      (error && (error.message || error.data?.message)) ||
      'Too many search attempts.';
    showCooldownAlert();
  }

  function mapUsernameResponseToPlayer(response) {
    if (!response) return null;

    const player = {
      username: response.name || response.username || '',
      region: response.region || 'N/A',
      overallRating: response.overallRating || response.overall || 0,
      rank: response.globalRank || null,
      blacklisted: response.blacklisted === true,
      gamemodeRatings: {},
      peakRatings: {},
      gamemodeMatchCount: {},
      achievementTitles: {},
      plus: null,
      userId: response.userId || null,
      retiredGamemodes: response.retiredGamemodes || {}
    };

    const rankings = response.rankings || {};
    Object.keys(rankings).forEach((gamemodeId) => {
      const r = rankings[gamemodeId] || {};
      player.gamemodeRatings[gamemodeId] = typeof r.rating === 'number' ? r.rating : 0;
      player.peakRatings[gamemodeId] = typeof r.peak_rating === 'number' ? r.peak_rating : player.gamemodeRatings[gamemodeId];
      player.gamemodeMatchCount[gamemodeId] = typeof r.games_played === 'number' ? r.games_played : 0;
    });

    return player;
  }

  async function performPlayerSearch() {
    const input = getSearchInput();
    if (!input || typeof apiService === 'undefined') return;

    const rawQuery = input.value || '';
    const query = rawQuery.trim();
    if (!query) return;

    const now = Date.now();
    if (searchCooldownUntilMs && now < searchCooldownUntilMs) {
      showCooldownAlert();
      return;
    }

    const previousDisabled = input.disabled;
    input.disabled = true;

    try {
      let response;
      try {
        if (typeof apiService.getPlayerByUsername === 'function') {
          response = await apiService.getPlayerByUsername(query);
        } else if (typeof apiService.get === 'function') {
          // Compatibility fallback for older apiService instances.
          response = await apiService.get(`/players/username/${encodeURIComponent(query)}`);
        } else {
          throw new Error('Player search API is unavailable');
        }
      } catch (error) {
        if (error && (error.isRateLimit || error.status === 429)) {
          startSearchCooldown(error);
          return;
        }

        if (error && error.status === 404) {
          if (typeof Swal === 'function') {
            Swal.fire({
              icon: 'info',
              title: 'Player Not Found',
              text: `No player found with username "${query}"`,
              timer: 2000,
              showConfirmButton: false
            });
          }
          return;
        }

        throw error;
      }

      const player = mapUsernameResponseToPlayer(response);
      if (!player) {
        if (typeof Swal === 'function') {
          Swal.fire({
            icon: 'info',
            title: 'Player Not Found',
            text: `No player found with username "${query}"`,
            timer: 2000,
            showConfirmButton: false
          });
        }
        return;
      }

      window.openPlayerModal(player);
    } catch (error) {
      console.error('Global player search error:', error);
      if (typeof Swal === 'function') {
        Swal.fire({
          icon: 'error',
          title: 'Search Error',
          text: error.message || 'Failed to search for player. Please try again.',
          timer: 2500,
          showConfirmButton: false
        });
      }
    } finally {
      input.value = '';
      input.disabled = previousDisabled;
    }
  }

  function ensureSearchHint() {
    const wrapper = document.querySelector('.navbar-search-wrapper');
    if (!wrapper) return;
    if (wrapper.querySelector('.search-shortcut-hint')) return;

    const hint = document.createElement('div');
    hint.className = 'search-shortcut-hint';
    hint.innerHTML = 'Press <kbd>/</kbd> to focus search, then <kbd>Enter</kbd> to open player popup.';
    wrapper.appendChild(hint);
  }

  function initGlobalPlayerSearch() {
    const input = getSearchInput();
    if (!input) return;

    ensureGlobalModalExists();
    ensureSearchHint();

    input.addEventListener('keypress', (event) => {
      if (event.key === 'Enter') {
        event.preventDefault();
        performPlayerSearch();
      }
    });

    if (!keyListenerAttached) {
      document.addEventListener('keydown', (event) => {
        if (
          event.key === '/' &&
          !event.ctrlKey &&
          !event.metaKey &&
          !event.altKey
        ) {
          const activeElement = document.activeElement;
          if (!activeElement) return;
          const tagName = activeElement.tagName;
          if (tagName === 'INPUT' || tagName === 'TEXTAREA') return;

          const searchInput = getSearchInput();
          if (searchInput) {
            event.preventDefault();
            searchInput.focus();
          }
        }
      });
      keyListenerAttached = true;
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initGlobalPlayerSearch);
  } else {
    initGlobalPlayerSearch();
  }
})();
