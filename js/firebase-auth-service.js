// MC Leaderboards - Firebase Authentication Service

const firebaseAuthService = {
  banStatusListener: null,
  _googleProvider: null,

  /**
   * Initialize auth state listener
   */
  async init() {
    // Wait for Firebase to be ready using the firebase-service
    if (typeof waitForFirebaseInit !== 'undefined') {
      const initialized = await waitForFirebaseInit();
      if (!initialized) {
        console.warn('Firebase auth init skipped because Firebase SDK is unavailable.');
        return;
      }
    } else {
      // Fallback: Wait for Firebase to be ready
    if (typeof firebase === 'undefined' || !firebase.apps || firebase.apps.length === 0) {
      console.log('Firebase not initialized, retrying in 100ms...');
      setTimeout(() => this.init(), 100);
      return;
      }
    }

    const auth = typeof getAuth === 'function' ? getAuth() : firebase.auth();

    // Listen for ID token changes (including refreshes)
    // This fires when tokens are available/updated
    auth.onIdTokenChanged(async (user) => {
      if (user) {
        try {
          const token = await user.getIdToken(true);
          apiService.setToken(token);

          // Only fetch profile after token is set
          if (!AppState.currentUser || AppState.currentUser.uid !== user.uid) {
            AppState.setUser(user);
            await this.fetchUserProfile(user.uid);
          }
        } catch (error) {
          console.error('Error getting ID token:', error);
          apiService.setToken(null);
        }
      } else {
        AppState.setUser(null);
        AppState.setProfile(null);
        apiService.setToken(null);
      }
    });

    // Listen for auth state changes (for logout handling)
    auth.onAuthStateChanged(async (user) => {
      if (!user) {
        AppState.setUser(null);
        AppState.setProfile(null);
        apiService.setToken(null);
      }
      // Profile fetching moved to onIdTokenChanged to ensure token is available
    });
  },

  /**
   * Fetch user profile from database
   */
  async fetchUserProfile(userId) {
    try {
      AppState.setLoading('profile', true);
      // Ensure API layer has a fresh token before profile requests.
      await this.ensureFreshIdToken(true);
      const profile = await apiService.getProfile();
      
      // Check if this is the admin UID and ensure admin/tierTester flags are set
      if (userId === 'Uy0ykS3uX8XzZ0CxVSs6O8n5By52') {
        if (!profile.admin || !profile.tierTester) {
          // Update profile to set admin and tierTester
          await apiService.updateProfile({
            admin: true,
            tester: true
          });
          profile.admin = true;
          profile.tierTester = true;
        }
      }
      
      AppState.setProfile(profile);

      // Set up realtime ban detection listener
      this.setupBanStatusListener(userId);

      return profile;
    } catch (error) {
      console.error('Error fetching profile:', error);
      // If profile doesn't exist, create it
      if (error.message.includes('not found') || error.message.includes('404')) {
        await this.createUserProfile(userId);
      }
    } finally {
      AppState.setLoading('profile', false);
    }
  },

  /**
   * Create user profile
   */
  async createUserProfile(userId) {
    try {
      const user = AppState.currentUser;
      
      // Admin status is determined server-side based on ADMIN_BYPASS_EMAIL env var
      const profile = {
        email: user.email,
        createdAt: new Date().toISOString(),
        admin: false,
        tierTester: false,
        minecraftUsername: null,
        minecraftVerified: false
      };
      
      await apiService.updateProfile(profile);
      AppState.setProfile(profile);
    } catch (error) {
      console.error('Error creating profile:', error);
    }
  },

  /**
   * Sign up with email and password
   */
  async signUp(email, password) {
    try {
      if (typeof firebase === 'undefined' || !firebase.auth) {
        throw new Error('Firebase not initialized');
      }
      const auth = typeof getAuth === 'function' ? getAuth() : firebase.auth();
      const userCredential = await auth.createUserWithEmailAndPassword(email, password);

      if (!userCredential || !userCredential.user) {
        throw new Error('Firebase signup failed: invalid response');
      }

      if (!userCredential.user.uid) {
        throw new Error('Firebase signup failed: no UID returned');
      }

      return userCredential.user;
    } catch (error) {
      console.error('Sign up error:', error);
      throw this.handleAuthError(error);
    }
  },

  /**
   * Sign in with email and password
   */
  async signIn(email, password) {
    try {
      // First check if email is banned before attempting Firebase auth
      const banCheck = await apiService.checkBanStatus(email);

      if (banCheck.banned) {
        // Show custom ban popup instead of proceeding with login
        this.showBanPopup({
          banReason: banCheck.reason,
          bannedAt: new Date().toLocaleDateString(), // Approximation
          banDuration: banCheck.timeRemainingText || (banCheck.isPermanent ? 'Permanent' : 'Unknown'),
          timeRemaining: banCheck.timeRemaining,
          isPermanent: banCheck.isPermanent
        });
        throw new Error('Account is banned');
      }

      const auth = typeof getAuth === 'function' ? getAuth() : firebase.auth();
      const userCredential = await auth.signInWithEmailAndPassword(email, password);
      return userCredential.user;
    } catch (error) {
      console.error('Sign in error:', error);
      throw this.handleAuthError(error);
    }
  },

  /**
   * Get (singleton) Google Auth provider
   */
  getGoogleProvider() {
    if (this._googleProvider) return this._googleProvider;
    if (typeof firebase === 'undefined' || !firebase.auth) {
      throw new Error('Firebase not initialized');
    }
    this._googleProvider = new firebase.auth.GoogleAuthProvider();
    // Keep it minimal to avoid extra consent prompts.
    this._googleProvider.setCustomParameters({ prompt: 'select_account' });
    return this._googleProvider;
  },

  /**
   * Ensure apiService has a current Firebase ID token (forces refresh optionally)
   */
  async ensureFreshIdToken(forceRefresh = false) {
    const auth = typeof getAuth === 'function' ? getAuth() : firebase.auth();
    const user = auth.currentUser;
    if (!user) {
      throw new Error('Not signed in');
    }
    const token = await user.getIdToken(forceRefresh === true);
    apiService.setToken(token);
    return token;
  },

  /**
   * Sign in with Google (for existing accounts)
   * - Blocks if the email already belongs to a password account that is not linked to Google.
   * - Ensures backend login tracking runs (alt detection, IP tracking, ban checks).
   */
  async signInWithGoogle({ clientIP = 'unknown' } = {}) {
    try {
      if (typeof firebase === 'undefined' || !firebase.auth) {
        throw new Error('Firebase not initialized');
      }

      const auth = typeof getAuth === 'function' ? getAuth() : firebase.auth();
      const provider = this.getGoogleProvider();

      const userCredential = await auth.signInWithPopup(provider);
      const user = userCredential?.user;
      if (!user) throw new Error('Google sign-in failed');

      // Set token immediately (don’t rely solely on onIdTokenChanged timing)
      await this.ensureFreshIdToken(false);

      // Track login with backend (also performs ban checks for existing profiles)
      const loginRequest = async (idToken) => fetch('/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${idToken}`
        },
        body: JSON.stringify({ clientIP })
      });
      let resp = await loginRequest(apiService.getToken());
      if (resp.status === 401) {
        // Token can be stale immediately after popup auth; refresh once and retry.
        const refreshedToken = await this.ensureFreshIdToken(true);
        resp = await loginRequest(refreshedToken);
      }

      if (!resp.ok) {
        let data = {};
        try { data = await resp.json(); } catch (_) {}

        // If profile doesn't exist yet, treat as "not registered" and redirect to signup flow
        if (resp.status === 404 && (data.code === 'USER_NOT_FOUND' || data.code === 'NOT_FOUND')) {
          // Avoid leaving the user signed-in without completing age verification/registration.
          await auth.signOut();
          const err = new Error('No account found for this Google email.');
          err.code = 'mclb/google-user-not-registered';
          err.userMessage = 'No account found for this Google email.';
          err.suggestion = 'Please use “Sign up with Google” to create your account (age verification is required).';
          err.action = 'go_to_google_signup';
          throw err;
        }

        // Bubble backend ban details if present
        const backendError = new Error(data.message || 'Login failed');
        backendError.code = resp.status === 429 ? 'RATE_LIMITED' : (data.code || 'SERVER_ERROR');
        throw backendError;
      }

      return user;
    } catch (error) {
      console.error('Google sign-in error:', error);
      throw this.handleAuthError(error);
    }
  },

  /**
   * Sign up with Google (new accounts) - requires age verification BEFORE registration.
   * This mirrors the email signup behavior by calling /api/auth/register with age.
   */
  async signUpWithGoogle({ age, clientIP = 'unknown' } = {}) {
    try {
      if (!age || typeof age !== 'number' || age < 13) {
        const err = new Error('You must be at least 13 years old to create an account.');
        err.code = 'AGE_VERIFICATION_FAILED';
        throw err;
      }
      if (typeof firebase === 'undefined' || !firebase.auth) {
        throw new Error('Firebase not initialized');
      }

      const auth = typeof getAuth === 'function' ? getAuth() : firebase.auth();
      const provider = this.getGoogleProvider();

      const userCredential = await auth.signInWithPopup(provider);
      const user = userCredential?.user;
      if (!user) throw new Error('Google sign-up failed');

      await this.ensureFreshIdToken(false);

      const token = apiService.getToken();
      const email = user.email || '';
      const firebaseUid = user.uid;

      const resp = await fetch('/api/auth/register', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
          email,
          firebaseUid,
          minecraftUsername: null,
          clientIP,
          age
        })
      });

      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        // If registration fails, try to remove the Firebase user to avoid orphaned accounts
        try {
          if (auth.currentUser && auth.currentUser.uid === firebaseUid) {
            await auth.currentUser.delete();
          }
        } catch (deleteErr) {
          console.warn('Failed to delete Firebase user after failed Google registration:', deleteErr);
        }

        const backendError = new Error(data.message || 'Registration failed');
        backendError.code = data.code || 'SERVER_ERROR';
        backendError.response = data;
        throw backendError;
      }

      return user;
    } catch (error) {
      console.error('Google sign-up error:', error);
      throw this.handleAuthError(error);
    }
  },

  /**
   * Link Google provider to the currently signed-in account.
   * Used from account settings.
   */
  async linkGoogleAccount() {
    try {
      const auth = typeof getAuth === 'function' ? getAuth() : firebase.auth();
      const user = auth.currentUser;
      if (!user) throw new Error('Not signed in');
      const provider = this.getGoogleProvider();
      const result = await user.linkWithPopup(provider);
      await this.ensureFreshIdToken(true);
      return result;
    } catch (error) {
      console.error('Link Google account error:', error);
      throw this.handleAuthError(error);
    }
  },

  /**
   * Set up realtime ban status listener
   */
  setupBanStatusListener(userId) {
    // Check if Firebase is initialized
    if (typeof firebase === 'undefined' || !firebase.apps || firebase.apps.length === 0) {
      console.warn('Firebase not initialized, cannot set up ban status listener');
      return;
    }

    // Clean up existing listener
    if (this.banStatusListener) {
      this.banStatusListener.off();
    }

    // Set up new listener for ban status changes
    const db = typeof getDatabase === 'function' ? getDatabase() : firebase.database();
    const userRef = db.ref(`users/${userId}`);
    this.banStatusListener = userRef;
    userRef.on('value', async (snapshot) => {
      if (!snapshot || !snapshot.exists()) {
        return; // User profile doesn't exist yet
      }

      const profile = snapshot.val();
      if (profile && profile.banned) {
        // Check if ban has expired
        if (profile.banExpires && profile.banExpires !== 'permanent') {
          const banExpires = new Date(profile.banExpires);
          const now = new Date();
          if (banExpires <= now) {
            // Ban has expired, ignore
            return;
          }
        }

        // User is banned - auto logout
        console.log('Ban detected via realtime listener, logging out user');
        await this.handleBanDetected(profile.banReason, profile.banExpires);
      }
    });
  },

  /**
   * Handle when ban is detected - logout user and show message
   */
  async handleBanDetected(reason, expires) {
    try {
      // Clean up listener
      if (this.banStatusListener) {
        this.banStatusListener.off();
        this.banStatusListener = null;
      }

      // Sign out user
      const auth = typeof getAuth === 'function' ? getAuth() : firebase.auth();
      await auth.signOut();

      // Reset app state
      AppState.reset();
      apiService.setToken(null);

      // Show detailed ban message
      const banReason = reason || 'Violation of terms of service';
      const bannedAt = new Date().toLocaleDateString(); // Current date as approximation
      let banDuration = 'Permanent';
      let timeRemaining = null;

      if (expires && expires !== 'permanent') {
        const expiryDate = new Date(expires);
        const now = new Date();
        if (expiryDate > now) {
          const daysLeft = Math.ceil((expiryDate - now) / (1000 * 60 * 60 * 24));
          banDuration = `${daysLeft} days remaining (expires ${expiryDate.toLocaleDateString()})`;
          timeRemaining = expiryDate - now;
        } else {
          banDuration = 'Expired (please contact support)';
          timeRemaining = 0;
        }
      }

      // Show custom ban popup
      this.showBanPopup({
        banReason: banReason,
        bannedAt: bannedAt,
        banDuration: banDuration,
        timeRemaining: timeRemaining,
        isPermanent: expires === 'permanent'
      });

    } catch (error) {
      console.error('Error handling ban detection:', error);
      // Force redirect even if logout fails
      window.location.href = '/login.html';
    }
  },

  /**
   * Sign out
   */
  async signOut() {
    try {
      // Clean up ban status listener
      if (this.banStatusListener) {
        this.banStatusListener.off();
        this.banStatusListener = null;
      }

      // If Firebase SDK isn't available for any reason, still allow a local "logout"
      // so pages like Support don't hard-crash.
      let auth = null;
      try {
        if (typeof getAuth === 'function') {
          auth = getAuth();
        } else if (typeof firebase !== 'undefined' && firebase && typeof firebase.auth === 'function') {
          auth = firebase.auth();
        }
      } catch (_) {
        auth = null;
      }

      if (auth && typeof auth.signOut === 'function') {
        await auth.signOut();
      }
      AppState.reset();
      apiService.setToken(null);
    } catch (error) {
      console.error('Sign out error:', error);
      throw error;
    }
  },

  /**
   * Show custom ban popup
   */
  showBanPopup(banData) {
    // Remove existing modals
    const existingModal = document.getElementById('banModal');
    if (existingModal) {
      existingModal.remove();
    }

    // Calculate time remaining if available
    let timeRemainingText = banData.banDuration;
    if (banData.timeRemaining && !banData.isPermanent) {
      const days = Math.floor(banData.timeRemaining / (1000 * 60 * 60 * 24));
      const hours = Math.floor((banData.timeRemaining % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
      const minutes = Math.floor((banData.timeRemaining % (1000 * 60 * 60)) / (1000 * 60));

      if (days > 0) {
        timeRemainingText = `${days} day${days > 1 ? 's' : ''}, ${hours} hour${hours > 1 ? 's' : ''}`;
      } else if (hours > 0) {
        timeRemainingText = `${hours} hour${hours > 1 ? 's' : ''}, ${minutes} minute${minutes > 1 ? 's' : ''}`;
      } else {
        timeRemainingText = `${minutes} minute${minutes > 1 ? 's' : ''}`;
      }
    }

    // Create custom ban modal
    const modal = document.createElement('div');
    modal.id = 'banModal';
    modal.style.cssText = `
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background-color: rgba(0, 0, 0, 0.8);
      display: flex;
      align-items: center;
      justify-content: center;
      z-index: 10000;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    `;

    modal.innerHTML = `
      <div style="
        background: white;
        border-radius: 12px;
        padding: 2rem;
        max-width: 500px;
        width: 90%;
        box-shadow: 0 20px 40px rgba(0, 0, 0, 0.3);
        position: relative;
      ">
        <div style="
          text-align: center;
          margin-bottom: 1.5rem;
        ">
          <div style="
            width: 80px;
            height: 80px;
            background: linear-gradient(135deg, #dc3545, #c82333);
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 1rem;
            box-shadow: 0 8px 16px rgba(220, 53, 69, 0.3);
          ">
            <i class="fas fa-ban" style="font-size: 2rem; color: white;"></i>
          </div>
          <h2 style="
            color: #dc3545;
            margin: 0 0 0.5rem 0;
            font-size: 1.5rem;
            font-weight: 600;
          ">Account Banned</h2>
        </div>

        <div style="
          margin-bottom: 1.5rem;
          line-height: 1.6;
        ">
          <div style="margin-bottom: 1rem;">
            <strong style="color: #495057;">Reason:</strong><br>
            <span style="color: #6c757d;">${banData.banReason}</span>
          </div>
          <div style="margin-bottom: 1rem;">
            <strong style="color: #495057;">Banned On:</strong><br>
            <span style="color: #6c757d;">${banData.bannedAt}</span>
          </div>
          <div style="margin-bottom: 1rem;">
            <strong style="color: #495057;">Time Remaining:</strong><br>
            <span style="color: #6c757d;">${timeRemainingText}</span>
          </div>
          ${banData.isPermanent ? '<div style="margin-bottom: 1rem;"><strong style="color: #dc3545;">This ban is permanent.</strong></div>' : ''}
        </div>

        <div style="
          background: #f8f9fa;
          padding: 1rem;
          border-radius: 8px;
          margin-bottom: 1.5rem;
          border-left: 4px solid #17a2b8;
        ">
          <i class="fas fa-info-circle" style="color: #17a2b8; margin-right: 0.5rem;"></i>
          <strong>If you believe this ban was issued in error, please contact support with your account details.</strong>
        </div>

        <div style="text-align: center;">
          <button onclick="closeBanPopup()" style="
            background: #6c757d;
            color: white;
            border: none;
            padding: 0.75rem 2rem;
            border-radius: 6px;
            font-size: 1rem;
            cursor: pointer;
            transition: background-color 0.2s;
          " onmouseover="this.style.background='#5a6268'" onmouseout="this.style.background='#6c757d'">
            <i class="fas fa-times"></i> Close
          </button>
        </div>
      </div>
    `;

    document.body.appendChild(modal);
  },

  /**
   * Send password reset email
   */
  async sendPasswordResetEmail(email) {
    try {
      const auth = typeof getAuth === 'function' ? getAuth() : firebase.auth();
      await auth.sendPasswordResetEmail(email);
    } catch (error) {
      console.error('Password reset error:', error);
      throw this.handleAuthError(error);
    }
  },

  /**
   * Handle authentication errors with user-friendly messages and suggestions
   */
  handleAuthError(error) {
    const errorInfo = {
      'auth/email-already-in-use': {
        message: 'This email is already registered.',
        suggestion: 'Try signing in instead, or use a different email address to create a new account.',
        action: 'signin'
      },
      'auth/account-exists-with-different-credential': {
        message: 'An account already exists for this email.',
        suggestion: 'Please sign in with email and password. Then go to the Account page and add your Google account.',
        action: 'signin_email_then_link_google'
      },
      'auth/popup-closed-by-user': {
        message: 'Google sign-in was cancelled.',
        suggestion: 'Please try again and complete the Google popup.',
        action: 'retry'
      },
      'auth/cancelled-popup-request': {
        message: 'Google sign-in was cancelled.',
        suggestion: 'Please try again.',
        action: 'retry'
      },
      'auth/popup-blocked': {
        message: 'Popup was blocked by your browser.',
        suggestion: 'Please allow popups for this site, then try again.',
        action: 'allow_popups'
      },
      'mclb/google-user-not-registered': {
        message: 'No account found for this Google email.',
        suggestion: 'Please use “Sign up with Google” to create your account (age verification is required).',
        action: 'go_to_google_signup'
      },
      'DUPLICATE_IP_DETECTED': {
        message: 'This network already has a registered account.',
        suggestion: 'Multiple accounts per network are not permitted. If you believe this is an error, please contact support.',
        action: 'contact_support'
      },
      'RATE_LIMITED': {
        message: 'Too many login attempts right now.',
        suggestion: 'Please wait a minute and try Google sign-in again.',
        action: 'wait'
      },
      'AGE_VERIFICATION_FAILED': {
        message: 'You must be at least 13 years old to create an account.',
        suggestion: 'Please enter a valid age (13+).',
        action: 'check_age'
      },
      'auth/invalid-email': {
        message: 'The email address you entered is not valid.',
        suggestion: 'Please check for typos and make sure you\'re using a valid email format (e.g., name@example.com).',
        action: 'check_email'
      },
      'auth/operation-not-allowed': {
        message: 'This sign-in method is not enabled.',
        suggestion: 'Please contact support if you believe this is an error.',
        action: 'contact_support'
      },
      'auth/weak-password': {
        message: 'Your password is too weak.',
        suggestion: 'Please use a password that is at least 6 characters long. For better security, use a mix of letters, numbers, and special characters.',
        action: 'strengthen_password'
      },
      'auth/user-disabled': {
        message: 'This account has been disabled.',
        suggestion: 'Your account may have been suspended or banned. Please contact support for assistance.',
        action: 'contact_support'
      },
      'auth/user-not-found': {
        message: 'No account found with this email address.',
        suggestion: 'Double-check your email for typos, or sign up for a new account if you don\'t have one yet.',
        action: 'check_email_or_signup'
      },
      'auth/wrong-password': {
        message: 'The password you entered is incorrect.',
        suggestion: 'Make sure Caps Lock is off and check for typos. If you\'ve forgotten your password, use the "Forgot password?" link to reset it.',
        action: 'reset_password'
      },
      'auth/invalid-credential': {
        message: 'The email or password you entered is incorrect.',
        suggestion: 'Please check both your email and password. Make sure Caps Lock is off. If you\'ve forgotten your password, use the "Forgot password?" link.',
        action: 'check_credentials'
      },
      'auth/invalid-verification-code': {
        message: 'The verification code is invalid or has expired.',
        suggestion: 'Please request a new verification code and try again.',
        action: 'request_new_code'
      },
      'auth/invalid-verification-id': {
        message: 'The verification link is invalid or has expired.',
        suggestion: 'Please request a new verification email and try again.',
        action: 'request_new_email'
      },
      'auth/too-many-requests': {
        message: 'Too many failed login attempts.',
        suggestion: 'For security, your account has been temporarily locked. Please wait a few minutes before trying again, or reset your password.',
        action: 'wait_or_reset'
      },
      'auth/network-request-failed': {
        message: 'Network connection error.',
        suggestion: 'Please check your internet connection and try again. If the problem persists, your firewall or network settings may be blocking the connection.',
        action: 'check_connection'
      },
      'auth/requires-recent-login': {
        message: 'For security, please sign in again.',
        suggestion: 'This action requires recent authentication. Please sign out and sign back in, then try again.',
        action: 're_signin'
      },
      'auth/quota-exceeded': {
        message: 'Service temporarily unavailable.',
        suggestion: 'The authentication service is experiencing high traffic. Please try again in a few minutes.',
        action: 'try_later'
      },
      'auth/unavailable': {
        message: 'Service temporarily unavailable.',
        suggestion: 'The authentication service is currently unavailable. Please try again in a few minutes.',
        action: 'try_later'
      },
      'auth/email-already-exists': {
        message: 'This email is already registered.',
        suggestion: 'Try signing in instead, or use a different email address to create a new account.',
        action: 'signin'
      },
      'auth/credential-already-in-use': {
        message: 'This account is already linked to another user.',
        suggestion: 'This email or account is already associated with a different account. Please sign in with your existing account.',
        action: 'signin'
      },
      'auth/invalid-action-code': {
        message: 'The verification link is invalid or has expired.',
        suggestion: 'Please request a new verification email and try again.',
        action: 'request_new_email'
      },
      'auth/expired-action-code': {
        message: 'The verification link has expired.',
        suggestion: 'Please request a new verification email and try again.',
        action: 'request_new_email'
      }
    };

    const errorCode = error.code || '';
    const info = errorInfo[errorCode];

    if (info) {
      const fullMessage = `${info.message}\n\n${info.suggestion}`;
      const enhancedError = new Error(fullMessage);
      enhancedError.code = errorCode;
      enhancedError.userMessage = info.message;
      enhancedError.suggestion = info.suggestion;
      enhancedError.action = info.action;
      return enhancedError;
    }

    // Fallback for unknown errors
    const fallbackMessage = error.message || 'An unexpected error occurred.';
    const enhancedError = new Error(`${fallbackMessage}\n\nIf this problem persists, please contact support.`);
    enhancedError.code = errorCode;
    enhancedError.userMessage = fallbackMessage;
    enhancedError.suggestion = 'If this problem persists, please contact support.';
    enhancedError.action = 'contact_support';
    return enhancedError;
  }
};

// Initialize on load
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    firebaseAuthService.init();
  });
} else {
  firebaseAuthService.init();
}

// Global function for closing ban popup
if (typeof window !== 'undefined') {
  window.closeBanPopup = function() {
    const modal = document.getElementById('banModal');
    if (modal) {
      modal.remove();
    }
    // Redirect to login page after closing ban popup
    window.location.href = '/login.html';
  };
  window.firebaseAuthService = firebaseAuthService;
}
