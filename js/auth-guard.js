// MC Leaderboards - Authentication Guard
// Ensures authentication is verified before any page operations

/**
 * Wait for authentication state to be determined
 * Returns a promise that resolves with the user if authenticated, or null if not
 */
async function waitForAuthState() {
    try {
    // Wait for Firebase to be initialized using the centralized service
    if (typeof waitForFirebaseInit !== 'undefined') {
      await waitForFirebaseInit();
    } else {
      // Fallback: Wait for Firebase to be initialized
      await new Promise((resolve) => {
      const checkFirebase = () => {
        if (typeof firebase !== 'undefined' && firebase.apps && firebase.apps.length > 0) {
            resolve();
          } else {
            setTimeout(checkFirebase, 100);
          }
        };
        checkFirebase();
      });
    }

    return new Promise((resolve) => {
          const auth = typeof getAuth === 'function' ? getAuth() : firebase.auth();
      
          // Check if already initialized and has user
          if (auth.currentUser !== null) {
            resolve(auth.currentUser);
            return;
          }

          // Wait for auth state change
          // This will fire immediately if user is already logged in
          const unsubscribe = auth.onAuthStateChanged((user) => {
            unsubscribe(); // Only listen once
            resolve(user);
          });

          // Timeout after 5 seconds to prevent hanging
          setTimeout(() => {
            unsubscribe();
            // If still no user after timeout, resolve with null
            if (auth.currentUser === null) {
              resolve(null);
            } else {
              resolve(auth.currentUser);
            }
          }, 5000);
    });
    } catch (error) {
      console.error('Error in waitForAuthState:', error);
    return null;
    }
}

// Track if redirect is in progress to prevent loops
let redirectInProgress = false;

/**
 * Guard function - checks authentication before allowing page operations
 * Redirects to login if not authenticated
 * @param {boolean} requireAdmin - If true, also requires admin role
 * @param {boolean} requireTierTester - If true, also requires tier tester role
 */
async function requireAuth(requireAdmin = false, requireTierTester = false) {
  // Prevent multiple simultaneous checks
  if (redirectInProgress) {
    return false;
  }

  // Wait for Firebase to be initialized using the centralized service
  if (typeof waitForFirebaseInit !== 'undefined') {
    await waitForFirebaseInit();
  } else {
    // Fallback: Wait for Firebase to be initialized
  if (typeof firebase === 'undefined' || !firebase.apps || firebase.apps.length === 0) {
    await new Promise((resolve) => {
      const checkFirebaseReady = () => {
        if (typeof firebase !== 'undefined' && firebase.apps && firebase.apps.length > 0) {
          resolve();
        } else {
          setTimeout(checkFirebaseReady, 100);
        }
      };
      checkFirebaseReady();
    });
    }
  }

  // Wait for auth state
  const user = await waitForAuthState();
  
  if (!user) {
    // Not authenticated - redirect to login
    if (!redirectInProgress) {
      redirectInProgress = true;
      window.location.href = 'login.html';
    }
    return false;
  }
  
  // Set user in AppState
  AppState.setUser(user);

  // Ensure API token is synchronized before any protected API call (prevents initial 401 race).
  try {
    if (typeof firebaseAuthService !== 'undefined' && typeof firebaseAuthService.ensureFreshIdToken === 'function') {
      await firebaseAuthService.ensureFreshIdToken(true);
    } else {
      const freshToken = await user.getIdToken(true);
      if (typeof apiService !== 'undefined' && typeof apiService.setToken === 'function') {
        apiService.setToken(freshToken);
      }
    }
  } catch (tokenError) {
    console.error('Error ensuring auth token in requireAuth:', tokenError);
  }
  
  // Wait for profile to be loaded (with retry)
  let profileLoaded = false;
  let retries = 0;
  const maxRetries = 3;
  let lastProfileError = null;
  
  while (!profileLoaded && retries < maxRetries) {
    try {
      await firebaseAuthService.fetchUserProfile(user.uid);
      profileLoaded = !!AppState.getProfile();
      if (!profileLoaded) {
        throw new Error('Profile not loaded yet');
      }
    } catch (error) {
      lastProfileError = error;
      retries++;
      if (retries < maxRetries) {
        // Wait a bit before retrying
        await new Promise(resolve => setTimeout(resolve, 500));
      } else {
        console.error('Error fetching profile after retries:', error);
        // Still allow access, profile might load later
      }
    }
  }

  if (!AppState.getProfile()) {
    if (!redirectInProgress) {
      redirectInProgress = true;
      const isRateLimited = lastProfileError?.isRateLimit === true || lastProfileError?.status === 429;
      if (isRateLimited) {
        const message = lastProfileError?.message || 'Your account profile is temporarily rate limited.';
        const suggestion = lastProfileError?.suggestion || 'Please wait a moment and try again.';
        Swal.fire({
          icon: 'warning',
          title: 'Profile Load Rate Limited',
          text: `${message} ${suggestion}`.trim(),
          confirmButtonText: 'Retry',
          showCancelButton: true,
          cancelButtonText: 'Go to Login'
        }).then((result) => {
          redirectInProgress = false;
          if (result.isConfirmed) {
            window.location.reload();
            return;
          }
          window.location.href = 'login.html';
        });
      } else {
        Swal.fire({
          icon: 'error',
          title: 'Session Error',
          text: 'Could not load your account profile. Please sign in again.',
          confirmButtonText: 'Go to Login'
        }).then(() => {
          window.location.href = 'login.html';
        });
      }
    }
    return false;
  }
  
  // Check if user is banned
  const profile = AppState.getProfile();
  if (profile && profile.banned) {
    // Check if ban has expired
    let isStillBanned = true;
    if (profile.banExpires && profile.banExpires !== 'permanent') {
      const banExpires = new Date(profile.banExpires);
      const now = new Date();
      if (banExpires <= now) {
        isStillBanned = false;
        // Clear ban status if expired
        await firebaseAuthService.updateProfile({ banned: false });
      }
    }

    if (isStillBanned) {
      if (!redirectInProgress) {
        redirectInProgress = true;

        // Format ban information
        const bannedAt = profile.bannedAt ? new Date(profile.bannedAt).toLocaleDateString() : 'Unknown';
        let banDuration = 'Permanent';

        if (profile.banExpires && profile.banExpires !== 'permanent') {
          const expiryDate = new Date(profile.banExpires);
          const now = new Date();
          if (expiryDate > now) {
            const daysLeft = Math.ceil((expiryDate - now) / (1000 * 60 * 60 * 24));
            banDuration = `${daysLeft} days remaining (expires ${expiryDate.toLocaleDateString()})`;
          }
        }

        const banReason = profile.banReason || 'Violation of terms of service';

        // Show ban message and prevent login
        Swal.fire({
          icon: 'error',
          title: 'Account Banned',
          html: `
            <div style="text-align: left; margin-bottom: 1rem;">
              <strong>Reason:</strong> ${banReason}<br>
              <strong>Banned On:</strong> ${bannedAt}<br>
              <strong>Duration:</strong> ${banDuration}
            </div>
            <div style="color: #dc3545; font-size: 0.9em;">
              If you believe this ban was issued in error, please contact support with your account details.
            </div>
          `,
          confirmButtonText: 'Logout',
          allowOutsideClick: false,
          allowEscapeKey: false,
          showCancelButton: false
        }).then(() => {
          // Force logout
          const auth = typeof getAuth === 'function' ? getAuth() : firebase.auth();
          auth.signOut().then(() => {
            window.location.href = 'login.html';
          }).catch(() => {
            window.location.href = 'login.html';
          });
        });
      }
      return false;
    }
  }
  
  // Check admin requirement
  if (requireAdmin && !AppState.isAdmin()) {
    if (!redirectInProgress) {
      redirectInProgress = true;
      Swal.fire({
        icon: 'error',
        title: 'Access Denied',
        text: 'You must be an admin to access this page.',
        confirmButtonText: 'Go to Dashboard'
      }).then(() => {
        window.location.href = 'dashboard.html';
      });
    }
    return false;
  }
  
  // Check tier tester requirement
  if (requireTierTester && !AppState.isTierTester()) {
    if (!redirectInProgress) {
      redirectInProgress = true;
      Swal.fire({
        icon: 'error',
        title: 'Access Denied',
        text: 'You must be a tier tester to access this page.',
        confirmButtonText: 'Go to Dashboard'
      }).then(() => {
        window.location.href = 'dashboard.html';
      });
    }
    return false;
  }

  // Check onboarding completion (skip for onboarding page itself)
  const currentPage = window.location.pathname.split('/').pop();
  if (currentPage !== 'onboarding.html' && !AppState.isOnboardingCompleted()) {
    if (!redirectInProgress) {
      redirectInProgress = true;
      window.location.href = 'onboarding.html';
    }
    return false;
  }

  return true;
}

/**
 * Guard function for public pages - redirects if already authenticated
 * Used for login/signup pages
 */
async function requireGuest() {
  // Prevent multiple simultaneous checks
  if (redirectInProgress) {
    return false;
  }

  // Wait for Firebase to be initialized using the centralized service
  if (typeof waitForFirebaseInit !== 'undefined') {
    await waitForFirebaseInit();
  } else {
    // Fallback: Wait for Firebase to be initialized
  if (typeof firebase === 'undefined' || !firebase.apps || firebase.apps.length === 0) {
    await new Promise((resolve) => {
      const checkFirebaseReady = () => {
        if (typeof firebase !== 'undefined' && firebase.apps && firebase.apps.length > 0) {
          resolve();
        } else {
          setTimeout(checkFirebaseReady, 100);
        }
      };
      checkFirebaseReady();
    });
    }
  }

  const user = await waitForAuthState();
  
  if (user) {
    let profile = null;
    try {
      if (typeof firebaseAuthService !== 'undefined' && typeof firebaseAuthService.ensureFreshIdToken === 'function') {
        await firebaseAuthService.ensureFreshIdToken(true);
      } else {
        const freshToken = await user.getIdToken(true);
        if (typeof apiService !== 'undefined' && typeof apiService.setToken === 'function') {
          apiService.setToken(freshToken);
        }
      }
    } catch (tokenError) {
      console.error('Error ensuring auth token in requireGuest:', tokenError);
    }

    // Already authenticated - check if banned before redirecting
    try {
      await firebaseAuthService.fetchUserProfile(user.uid);
      profile = AppState.getProfile();

      if (profile && profile.banned) {
        // Check if ban has expired
        let isStillBanned = true;
        if (profile.banExpires && profile.banExpires !== 'permanent') {
          const banExpires = new Date(profile.banExpires);
          const now = new Date();
          if (banExpires <= now) {
            isStillBanned = false;
            // Clear ban status if expired
            await firebaseAuthService.updateProfile({ banned: false });
          }
        }

        if (isStillBanned) {
          if (!redirectInProgress) {
            redirectInProgress = true;

            // Format ban information
            const bannedAt = profile.bannedAt ? new Date(profile.bannedAt).toLocaleDateString() : 'Unknown';
            let banDuration = 'Permanent';

            if (profile.banExpires && profile.banExpires !== 'permanent') {
              const expiryDate = new Date(profile.banExpires);
              const now = new Date();
              if (expiryDate > now) {
                const daysLeft = Math.ceil((expiryDate - now) / (1000 * 60 * 60 * 24));
                banDuration = `${daysLeft} days remaining (expires ${expiryDate.toLocaleDateString()})`;
              }
            }

            const banReason = profile.banReason || 'Violation of terms of service';

            // Show ban message on login page
            Swal.fire({
              icon: 'error',
              title: 'Account Banned',
              html: `
                <div style="text-align: left; margin-bottom: 1rem;">
                  <strong>Reason:</strong> ${banReason}<br>
                  <strong>Banned On:</strong> ${bannedAt}<br>
                  <strong>Duration:</strong> ${banDuration}
                </div>
                <div style="color: #dc3545; font-size: 0.9em;">
                  If you believe this ban was issued in error, please contact support with your account details.
                </div>
              `,
              confirmButtonText: 'Logout',
              allowOutsideClick: false,
              allowEscapeKey: false,
              showCancelButton: false
            }).then(() => {
              // Force logout
              const auth = typeof getAuth === 'function' ? getAuth() : firebase.auth();
              auth.signOut().then(() => {
                window.location.reload();
              }).catch(() => {
                window.location.reload();
              });
            });
          }
          return false;
        }
      }
    } catch (error) {
      console.error('Error checking ban status in requireGuest:', error);
      // Continue with redirect if we can't check ban status
    }

    // Not banned - redirect based on onboarding completion
    if (!redirectInProgress) {
      redirectInProgress = true;
      const redirectTarget = (profile && profile.onboardingCompleted) ? 'dashboard.html' : 'onboarding.html';
      window.location.href = redirectTarget;
    }
    return false;
  }
  
  return true;
}

// Export for use in other scripts
if (typeof window !== 'undefined') {
  window.requireAuth = requireAuth;
  window.requireGuest = requireGuest;
  window.waitForAuthState = waitForAuthState;
}
