// MC Leaderboards - Global reCAPTCHA Service
// Provides a centralized reCAPTCHA popup that works across all pages

// Site key is centralised in js/config.js (CONFIG.RECAPTCHA_SITE_KEY)
function getRecaptchaSiteKey() {
  return (typeof CONFIG !== 'undefined' && CONFIG.RECAPTCHA_SITE_KEY)
    ? CONFIG.RECAPTCHA_SITE_KEY
    : '6LfWJFcsAAAAACjm-s-Nll5RzUzLvN5tExAKSNGp'; // fallback
}
let recaptchaLoaded = false;
let recaptchaLoading = false;

/**
 * Initialize reCAPTCHA service
 */
function initRecaptchaService() {
  if (recaptchaLoaded || recaptchaLoading) return;
  
  recaptchaLoading = true;
  
  // Check if grecaptcha is already available
  if (typeof grecaptcha !== 'undefined') {
    recaptchaLoaded = true;
    recaptchaLoading = false;
    return;
  }
  
  // Load reCAPTCHA v3 script if not already loaded
  if (!document.querySelector('script[src*="recaptcha/api.js"]')) {
    const script = document.createElement('script');
    script.src = `https://www.google.com/recaptcha/api.js?render=${getRecaptchaSiteKey()}`;
    script.async = true;
    script.defer = true;
    // Note: Do NOT set crossOrigin on script tags - it causes CORS errors
    
    script.onload = () => {
      // Wait a bit for grecaptcha to be available after script loads
      setTimeout(() => {
        if (typeof grecaptcha !== 'undefined') {
          recaptchaLoaded = true;
          recaptchaLoading = false;
          console.log('reCAPTCHA v3 script loaded successfully');
        } else {
          // Script loaded but grecaptcha not available yet, wait for it
          const checkInterval = setInterval(() => {
            if (typeof grecaptcha !== 'undefined') {
              recaptchaLoaded = true;
              recaptchaLoading = false;
              clearInterval(checkInterval);
              console.log('reCAPTCHA v3 script loaded successfully');
            }
          }, 100);
          
          // Timeout after 5 seconds
          setTimeout(() => {
            if (!recaptchaLoaded) {
              clearInterval(checkInterval);
              recaptchaLoading = false;
              console.warn('reCAPTCHA script loaded but grecaptcha not available');
            }
          }, 5000);
        }
      }, 100);
    };
    
    script.onerror = (error) => {
      recaptchaLoading = false;
      console.error('Failed to load reCAPTCHA script. This may be due to:', {
        reason: 'Network error, ad blocker, or CORS issue',
        error: error,
        suggestion: 'Check browser console for network errors or disable ad blockers'
      });
      
      // Try alternative loading method
      setTimeout(() => {
        if (!recaptchaLoaded && !recaptchaLoading) {
          console.warn('Attempting to reload reCAPTCHA script...');
          // Remove the failed script
          const failedScript = document.querySelector('script[src*="recaptcha/api.js"]');
          if (failedScript) {
            failedScript.remove();
          }
          // Retry initialization after a delay
          setTimeout(() => {
            initRecaptchaService();
          }, 2000);
        }
      }, 1000);
    };
    
    document.head.appendChild(script);
  } else {
    // Script already exists, wait for it to load
    const checkInterval = setInterval(() => {
      if (typeof grecaptcha !== 'undefined') {
        recaptchaLoaded = true;
        recaptchaLoading = false;
        clearInterval(checkInterval);
      }
    }, 100);
    
    // Timeout after 10 seconds
    setTimeout(() => {
      if (!recaptchaLoaded) {
        clearInterval(checkInterval);
        recaptchaLoading = false;
      }
    }, 10000);
  }
}

/**
 * Wait for reCAPTCHA to be ready
 */
function waitForRecaptcha() {
  return new Promise((resolve, reject) => {
    if (typeof grecaptcha !== 'undefined' && grecaptcha.ready) {
      grecaptcha.ready(() => resolve());
      return;
    }
    
    // Try to initialize
    initRecaptchaService();
    
    // Wait for it to load
    const checkInterval = setInterval(() => {
      if (typeof grecaptcha !== 'undefined' && grecaptcha.ready) {
        clearInterval(checkInterval);
        grecaptcha.ready(() => resolve());
      }
    }, 100);
    
    // Timeout after 10 seconds
    setTimeout(() => {
      clearInterval(checkInterval);
      reject(new Error('reCAPTCHA failed to load. Please refresh the page.'));
    }, 10000);
  });
}

/**
 * Get reCAPTCHA token using v3 (invisible)
 * This is the main function to use throughout the app
 * @param {string} action - Action name for reCAPTCHA (default: 'submit')
 * @returns {Promise<string>} reCAPTCHA token
 */
async function getRecaptchaToken(action = 'submit') {
  try {
    // Show loading indicator if Swal is available
    let loadingToast = null;
    if (typeof Swal !== 'undefined') {
      try {
        loadingToast = Swal.fire({
          title: 'Verifying...',
          text: 'Please wait while we verify you',
          allowOutsideClick: false,
          allowEscapeKey: false,
          showConfirmButton: false,
          didOpen: () => {
            Swal.showLoading();
          }
        });
      } catch (swalError) {
        console.warn('Could not show loading toast:', swalError);
      }
    }
    
    // Wait for reCAPTCHA to be ready (with retry logic)
    let retries = 0;
    const maxRetries = 5;
    let lastError = null;
    
    // First, ensure initialization is attempted
    if (!recaptchaLoaded && !recaptchaLoading) {
      initRecaptchaService();
      // Wait a bit for initialization to start
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    while (retries < maxRetries) {
      try {
        await waitForRecaptcha();
        break; // Success, exit retry loop
      } catch (error) {
        lastError = error;
        retries++;
        if (retries < maxRetries) {
          // Wait a bit before retrying
          await new Promise(resolve => setTimeout(resolve, 1000));
          // Reset loading state and try to reinitialize
          recaptchaLoading = false;
          recaptchaLoaded = false;
          initRecaptchaService();
          // Wait for initialization to start
          await new Promise(resolve => setTimeout(resolve, 500));
        }
      }
    }
    
    if (retries >= maxRetries) {
      const errorMsg = lastError?.message || 'reCAPTCHA failed to load after multiple attempts';
      throw new Error(`${errorMsg}. Please check your internet connection, disable ad blockers, and refresh the page.`);
    }
    
    // Verify grecaptcha is available and has execute method
    if (typeof grecaptcha === 'undefined') {
      // Check if script was blocked
      const scriptTag = document.querySelector('script[src*="recaptcha/api.js"]');
      if (!scriptTag) {
        throw new Error('reCAPTCHA script was not loaded. This may be due to an ad blocker or network issue. Please disable ad blockers and refresh the page.');
      }
      throw new Error('reCAPTCHA script loaded but grecaptcha is not available. Please refresh the page.');
    }
    
    if (typeof grecaptcha.execute !== 'function') {
      throw new Error('reCAPTCHA is not properly initialized. The execute method is not available.');
    }
    
    // Execute reCAPTCHA
    const token = await grecaptcha.execute(getRecaptchaSiteKey(), { action });
    
    // Validate token
    if (!token || typeof token !== 'string' || token.length === 0) {
      throw new Error('Invalid reCAPTCHA token received');
    }
    
    // Close loading indicator
    if (loadingToast && typeof Swal !== 'undefined') {
      try {
        Swal.close();
      } catch (closeError) {
        console.warn('Could not close loading toast:', closeError);
      }
    }
    
    return token;
  } catch (error) {
    // Close loading indicator if open
    if (typeof Swal !== 'undefined') {
      try {
        Swal.close();
      } catch (closeError) {
        console.warn('Could not close loading toast on error:', closeError);
      }
    }
    
    // Show user-friendly error
    if (typeof Swal !== 'undefined') {
      try {
        await Swal.fire({
          icon: 'error',
          title: 'Verification Required',
          text: error.message || 'reCAPTCHA verification is required. Please try again.',
          confirmButtonText: 'OK'
        });
      } catch (swalError) {
        console.error('Could not show error message:', swalError);
        // Fallback to console if Swal fails
        console.error('reCAPTCHA Error:', error.message || error);
      }
    } else {
      // No Swal available, log to console
      console.error('reCAPTCHA Error:', error.message || error);
    }
    throw error;
  }
}

// Initialize on load
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initRecaptchaService);
} else {
  initRecaptchaService();
}

// Export for use in other scripts
if (typeof window !== 'undefined') {
  window.RecaptchaService = {
    getToken: getRecaptchaToken,
    init: initRecaptchaService,
    isAvailable: () => {
      return typeof window !== 'undefined' && 
             typeof window.RecaptchaService !== 'undefined' && 
             typeof window.RecaptchaService.getToken === 'function';
    }
  };
  
  // Ensure service is always available, even if script loads late
  // This helps with race conditions where other scripts load before this one
  if (!window.RecaptchaService || typeof window.RecaptchaService.getToken !== 'function') {
    console.warn('RecaptchaService not properly initialized, re-initializing...');
    initRecaptchaService();
  }
}

