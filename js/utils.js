// MC Leaderboards - Utility Functions
// Performance and helper utilities

/**
 * Debounce function - delays execution until after wait milliseconds have elapsed
 * @param {Function} func - Function to debounce
 * @param {number} wait - Milliseconds to wait
 * @returns {Function} Debounced function
 */
function debounce(func, wait = 300) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}

/**
 * Throttle function - ensures function is called at most once per wait period
 * @param {Function} func - Function to throttle
 * @param {number} wait - Milliseconds to wait between calls
 * @returns {Function} Throttled function
 */
function throttle(func, wait = 300) {
  let inThrottle;
  return function executedFunction(...args) {
    if (!inThrottle) {
      func.apply(this, args);
      inThrottle = true;
      setTimeout(() => inThrottle = false, wait);
    }
  };
}

/**
 * Lazy load images with Intersection Observer
 */
function lazyLoadImages() {
  if ('IntersectionObserver' in window) {
    const imageObserver = new IntersectionObserver((entries, observer) => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          const img = entry.target;
          if (img.dataset.src) {
            img.src = img.dataset.src;
            img.removeAttribute('data-src');
          }
          if (img.dataset.srcset) {
            img.srcset = img.dataset.srcset;
            img.removeAttribute('data-srcset');
          }
          img.classList.add('loaded');
          observer.unobserve(img);
        }
      });
    });

    document.querySelectorAll('img[data-src]').forEach(img => {
      imageObserver.observe(img);
    });
  } else {
    // Fallback for browsers that don't support IntersectionObserver
    document.querySelectorAll('img[data-src]').forEach(img => {
      if (img.dataset.src) {
        img.src = img.dataset.src;
        img.removeAttribute('data-src');
      }
    });
  }
}

/**
 * Request Animation Frame throttle - limits function calls to animation frames
 * @param {Function} func - Function to throttle
 * @returns {Function} RAF-throttled function
 */
function rafThrottle(func) {
  let rafId = null;
  return function(...args) {
    if (rafId === null) {
      rafId = requestAnimationFrame(() => {
        func.apply(this, args);
        rafId = null;
      });
    }
  };
}

/**
 * Batch DOM reads and writes to avoid layout thrashing
 */
class DOMBatcher {
  constructor() {
    this.reads = [];
    this.writes = [];
    this.scheduled = false;
  }

  read(fn) {
    this.reads.push(fn);
    this.schedule();
  }

  write(fn) {
    this.writes.push(fn);
    this.schedule();
  }

  schedule() {
    if (this.scheduled) return;
    this.scheduled = true;
    
    requestAnimationFrame(() => {
      // Execute all reads first
      const reads = this.reads.slice();
      this.reads = [];
      reads.forEach(fn => fn());

      // Then execute all writes
      const writes = this.writes.slice();
      this.writes = [];
      writes.forEach(fn => fn());

      this.scheduled = false;
    });
  }
}

// Create global DOM batcher instance
const domBatcher = new DOMBatcher();

/**
 * Memoize function results
 * @param {Function} func - Function to memoize
 * @returns {Function} Memoized function
 */
function memoize(func) {
  const cache = new Map();
  return function(...args) {
    const key = JSON.stringify(args);
    if (cache.has(key)) {
      return cache.get(key);
    }
    const result = func.apply(this, args);
    cache.set(key, result);
    return result;
  };
}

/**
 * Format number with commas
 */
const formatNumber = memoize((num) => {
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
});

/**
 * Check if element is in viewport
 */
function isInViewport(element) {
  const rect = element.getBoundingClientRect();
  return (
    rect.top >= 0 &&
    rect.left >= 0 &&
    rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
    rect.right <= (window.innerWidth || document.documentElement.clientWidth)
  );
}

/**
 * Preload critical resources
 */
function preloadResource(href, as) {
  const link = document.createElement('link');
  link.rel = 'preload';
  link.as = as;
  link.href = href;
  document.head.appendChild(link);
}

/**
 * Measure performance timing
 */
function measurePerformance(name, fn) {
  const start = performance.now();
  const result = fn();
  const end = performance.now();
  if (CONFIG.DEBUG_MODE) {
    console.log(`[Performance] ${name}: ${(end - start).toFixed(2)}ms`);
  }
  return result;
}

// Export utilities
if (typeof window !== 'undefined') {
  window.debounce = debounce;
  window.throttle = throttle;
  window.rafThrottle = rafThrottle;
  window.lazyLoadImages = lazyLoadImages;
  window.domBatcher = domBatcher;
  window.memoize = memoize;
  window.formatNumber = formatNumber;
  window.isInViewport = isInViewport;
  window.preloadResource = preloadResource;
  window.measurePerformance = measurePerformance;
}
