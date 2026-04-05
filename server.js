// MC Leaderboards - Backend Server
// Express.js API server with Firebase Admin SDK

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const rateLimit = require('express-rate-limit');
const slowDown = require('express-slow-down');

// Initialize Firebase Admin
const admin = require('firebase-admin');

// Server-side configuration
const CONFIG = {
  GAMEMODES: [
    { id: 'overall', name: 'Overall', icon: 'assets/overall.svg' },
    { id: 'vanilla', name: 'Vanilla', icon: 'assets/vanilla.svg' },
    { id: 'uhc', name: 'UHC', icon: 'assets/uhc.svg' },
    { id: 'pot', name: 'Pot', icon: 'assets/pot.svg' },
    { id: 'nethop', name: 'NethOP', icon: 'assets/nethop.svg' },
    { id: 'smp', name: 'SMP', icon: 'assets/smp.svg' },
    { id: 'sword', name: 'Sword', icon: 'assets/sword.svg' },
    { id: 'axe', name: 'Axe', icon: 'assets/axe.svg' },
    { id: 'mace', name: 'Mace', icon: 'assets/mace.svg' }
  ],
  TITLES: {
    overall: [
      { minRating: 0, title: 'Rookie', color: '#6c7178', icon: 'assets/badgeicons/rookie.svg' },
      { minRating: 300, title: 'Combat Novice', color: '#9291d9', icon: 'assets/badgeicons/combat_novice.svg' },
      { minRating: 500, title: 'Combat Cadet', color: '#9291d9', icon: 'assets/badgeicons/combat_cadet.svg' },
      { minRating: 1000, title: 'Combat Specialist', color: '#ad78d8', icon: 'assets/badgeicons/combat_specialist.svg' },
      { minRating: 1300, title: 'Combat Ace', color: '#cd285c', icon: 'assets/badgeicons/combat_ace.svg' },
      { minRating: 1500, title: 'Combat Master', color: '#FF5722', icon: 'assets/badgeicons/combat_master.webp' },
      { minRating: 2000, title: 'Combat Grandmaster', color: '#FFD700', icon: 'assets/badgeicons/combat_grandmaster.webp' }
    ],
    vanilla: [
      { minRating: 0, title: 'Rookie', color: '#6c7178', icon: 'assets/badgeicons/rookie.svg' },
      { minRating: 300, title: 'Combat Novice', color: '#9291d9', icon: 'assets/badgeicons/combat_novice.svg' },
      { minRating: 500, title: 'Combat Cadet', color: '#9291d9', icon: 'assets/badgeicons/combat_cadet.svg' },
      { minRating: 1000, title: 'Combat Specialist', color: '#ad78d8', icon: 'assets/badgeicons/combat_specialist.svg' },
      { minRating: 1300, title: 'Combat Ace', color: '#cd285c', icon: 'assets/badgeicons/combat_ace.svg' },
      { minRating: 1500, title: 'Combat Master', color: '#FF5722', icon: 'assets/badgeicons/combat_master.webp' },
      { minRating: 2000, title: 'Combat Grandmaster', color: '#FFD700', icon: 'assets/badgeicons/combat_grandmaster.webp' }
    ],
    uhc: [
      { minRating: 0, title: 'Rookie', color: '#6c7178', icon: 'assets/badgeicons/rookie.svg' },
      { minRating: 300, title: 'Combat Novice', color: '#9291d9', icon: 'assets/badgeicons/combat_novice.svg' },
      { minRating: 500, title: 'Combat Cadet', color: '#9291d9', icon: 'assets/badgeicons/combat_cadet.svg' },
      { minRating: 1000, title: 'Combat Specialist', color: '#ad78d8', icon: 'assets/badgeicons/combat_specialist.svg' },
      { minRating: 1300, title: 'Combat Ace', color: '#cd285c', icon: 'assets/badgeicons/combat_ace.svg' },
      { minRating: 1500, title: 'Combat Master', color: '#FF5722', icon: 'assets/badgeicons/combat_master.webp' },
      { minRating: 2000, title: 'Combat Grandmaster', color: '#FFD700', icon: 'assets/badgeicons/combat_grandmaster.webp' }
    ],
    pot: [
      { minRating: 0, title: 'Rookie', color: '#6c7178', icon: 'assets/badgeicons/rookie.svg' },
      { minRating: 300, title: 'Combat Novice', color: '#9291d9', icon: 'assets/badgeicons/combat_novice.svg' },
      { minRating: 500, title: 'Combat Cadet', color: '#9291d9', icon: 'assets/badgeicons/combat_cadet.svg' },
      { minRating: 1000, title: 'Combat Specialist', color: '#ad78d8', icon: 'assets/badgeicons/combat_specialist.svg' },
      { minRating: 1300, title: 'Combat Ace', color: '#cd285c', icon: 'assets/badgeicons/combat_ace.svg' },
      { minRating: 1500, title: 'Combat Master', color: '#FF5722', icon: 'assets/badgeicons/combat_master.webp' },
      { minRating: 2000, title: 'Combat Grandmaster', color: '#FFD700', icon: 'assets/badgeicons/combat_grandmaster.webp' }
    ],
    nethop: [
      { minRating: 0, title: 'Rookie', color: '#6c7178', icon: 'assets/badgeicons/rookie.svg' },
      { minRating: 300, title: 'Combat Novice', color: '#9291d9', icon: 'assets/badgeicons/combat_novice.svg' },
      { minRating: 500, title: 'Combat Cadet', color: '#9291d9', icon: 'assets/badgeicons/combat_cadet.svg' },
      { minRating: 1000, title: 'Combat Specialist', color: '#ad78d8', icon: 'assets/badgeicons/combat_specialist.svg' },
      { minRating: 1300, title: 'Combat Ace', color: '#cd285c', icon: 'assets/badgeicons/combat_ace.svg' },
      { minRating: 1500, title: 'Combat Master', color: '#FF5722', icon: 'assets/badgeicons/combat_master.webp' },
      { minRating: 2000, title: 'Combat Grandmaster', color: '#FFD700', icon: 'assets/badgeicons/combat_grandmaster.webp' }
    ],
    smp: [
      { minRating: 0, title: 'Rookie', color: '#6c7178', icon: 'assets/badgeicons/rookie.svg' },
      { minRating: 300, title: 'Combat Novice', color: '#9291d9', icon: 'assets/badgeicons/combat_novice.svg' },
      { minRating: 500, title: 'Combat Cadet', color: '#9291d9', icon: 'assets/badgeicons/combat_cadet.svg' },
      { minRating: 1000, title: 'Combat Specialist', color: '#ad78d8', icon: 'assets/badgeicons/combat_specialist.svg' },
      { minRating: 1300, title: 'Combat Ace', color: '#cd285c', icon: 'assets/badgeicons/combat_ace.svg' },
      { minRating: 1500, title: 'Combat Master', color: '#FF5722', icon: 'assets/badgeicons/combat_master.webp' },
      { minRating: 2000, title: 'Combat Grandmaster', color: '#FFD700', icon: 'assets/badgeicons/combat_grandmaster.webp' }
    ],
    sword: [
      { minRating: 0, title: 'Rookie', color: '#6c7178', icon: 'assets/badgeicons/rookie.svg' },
      { minRating: 300, title: 'Combat Novice', color: '#9291d9', icon: 'assets/badgeicons/combat_novice.svg' },
      { minRating: 500, title: 'Combat Cadet', color: '#9291d9', icon: 'assets/badgeicons/combat_cadet.svg' },
      { minRating: 1000, title: 'Combat Specialist', color: '#ad78d8', icon: 'assets/badgeicons/combat_specialist.svg' },
      { minRating: 1300, title: 'Combat Ace', color: '#cd285c', icon: 'assets/badgeicons/combat_ace.svg' },
      { minRating: 1500, title: 'Combat Master', color: '#FF5722', icon: 'assets/badgeicons/combat_master.webp' },
      { minRating: 2000, title: 'Combat Grandmaster', color: '#FFD700', icon: 'assets/badgeicons/combat_grandmaster.webp' }
    ],
    axe: [
      { minRating: 0, title: 'Rookie', color: '#6c7178', icon: 'assets/badgeicons/rookie.svg' },
      { minRating: 300, title: 'Combat Novice', color: '#9291d9', icon: 'assets/badgeicons/combat_novice.svg' },
      { minRating: 500, title: 'Combat Cadet', color: '#9291d9', icon: 'assets/badgeicons/combat_cadet.svg' },
      { minRating: 1000, title: 'Combat Specialist', color: '#ad78d8', icon: 'assets/badgeicons/combat_specialist.svg' },
      { minRating: 1300, title: 'Combat Ace', color: '#cd285c', icon: 'assets/badgeicons/combat_ace.svg' },
      { minRating: 1500, title: 'Combat Master', color: '#FF5722', icon: 'assets/badgeicons/combat_master.webp' },
      { minRating: 2000, title: 'Combat Grandmaster', color: '#FFD700', icon: 'assets/badgeicons/combat_grandmaster.webp' }
    ],
    mace: [
      { minRating: 0, title: 'Rookie', color: '#6c7178', icon: 'assets/badgeicons/rookie.svg' },
      { minRating: 300, title: 'Combat Novice', color: '#9291d9', icon: 'assets/badgeicons/combat_novice.svg' },
      { minRating: 500, title: 'Combat Cadet', color: '#9291d9', icon: 'assets/badgeicons/combat_cadet.svg' },
      { minRating: 1000, title: 'Combat Specialist', color: '#ad78d8', icon: 'assets/badgeicons/combat_specialist.svg' },
      { minRating: 1300, title: 'Combat Ace', color: '#cd285c', icon: 'assets/badgeicons/combat_ace.svg' },
      { minRating: 1500, title: 'Combat Master', color: '#FF5722', icon: 'assets/badgeicons/combat_master.webp' },
      { minRating: 2000, title: 'Combat Grandmaster', color: '#FFD700', icon: 'assets/badgeicons/combat_grandmaster.webp' }
    ]
  },
  // First-to values for each gamemode
  FIRST_TO: {
    sword: 6,
    axe: 10,
    nethop: 3,
    pot: 5,
    smp: 2,
    uhc: 4,
    vanilla: 3,
    mace: 3
  }
};

// Load configuration from key.json
let serviceAccount;
let config = {};

// Check if key.json exists before trying to require it
const fs = require('fs');
const path = require('path');

if (!fs.existsSync(path.join(__dirname, 'key.json'))) {
  console.error('❌ CRITICAL ERROR: Firebase credentials file (key.json) not found!');
  console.error('\n📝 SOLUTION:');
  console.error('1. Download your Firebase service account key from:');
  console.error('   Firebase Console → Project Settings → Service Accounts → Generate Key');
  console.error('2. Save the downloaded JSON file as "key.json" in the backend/ directory');
  console.error('3. Restart the server');
  console.error('\n🚫 Server cannot start without Firebase credentials.');
  process.exit(1);
}

try {
  serviceAccount = require('./key.json');

  if (!serviceAccount) {
    throw new Error('key.json file is empty or invalid');
  }

  // Extract configuration from key.json
  // Firebase service account JSON contains project_id
  // Database URL format: https://{project-id}-default-rtdb.firebaseio.com
  config.databaseURL = serviceAccount.databaseURL || 
    `https://${serviceAccount.project_id}-default-rtdb.firebaseio.com`;
  config.port = serviceAccount.port || 3000;
  config.nodeEnv = serviceAccount.nodeEnv || 'development';
  config.jwtSecret = serviceAccount.jwtSecret || 'change-this-in-production';
  config.jwtExpiresIn = serviceAccount.jwtExpiresIn || '1h';
  
  // Initialize Firebase Admin SDK
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: config.databaseURL
  });
  console.log('Firebase Admin initialized successfully');
  console.log(`Database URL: ${config.databaseURL}`);

  // Load plugin API key from key.json
  let PLUGIN_API_KEY;
  try {
    PLUGIN_API_KEY = serviceAccount.plugin_api_key;
    if (!PLUGIN_API_KEY) {
      throw new Error('plugin_api_key field not found in key.json');
    }
    console.log('Plugin API key loaded from key.json');
  } catch (error) {
    console.error('Error loading plugin API key from key.json:', error.message);
    console.log('Falling back to environment variable or default key');
    PLUGIN_API_KEY = process.env.PLUGIN_API_KEY || 'replace-with-a-random-shared-secret';
    console.log('Using fallback API key');
  }

  // Load admin bypass email from key.json
  config.adminBypassEmail = serviceAccount.admin_bypass_email || process.env.ADMIN_BYPASS_EMAIL || null;
  if (config.adminBypassEmail) {
    console.log(`Admin bypass email configured: ${config.adminBypassEmail}`);
  }

} catch (error) {
  console.error('❌ CRITICAL ERROR: Failed to initialize Firebase Admin SDK');
  console.error('Error details:', error.message);

  if (error.code === 'MODULE_NOT_FOUND') {
    console.error('\n🔑 MISSING FIREBASE CREDENTIALS:');
    console.error('The key.json file is missing from the backend directory.');
    console.error('This file contains your Firebase service account credentials.');
    console.error('\n📝 To fix this:');
    console.error('1. Go to Firebase Console → Project Settings → Service Accounts');
    console.error('2. Generate a new private key and download the JSON file');
    console.error('3. Rename it to "key.json" and place it in the backend/ directory');
    console.error('4. Make sure the file is NOT committed to version control');
  } else {
    console.error('\n🔧 FIREBASE CONFIGURATION ERROR:');
    console.error('The key.json file exists but contains invalid credentials.');
    console.error('Please check that you downloaded the correct service account key.');
  }

  console.error('\n🚫 SERVER CANNOT START without valid Firebase credentials.');
  process.exit(1);
}

const db = admin.database();

// Initialize Express app
const app = express();
const PORT = config.port;

// ===== Middleware =====

// Trust proxy - Required when behind Nginx reverse proxy
// This allows Express to trust X-Forwarded-* headers from the proxy
app.set('trust proxy', true);

// Security Headers - Enhanced
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'", 'https://cdn.jsdelivr.net', 'https://www.gstatic.com'],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", 'data:', 'https:'],
      connectSrc: ["'self'", 'https://mcleaderboards.org', 'https://*.firebaseio.com'],
      fontSrc: ["'self'", 'data:'],
      frameSrc: ["'none'"]
    }
  },
  hsts: {
    maxAge: 31536000, // 1 year
    includeSubDomains: true,
    preload: true
  },
  noSniff: true,
  xssFilter: true,
  referrerPolicy: { policy: 'strict-origin-when-cross-origin' }
}));

// CORS - Restrict to known origins
const allowedOrigins = config.nodeEnv === 'production'
  ? [
      'https://mcleaderboards.org',
      'https://www.mcleaderboards.org'
    ]
  : [
      'http://localhost:3000',
      'http://localhost:8000',
      'http://127.0.0.1:5500',
      'http://localhost:5500',
      'http://localhost:3001'
    ];

app.use(cors({
  origin: (origin, callback) => {
    if (!origin) return callback(null, true); // Allow non-browser requests
    if (allowedOrigins.includes(origin)) {
      return callback(null, true);
    }
    return callback(new Error('CORS policy violation: origin not allowed'));
  },
  credentials: true,
  optionsSuccessStatus: 200,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

// Body parsing
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Logging
if (config.nodeEnv !== 'production') {
  app.use(morgan('dev'));
} else {
  app.use(morgan('combined'));
}

// Rate limiting - configured for proxy environment
const limiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 100, // 100 requests per minute
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true, // Return rate limit info in the `RateLimit-*` headers
  legacyHeaders: false, // Disable the `X-RateLimit-*` headers
  // Use a custom key generator that properly handles proxy headers
  keyGenerator: (req) => {
    // Get the real client IP from proxy headers or fallback to direct IP
    const clientIP = getClientIP(req);
    return clientIP;
  }
});

const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 5, // 5 requests per 15 minutes
  message: 'Too many authentication attempts, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    const clientIP = getClientIP(req);
    return clientIP;
  }
});

// Stricter rate limiting for admin operations
const adminLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 10, // 10 admin requests per minute
  message: 'Too many admin requests, please slow down.',
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => {
    const clientIP = getClientIP(req);
    return clientIP;
  }
});

app.use('/api/', limiter);
app.use('/api/auth/', authLimiter);

// Slow down - configured for proxy environment
app.use(slowDown({
  windowMs: 15 * 60 * 1000, // 15 minutes
  delayAfter: 50, // Allow 50 requests per 15 minutes at full speed
  delayMs: 100, // Add 100ms delay per request after 50
  // Use a custom key generator that properly handles proxy headers
  keyGenerator: (req) => {
    const clientIP = getClientIP(req);
    return clientIP;
  }
}));

// ===== Authentication Middleware =====

/**
 * Verify Firebase ID token
 */
async function verifyAuth(req, res, next) {
  try {
    console.log('Verifying auth for admin rating endpoint');
    const authHeader = req.headers.authorization;
    console.log('Auth header present:', !!authHeader);

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      console.log('No auth header or invalid format');
      return res.status(401).json({
        error: true,
        code: 'AUTH_REQUIRED',
        message: 'Authentication required'
      });
    }

    const token = authHeader.split('Bearer ')[1];
    console.log('Token extracted, verifying...');
    const decodedToken = await admin.auth().verifyIdToken(token);
    console.log('Token verified for user:', decodedToken.uid);

    req.user = {
      uid: decodedToken.uid,
      email: decodedToken.email
    };

    next();
  } catch (error) {
    console.error('Auth verification error:', error);
    return res.status(401).json({
      error: true,
      code: 'AUTH_INVALID',
      message: 'Invalid or expired token'
    });
  }
}

/**
 * Combined auth and ban check
 */
async function verifyAuthAndNotBanned(req, res, next) {
  await verifyAuth(req, res, (err) => {
    if (err || res.headersSent) return;
    checkBanned(req, res, next);
  });
}

/**
 * Verify admin or tester role
 */
async function verifyAdminOrTester(req, res, next) {
  try {
    console.log('Verifying admin/tester for user:', req.user.uid);

    // Check if this is the admin email
    const isAdminEmail = config.adminBypassEmail && req.user.email === config.adminBypassEmail;
    if (isAdminEmail) {
      console.log('User is admin by email');
      req.userProfile = { admin: true, tester: true };
      return next();
    }

    const userProfile = await db.ref(`users/${req.user.uid}`).once('value');
    const profile = userProfile.val();
    console.log('User profile:', profile);

    if (!profile || (!profile.admin && !profile.tester)) {
      console.log('User does not have admin or tester permissions');
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'Admin or tester access required'
      });
    }

    console.log('User has permissions - admin:', profile.admin, 'tester:', profile.tester);
    req.userProfile = profile;
    next();
  } catch (error) {
    console.error('Admin/tester verification error:', error);
    return res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Verification error'
    });
  }
}

/**
 * Verify admin role
 */
async function verifyAdmin(req, res, next) {
  try {
    // Check if this is the admin email
    const isAdminEmail = config.adminBypassEmail && req.user.email === config.adminBypassEmail;
    if (isAdminEmail) {
      console.log('User is admin by email');
      req.userProfile = { admin: true, tester: true };
      return next();
    }

    const userProfile = await db.ref(`users/${req.user.uid}`).once('value');
    const profile = userProfile.val();

    if (!profile || !profile.admin) {
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'Admin access required'
      });
    }

    req.userProfile = profile;
    next();
  } catch (error) {
    console.error('Admin verification error:', error);
    return res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error verifying admin status'
    });
  }
}

/**
 * Verify tester role
 */
async function verifyTester(req, res, next) {
  try {
    const userProfile = await db.ref(`users/${req.user.uid}`).once('value');
    const profile = userProfile.val();

    if (!profile || (!profile.tester && !profile.admin)) {
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'Tester access required'
      });
    }

    req.userProfile = profile;
    next();
  } catch (error) {
    console.error('Tier tester verification error:', error);
    return res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error verifying tier tester status'
    });
  }
}

/**
 * Generate a unique group ID for related accounts
 */
function generateAltGroupId(primaryAccount, suspiciousAccounts) {
  // Create a consistent group ID based on all account UIDs
  const allUids = [primaryAccount, ...suspiciousAccounts.map(acc => acc.uid)].sort();
  return allUids.join('_');
}

/**
 * Create or update consolidated alt report
 */
async function createConsolidatedAltReport(primaryAccount, suspiciousAccounts, clientIP, detectionReason, type) {
  try {
    const groupId = generateAltGroupId(primaryAccount, suspiciousAccounts);

    // Check if report already exists for this group
    const reportsRef = db.ref('altReports');
    const existingReportsSnapshot = await reportsRef.orderByChild('groupId').equalTo(groupId).once('value');
    const existingReports = existingReportsSnapshot.val();

    let reportRef;
    let reportData;

    if (existingReports) {
      // Update existing report
      const reportId = Object.keys(existingReports)[0];
      reportRef = db.ref(`altReports/${reportId}`);

      const existingReport = existingReports[reportId];
      reportData = {
        ...existingReport,
        suspiciousAccounts: [...new Set([...existingReport.suspiciousAccounts, ...suspiciousAccounts])],
        flagCount: (existingReport.flagCount || 1) + 1,
        lastFlaggedAt: new Date().toISOString(),
        lastDetectionReason: detectionReason,
        lastClientIP: clientIP,
        updatedAt: new Date().toISOString()
      };
    } else {
      // Create new consolidated report
      reportRef = db.ref('altReports').push();
      reportData = {
        groupId,
        primaryAccount,
        suspiciousAccounts,
        flagCount: 1,
        detectionReason,
        clientIP,
        reportedAt: new Date().toISOString(),
        status: 'reported',
        type,
        lastFlaggedAt: new Date().toISOString(),
        lastDetectionReason: detectionReason,
        lastClientIP: clientIP
      };
    }

    await reportRef.set(reportData);
    return { reportId: reportRef.key, isNew: !existingReports, flagCount: reportData.flagCount };

  } catch (error) {
    console.error('Error creating consolidated alt report:', error);
    return null;
  }
}

/**
 * Get client IP address
 */
function getClientIP(req) {
  // Check for forwarded headers (when behind proxy/load balancer)
  const forwarded = req.headers['x-forwarded-for'];
  if (forwarded) {
    return forwarded.split(',')[0].trim();
  }

  // Check for real IP header
  const realIP = req.headers['x-real-ip'];
  if (realIP) {
    return realIP;
  }

  // Fallback to connection remote address
  return req.connection.remoteAddress || req.socket.remoteAddress || req.ip || 'unknown';
}

/**
 * Find existing accounts that have used an exact IP address.
 */
async function findAccountsByExactIP(clientIP, excludeUid = null) {
  if (!clientIP || clientIP === 'unknown') return [];

  const usersSnapshot = await db.ref('users').once('value');
  const allUsers = usersSnapshot.val() || {};
  const matches = [];

  for (const [uid, userData] of Object.entries(allUsers)) {
    if (excludeUid && uid === excludeUid) continue;
    if (!userData || typeof userData !== 'object') continue;

    const knownIPs = new Set();
    const ipAddresses = userData.ipAddresses;

    if (Array.isArray(ipAddresses)) {
      for (const ip of ipAddresses) {
        if (typeof ip === 'string' && ip.trim()) knownIPs.add(ip.trim());
      }
    } else if (typeof ipAddresses === 'string' && ipAddresses.trim()) {
      knownIPs.add(ipAddresses.trim());
    }

    if (typeof userData.lastLoginIP === 'string' && userData.lastLoginIP.trim()) {
      knownIPs.add(userData.lastLoginIP.trim());
    }
    if (typeof userData.ipAddress === 'string' && userData.ipAddress.trim()) {
      knownIPs.add(userData.ipAddress.trim());
    }

    if (knownIPs.has(clientIP)) {
      matches.push({
        uid,
        email: userData.email || null,
        minecraftUsername: userData.minecraftUsername || null,
        reason: `Same IP address: ${clientIP}`,
        confidence: 'high'
      });
    }
  }

  return matches;
}

async function cleanupOldAltReports(maxAgeDays = 7) {
  const cutoffMs = Date.now() - (maxAgeDays * 24 * 60 * 60 * 1000);
  const reportsRef = db.ref('altReports');
  const snapshot = await reportsRef.once('value');
  const reports = snapshot.val() || {};
  const updates = {};
  let removed = 0;

  for (const [reportId, report] of Object.entries(reports)) {
    const reportTsMs = new Date(
      report?.lastFlaggedAt || report?.updatedAt || report?.reportedAt || 0
    ).getTime();

    if (!Number.isFinite(reportTsMs) || reportTsMs < cutoffMs) {
      updates[reportId] = null;
      removed++;
    }
  }

  if (removed > 0) {
    await reportsRef.update(updates);
    console.log(`[ALT] Cleanup complete: removed ${removed} alt reports older than ${maxAgeDays} day(s)`);
  }
}

/**
 * Check if IP address is from a VPN
 */
/**
 * Simple custom VPN detection based on datacenter IPs and known VPN patterns
 */
async function isVPNIP(clientIP) {
  try {
    // Skip private IPs
    if (clientIP === '127.0.0.1' || clientIP === '::1' || 
        clientIP.startsWith('192.168.') || clientIP.startsWith('10.') || 
        clientIP.startsWith('172.')) {
      return false;
    }

    // Known datacenter IP ranges (simple VPN detection)
    // These are common datacenter providers used by VPN services
    const dataCenterRanges = [
      { start: '1.0.0.0', end: '1.0.0.255' },
      { start: '42.112.0.0', end: '42.115.255.255' },
      { start: '45.32.0.0', end: '45.77.255.255' },
      { start: '45.76.0.0', end: '45.76.255.255' },
      { start: '45.33.0.0', end: '45.33.255.255' },
      { start: '104.200.0.0', end: '104.203.255.255' },
      { start: '108.61.0.0', end: '108.61.255.255' },
      { start: '108.170.0.0', end: '108.170.255.255' },
      { start: '149.28.0.0', end: '149.28.255.255' },
      { start: '169.254.0.0', end: '169.254.255.255' },
      { start: '172.96.0.0', end: '172.96.255.255' },
      { start: '185.112.0.0', end: '185.112.255.255' },
      { start: '192.241.0.0', end: '192.241.255.255' },
      { start: '199.247.0.0', end: '199.247.255.255' }
    ];

    // Convert IP to number for comparison
    const ipParts = clientIP.split('.');
    if (ipParts.length !== 4) return false;

    const ipNum = (parseInt(ipParts[0]) << 24) + 
                  (parseInt(ipParts[1]) << 16) + 
                  (parseInt(ipParts[2]) << 8) + 
                  parseInt(ipParts[3]);

    // Check against known VPN datacenter ranges
    for (const range of dataCenterRanges) {
      const startParts = range.start.split('.');
      const endParts = range.end.split('.');
      
      const startNum = (parseInt(startParts[0]) << 24) + 
                       (parseInt(startParts[1]) << 16) + 
                       (parseInt(startParts[2]) << 8) + 
                       parseInt(startParts[3]);
      
      const endNum = (parseInt(endParts[0]) << 24) + 
                     (parseInt(endParts[1]) << 16) + 
                     (parseInt(endParts[2]) << 8) + 
                     parseInt(endParts[3]);

      if (ipNum >= startNum && ipNum <= endNum) {
        return true; // Likely a VPN/datacenter
      }
    }

    return false;

  } catch (error) {
    console.error('Error checking VPN IP:', error);
    return false;
  }
}

/**
 * Check if user is whitelisted for a specific detection type
 */
function isWhitelistedForType(whitelist, uid, type) {
  if (!whitelist[uid]) return false;
  // Full whitelist (no types specified) covers all types
  if (!whitelist[uid].types) return true;
  // Check if specific type is whitelisted
  return whitelist[uid].types.includes(type);
}

/**
 * Alt account detection algorithm
 */
async function detectAltAccount(email, clientIP, minecraftUsername = null) {
  try {
    const usersRef = db.ref('users');
    const usersSnapshot = await usersRef.once('value');
    const allUsers = usersSnapshot.val() || {};

    const suspiciousAccounts = [];
    const whitelistRef = db.ref('altWhitelist');
    const whitelistSnapshot = await whitelistRef.once('value');
    const whitelist = whitelistSnapshot.val() || {};

    // Check if this account is whitelisted for all detections
    for (const [uid, userData] of Object.entries(allUsers)) {
      if ((userData.email === email || userData.firebaseUid === email) && !whitelist[uid].types) {
        return { isAlt: false, reason: 'Account is whitelisted' };
      }
    }

    // Check IP-based detection
    for (const [uid, userData] of Object.entries(allUsers)) {
      if (!userData.ipAddresses) continue;

      const userIPs = Array.isArray(userData.ipAddresses) ? userData.ipAddresses : [userData.ipAddresses];

      // Exact IP match (strong evidence)
      if (userIPs.includes(clientIP)) {
        if (userData.email !== email && uid !== email) {
          suspiciousAccounts.push({
            uid,
            email: userData.email,
            minecraftUsername: userData.minecraftUsername,
            reason: `Same IP address: ${clientIP}`,
            confidence: 'high'
          });
        }
      }

      // Check for IP ranges (subnet matching - medium evidence)
      const clientIPParts = clientIP.split('.');
      for (const userIP of userIPs) {
        const userIPParts = userIP.split('.');
        if (clientIPParts.length === 4 && userIPParts.length === 4) {
          // Match first 3 octets (same subnet)
          if (clientIPParts[0] === userIPParts[0] &&
              clientIPParts[1] === userIPParts[1] &&
              clientIPParts[2] === userIPParts[2] &&
              clientIPParts[3] !== userIPParts[3]) {
            if (userData.email !== email && uid !== email) {
              suspiciousAccounts.push({
                uid,
                email: userData.email,
                minecraftUsername: userData.minecraftUsername,
                reason: `Same subnet: ${clientIPParts[0]}.${clientIPParts[1]}.${clientIPParts[2]}.x`,
                confidence: 'medium'
              });
            }
          }
        }
      }
    }

    // VPN detection
    const isVPN = await isVPNIP(clientIP);
    if (isVPN) {
      // Check if user has whitelisted VPN detection
      let vpnWhitelisted = false;
      for (const [uid, userData] of Object.entries(allUsers)) {
        if ((userData.email === email || userData.firebaseUid === email)) {
          if (whitelist[uid] && (whitelist[uid].types?.includes('vpn') || whitelist[uid].whitelistedAt)) {
            vpnWhitelisted = true;
            break;
          }
        }
      }

      if (!vpnWhitelisted) {
        return {
          isAlt: true,
          reason: `VPN usage detected: ${clientIP}`,
          suspiciousAccounts: [],
          detectionType: 'vpn'
        };
      }
    }

    // Minecraft username patterns (if provided)
    if (minecraftUsername) {
      const usernamePatterns = [
        minecraftUsername.toLowerCase(),
        minecraftUsername.replace(/[^a-zA-Z0-9]/g, '').toLowerCase(),
        minecraftUsername.replace(/[0-9]/g, '').toLowerCase()
      ];

      for (const [uid, userData] of Object.entries(allUsers)) {
        if (!userData.minecraftUsername) continue;

        const existingUsername = userData.minecraftUsername.toLowerCase();
        const existingClean = userData.minecraftUsername.replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
        const existingNoNumbers = userData.minecraftUsername.replace(/[0-9]/g, '').toLowerCase();

        for (const pattern of usernamePatterns) {
          // Check for similar usernames
          if (existingUsername.includes(pattern) ||
              existingClean.includes(pattern) ||
              existingNoNumbers.includes(pattern) ||
              pattern.includes(existingClean) ||
              pattern.includes(existingNoNumbers)) {

            if (Math.abs(pattern.length - existingClean.length) <= 2) {
              if (userData.email !== email && uid !== email) {
                suspiciousAccounts.push({
                  uid,
                  email: userData.email,
                  minecraftUsername: userData.minecraftUsername,
                  reason: `Similar Minecraft username: ${userData.minecraftUsername}`,
                  confidence: 'medium'
                });
              }
            }
          }
        }
      }
    }

    // Remove duplicates and filter by confidence
    const uniqueSuspicious = suspiciousAccounts.filter((account, index, self) =>
      index === self.findIndex(a => a.uid === account.uid)
    );

    const highConfidence = uniqueSuspicious.filter(acc => acc.confidence === 'high');
    const totalSuspicious = uniqueSuspicious.length;

    // Decision logic - VERY AGGRESSIVE: Flag on ANY suspicion
    if (totalSuspicious > 0) {
      const confidenceLevels = uniqueSuspicious.map(acc => acc.confidence).join(', ');
      return {
        isAlt: true,
        reason: `Suspicious activity detected (${totalSuspicious} account(s), confidence: ${confidenceLevels}): ${uniqueSuspicious.map(acc => acc.reason).join('; ')}`,
        suspiciousAccounts: uniqueSuspicious
      };
    }

    // Additional aggressive checks - report on any account with similar email patterns
    const emailParts = email.toLowerCase().split('@');
    if (emailParts.length === 2) {
      const [localPart, domain] = emailParts;

      for (const [uid, userData] of Object.entries(allUsers)) {
        if (userData.email && userData.email !== email) {
          const existingEmail = userData.email.toLowerCase();
          // Check for similar local parts (e.g., john1 vs john2)
          if (existingEmail.includes(domain) &&
              (localPart.includes(existingEmail.split('@')[0].substring(0, 4)) ||
               existingEmail.split('@')[0].includes(localPart.substring(0, 4)))) {
            return {
              isAlt: true,
              reason: `Potential alt account - similar email pattern detected`,
              suspiciousAccounts: [{
                uid,
                email: userData.email,
                minecraftUsername: userData.minecraftUsername,
                reason: `Similar email pattern`,
                confidence: 'low'
              }]
            };
          }
        }
      }
    }

    return { isAlt: false, suspiciousAccounts: uniqueSuspicious };

  } catch (error) {
    console.error('Error in alt detection:', error);
    return { isAlt: false, error: error.message };
  }
}

/**
 * Check if user is banned
 */
async function checkBanned(req, res, next) {
  try {
    const userProfile = await db.ref(`users/${req.user.uid}`).once('value');
    const profile = userProfile.val();

    // Allow access to profile and onboarding endpoints for users with unverified accounts
    const isProfileOrOnboardingEndpoint = req.path.startsWith('/api/users/me') && req.method === 'GET';
    const isOnboardingStatusEndpoint = req.path === '/api/onboarding/status';

    // Check if user has an expired pending verification (treat as banned)
    if (profile && profile.minecraftUsername && profile.minecraftVerified === false) {
      // Check if there's an expired pending verification for this user
      const pendingVerificationsRef = db.ref('pendingVerifications');
      const pendingSnapshot = await pendingVerificationsRef.once('value');
      const pendingVerifications = pendingSnapshot.val() || {};

      let hasExpiredVerification = false;
      const now = Date.now();

      // Check if user has any expired pending verifications
      for (const [key, verification] of Object.entries(pendingVerifications)) {
        if (verification.userId === req.user.uid && verification.expiresAt < now) {
          hasExpiredVerification = true;
          break;
        }
      }

      // Only block if they have an expired pending verification AND it's not a profile/onboarding endpoint
      if (hasExpiredVerification && !isProfileOrOnboardingEndpoint && !isOnboardingStatusEndpoint) {
        return res.status(403).json({
          error: true,
          code: 'VERIFICATION_EXPIRED',
          message: 'Your Minecraft account verification has expired. Please restart the linking process.',
          banExpires: null,
          timeRemaining: null,
          isPermanent: false,
          isVerificationExpired: true
        });
      }
    }

    if (profile && profile.banned) {
      // Check if ban has expired
      if (profile.banExpires && profile.banExpires !== 'permanent') {
        const banExpires = new Date(profile.banExpires);
        const now = new Date();

        if (banExpires <= now) {
          // Ban has expired, remove ban fields
          console.log(`Auto-unbanning expired ban for user: ${req.user.uid}`);
          await db.ref(`users/${req.user.uid}`).update({
            banned: null,
            bannedAt: null,
            bannedBy: null,
            banExpires: null,
            banReason: null
          });
          next();
          return;
        }
      }

      // User is still banned - calculate time remaining
      let timeRemaining = null;
      if (profile.banExpires && profile.banExpires !== 'permanent') {
        const banExpires = new Date(profile.banExpires);
        const now = new Date();
        timeRemaining = Math.max(0, banExpires - now);
      }

      return res.status(403).json({
        error: true,
        code: 'ACCOUNT_BANNED',
        message: profile.banReason || 'Your account has been banned',
        banExpires: profile.banExpires,
        timeRemaining: timeRemaining,
        isPermanent: profile.banExpires === 'permanent'
      });
    }

    next();
  } catch (error) {
    console.error('Ban check error:', error);
    return res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error checking account status'
    });
  }
}

// ===== API Routes =====

// Health check
app.get('/api/health', (req, res) => {
  const mem = process.memoryUsage();
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    memory: {
      rssMB: Math.round(mem.rss / 1024 / 1024),
      heapUsedMB: Math.round(mem.heapUsed / 1024 / 1024),
      heapTotalMB: Math.round(mem.heapTotal / 1024 / 1024)
    },
    matchmaking: {
      running: matchmakingJobRunning,
      lastDurationMs: lastMatchmakingDurationMs
    }
  });
});

// ===== User Routes =====

/**
 * GET /api/users/me - Get current user profile
 */
app.get('/api/users/me', verifyAuthAndNotBanned, async (req, res) => {
  try {
    const userRef = db.ref(`users/${req.user.uid}`);
    const snapshot = await userRef.once('value');
    const profile = snapshot.val();
    
    if (!profile) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'User profile not found'
      });
    }
    
    res.json(profile);
  } catch (error) {
    console.error('Error fetching user profile:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching user profile'
    });
  }
});

/**
 * PUT /api/users/me - Update user profile
 */
app.put('/api/users/me', verifyAuthAndNotBanned, async (req, res) => {
  try {
    const updates = req.body;
    const userRef = db.ref(`users/${req.user.uid}`);
    
    // Get existing profile
    const snapshot = await userRef.once('value');
    const existing = snapshot.val() || {};
    
    // Merge updates
    const updated = {
      ...existing,
      ...updates,
      updatedAt: new Date().toISOString()
    };
    
    await userRef.set(updated);
    
    res.json(updated);
  } catch (error) {
    console.error('Error updating user profile:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error updating user profile'
    });
  }
});

/**
 * GET /api/users/me/recent-matches - Get recent matches for current user
 */
app.get('/api/users/me/recent-matches', verifyAuthAndNotBanned, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 5;

    // Get all matches where user is player or tester
    const matchesRef = db.ref('matches');
    const playerMatchesSnapshot = await matchesRef
      .orderByChild('playerId')
      .equalTo(req.user.uid)
      .once('value');

    const testerMatchesSnapshot = await matchesRef
      .orderByChild('testerId')
      .equalTo(req.user.uid)
      .once('value');

    const allMatches = [];

    // Add player matches
    if (playerMatchesSnapshot.exists()) {
      const playerMatches = playerMatchesSnapshot.val();
      Object.keys(playerMatches).forEach(matchId => {
        const match = playerMatches[matchId];
        if (match.status === 'ended' && match.finalized) {
          allMatches.push({
            ...match,
            matchId,
            userRole: 'player',
            opponentName: match.testerUsername,
            userScore: match.finalizationData?.playerScore || 0,
            opponentScore: match.finalizationData?.testerScore || 0
          });
        }
      });
    }

    // Add tester matches
    if (testerMatchesSnapshot.exists()) {
      const testerMatches = testerMatchesSnapshot.val();
      Object.keys(testerMatches).forEach(matchId => {
        const match = testerMatches[matchId];
        if (match.status === 'ended' && match.finalized) {
          allMatches.push({
            ...match,
            matchId,
            userRole: 'tester',
            opponentName: match.playerUsername,
            userScore: match.finalizationData?.testerScore || 0,
            opponentScore: match.finalizationData?.playerScore || 0
          });
        }
      });
    }

    // Sort by most recent first and limit
    allMatches.sort((a, b) => new Date(b.finalizedAt || b.createdAt) - new Date(a.finalizedAt || a.createdAt));
    const recentMatches = allMatches.slice(0, limit);

    res.json({ matches: recentMatches });
  } catch (error) {
    console.error('Error fetching recent matches:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching recent matches'
    });
  }
});

/**
 * POST /api/users/me/minecraft - Initiate Minecraft username linking (creates pending verification)
 */
app.post('/api/users/me/minecraft', verifyAuthAndNotBanned, async (req, res) => {
  try {
    const { username, region } = req.body;

    if (!username || typeof username !== 'string' || username.trim().length === 0) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Minecraft username is required'
      });
    }

    if (!region || typeof region !== 'string' || region.trim().length === 0) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Region selection is required'
      });
    }

    // Check if username is locked (unless user is admin)
    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val() || {};

    if (userProfile.usernameLocked && !userProfile.admin) {
      return res.status(403).json({
        error: true,
        code: 'USERNAME_LOCKED',
        message: 'Username linking is locked. Contact an administrator to unlock.'
      });
    }

    // Clean up any expired pending verifications for this user
    const pendingVerificationsRef = db.ref('pendingVerifications');
    const allPendingSnapshot = await pendingVerificationsRef.once('value');
    const allPending = allPendingSnapshot.val() || {};
    const now = Date.now();

    const expiredVerifications = [];
    for (const [key, verification] of Object.entries(allPending)) {
      if (verification.userId === req.user.uid && verification.expiresAt < now) {
        expiredVerifications.push(key);
      }
    }

    // Remove expired verifications
    if (expiredVerifications.length > 0) {
      console.log(`Cleaning up ${expiredVerifications.length} expired verifications for user ${req.user.uid}`);
      for (const key of expiredVerifications) {
        await pendingVerificationsRef.child(key).remove();
      }
    }

    // Check if username is already linked to another verified account
    const playersRef = db.ref('players');
    const normalizedUsername = username.trim().toLowerCase();
    const existingPlayersSnapshot = await playersRef.once('value');
    const existingPlayers = existingPlayersSnapshot.val() || {};

    for (const [key, player] of Object.entries(existingPlayers)) {
      if (player.username?.toLowerCase() === normalizedUsername && player.userId && player.userId !== req.user.uid) {
        return res.status(409).json({
          error: true,
          code: 'USERNAME_ALREADY_LINKED',
          message: 'This Minecraft username is already linked to another account. Each username can only be linked to one account.'
        });
      }
    }

    // Update user profile with pending verification
    await userRef.update({
      minecraftUsername: username.trim(),
      region: region.trim(),
      minecraftVerified: false,
      usernameLocked: true, // Lock username once linking is initiated
      updatedAt: new Date().toISOString()
    });

    // Generate 6-digit verification code
    const verificationCode = Math.floor(100000 + Math.random() * 900000).toString();

    // Create pending verification entry (reuse existing pendingVerificationsRef)
    const verificationData = {
      userId: req.user.uid,
      playerName: username.trim(),
      region: region.trim(),
      verificationCode: verificationCode,
      serverName: null, // Will be set when player runs /link command
      playerUUID: null, // Will be set when player runs /link command
      createdAt: Date.now(),
      expiresAt: Date.now() + (15 * 60 * 1000) // 15 minutes expiry
    };

    await pendingVerificationsRef.push(verificationData);

    res.json({
      success: true,
      message: 'Minecraft username linking initiated. Please join one of our servers and run /link with your verification code to complete verification.',
      verificationCode: verificationCode,
      instructions: `Join mc.sidastuff.com or spectorsmp.sidastuff.com and run the /link ${verificationCode} command to verify your account.`
    });
  } catch (error) {
    console.error('Error initiating Minecraft username linking:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error initiating Minecraft username linking'
    });
  }
});

// ===== News Routes =====

/**
 * GET /api/news - Get news posts
 */
app.get('/api/news', verifyAuthAndNotBanned, async (req, res) => {
  try {
    const { limit = 10, offset = 0 } = req.query;

    const newsRef = db.ref('news')
      .orderByChild('createdAt')
      .limitToLast(parseInt(limit) + parseInt(offset));

    const snapshot = await newsRef.once('value');
    const newsData = snapshot.val() || {};

    // Convert to array and sort by createdAt descending
    const newsArray = Object.entries(newsData)
      .map(([id, data]) => ({ id, ...data }))
      .sort((a, b) => b.createdAt - a.createdAt)
      .slice(parseInt(offset), parseInt(offset) + parseInt(limit));

    res.json({
      success: true,
      news: newsArray,
      hasMore: newsArray.length === parseInt(limit)
    });
  } catch (error) {
    console.error('Error fetching news:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching news'
    });
  }
});

/**
 * GET /api/news/:id - Get specific news post
 */
app.get('/api/news/:id', verifyAuthAndNotBanned, async (req, res) => {
  try {
    const { id } = req.params;

    const newsRef = db.ref(`news/${id}`);
    const snapshot = await newsRef.once('value');
    const newsData = snapshot.val();

    if (!newsData) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'News post not found'
      });
    }

    res.json({
      success: true,
      news: { id, ...newsData }
    });
  } catch (error) {
    console.error('Error fetching news post:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching news post'
    });
  }
});

/**
 * POST /api/news - Create news post (Admin only)
 */
app.post('/api/news', verifyAuthAndNotBanned, verifyAdmin, async (req, res) => {
  try {
    const { title, content, coverImage } = req.body;

    if (!title || !content) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Title and content are required'
      });
    }

    if (title.length > 200) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Title must be 200 characters or less'
      });
    }

    if (content.length > 10000) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Content must be 10,000 characters or less'
      });
    }

    let coverImageUrl = null;

    // Process cover image if provided
    if (coverImage) {
      try {
        coverImageUrl = await processNewsCoverImage(coverImage);
      } catch (imageError) {
        console.error('Error processing cover image:', imageError);
        // Continue without cover image rather than failing
      }
    }

    // Get admin user info
    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userData = userSnapshot.val() || {};

    // Create news post
    const newsRef = db.ref('news').push();
    const newsData = {
      title: title.trim(),
      content: content.trim(),
      coverImageUrl,
      createdAt: Date.now(),
      authorId: req.user.uid,
      authorName: userData.displayName || userData.email || 'Admin'
    };

    await newsRef.set(newsData);

    // Log admin action
    await logAdminAction(req, req.user.uid, 'CREATE_NEWS', newsRef.key, {
      title: newsData.title
    });

    res.json({
      success: true,
      message: 'News post created successfully',
      newsId: newsRef.key
    });
  } catch (error) {
    console.error('Error creating news post:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error creating news post'
    });
  }
});

/**
 * PUT /api/news/:id - Update news post (Admin only)
 */
app.put('/api/news/:id', verifyAuthAndNotBanned, verifyAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { title, content, coverImage } = req.body;

    if (!title || !content) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Title and content are required'
      });
    }

    // Check if news post exists
    const newsRef = db.ref(`news/${id}`);
    const snapshot = await newsRef.once('value');
    const existingNews = snapshot.val();

    if (!existingNews) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'News post not found'
      });
    }

    let coverImageUrl = existingNews.coverImageUrl;

    // Process new cover image if provided
    if (coverImage) {
      try {
        coverImageUrl = await processNewsCoverImage(coverImage);
      } catch (imageError) {
        console.error('Error processing cover image:', imageError);
        // Continue with existing image rather than failing
      }
    }

    // Update news post
    const updateData = {
      title: title.trim(),
      content: content.trim(),
      coverImageUrl,
      updatedAt: Date.now()
    };

    await newsRef.update(updateData);

    // Log admin action
    await logAdminAction(req, req.user.uid, 'UPDATE_NEWS', id, {
      title: updateData.title,
      oldTitle: existingNews.title
    });

    res.json({
      success: true,
      message: 'News post updated successfully'
    });
  } catch (error) {
    console.error('Error updating news post:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error updating news post'
    });
  }
});

/**
 * DELETE /api/news/:id - Delete news post (Admin only)
 */
app.delete('/api/news/:id', verifyAuthAndNotBanned, verifyAdmin, async (req, res) => {
  try {
    const { id } = req.params;

    // Check if news post exists
    const newsRef = db.ref(`news/${id}`);
    const snapshot = await newsRef.once('value');
    const existingNews = snapshot.val();

    if (!existingNews) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'News post not found'
      });
    }

    // Delete news post
    await newsRef.remove();

    // Log admin action
    await logAdminAction(req, req.user.uid, 'DELETE_NEWS', id, {
      title: existingNews.title
    });

    res.json({
      success: true,
      message: 'News post deleted successfully'
    });
  } catch (error) {
    console.error('Error deleting news post:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error deleting news post'
    });
  }
});

/**
 * Process and resize news cover image
 */
async function processNewsCoverImage(base64Image) {
  try {
    // Remove data URL prefix if present
    const imageData = base64Image.replace(/^data:image\/[a-z]+;base64,/, '');

    // Convert base64 to buffer
    const imageBuffer = Buffer.from(imageData, 'base64');

    // For now, return the base64 data URL (in production, you'd upload to cloud storage)
    // This maintains compatibility while allowing backend processing
    // In production, you would:
    // 1. Use sharp or similar library to resize/crop the image
    // 2. Upload to cloud storage (AWS S3, Cloudinary, etc.)
    // 3. Return the public URL

    return `data:image/jpeg;base64,${imageData}`;
  } catch (error) {
    console.error('Error processing news cover image:', error);
    throw new Error('Failed to process image');
  }
}

// ===== Player Routes =====

/**
 * GET /api/players - Get all players
 */
app.get('/api/players', async (req, res) => {
  try {
    const { gamemode } = req.query;
    const playersRef = db.ref('players');
    const snapshot = await playersRef.once('value');
    const players = snapshot.val() || {};
    
    // Get blacklist
    const blacklistRef = db.ref('blacklist');
    const blacklistSnapshot = await blacklistRef.once('value');
    const blacklist = blacklistSnapshot.val() || {};
    const blacklistedUsernames = new Set(
      Object.values(blacklist).map(entry => entry.username?.toLowerCase())
    );
    
    let playersArray = Object.keys(players).map(key => {
      const player = {
        id: key,
        ...players[key]
      };

      // Check if player is blacklisted
      const isBlacklisted = player.username && blacklistedUsernames.has(player.username.toLowerCase());
      if (isBlacklisted) {
        player.blacklisted = true;
        // Remove ratings for blacklisted players
        player.gamemodeRatings = {};
        player.overallRating = 0;
      }

      // Ensure all players have overallRating calculated
      if (player.overallRating === undefined || player.overallRating === null) {
        if (player.gamemodeRatings && Object.keys(player.gamemodeRatings).length > 0) {
          player.overallRating = calculateOverallRating(player.gamemodeRatings);
        } else if (isBlacklisted) {
          player.overallRating = 0;
        } else {
          player.overallRating = 1000; // Default for new players
        }
      }

      // Ensure gamemodeMatchCount exists
      if (!player.gamemodeMatchCount) {
        player.gamemodeMatchCount = {};
      }

      return player;
    });

    // Add verified role information to players
    // Only include roles that have been explicitly verified through badge reload
    for (const player of playersArray) {
      // Only set verifiedRoles if player record has roles from badge reload
      if (player.roles && (player.roles.admin === true || player.roles.tester === true)) {
        player.verifiedRoles = {
          admin: player.roles.admin === true,
          tester: player.roles.tester === true
        };
      } else {
        // No verified roles for this player
        player.verifiedRoles = { admin: false, tester: false };
      }
    }
    
    // Filter by gamemode if specified
    if (gamemode && gamemode !== 'overall') {
      playersArray = playersArray.filter(player =>
        player.gamemodeRatings && player.gamemodeRatings[gamemode]
      );
    }

    // Add achievement titles for each gamemode
    for (const player of playersArray) {
      player.achievementTitles = {};

      // Add title for overall rating
      const overallTitle = getAchievementTitle('overall', player.overallRating || 1000);
      player.achievementTitles.overall = overallTitle;

      // Add titles for each gamemode the player has ratings in
      if (player.gamemodeRatings) {
        for (const [gm, rating] of Object.entries(player.gamemodeRatings)) {
          player.achievementTitles[gm] = getAchievementTitle(gm, rating);
        }
      }
    }

    res.json({ players: playersArray });
  } catch (error) {
    console.error('Error fetching players:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching players'
    });
  }
});

/**
 * POST /api/players - Create a new player
 */
app.post('/api/players', verifyAuth, verifyTester, async (req, res) => {
  try {
    const { username, region } = req.body;

    if (!username || !username.trim()) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Username is required'
      });
    }

    const trimmedUsername = username.trim();

    // Check if player already exists
    const playersRef = db.ref('players');
    const existingPlayerSnapshot = await playersRef.orderByChild('username').equalTo(trimmedUsername).once('value');

    if (existingPlayerSnapshot.exists()) {
      return res.status(409).json({
        error: true,
        code: 'PLAYER_EXISTS',
        message: 'Player with this username already exists'
      });
    }

    // Create new player
    const newPlayerRef = playersRef.push();
    const newPlayer = {
      username: trimmedUsername,
      region: region || null,
      gamemodeRatings: {},
      gamemodePoints: {},
      totalPoints: 0,
      lastTested: {},
      createdAt: new Date().toISOString(),
      createdBy: req.user.uid
    };

    await newPlayerRef.set(newPlayer);

    res.status(201).json({
      player: {
        id: newPlayerRef.key,
        ...newPlayer
      }
    });
  } catch (error) {
    console.error('Error creating player:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error creating player'
    });
  }
});

/**
 * GET /api/players/:id - Get player by ID
 */
app.get('/api/players/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const playerRef = db.ref(`players/${id}`);
    const snapshot = await playerRef.once('value');
    const player = snapshot.val();

    if (!player) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Player not found'
      });
    }

    res.json({ ...player, id });
  } catch (error) {
    console.error('Error fetching player:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching player'
    });
  }
});

// ===== Alt Detection Routes =====

/**
 * POST /api/auth/check-ban - Check if an email is banned before login
 */
app.post('/api/auth/check-ban', async (req, res) => {
  try {
    const { email } = req.body;

    if (!email) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_EMAIL',
        message: 'Email is required'
      });
    }

    // Find user by email
    const usersRef = db.ref('users');
    const userSnapshot = await usersRef.orderByChild('email').equalTo(email.toLowerCase()).once('value');
    const users = userSnapshot.val();

    if (!users) {
      // Email not found, not banned
      return res.json({
        banned: false,
        message: 'Email not banned'
      });
    }

    const userId = Object.keys(users)[0];
    const userProfile = users[userId];

    if (!userProfile.banned) {
      return res.json({
        banned: false,
        message: 'Email not banned'
      });
    }

    // Check if ban has expired
    if (userProfile.banExpires && userProfile.banExpires !== 'permanent') {
      const banExpires = new Date(userProfile.banExpires);
      const now = new Date();

      if (banExpires <= now) {
        // Ban has expired, auto-unban
        console.log(`Auto-unbanning expired ban for email: ${email}`);
        await db.ref(`users/${userId}`).update({
          banned: null,
          bannedAt: null,
          bannedBy: null,
          banExpires: null,
          banReason: null
        });

        return res.json({
          banned: false,
          message: 'Ban expired, account unbanned'
        });
      }
    }

    // User is still banned - calculate time remaining
    let timeRemaining = null;
    let timeRemainingText = '';

    if (userProfile.banExpires && userProfile.banExpires !== 'permanent') {
      const banExpires = new Date(userProfile.banExpires);
      const now = new Date();
      timeRemaining = Math.max(0, banExpires - now);

      // Format time remaining
      const days = Math.floor(timeRemaining / (1000 * 60 * 60 * 24));
      const hours = Math.floor((timeRemaining % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
      const minutes = Math.floor((timeRemaining % (1000 * 60 * 60)) / (1000 * 60));

      if (days > 0) {
        timeRemainingText = `${days} day${days > 1 ? 's' : ''}, ${hours} hour${hours > 1 ? 's' : ''}`;
      } else if (hours > 0) {
        timeRemainingText = `${hours} hour${hours > 1 ? 's' : ''}, ${minutes} minute${minutes > 1 ? 's' : ''}`;
      } else {
        timeRemainingText = `${minutes} minute${minutes > 1 ? 's' : ''}`;
      }
    }

    return res.json({
      banned: true,
      reason: userProfile.banReason || 'Your account has been banned',
      banExpires: userProfile.banExpires,
      timeRemaining: timeRemaining,
      timeRemainingText: timeRemainingText,
      isPermanent: userProfile.banExpires === 'permanent'
    });

  } catch (error) {
    console.error('Error checking ban status:', error);
    return res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error checking ban status'
    });
  }
});

/**
 * POST /api/admin/audit-log - Log admin action (internal use)
 */
async function logAdminAction(action, targetUser, details, adminUid) {
  try {
    const auditLogRef = db.ref('adminAuditLog').push();
    await auditLogRef.set({
      id: auditLogRef.key,
      action: action,
      targetUser: targetUser,
      details: details,
      adminUid: adminUid,
      timestamp: new Date().toISOString(),
      ipAddress: getClientIP({ headers: {}, connection: {}, socket: {} }) // Best effort
    });
  } catch (error) {
    console.error('Failed to log admin action:', error);
    // Don't throw - audit logging failure shouldn't break main functionality
  }
}

/**
 * GET /api/admin/audit-log - Get audit logs (admin only)
 */
app.get('/api/admin/audit-log', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { action, adminUid, targetUser, limit = 100, offset = 0 } = req.query;

    let query = db.ref('adminAuditLog').orderByChild('timestamp').limitToLast(parseInt(limit));

    // Apply filters if provided
    if (action) {
      // For filtered queries, we need to fetch all and filter client-side
      // This is acceptable for admin-only endpoint with rate limiting
      const snapshot = await db.ref('adminAuditLog').once('value');
      let logs = Object.values(snapshot.val() || {});

      // Apply filters
      if (action) {
        logs = logs.filter(log => log.action === action);
      }
      if (adminUid) {
        logs = logs.filter(log => log.adminUid === adminUid);
      }
      if (targetUser) {
        logs = logs.filter(log => log.targetUser === targetUser);
      }

      // Sort by timestamp descending and apply pagination
      logs.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      logs = logs.slice(parseInt(offset), parseInt(offset) + parseInt(limit));

      return res.json({
        logs: logs,
        total: logs.length,
        hasMore: false // Since we're filtering client-side
      });
    }

    // Non-filtered query
    const snapshot = await query.once('value');
    const logs = Object.values(snapshot.val() || {});
    logs.reverse(); // Firebase returns ascending, we want descending

    res.json({
      logs: logs,
      total: logs.length,
      hasMore: logs.length === parseInt(limit)
    });

  } catch (error) {
    console.error('Error fetching audit logs:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching audit logs'
    });
  }
});

/**
 * POST /api/auth/acknowledge-warning - Acknowledge a warning
 */
app.post('/api/auth/acknowledge-warning', verifyAuth, async (req, res) => {
  try {
    const { warningId } = req.body;

    if (!warningId) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_WARNING_ID',
        message: 'warningId is required'
      });
    }

    // Get user profile
    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val();

    if (!userProfile || !userProfile.warnings) {
      return res.status(404).json({
        error: true,
        code: 'WARNING_NOT_FOUND',
        message: 'Warning not found'
      });
    }

    // Find and acknowledge the warning
    const warnings = userProfile.warnings.map(warning => {
      if (warning.id === warningId) {
        return { ...warning, acknowledged: true, acknowledgedAt: new Date().toISOString() };
      }
      return warning;
    });

    await userRef.update({
      warnings: warnings,
      updatedAt: new Date().toISOString()
    });

    res.json({
      success: true,
      message: 'Warning acknowledged'
    });

  } catch (error) {
    console.error('Error acknowledging warning:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error acknowledging warning'
    });
  }
});

/**
 * Log admin action to audit log
 */
async function logAdminAction(req, adminUid, action, targetUserId = null, details = {}) {
  try {
    const auditLogRef = db.ref('adminAuditLog');
    const logEntry = {
      id: Date.now().toString(),
      timestamp: new Date().toISOString(),
      adminUid: adminUid,
      action: action,
      targetUserId: targetUserId,
      details: details,
      ipAddress: getClientIP(req),
      userAgent: req.headers['user-agent'] || 'Unknown'
    };

    await auditLogRef.push(logEntry);
    console.log(`Admin audit log: ${adminUid} performed ${action}`);
  } catch (error) {
    console.error('Error logging admin action:', error);
  }
}

/**
 * POST /api/admin/warn - Warn a player (admin only)
 */
app.post('/api/admin/warn', adminLimiter, verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { userId, reason } = req.body;

    if (!userId || !reason) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_DATA',
        message: 'userId and reason are required'
      });
    }

    // Get user profile
    const userRef = db.ref(`users/${userId}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val();

    if (!userProfile) {
      return res.status(404).json({
        error: true,
        code: 'USER_NOT_FOUND',
        message: 'User not found'
      });
    }

    // Add warning to user's warnings array
    const warnings = userProfile.warnings || [];
    warnings.push({
      id: Date.now().toString(),
      reason: reason,
      warnedBy: req.user.uid,
      warnedAt: new Date().toISOString(),
      acknowledged: false
    });

    await userRef.update({
      warnings: warnings,
      updatedAt: new Date().toISOString()
    });

    // Log admin action
    await logAdminAction(req, req.user.uid, 'WARN_USER', userId, { reason });

    res.json({
      success: true,
      message: 'Warning issued successfully'
    });

  } catch (error) {
    console.error('Error issuing warning:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error issuing warning'
    });
  }
});

/**
 * GET /api/admin/audit-log - Get admin audit log (admin only)
 */
app.get('/api/admin/audit-log', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { limit = 50, offset = 0, action, adminUid, targetUserId, startDate, endDate } = req.query;

    let auditLogRef = db.ref('adminAuditLog').orderByChild('timestamp');

    // Apply date filters if provided
    if (startDate) {
      auditLogRef = auditLogRef.startAt(startDate);
    }
    if (endDate) {
      auditLogRef = auditLogRef.endAt(endDate);
    }

    const snapshot = await auditLogRef.once('value');
    let logs = [];

    snapshot.forEach(childSnapshot => {
      const log = { id: childSnapshot.key, ...childSnapshot.val() };
      logs.push(log);
    });

    // Sort by timestamp descending (newest first)
    logs.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

    // Apply additional filters
    if (action) {
      logs = logs.filter(log => log.action === action);
    }
    if (adminUid) {
      logs = logs.filter(log => log.adminUid === adminUid);
    }
    if (targetUserId) {
      logs = logs.filter(log => log.targetUserId === targetUserId);
    }

    // Apply pagination
    const startIndex = parseInt(offset);
    const endIndex = startIndex + parseInt(limit);
    const paginatedLogs = logs.slice(startIndex, endIndex);

    res.json({
      success: true,
      logs: paginatedLogs,
      total: logs.length,
      hasMore: endIndex < logs.length
    });

  } catch (error) {
    console.error('Error fetching audit log:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching audit log'
    });
  }
});

/**
 * POST /api/auth/register - Register new user with alt detection
 */
app.post('/api/auth/register', async (req, res) => {
  try {
    const { email, firebaseUid, minecraftUsername, clientIP } = req.body;
    // Use provided clientIP, fallback to header extraction if not provided
    const realClientIP = clientIP || getClientIP(req);

    if (!email || !firebaseUid) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_DATA',
        message: 'Email and Firebase UID are required'
      });
    }

    // Block signups that reuse an IP linked to another account.
    const exactIpMatches = await findAccountsByExactIP(realClientIP, firebaseUid);
    if (exactIpMatches.length > 0) {
      await createConsolidatedAltReport(
        firebaseUid,
        exactIpMatches,
        realClientIP,
        `Signup blocked: IP ${realClientIP} is already linked to ${exactIpMatches.length} existing account(s)`,
        'registration_ip_conflict'
      );

      return res.status(409).json({
        error: true,
        code: 'IP_ALREADY_LINKED',
        message: 'Signup blocked: this IP address is already linked to another account'
      });
    }

    // Check for alt accounts
    const altDetection = await detectAltAccount(email, realClientIP, minecraftUsername);

    if (altDetection.isAlt) {
      // Create consolidated alt report
      const reportResult = await createConsolidatedAltReport(
        firebaseUid,
        altDetection.suspiciousAccounts,
        realClientIP,
        altDetection.reason,
        'registration'
      );

      if (reportResult) {
        console.log(`Suspicious registration detected: ${altDetection.reason} (Group flagged ${reportResult.flagCount} times)`);
      }
      // Continue with registration instead of blocking
    }

    // Create user profile with IP tracking
    const userRef = db.ref(`users/${firebaseUid}`);
    const userProfile = {
      email,
      firebaseUid,
      ipAddresses: [realClientIP],
      createdAt: new Date().toISOString(),
      lastLoginAt: new Date().toISOString(),
      lastLoginIP: realClientIP
    };

    // Add Minecraft username if provided
    if (minecraftUsername) {
      userProfile.minecraftUsername = minecraftUsername;
    }

    await userRef.set(userProfile);

    res.json({
      success: true,
      message: 'User registered successfully',
      user: {
        uid: firebaseUid,
        email,
        minecraftUsername
      }
    });

  } catch (error) {
    console.error('Error registering user:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error registering user'
    });
  }
});

/**
 * GET /api/auth/verification-code - Get current user's verification code
 */
app.get('/api/auth/verification-code', verifyAuth, async (req, res) => {
  try {
    console.log('GET /api/auth/verification-code called for user:', req.user.uid);
    // Get current user's pending verifications
    const pendingVerificationsRef = db.ref('pendingVerifications');
    const pendingSnapshot = await pendingVerificationsRef.once('value');
    const pendingVerifications = pendingSnapshot.val() || {};

    // Find the most recent pending verification for this user
    let latestVerification = null;
    let latestTimestamp = 0;

    for (const [key, verification] of Object.entries(pendingVerifications)) {
      if (verification.userId === req.user.uid && verification.expiresAt > Date.now()) {
        if (verification.createdAt > latestTimestamp) {
          latestVerification = verification;
          latestTimestamp = verification.createdAt;
        }
      }
    }

    if (!latestVerification) {
      console.log('No active verification code found for user:', req.user.uid);
      return res.status(404).json({
        error: true,
        code: 'NO_VERIFICATION_CODE',
        message: 'No active verification code found'
      });
    }

    res.json({
      success: true,
      verificationCode: latestVerification.verificationCode,
      expiresAt: latestVerification.expiresAt,
      serverName: latestVerification.serverName
    });

  } catch (error) {
    console.error('Error retrieving verification code:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error retrieving verification code'
    });
  }
});

/**
 * POST /api/auth/cleanup-minecraft - Clean up incomplete Minecraft linking data
 */
app.post('/api/auth/cleanup-minecraft', verifyAuth, async (req, res) => {
  try {
    const userId = req.user.uid;
    console.log('POST /api/auth/cleanup-minecraft called for user:', userId);

    // Securely clean up all Minecraft-related data for this authenticated user only
    const userRef = db.ref(`users/${userId}`);
    await userRef.update({
      minecraftUsername: null,
      minecraftVerified: false,
      region: null,
      minecraftUUID: null,
      verifiedAt: null
    });

    // Clean up ALL pending verifications for this user (active and expired)
    const pendingVerificationsRef = db.ref('pendingVerifications');
    const pendingSnapshot = await pendingVerificationsRef.once('value');
    const pendingVerifications = pendingSnapshot.val() || {};

    const updates = {};
    let cleanupCount = 0;
    for (const [key, verification] of Object.entries(pendingVerifications)) {
      if (verification.userId === userId) {
        updates[key] = null; // Mark for deletion
        cleanupCount++;
      }
    }

    if (cleanupCount > 0) {
      await pendingVerificationsRef.update(updates);
      console.log(`Cleaned up ${cleanupCount} pending verifications for user: ${userId}`);
    }

    console.log(`Successfully cleaned up all Minecraft data for user: ${userId}`);
    res.json({
      success: true,
      message: 'Minecraft linking data cleaned up successfully',
      cleanedVerifications: cleanupCount
    });

  } catch (error) {
    console.error('Error cleaning up Minecraft data for user:', req.user.uid, error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error cleaning up Minecraft data'
    });
  }
});

/**
 * POST /api/auth/verify-minecraft - Verify Minecraft account from plugin
 */
app.post('/api/auth/verify-minecraft', async (req, res) => {
  try {
    // Check API key for plugin authentication
    const authHeader = req.headers.authorization;
    const expectedApiKey = PLUGIN_API_KEY;

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        error: true,
        code: 'INVALID_AUTH',
        message: 'Missing or invalid authorization header'
      });
    }

    const apiKey = authHeader.split('Bearer ')[1];
    if (apiKey !== expectedApiKey) {
      return res.status(401).json({
        error: true,
        code: 'INVALID_API_KEY',
        message: 'Invalid API key'
      });
    }

    const { playerUUID, playerName, serverName, verificationCode } = req.body;

    if (!playerUUID || !playerName || !serverName || !verificationCode) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_DATA',
        message: 'Player UUID, name, server name, and verification code are required'
      });
    }

    // Validate server name
    const allowedServers = ['mc.sidastuff.com', 'spectorsmp.sidastuff.com'];
    if (!allowedServers.includes(serverName)) {
      return res.status(400).json({
        error: true,
        code: 'INVALID_SERVER',
        message: 'Invalid server name'
      });
    }

    // Find pending verification request for this player
    const pendingVerificationsRef = db.ref('pendingVerifications');
    const pendingSnapshot = await pendingVerificationsRef.once('value');
    const pendingVerifications = pendingSnapshot.val() || {};

    let verificationKey = null;
    let userId = null;

    // Find matching pending verification
    // Note: playerUUID is not set initially, so we match on code, name, and server
    console.log(`Looking for verification: player=${playerName}, server=${serverName}, code=${verificationCode}`);
    for (const [key, verification] of Object.entries(pendingVerifications)) {
      if (verification.playerName === playerName &&
          verification.serverName === serverName &&
          verification.verificationCode === verificationCode &&
          verification.expiresAt > Date.now()) {
        verificationKey = key;
        userId = verification.userId;
        console.log(`Found matching verification for user ${userId}`);
        break;
      }
    }

    if (!verificationKey) {
      return res.status(404).json({
        error: true,
        code: 'NO_PENDING_VERIFICATION',
        message: 'No pending verification found for this player'
      });
    }

    // Update the pending verification with the player's UUID for future reference
    await pendingVerificationsRef.child(verificationKey).update({
      playerUUID: playerUUID
    });

    // Update user profile with verified status
    const userRef = db.ref(`users/${userId}`);
    await userRef.update({
      minecraftVerified: true,
      minecraftUUID: playerUUID,
      verifiedAt: new Date().toISOString()
    });

    // Remove the pending verification
    await pendingVerificationsRef.child(verificationKey).remove();

    // Now create the player record since verification is complete
    const playersRef = db.ref('players');
    const normalizedUsername = playerName.trim().toLowerCase();

    // Check if player already exists
    const existingPlayersSnapshot = await playersRef.once('value');
    const existingPlayers = existingPlayersSnapshot.val() || {};
    let playerKey = null;

    // Find existing player by normalized username
    for (const [key, player] of Object.entries(existingPlayers)) {
      if (player.username?.toLowerCase() === normalizedUsername) {
        playerKey = key;
        break;
      }
    }

    const playerData = {
      username: playerName.trim(),
      userId: userId,
      region: 'Unknown', // Will be updated when user sets region
      blacklisted: false,
      roles: {
        admin: false,
        tester: false
      },
      updatedAt: new Date().toISOString(),
      verifiedAt: new Date().toISOString()
    };

    if (playerKey) {
      // Update existing player
      await playersRef.child(playerKey).update(playerData);
    } else {
      // Create new player record
      await playersRef.push(playerData);
    }

    res.json({
      success: true,
      message: 'Minecraft account verified successfully'
    });

  } catch (error) {
    console.error('Error verifying Minecraft account:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error verifying Minecraft account'
    });
  }
});

/**
 * POST /api/auth/login - Track login with alt detection
 */
/**
 * Check if email is banned before login
 */
app.post('/api/auth/check-ban', async (req, res) => {
  try {
    const { email } = req.body;

    if (!email) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_EMAIL',
        message: 'Email is required'
      });
    }

    // Find user by email
    const usersRef = db.ref('users');
    const userSnapshot = await usersRef.orderByChild('email').equalTo(email).once('value');
    const users = userSnapshot.val();

    if (!users) {
      // Email not found - allow login (user doesn't exist yet)
      return res.json({ banned: false });
    }

    const userId = Object.keys(users)[0];
    const userProfile = users[userId];

    // Check if banned
    if (userProfile.banned) {
      // Check if ban has expired
      if (userProfile.banExpires && userProfile.banExpires !== 'permanent') {
        const banExpires = new Date(userProfile.banExpires);
        const now = new Date();

        if (banExpires <= now) {
          // Ban has expired, auto-unban
          await usersRef.child(userId).update({
            banned: null,
            bannedAt: null,
            bannedBy: null,
            banExpires: null,
            banReason: null
          });

          return res.json({ banned: false });
        }
      }

      // User is still banned
      return res.json({
        banned: true,
        reason: userProfile.banReason || 'Your account has been banned',
        expires: userProfile.banExpires,
        bannedAt: userProfile.bannedAt,
        permanent: userProfile.banExpires === 'permanent'
      });
    }

    // Check for warnings
    const warnings = userProfile.warnings || [];
    const activeWarnings = warnings.filter(w => !w.acknowledged);

    return res.json({
      banned: false,
      warnings: activeWarnings
    });

  } catch (error) {
    console.error('Error checking ban status:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error checking ban status'
    });
  }
});

app.post('/api/auth/login', verifyAuth, checkBanned, async (req, res) => {
  try {
    const { clientIP } = req.body;
    // Use provided clientIP, fallback to header extraction if not provided
    const realClientIP = clientIP || getClientIP(req);
    const userRef = db.ref(`users/${req.user.uid}`);

    // Get current user profile
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val();

    if (!userProfile) {
      return res.status(404).json({
        error: true,
        code: 'USER_NOT_FOUND',
        message: 'User profile not found'
      });
    }

    // Check for alt accounts on login
    const altDetection = await detectAltAccount(req.user.email, realClientIP, userProfile.minecraftUsername);

    if (altDetection.isAlt) {
      // Create consolidated alt report
      const reportResult = await createConsolidatedAltReport(
        req.user.uid,
        altDetection.suspiciousAccounts,
        realClientIP,
        altDetection.reason,
        'login'
      );

      // Still allow login but log the suspicious activity
      if (reportResult) {
        console.log(`Suspicious login detected: ${altDetection.reason} (Group flagged ${reportResult.flagCount} times)`);
      }
    }

    // Update IP tracking
    const currentIPs = Array.isArray(userProfile.ipAddresses) ? userProfile.ipAddresses : [];
    if (!currentIPs.includes(realClientIP)) {
      currentIPs.push(realClientIP);
      // Keep only last 10 IPs
      if (currentIPs.length > 10) {
        currentIPs.shift();
      }
    }

    await userRef.update({
      ipAddresses: currentIPs,
      lastLoginAt: new Date().toISOString(),
      lastLoginIP: realClientIP
    });

    res.json({
      success: true,
      message: 'Login tracked successfully'
    });

  } catch (error) {
    console.error('Error tracking login:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error tracking login'
    });
  }
});

// ===== Queue Routes =====

/**
 * POST /api/queue/join - Join queue
 */
app.post('/api/queue/join', verifyAuthAndNotBanned, async (req, res) => {
  // Extract and sanitize inputs for error logging
  const { gamemode, region, serverIP } = req.body;
  const sanitizedGamemode = gamemode?.toString().trim();
  const sanitizedRegion = region?.toString().trim();
  const sanitizedServerIP = serverIP?.toString().trim();

  try {

    if (!sanitizedGamemode || !sanitizedRegion || !sanitizedServerIP) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'gamemode, region, and serverIP are required'
      });
    }

    // Validate gamemode exists
    const validGamemode = CONFIG.GAMEMODES.find(g => g.id === sanitizedGamemode);
    console.log('Validating gamemode:', sanitizedGamemode, 'found:', !!validGamemode);
    if (!validGamemode) {
      console.log('Available gamemodes:', CONFIG.GAMEMODES.map(g => g.id));
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: `Invalid gamemode: ${sanitizedGamemode}`
      });
    }
    
    // Get user profile
    console.log('Getting user profile for:', req.user.uid);
    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val();
    console.log('User profile exists:', !!userProfile, 'has minecraft username:', !!userProfile?.minecraftUsername);

    if (!userProfile || !userProfile.minecraftUsername) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Minecraft username must be linked first'
      });
    }

    // Check if testers are available for this specific gamemode and region
    const testerAvailabilityRef = db.ref('testerAvailability');
    const testerAvailabilitySnapshot = await testerAvailabilityRef.once('value');
    const testerAvailability = testerAvailabilitySnapshot.val() || {};

    // Get all players for region info
    const playersRef = db.ref('players');
    const playersSnapshot = await playersRef.once('value');
    const allPlayers = playersSnapshot.val() || {};

    // Check for available testers in the same gamemode and region
    let availableTestersCount = 0;
    for (const [userId, availability] of Object.entries(testerAvailability)) {
      if (!availability.available) continue;

      // Check if availability hasn't expired
      if (availability.timeoutAt && new Date(availability.timeoutAt) < new Date()) {
        continue;
      }

      // Check if gamemode matches
      if (availability.gamemode !== sanitizedGamemode) continue;

      // Get tester's region from player data
      const testerPlayer = Object.values(allPlayers).find(p => p.userId === userId);
      if (!testerPlayer || testerPlayer.region !== sanitizedRegion) continue;

      // Check if tester is not in an active match
      const activeMatchesRef = db.ref('matches');
      const activeMatchesSnapshot = await activeMatchesRef
        .orderByChild('status')
        .equalTo('active')
        .once('value');
      const activeMatches = activeMatchesSnapshot.val() || {};
      const isInActiveMatch = Object.values(activeMatches).some(match =>
        match.testerId === userId
      );
      if (isInActiveMatch) continue;

      availableTestersCount++;
    }

    if (availableTestersCount === 0) {
      return res.status(403).json({
        error: true,
        code: 'NO_TESTERS_AVAILABLE',
        message: `No tier testers are currently available for ${sanitizedGamemode} in ${sanitizedRegion}. Please try again later.`
      });
    }

    // Check if user has skill level for this gamemode, if not, set a default
    console.log('Checking skill level for gamemode:', sanitizedGamemode, 'ratings:', userProfile.gamemodeRatings);
    if (!userProfile.gamemodeRatings?.[sanitizedGamemode]) {
      console.log('Skill level missing for gamemode:', sanitizedGamemode, 'setting default rating of 1000');

      // Set default rating for this gamemode
      const gamemodeRatings = { ...(userProfile.gamemodeRatings || {}) };
      gamemodeRatings[sanitizedGamemode] = 1000; // Default skill level

      // Calculate new overall rating
      const overallRating = Object.keys(gamemodeRatings).length > 0
        ? Math.round(Object.values(gamemodeRatings).reduce((sum, rating) => sum + rating, 0) / Object.keys(gamemodeRatings).length)
        : 1000;

      // Update user profile with default rating
      await userRef.update({
        gamemodeRatings: gamemodeRatings,
        overallRating: overallRating,
        updatedAt: new Date().toISOString()
      });

      // Update userProfile for subsequent checks
      userProfile.gamemodeRatings = gamemodeRatings;
      userProfile.overallRating = overallRating;

      // Also update the player record with the default rating
      const playersRef = db.ref('players');
      const playerSnapshot = await playersRef.orderByChild('username').equalTo(userProfile.minecraftUsername).once('value');

      if (playerSnapshot.exists()) {
        const players = playerSnapshot.val();
        const playerId = Object.keys(players)[0];
        const playerRef = playersRef.child(playerId);

        const playerUpdates = {
          [`gamemodeRatings/${sanitizedGamemode}`]: 1000,
          updatedAt: new Date().toISOString()
        };

        // Recalculate overall rating for player
        const existingRatings = players[playerId].gamemodeRatings || {};
        const updatedRatings = { ...existingRatings, [sanitizedGamemode]: 1000 };
        playerUpdates.overallRating = Object.keys(updatedRatings).length > 0
          ? Math.round(Object.values(updatedRatings).reduce((sum, rating) => sum + rating, 0) / Object.keys(updatedRatings).length)
          : 1000;

        await playerRef.update(playerUpdates);
      }
    }
    
    // Check if user is blacklisted
    const blacklistRef = db.ref('blacklist');
    const blacklistSnapshot = await blacklistRef.once('value');
    const blacklist = blacklistSnapshot.val() || {};
    const isBlacklisted = Object.values(blacklist).some(entry => 
      entry.username?.toLowerCase() === userProfile.minecraftUsername.toLowerCase()
    );
    
    if (isBlacklisted) {
      return res.status(403).json({
        error: true,
        code: 'BLACKLISTED',
        message: 'You are blacklisted and cannot join the queue'
      });
    }
    
    // Check if already in queue
    const queueRef = db.ref('queue');
    const queueSnapshot = await queueRef.orderByChild('userId').equalTo(req.user.uid).once('value');

    if (queueSnapshot.exists()) {
      return res.status(400).json({
        error: true,
        code: 'ALREADY_IN_QUEUE',
        message: 'You are already in the queue'
      });
    }

    // Check if user has cooldown for this gamemode
    const lastTested = userProfile.lastTested || {};
    if (lastTested[sanitizedGamemode]) {
      const lastTestedTime = new Date(lastTested[sanitizedGamemode]);
      const elapsed = new Date() - lastTestedTime;
      const cooldownMs = 60 * 60 * 1000; // 1 hour

      if (elapsed < cooldownMs) {
        const remainingMinutes = Math.ceil((cooldownMs - elapsed) / (60 * 1000));
        return res.status(400).json({
          error: true,
          code: 'COOLDOWN_ACTIVE',
          message: `You must wait ${remainingMinutes} minutes before queuing for ${sanitizedGamemode.toUpperCase()} again.`
        });
      }
    }
    
    // Create queue entry
    const newQueueRef = queueRef.push();
    const queueEntry = {
      queueId: newQueueRef.key,
      userId: req.user.uid,
      minecraftUsername: userProfile.minecraftUsername,
      gamemode: sanitizedGamemode,
      region: sanitizedRegion,
      serverIP: sanitizedServerIP,
      status: 'waiting',
      joinedAt: new Date().toISOString()
    };
    
    await newQueueRef.set(queueEntry);

    // Attempt immediate matchmaking with available testers
    const matchResult = await attemptImmediateMatchmaking(queueEntry, userProfile);

    if (matchResult) {
      // Match found and created
      res.json({
        success: true,
        queueId: newQueueRef.key,
        matched: true,
        matchId: matchResult.matchId,
        testerUsername: matchResult.testerUsername,
        message: 'Match found! Redirecting to testing page...'
      });
    } else {
      // No match found, player remains in queue
      res.json({
        success: true,
        queueId: newQueueRef.key,
        matched: false,
        message: 'Added to queue. Waiting for available tester...'
      });
    }
  } catch (error) {
    console.error('Error joining queue:', error);
    console.error('Error details:', {
      message: error.message,
      stack: error.stack,
      userId: req.user?.uid,
      gamemode: sanitizedGamemode,
      region: sanitizedRegion,
      serverIP: sanitizedServerIP
    });
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error joining queue',
      details: error.message
    });
  }
});

/**
 * GET /api/queue/stats - Get queue statistics
 */
app.get('/api/queue/stats', verifyAuthAndNotBanned, async (req, res) => {
  try {
    // Get all queue entries
    const queueRef = db.ref('queue');
    const queueSnapshot = await queueRef.once('value');
    const queueEntries = queueSnapshot.val() || {};

    // Get all tester availabilities
    const availabilityRef = db.ref('testerAvailability');
    const availabilitySnapshot = await availabilityRef.once('value');
    const testerAvailabilities = availabilitySnapshot.val() || {};

    // Get all players for region info
    const playersRef = db.ref('players');
    const playersSnapshot = await playersRef.once('value');
    const allPlayers = playersSnapshot.val() || {};

    // Get active matches to exclude busy testers
    const activeMatchesRef = db.ref('matches');
    const activeMatchesSnapshot = await activeMatchesRef
      .orderByChild('status')
      .equalTo('active')
      .once('value');
    const activeMatches = activeMatchesSnapshot.val() || {};

    // Count players queued by gamemode and region
    const playersQueued = {};
    Object.values(queueEntries).forEach(entry => {
      const gamemode = entry.gamemode;
      const region = entry.region;

      if (!playersQueued[gamemode]) {
        playersQueued[gamemode] = {};
      }
      if (!playersQueued[gamemode][region]) {
        playersQueued[gamemode][region] = 0;
      }
      playersQueued[gamemode][region]++;
    });

    // Count available testers by gamemode and region
    const testersAvailable = {};
    for (const [userId, availability] of Object.entries(testerAvailabilities)) {
      if (!availability.available) continue;

      // Check if availability hasn't expired
      if (availability.timeoutAt && new Date(availability.timeoutAt) < new Date()) {
        continue;
      }

      // Check if tester is not in an active match
      const isInActiveMatch = Object.values(activeMatches).some(match =>
        match.testerId === userId
      );
      if (isInActiveMatch) continue;

      // Get tester's region from player data
      const testerPlayer = Object.values(allPlayers).find(p => p.userId === userId);
      if (!testerPlayer) continue;

      const gamemode = availability.gamemode;
      const region = testerPlayer.region;

      if (!testersAvailable[gamemode]) {
        testersAvailable[gamemode] = {};
      }
      if (!testersAvailable[gamemode][region]) {
        testersAvailable[gamemode][region] = 0;
      }
      testersAvailable[gamemode][region]++;
    }

    res.json({
      success: true,
      playersQueued,
      testersAvailable,
      totalQueued: Object.values(queueEntries).length,
      totalAvailableTesters: Object.keys(testerAvailabilities).length
    });
  } catch (error) {
    console.error('Error getting queue stats:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error getting queue statistics'
    });
  }
});

/**
 * GET /api/user/cooldowns - Get user's active cooldowns
 */
app.get('/api/user/cooldowns', verifyAuthAndNotBanned, async (req, res) => {
  try {
    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userData = userSnapshot.val() || {};

    const lastTested = userData.lastTested || {};
    const cooldowns = [];
    const now = new Date();

    // Check each gamemode for active cooldowns
    const gamemodes = ['vanilla', 'uhc', 'pot', 'nethop', 'smp', 'sword', 'axe', 'mace'];

    gamemodes.forEach(gamemode => {
      if (lastTested[gamemode]) {
        const lastTestedTime = new Date(lastTested[gamemode]);
        const elapsed = now - lastTestedTime;
        const cooldownMs = 60 * 60 * 1000; // 1 hour
        const remainingMs = cooldownMs - elapsed;

        if (remainingMs > 0) {
          cooldowns.push({
            gamemode,
            lastTested: lastTested[gamemode],
            remainingMs
          });
        }
      }
    });

    res.json({
      success: true,
      cooldowns
    });
  } catch (error) {
    console.error('Error getting user cooldowns:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error retrieving cooldowns'
    });
  }
});

/**
 * POST /api/queue/leave - Leave queue
 */
app.post('/api/queue/leave', verifyAuthAndNotBanned, async (req, res) => {
  try {
    const queueRef = db.ref('queue');
    const queueSnapshot = await queueRef.orderByChild('userId').equalTo(req.user.uid).once('value');
    
    if (!queueSnapshot.exists()) {
      return res.status(404).json({
        error: true,
        code: 'NOT_IN_QUEUE',
        message: 'You are not in the queue'
      });
    }
    
    // Remove all queue entries for this user
    const updates = {};
    queueSnapshot.forEach(child => {
      updates[child.key] = null;
    });
    
    await queueRef.update(updates);
    
    res.json({ success: true, message: 'Left queue successfully' });
  } catch (error) {
    console.error('Error leaving queue:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error leaving queue'
    });
  }
});

/**
 * GET /api/queue/status - Get queue status for current user
 */
app.get('/api/queue/status', verifyAuthAndNotBanned, async (req, res) => {
  try {
    const queueRef = db.ref('queue');
    const queueSnapshot = await queueRef.orderByChild('userId').equalTo(req.user.uid).once('value');
    
    if (!queueSnapshot.exists()) {
      return res.json({ inQueue: false });
    }
    
    const queueEntry = queueSnapshot.val();
    const entryKey = Object.keys(queueEntry)[0];
    const entry = queueEntry[entryKey];
    
    res.json({ inQueue: true, queueEntry: entry });
  } catch (error) {
    console.error('Error getting queue status:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error getting queue status'
    });
  }
});

// Start matchmaking interval
let matchmakingJobRunning = false;
let lastMatchmakingDurationMs = 0;
const scheduledMatchTimers = {
  inactivity: new Map(),
  playerJoin: new Map(),
  matchStart: new Map()
};

function scheduleMatchTimer(timerMap, matchId, delayMs, callback) {
  if (timerMap.has(matchId)) {
    return false;
  }

  const timeoutId = setTimeout(async () => {
    timerMap.delete(matchId);
    try {
      await callback();
    } catch (error) {
      console.error('Timer execution error for match', matchId, ':', error);
    }
  }, delayMs);

  timerMap.set(matchId, timeoutId);
  return true;
}

function clearAllMatchTimers(matchId) {
  for (const timerMap of Object.values(scheduledMatchTimers)) {
    const existing = timerMap.get(matchId);
    if (existing) {
      clearTimeout(existing);
      timerMap.delete(matchId);
    }
  }
}

setInterval(async () => {
  if (matchmakingJobRunning) {
    return;
  }
  matchmakingJobRunning = true;
  const startedAt = Date.now();
  try {
    await attemptMatchmaking();
  } catch (error) {
    console.error('Matchmaking error:', error);
  } finally {
    lastMatchmakingDurationMs = Date.now() - startedAt;
    if (lastMatchmakingDurationMs > 8000) {
      console.warn(`[PERF] attemptMatchmaking took ${lastMatchmakingDurationMs}ms`);
    }
    matchmakingJobRunning = false;
  }
}, 10000); // Run every 10 seconds

cleanupOldAltReports(7).catch((error) => {
  console.error('Error running startup alt report cleanup:', error);
});

setInterval(async () => {
  try {
    await cleanupOldAltReports(7);
  } catch (error) {
    console.error('Error in periodic alt report cleanup:', error);
  }
}, 6 * 60 * 60 * 1000); // Every 6 hours

/**
 * Check if player should be matched with evaluation tester
 */
/**
 * Calculate Elo-based match suitability (lower score = better match)
 */
function calculateMatchScore(playerRating, testerRating) {
  const ratingDiff = Math.abs(playerRating - testerRating);

  // Expanded matchmaking range for broader compatibility
  // Optimal difference is around 200-600 Elo points
  if (ratingDiff < 25) return ratingDiff + 1500; // Too close, higher penalty
  if (ratingDiff > 1200) return ratingDiff + 300; // Too far, lower penalty

  return ratingDiff; // Broader optimal range, no penalty
}

/**
 * Get player Elo rating for a gamemode
 */
async function getPlayerRating(userId, gamemode) {
  try {
    // First check if user has a profile rating
    const userRef = db.ref(`users/${userId}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val();

    if (userProfile?.gamemodeRatings?.[gamemode]) {
      return userProfile.gamemodeRatings[gamemode];
    }

    // Fallback to player record
    const playersRef = db.ref('players');
    const playersSnapshot = await playersRef.orderByChild('userId').equalTo(userId).once('value');
    const players = playersSnapshot.val() || {};
    const playerData = Object.values(players).find(p => p.userId === userId);

    return playerData?.gamemodeRatings?.[gamemode] || 1000; // Default to 1000 if no rating
  } catch (error) {
    console.error('Error getting player rating:', error);
    return 1000; // Default rating
  }
}

async function getPlayerGlicko2Data(userId, gamemode) {
  try {
    // First check if user has Glicko-2 data
    const userRef = db.ref(`users/${userId}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val();

    if (userProfile?.glicko2Params?.[gamemode]) {
      return userProfile.glicko2Params[gamemode];
    }

    // Fallback to player record
    const playersRef = db.ref('players');
    const playersSnapshot = await playersRef.orderByChild('userId').equalTo(userId).once('value');
    const players = playersSnapshot.val() || {};
    const playerData = Object.values(players).find(p => p.userId === userId);

    return playerData?.glicko2Params?.[gamemode] || {
      rd: GLICKO2_DEFAULT_RD,
      volatility: GLICKO2_DEFAULT_VOLATILITY
    };
  } catch (error) {
    console.error('Error getting player Glicko-2 data:', error);
    return {
      rd: GLICKO2_DEFAULT_RD,
      volatility: GLICKO2_DEFAULT_VOLATILITY
    };
  }
}

/**
 * Matchmaking algorithm
 */
async function attemptMatchmaking() {
  const gamemodes = ['vanilla', 'uhc', 'pot', 'nethop', 'smp', 'sword', 'axe', 'mace'];
  
  for (const gamemode of gamemodes) {
    try {
      await attemptMatchmakingForGamemode(gamemode);
    } catch (error) {
      console.error(`Matchmaking error for ${gamemode}:`, error);
    }
  }
}

/**
 * Attempt matchmaking for a specific gamemode
 */
async function attemptMatchmakingForGamemode(gamemode) {
  const queueRef = db.ref('queue');
  const queueSnapshot = await queueRef.orderByChild('gamemode').equalTo(gamemode).once('value');

  if (!queueSnapshot.exists()) {
    return; // No one in queue for this gamemode
  }

  const queueEntries = queueSnapshot.val();
  const entries = Object.keys(queueEntries).map(key => ({
    key,
    ...queueEntries[key]
  }));

  if (entries.length < 2) {
    return; // Need at least 2 people
  }

  // Batch fetch all user profiles at once
  const userIds = [...new Set(entries.map(e => e.userId))];
  const userProfiles = {};
  
  // Fetch all user profiles efficiently
  const usersRef = db.ref('users');
  for (const userId of userIds) {
    const snapshot = await usersRef.child(userId).once('value');
    if (snapshot.exists()) {
      userProfiles[userId] = snapshot.val();
    }
  }

  // Separate players and testers
  const players = [];
  const testers = [];

  for (const entry of entries) {
    const userProfile = userProfiles[entry.userId];
    if (userProfile && userProfile.tester) { // Changed from tierTester to tester
      testers.push(entry);
    } else {
      players.push(entry);
    }
  }

  if (players.length === 0 || testers.length === 0) {
    return; // Need both players and testers
  }

  // Get all player ratings in a single batch
  const playersRef = db.ref('players');
  const allPlayersSnapshot = await playersRef.once('value');
  const allPlayers = allPlayersSnapshot.val() || {};
  
  // Create a rating cache: userId -> rating
  const ratingCache = {};
  for (const playerId in allPlayers) {
    const playerData = allPlayers[playerId];
    if (playerData.userId) {
      ratingCache[playerData.userId] = playerData.gamemodeRatings?.[gamemode] || 1000;
    }
  }

  // Try to find the best match
  const now = new Date();
  let bestMatch = null;
  let bestScore = Infinity;

  for (const player of players) {
    const playerRating = ratingCache[player.userId] || 1000;

    for (const tester of testers) {
      // Skip if same person
      if (player.userId === tester.userId) {
        continue;
      }

      const testerRating = ratingCache[tester.userId] || 1000;
      const matchScore = calculateMatchScore(playerRating, testerRating);

      // Check if both waited 5+ seconds
      const playerWaitTime = now - new Date(player.joinedAt);
      const testerWaitTime = now - new Date(tester.joinedAt);

      if (playerWaitTime < 5000 || testerWaitTime < 5000) {
        continue;
      }

      // Check overlap (both in queue together for 5+ seconds)
      const overlapStart = new Date(Math.max(
        new Date(player.joinedAt).getTime(),
        new Date(tester.joinedAt).getTime()
      ));
      const overlapTime = now - overlapStart;

      if (overlapTime >= 5000 && matchScore < bestScore) {
        bestMatch = { player, tester };
        bestScore = matchScore;
      }
    }
  }

  // Create the best match found
  if (bestMatch) {
    await createMatch(bestMatch.player, bestMatch.tester);
  }
}

/**
 * Create a match between player and tester
 */
async function createMatch(player, tester, matchType = 'regular') {
  try {
    // Batch fetch user profiles and player data in parallel
    const [playerUserSnapshot, testerUserSnapshot, playersSnapshot] = await Promise.all([
      db.ref(`users/${player.userId}`).once('value'),
      db.ref(`users/${tester.userId}`).once('value'),
      db.ref('players').once('value')
    ]);
    
    const playerUser = playerUserSnapshot.val();
    const testerUser = testerUserSnapshot.val();
    const allPlayers = playersSnapshot.val() || {};

    // Find player data by userId
    const playerData = Object.values(allPlayers).find(p => p.userId === player.userId);
    const playerCurrentRating = playerData?.gamemodeRatings?.[player.gamemode] || 1000;

    // Create match
    const matchesRef = db.ref('matches');
    const newMatchRef = matchesRef.push();
    const matchId = newMatchRef.key;

    const match = {
      matchId,
      playerId: player.userId,
      playerUsername: player.minecraftUsername,
      playerEmail: playerUser.email,
      testerId: tester.userId, // Changed from tierTesterId
      testerUsername: tester.minecraftUsername, // Changed from tierTesterUsername
      testerEmail: testerUser.email, // Changed from tierTesterEmail
      gamemode: player.gamemode,
      region: player.region,
      serverIP: player.serverIP,
      status: 'active',
      matchType: 'regular', // All matches are now regular Elo matches
      playerCurrentRating, // Changed from playerCurrentTier
      createdAt: new Date().toISOString(),
      finalized: false,
      matchStarted: false,
      matchStartedAt: null,
      countdownStartedAt: null,
      firstTo: CONFIG.FIRST_TO[player.gamemode] || 3,
      chat: {},
      participants: {},
      presence: {},
      pagestats: {
        playerJoined: false,
        testerJoined: false,
        lastUpdate: null
      }
    };
    
    await newMatchRef.set(match);
    
    // Remove from queue and tester availability in parallel
    const queueRef = db.ref('queue');
    await Promise.all([
      queueRef.child(player.key).remove(),
      queueRef.child(tester.key).remove(),
      db.ref(`testerAvailability/${tester.userId}`).remove()
    ]);

    // Set up 3-minute inactivity timer (deduped per match)
    const INACTIVITY_TIMEOUT_MS = 3 * 60 * 1000;
    scheduleMatchTimer(scheduledMatchTimers.inactivity, matchId, INACTIVITY_TIMEOUT_MS, async () => {
      console.log(`Checking inactivity for match ${matchId} after 3 minutes...`);
      await handleMatchInactivity(matchId);
    });

    console.log(`Match created: ${matchId} between ${player.minecraftUsername} and ${tester.minecraftUsername} (3-minute timer set)`);
  } catch (error) {
    console.error('Error creating match:', error);
    throw error;
  }
}

// ===== Tier Tester Routes =====

/**
 * POST /api/tester/availability - Set tester availability
 */
app.post('/api/tester/availability', verifyAuthAndNotBanned, verifyTester, async (req, res) => {
  try {
    const { available, gamemode, region } = req.body;

    if (typeof available !== 'boolean' || !gamemode) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'available (boolean) and gamemode are required'
      });
    }

    if (available && !region) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'region is required when setting availability'
      });
    }
    
    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val();
    
    if (available) {
      // Check if tester is tiered appropriately
      // First try to find by userId, then fallback to username matching
      const playersRef = db.ref('players');
      const playersSnapshot = await playersRef.once('value');
      const players = playersSnapshot.val() || {};

      // Try to find by Firebase userId first
      let testerPlayer = Object.values(players).find(p => p.userId === req.user.uid);

      // If not found by userId, try to find by Minecraft username from profile
      if (!testerPlayer && userProfile.minecraftUsername) {
        testerPlayer = Object.values(players).find(p => p.username === userProfile.minecraftUsername);

        // If found by username, update the record to include userId for future lookups
        if (testerPlayer && !testerPlayer.userId) {
          const playerKey = Object.keys(players).find(key => players[key].username === userProfile.minecraftUsername);
          if (playerKey) {
            await playersRef.child(playerKey).update({ userId: req.user.uid });
            testerPlayer.userId = req.user.uid; // Update local copy
          }
        }
      }

      if (!testerPlayer) {
        return res.status(400).json({
          error: true,
          code: 'NOT_TIERED',
          message: 'You must have a linked Minecraft account with tier data to become a tester'
        });
      }

      // Testers can join any queue regardless of rating

      // Set availability (all testers are now regular in Elo system)
      const availabilityRef = db.ref(`testerAvailability/${req.user.uid}`);
      await availabilityRef.set({
        userId: req.user.uid,
        available: true,
        gamemode,
        region,
        availableAt: new Date().toISOString(),
        timeoutAt: new Date(Date.now() + 30 * 60 * 1000).toISOString() // 30 minutes from now
      });

      // Trigger immediate matchmaking for this gamemode to auto-pair the tester
      setImmediate(async () => {
        try {
          console.log('🎯 Auto-pairing tester', req.user.uid, 'for gamemode', gamemode);
          await attemptMatchmakingForGamemode(gamemode);
        } catch (error) {
          console.error('Error during auto-pairing for gamemode', gamemode, ':', error);
        }
      });

      res.json({
        success: true,
        message: 'Availability set as tester'
      });
    } else {
      // Remove availability
      const availabilityRef = db.ref(`testerAvailability/${req.user.uid}`);
      await availabilityRef.remove();
      
      // Leave queue
      const queueRef = db.ref('queue');
      const queueSnapshot = await queueRef.orderByChild('userId').equalTo(req.user.uid).once('value');
      if (queueSnapshot.exists()) {
        const updates = {};
        queueSnapshot.forEach(child => {
          updates[child.key] = null;
        });
        await queueRef.update(updates);
      }
      
      res.json({ success: true, message: 'Availability removed' });
    }
  } catch (error) {
    console.error('Error setting tier tester availability:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error setting availability'
    });
  }
});

/**
 * GET /api/tester/availability - Get tester availability
 */
app.get('/api/tester/availability', verifyAuthAndNotBanned, verifyTester, async (req, res) => {
  try {
    const availabilityRef = db.ref(`testerAvailability/${req.user.uid}`);
    const snapshot = await availabilityRef.once('value');
    const availability = snapshot.val();
    
    res.json({ available: availability !== null, availability: availability || null });
  } catch (error) {
    console.error('Error getting tier tester availability:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error getting availability'
    });
  }
});

// ===== Match Routes =====

/**
 * GET /api/match/active - Get active match for current user
 */
app.get('/api/match/active', verifyAuthAndNotBanned, async (req, res) => {
  try {
    const matchesRef = db.ref('matches');
    const matchesSnapshot = await matchesRef
      .orderByChild('status')
      .equalTo('active')
      .once('value');
    
    if (!matchesSnapshot.exists()) {
      return res.json({ hasMatch: false });
    }
    
    const matches = matchesSnapshot.val();
    
    // Find match where user is participant
    for (const matchId in matches) {
      const match = matches[matchId];
      if (match.playerId === req.user.uid || match.testerId === req.user.uid) {
        return res.json({ hasMatch: true, match: { ...match, matchId } });
      }
    }
    
    res.json({ hasMatch: false });
  } catch (error) {
    console.error('Error getting active match:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error getting active match'
    });
  }
});

/**
 * GET /api/match/:matchId - Get match details
 */
app.get('/api/match/:matchId', verifyAuthAndNotBanned, async (req, res) => {
  try {
    const { matchId } = req.params;
    const matchRef = db.ref(`matches/${matchId}`);
    const snapshot = await matchRef.once('value');
    const match = snapshot.val();
    
    if (!match) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Match not found'
      });
    }
    
    // Read-only access is allowed to any authenticated user (spectator mode).
    res.json({ ...match, matchId });
  } catch (error) {
    console.error('Error getting match:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error getting match'
    });
  }
});

/**
 * POST /api/match/:matchId/join - Join match
 */
app.post('/api/match/:matchId/join', verifyAuth, async (req, res) => {
  try {
    const { matchId } = req.params;
    const matchRef = db.ref(`matches/${matchId}`);
    const snapshot = await matchRef.once('value');
    const match = snapshot.val();
    
    if (!match) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Match not found'
      });
    }
    
    // Verify user is participant
    if (match.playerId !== req.user.uid && match.testerId !== req.user.uid) {
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'You are not a participant in this match'
      });
    }
    
    // Add to participants
    const participantsRef = matchRef.child('participants');
    await participantsRef.child(req.user.uid).set({
      joinedAt: new Date().toISOString()
    });
    
    res.json({ success: true, message: 'Joined match' });
  } catch (error) {
    console.error('Error joining match:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error joining match'
    });
  }
});

/**
 * POST /api/match/:matchId/started - Mark match as started
 */
app.post('/api/match/:matchId/started', verifyAuth, async (req, res) => {
  try {
    const { matchId } = req.params;
    const matchRef = db.ref(`matches/${matchId}`);
    const snapshot = await matchRef.once('value');
    const match = snapshot.val();
    
    if (!match) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Match not found'
      });
    }
    
    // Verify user is participant
    if (match.playerId !== req.user.uid && match.testerId !== req.user.uid) {
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'You are not a participant in this match'
      });
    }
    
    // Check if both players have joined
    const playerJoined = match.pagestats?.playerJoined || false;
    const testerJoined = match.pagestats?.testerJoined || false;
    
    if (!playerJoined || !testerJoined) {
      return res.status(400).json({
        error: true,
        code: 'NOT_READY',
        message: 'Both players must join before starting the match'
      });
    }
    
    // Check if match already started
    if (match.matchStarted) {
      return res.status(400).json({
        error: true,
        code: 'ALREADY_STARTED',
        message: 'Match has already started'
      });
    }
    
    // Mark as started
    await matchRef.update({
      matchStarted: true,
      matchStartedAt: new Date().toISOString(),
      countdownStartedAt: null // Clear countdown since match started
    });
    const matchStartTimer = scheduledMatchTimers.matchStart.get(matchId);
    if (matchStartTimer) {
      clearTimeout(matchStartTimer);
      scheduledMatchTimers.matchStart.delete(matchId);
    }
    
    res.json({ success: true, message: 'Match marked as started' });
  } catch (error) {
    console.error('Error marking match as started:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error marking match as started'
    });
  }
});

/**
 * POST /api/match/:matchId/presence - Update presence
 */
app.post('/api/match/:matchId/presence', verifyAuth, async (req, res) => {
  try {
    const { matchId } = req.params;
    const { onPage } = req.body;
    const baseMatchRef = db.ref(`matches/${matchId}`);
    const snapshot = await baseMatchRef.once('value');
    const match = snapshot.val();

    if (!match) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Match not found'
      });
    }

    if (match.playerId !== req.user.uid && match.testerId !== req.user.uid) {
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'Only match participants can update presence'
      });
    }

    const matchRef = db.ref(`matches/${matchId}/presence`);
    
    await matchRef.child(req.user.uid).set({
      onPage: onPage === true,
      lastSeen: new Date().toISOString()
    });
    
    res.json({ success: true });
  } catch (error) {
    console.error('Error updating presence:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error updating presence'
    });
  }
});

/**
 * POST /api/match/:matchId/pagestats - Update page stats
 */
app.post('/api/match/:matchId/pagestats', verifyAuth, async (req, res) => {
  try {
    const { matchId } = req.params;
    const { isPlayer } = req.body;
    const matchRef = db.ref(`matches/${matchId}`);
    const snapshot = await matchRef.once('value');
    const match = snapshot.val();
    
    if (!match) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Match not found'
      });
    }

    if (match.playerId !== req.user.uid && match.testerId !== req.user.uid) {
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'Only match participants can update page stats'
      });
    }
    
    const pagestatsRef = matchRef.child('pagestats');
    const pagestats = {
      playerJoined: isPlayer ? true : (match.pagestats?.playerJoined || false),
      testerJoined: !isPlayer ? true : (match.pagestats?.testerJoined || false),
      lastUpdate: new Date().toISOString()
    };
    
    await pagestatsRef.set(pagestats);

    // If tester just joined, start player join timeout
    if (!isPlayer && pagestats.testerJoined && !match.pagestats?.testerJoined) {
      const playerJoinTimeout = {
        startedAt: new Date().toISOString(),
        timeoutMinutes: 3,
        autoEndEnabled: true
      };

      await matchRef.child('playerJoinTimeout').set(playerJoinTimeout);

      // Set up 3-minute timeout for player to join (deduped per match)
      const PLAYER_JOIN_TIMEOUT_MS = 3 * 60 * 1000;
      scheduleMatchTimer(scheduledMatchTimers.playerJoin, matchId, PLAYER_JOIN_TIMEOUT_MS, async () => {
        console.log(`Checking player join timeout for match ${matchId}...`);
        await handlePlayerJoinTimeout(matchId);
      });
    }

    // If both players just joined, start the match countdown timer (5 minutes)
    if (pagestats.playerJoined && pagestats.testerJoined && 
        (!match.pagestats?.playerJoined || !match.pagestats?.testerJoined)) {
      // Both players just became joined
      await matchRef.update({
        countdownStartedAt: new Date().toISOString()
      });

      // Set up 5-minute countdown for match to be started (deduped per match)
      const MATCH_START_COUNTDOWN_MS = 5 * 60 * 1000;
      scheduleMatchTimer(scheduledMatchTimers.matchStart, matchId, MATCH_START_COUNTDOWN_MS, async () => {
        console.log(`Checking match start countdown for match ${matchId}...`);
        await handleMatchStartCountdown(matchId);
      });
    }

    res.json({ success: true, pagestats });
  } catch (error) {
    console.error('Error updating page stats:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error updating page stats'
    });
  }
});

/**
 * POST /api/match/:matchId/message - Send chat message
 */
app.post('/api/match/:matchId/message', verifyAuth, async (req, res) => {
  try {
    const { matchId } = req.params;
    const { text } = req.body;

    if (!text || typeof text !== 'string' || text.trim().length === 0) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Message text is required'
      });
    }

    const matchRef = db.ref(`matches/${matchId}`);
    const snapshot = await matchRef.once('value');
    const match = snapshot.val();

    if (!match) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Match not found'
      });
    }

    // Check if match has ended - block chat
    if (match.status === 'ended' || match.finalized) {
      return res.status(403).json({
        error: true,
        code: 'MATCH_ENDED',
        message: 'Cannot send messages - match has ended'
      });
    }

    // Verify user is participant
    if (match.playerId !== req.user.uid && match.testerId !== req.user.uid) {
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'You are not a participant in this match'
      });
    }
    
    // Get user profile for username
    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val();
    
    // Add message to chat
    const chatRef = matchRef.child('chat');
    const newMessageRef = chatRef.push();
    await newMessageRef.set({
      userId: req.user.uid,
      username: userProfile.minecraftUsername || userProfile.email,
      text: text.trim(),
      timestamp: Date.now()
    });
    
    res.json({ success: true, messageId: newMessageRef.key });
  } catch (error) {
    console.error('Error sending message:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error sending message'
    });
  }
});

/**
 * DELETE /api/match/:matchId/message/:messageId - Delete chat message (tier tester only)
 */
app.delete('/api/match/:matchId/message/:messageId', verifyAuth, verifyTester, async (req, res) => {
  try {
    const { matchId, messageId } = req.params;

    const matchRef = db.ref(`matches/${matchId}`);
    const snapshot = await matchRef.once('value');
    const match = snapshot.val();

    if (!match) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Match not found'
      });
    }

    // Check if user is the tier tester for this match
    if (match.testerId !== req.user.uid) {
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'Only the tier tester can delete messages'
      });
    }

    // Check if message exists
    const messageRef = matchRef.child(`chat/${messageId}`);
    const messageSnapshot = await messageRef.once('value');
    const message = messageSnapshot.val();

    if (!message) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Message not found'
      });
    }

    // Delete the message
    await messageRef.remove();

    res.json({ success: true, message: 'Message deleted' });
  } catch (error) {
    console.error('Error deleting message:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error deleting message'
    });
  }
});


/**
 * POST /api/match/:matchId/finalize - Finalize match (tier tester only)
 */
app.post('/api/match/:matchId/finalize', verifyAuth, verifyTester, async (req, res) => {
  try {
    const { matchId } = req.params;
    const { playerScore, testerScore } = req.body;

    // Validate scores
    if (typeof playerScore !== 'number' || typeof testerScore !== 'number') {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'playerScore and testerScore must be numbers'
      });
    }

    if (playerScore < 0 || testerScore < 0 || (playerScore + testerScore) < 1) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Invalid match scores'
      });
    }

    const matchRef = db.ref(`matches/${matchId}`);
    const snapshot = await matchRef.once('value');
    const match = snapshot.val();

    if (!match) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Match not found'
      });
    }

    if (match.testerId !== req.user.uid) { // Changed from tierTesterId
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'Only the tester can finalize this match'
      });
    }

    if (match.finalized) {
      return res.status(400).json({
        error: true,
        code: 'ALREADY_FINALIZED',
        message: 'Match has already been finalized'
      });
    }

    // Validate scores against firstTo value
    const firstTo = match.firstTo || CONFIG.FIRST_TO[match.gamemode] || 3;
    const maxScore = Math.max(playerScore, testerScore);
    const minScore = Math.min(playerScore, testerScore);

    // At least one player must reach the firstTo value
    if (maxScore < firstTo) {
      return res.status(400).json({
        error: true,
        code: 'INVALID_SCORES',
        message: `Invalid scores for ${match.gamemode} (First to ${firstTo}). Winner must reach ${firstTo} points. Scores: ${playerScore}-${testerScore}`
      });
    }

    // The loser must have fewer points than the winner
    if (playerScore === testerScore) {
      return res.status(400).json({
        error: true,
        code: 'INVALID_SCORES',
        message: 'Scores cannot be equal. One player must win.'
      });
    }

    // Handle Elo-based finalization
    const ratingChanges = await handleManualFinalization(match, { playerScore, testerScore });

    // Update match
    await matchRef.update({
      finalized: true,
      finalizedAt: new Date().toISOString(),
      status: 'ended',
      finalizationData: {
        type: 'elo_rating',
        playerScore: playerScore,
        testerScore: testerScore,
        ratingChanges: ratingChanges,
        playerUsername: match.playerUsername,
        gamemode: match.gamemode
      }
    });
    clearAllMatchTimers(matchId);

    // Update player's last tested timestamp for cooldown
    const userRef = db.ref(`users/${match.playerId}`);
    const userSnapshot = await userRef.once('value');
    const userData = userSnapshot.val() || {};

    const lastTested = userData.lastTested || {};
    lastTested[match.gamemode] = new Date().toISOString();

    await userRef.update({
      lastTested: lastTested
    });

    res.json({
      success: true,
      message: 'Match finalized successfully',
      finalizationData: {
        type: 'elo_rating',
        playerScore: playerScore,
        testerScore: testerScore,
        ratingChanges: ratingChanges,
        playerUsername: match.playerUsername,
        gamemode: match.gamemode
      }
    });
  } catch (error) {
    console.error('Error finalizing match:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: error.message || 'Error finalizing match'
    });
  }
});

/**
 * Handle evaluation testing finalization
 */
async function handleEvaluationFinalization(match, evaluationResult, assignedTier) {
  const playersRef = db.ref('players');
  let playerSnapshot = await playersRef.orderByChild('username').equalTo(match.playerUsername).once('value');
  let playerId = null;
  let player = null;

  // Get or create player record
  if (playerSnapshot.exists()) {
    const players = playerSnapshot.val();
    playerId = Object.keys(players)[0];
    player = players[playerId];
  } else {
    // Create new player record
    const newPlayerRef = playersRef.push();
    playerId = newPlayerRef.key;
    player = {
      id: playerId,
      userId: match.playerId,
      username: match.playerUsername,
      region: match.region,
      gamemodeRatings: {},
      createdAt: new Date().toISOString()
    };
    await newPlayerRef.set(player);
  }

  // Handle evaluation result
  if (evaluationResult === 'pass_to_ht3') {
    // Player passed evaluation - mark as LT3 and eligible for HT3 testing
    player.gamemodeTiers = player.gamemodeTiers || {};
    player.gamemodeTiers[match.gamemode] = 'LT3';
    player.evaluationStatus = player.evaluationStatus || {};
    player.evaluationStatus[match.gamemode] = 'passed';

    // Update player record
    await playersRef.child(playerId).update({
      gamemodeTiers: player.gamemodeTiers,
      evaluationStatus: player.evaluationStatus,
      updatedAt: new Date().toISOString()
    });

    // Create HT3 testing eligibility record
    const ht3QueueRef = db.ref('ht3TestingQueue');
    await ht3QueueRef.child(playerId).set({
      playerId,
      username: match.playerUsername,
      gamemode: match.gamemode,
      evaluationPassedAt: new Date().toISOString(),
      testerId: match.testerId,
      status: 'eligible'
    });

  } else if (evaluationResult === 'assign_lower_tier' && assignedTier) {
    // Assign lower tier (LT5, HT5, LT4, HT4)
    const validLowerTiers = ['LT5', 'HT5', 'LT4', 'HT4'];
    if (!validLowerTiers.includes(assignedTier)) {
      throw new Error('Invalid lower tier assignment');
    }

    player.gamemodeTiers = player.gamemodeTiers || {};
    player.gamemodeTiers[match.gamemode] = assignedTier;
    player.evaluationStatus = player.evaluationStatus || {};
    player.evaluationStatus[match.gamemode] = 'assigned_lower_tier';

    // Update player record
    await playersRef.child(playerId).update({
      gamemodeTiers: player.gamemodeTiers,
      evaluationStatus: player.evaluationStatus,
      updatedAt: new Date().toISOString()
    });
  } else {
    throw new Error('Invalid evaluation result');
  }

  // Mark match as finalized
  await db.ref(`matches/${match.matchId}`).update({
    finalized: true,
    finalizedAt: new Date().toISOString(),
    finalizationData: {
      evaluationResult,
      assignedTier,
      finalizedBy: match.testerId
    }
  });

  // Update player's last tested timestamp for cooldown
  const userRef = db.ref(`users/${match.playerId}`);
  const userSnapshot = await userRef.once('value');
  const userData = userSnapshot.val() || {};

  const lastTested = userData.lastTested || {};
  lastTested[match.gamemode] = new Date().toISOString();

  await userRef.update({
    lastTested: lastTested
  });
}


/**
 * Glicko-2 Rating System Constants
 */
const GLICKO2_SCALE = 173.7178;
const GLICKO2_CONVERGENCE_TOLERANCE = 0.000001;
const GLICKO2_DEFAULT_RD = 350;
const GLICKO2_DEFAULT_VOLATILITY = 0.06;

/**
 * Convert rating from Glicko-2 scale to display scale
 */
function glicko2ToDisplay(rating) {
  return rating * GLICKO2_SCALE / 173.7178;
}

/**
 * Convert rating from display scale to Glicko-2 scale
 */
function displayToGlicko2(rating) {
  return rating * 173.7178 / GLICKO2_SCALE;
}

/**
 * Calculate rating changes using a hybrid Elo/Glicko-2 system
 * Uses standard Elo formula but incorporates RD for dynamic K-factor
 */
function calculateGlicko2Change(player, opponent, score) {
  const rating1 = player.rating;
  const rating2 = opponent.rating;
  const rd1 = Math.max(player.rd || GLICKO2_DEFAULT_RD, 50); // Minimum RD of 50
  const rd2 = Math.max(opponent.rd || GLICKO2_DEFAULT_RD, 50); // Minimum RD of 50

  // Calculate expected score using standard Elo formula
  const expectedScore1 = 1 / (1 + Math.pow(10, (rating2 - rating1) / 400));
  const expectedScore2 = 1 - expectedScore1;

  // Use RD to modify K-factor: lower RD = more stable rating = lower K-factor
  const kFactor1 = Math.max(8, 32 * (rd1 / GLICKO2_DEFAULT_RD)); // K-factor between 8-32
  const kFactor2 = Math.max(8, 32 * (rd2 / GLICKO2_DEFAULT_RD)); // K-factor between 8-32

  // Calculate rating changes
  const actualScore1 = score; // 1 for win, 0 for loss, 0.5 for draw
  const actualScore2 = 1 - score;

  const ratingChange1 = Math.round(kFactor1 * (actualScore1 - expectedScore1));
  const ratingChange2 = Math.round(kFactor2 * (actualScore2 - expectedScore2));

  // Update RD: increases after each match (becomes more uncertain)
  const newRD1 = Math.min(GLICKO2_DEFAULT_RD, rd1 + 50); // RD increases by 50, max at default
  const newRD2 = Math.min(GLICKO2_DEFAULT_RD, rd2 + 50); // RD increases by 50, max at default

  // Simple volatility calculation (not full Glicko-2 complexity)
  const newVolatility1 = player.volatility || GLICKO2_DEFAULT_VOLATILITY;
  const newVolatility2 = opponent.volatility || GLICKO2_DEFAULT_VOLATILITY;

  return {
    ratingChange: ratingChange1,
    newRating: rating1 + ratingChange1,
    newRD: newRD1,
    newVolatility: newVolatility1
  };

  // Step 1: Compute v (estimated variance of the player's rating based on game outcomes)
  const gRD2 = 1 / Math.sqrt(1 + 3 * Math.pow(RD2, 2) / Math.pow(Math.PI, 2));
  const E1 = 1 / (1 + Math.exp(-gRD2 * (r1 - r2) / 400));
  const v = Math.pow(gRD2, 2) * E1 * (1 - E1);

  // Step 2: Compute Δ (estimated improvement in rating)
  const delta = v * gRD2 * (score - E1);

  // Step 3: Determine the new value of the volatility σ
  const a = Math.log(Math.pow(sigma, 2));
  const A = a;
  const B = delta > 0 ? Math.log(delta * delta - RD1 * RD1 - v - Math.exp(a)) : a;

  let fa, fb;
  let k = 1;
  let A_k = A;
  let B_k = B;

  // Iterative algorithm to find the new volatility
  while (Math.abs(A_k - B_k) > GLICKO2_CONVERGENCE_TOLERANCE) {
    A_k = A;
    B_k = B;

    fa = (Math.exp(A_k) * (Math.pow(delta, 2) - Math.pow(RD1, 2) - v - Math.exp(A_k))) /
         (2 * Math.pow(Math.pow(RD1, 2) + v + Math.exp(A_k), 2)) -
         (A_k - a) / Math.pow(sigma, 2);

    fb = (Math.exp(B_k) * (Math.pow(delta, 2) - Math.pow(RD1, 2) - v - Math.exp(B_k))) /
         (2 * Math.pow(Math.pow(RD1, 2) + v + Math.exp(B_k), 2)) -
         (B_k - a) / Math.pow(sigma, 2);

    const C_k = fa * (A_k - B_k) / (fb - fa);

    if (C_k < 0) {
      A_k = A_k;
      B_k = A_k - C_k;
    } else {
      A_k = B_k + C_k;
      B_k = B_k;
    }

    k++;
    if (k > 10) break; // Prevent infinite loops
  }

  const sigma_prime = Math.exp(A_k / 2);

  // Step 4: Update the RD to the new pre-rating period value
  const RD1_prime = Math.sqrt(Math.pow(RD1, 2) + Math.pow(sigma_prime, 2));

  // Step 5: Update the rating and RD to the new values
  const r1_prime = r1 + Math.pow(RD1_prime, 2) * gRD2 * (score - E1) / (Math.pow(RD1, 2) + 1 / v);
  const RD1_prime_final = Math.sqrt(1 / (1 / Math.pow(RD1_prime, 2) + 1 / v));

  // Convert back to display scale
  const newRating = glicko2ToDisplay(r1_prime);
  const newRD = glicko2ToDisplay(RD1_prime_final);
  const newVolatility = sigma_prime;

  // Calculate rating change before rounding
  const rawRatingChange = newRating - player.rating;

  return {
    ratingChange: Math.round(rawRatingChange),
    newRating: Math.round(newRating),
    newRD: Math.round(newRD),
    newVolatility: newVolatility
  };
}

/**
 * Handle player join timeout (tester joined, waiting for player)
 */
async function handlePlayerJoinTimeout(matchId) {
  try {
    const matchRef = db.ref(`matches/${matchId}`);
    const matchSnapshot = await matchRef.once('value');
    const match = matchSnapshot.val();

    if (!match || match.status !== 'active' || match.finalized) {
      return; // Match already handled or doesn't exist
    }

    // Check if player has joined
    const playerJoined = match.pagestats?.playerJoined || false;

    if (!playerJoined) {
      // Player did not join within 3 minutes, tester wins 3-0
      console.log(`Match ${matchId}: Player did not join within 3 minutes after tester joined, auto-finalizing with tester win (3-0)`);

      // Report the player for not showing up
      if (match.playerId) {
        const reportsRef = db.ref('reports');
        await reportsRef.push({
          reportedUserId: match.playerId,
          reportedByUserId: match.testerId,
          reason: 'Did not join match within 3 minutes after tester was ready',
          matchId: matchId,
          timestamp: new Date().toISOString(),
          type: 'no_show'
        });
        console.log(`Reported player ${match.playerId} for not showing up in match ${matchId}`);
      }

      await handleManualFinalization(match, { playerScore: 0, testerScore: 3 });
      await matchRef.update({
        status: 'ended',
        finalized: true,
        finalizedAt: new Date().toISOString(),
        result: { playerScore: 0, testerScore: 3 },
        reason: 'Player did not join within 3 minutes'
      });
      clearAllMatchTimers(matchId);

      // Update player's last tested timestamp for cooldown
      const userRef = db.ref(`users/${match.playerId}`);
      const userSnapshot = await userRef.once('value');
      const userData = userSnapshot.val() || {};

      const lastTested = userData.lastTested || {};
      lastTested[match.gamemode] = new Date().toISOString();

      await userRef.update({
        lastTested: lastTested
      });
    } else {
      // Player did join, no action needed
      console.log('Match', matchId, ': Player joined in time, no timeout action needed');
    }
  } catch (error) {
    console.error('Error handling player join timeout for match', matchId, ':', error);
  }
}

/**
 * Handle match start countdown (5 minutes after both players join)
 */
async function handleMatchStartCountdown(matchId) {
  try {
    const matchRef = db.ref(`matches/${matchId}`);
    const matchSnapshot = await matchRef.once('value');
    const match = matchSnapshot.val();

    if (!match || match.status !== 'active' || match.finalized) {
      return; // Match already handled or doesn't exist
    }

    // Check if match has been started
    const matchStarted = match.matchStarted || false;

    if (!matchStarted) {
      // Match was not started within 5 minutes after both players joined
      console.log(`Match ${matchId}: Match not started within 5 minutes, auto-finalizing with no winner`);

      // Auto-finalize with no winner (draw)
      await matchRef.update({
        status: 'ended',
        finalized: true,
        finalizedAt: new Date().toISOString(),
        result: { playerScore: 0, testerScore: 0 },
        reason: 'Match not started within 5 minutes countdown'
      });
      clearAllMatchTimers(matchId);

      console.log(`Match ${matchId} auto-finalized due to not being marked as started within 5 minutes`);
    } else {
      // Match was started in time, no action needed
      console.log('Match', matchId, ': Match was started in time, no countdown action needed');
    }
  } catch (error) {
    console.error('Error handling match start countdown for match', matchId, ':', error);
  }
}

/**
 * Handle inactivity timeouts for matches
 */
async function handleMatchInactivity(matchId) {
  try {
    const matchRef = db.ref(`matches/${matchId}`);
    const matchSnapshot = await matchRef.once('value');
    const match = matchSnapshot.val();

    if (!match || match.status !== 'active' || match.finalized) {
      return; // Match already handled or doesn't exist
    }

    // Check if tester has joined within 3 minutes
    const testerJoined = match.pagestats?.testerJoined || false;

    if (!testerJoined) {
      // Tester did not join within 3 minutes, player wins 3-0
      console.log(`Match ${matchId}: Tester did not join within 3 minutes, auto-finalizing with player win (3-0)`);
      await handleManualFinalization(match, { playerScore: 3, testerScore: 0 });
      await matchRef.update({
        status: 'ended',
        finalized: true,
        finalizedAt: new Date().toISOString(),
        result: { playerScore: 3, testerScore: 0 },
        reason: 'Tester did not join within 3 minutes'
      });
      clearAllMatchTimers(matchId);

      // Update player's last tested timestamp for cooldown
      const userRef = db.ref(`users/${match.playerId}`);
      const userSnapshot = await userRef.once('value');
      const userData = userSnapshot.val() || {};

      const lastTested = userData.lastTested || {};
      lastTested[match.gamemode] = new Date().toISOString();

      await userRef.update({
        lastTested: lastTested
      });
    } else {
      // This shouldn't happen since we're only called after 3 minutes, but just in case
      console.log(`Match ${matchId}: Tester has joined, no action needed`);
    }
  } catch (error) {
    console.error(`Error handling inactivity for match ${matchId}:`, error);
  }
}

/**
 * Centralized rating update function that handles both user and player records
 */
async function updatePlayerRating(userId, gamemode, ratingChange, newRating, newRD, newVolatility) {
  try {
    // Update player record
    const playersRef = db.ref('players');
    const playerSnapshot = await playersRef.orderByChild('userId').equalTo(userId).once('value');
    const players = playerSnapshot.val() || {};
    const playerId = Object.keys(players).find(key => players[key].userId === userId);

    if (playerId) {
      const playerRef = playersRef.child(playerId);
      const player = players[playerId];

      // Update gamemode rating
      const gamemodeRatings = player.gamemodeRatings || {};
      gamemodeRatings[gamemode] = newRating;

      // Update Glicko-2 parameters
      const glicko2Params = player.glicko2Params || {};
      glicko2Params[gamemode] = {
        rd: newRD,
        volatility: newVolatility
      };

      // Recalculate overall rating
      const overallRating = calculateOverallRating(gamemodeRatings);

      // Increment match count
      const gamemodeMatchCount = player.gamemodeMatchCount || {};
      gamemodeMatchCount[gamemode] = (gamemodeMatchCount[gamemode] || 0) + 1;

      await playerRef.update({
        gamemodeRatings,
        glicko2Params,
        overallRating,
        gamemodeMatchCount,
        [`lastTested/${gamemode}`]: new Date().toISOString()
      });
    }

    // Update user profile
    const userRef = db.ref(`users/${userId}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val() || {};

    const userGamemodeRatings = userProfile.gamemodeRatings || {};
    userGamemodeRatings[gamemode] = newRating;

    const userGlicko2Params = userProfile.glicko2Params || {};
    userGlicko2Params[gamemode] = {
      rd: newRD,
      volatility: newVolatility
    };

    await userRef.update({
      gamemodeRatings: userGamemodeRatings,
      glicko2Params: userGlicko2Params,
      overallRating: calculateOverallRating(userGamemodeRatings)
    });

  } catch (error) {
    console.error('Error updating player rating:', error);
    throw error;
  }
}

/**
 * Handle match finalization with Glicko-2 calculations
 */
async function handleManualFinalization(match, result) {
  // result format: { playerScore: number, testerScore: number }
  const playerScore = result.playerScore;
  const testerScore = result.testerScore;
  const playerWon = playerScore > testerScore;

  // Convert scores to Glicko-2 compatible scores (0, 0.5, or 1)
  let playerGlicko2Score;
  if (playerScore > testerScore) {
    playerGlicko2Score = 1; // Win
  } else if (playerScore < testerScore) {
    playerGlicko2Score = 0; // Loss
  } else {
    playerGlicko2Score = 0.5; // Draw
  }

  // Get current player data
  const playersRef = db.ref('players');
  const playerSnapshot = await playersRef.orderByChild('userId').equalTo(match.playerId).once('value');
  const testerSnapshot = await playersRef.orderByChild('userId').equalTo(match.testerId).once('value');

  const players = playerSnapshot.val() || {};
  const testers = testerSnapshot.val() || {};

  const playerData = Object.values(players).find(p => p.userId === match.playerId) || {
    gamemodeRatings: {},
    glicko2Params: {}
  };
  const testerData = Object.values(testers).find(p => p.userId === match.testerId) || {
    gamemodeRatings: {},
    glicko2Params: {}
  };

  // Get current ratings and Glicko-2 parameters
  const playerRating = playerData.gamemodeRatings?.[match.gamemode] || 1000;
  const testerRating = testerData.gamemodeRatings?.[match.gamemode] || 1000;

  const playerRD = playerData.glicko2Params?.[match.gamemode]?.rd || GLICKO2_DEFAULT_RD;
  const testerRD = testerData.glicko2Params?.[match.gamemode]?.rd || GLICKO2_DEFAULT_RD;

  const playerVolatility = playerData.glicko2Params?.[match.gamemode]?.volatility || GLICKO2_DEFAULT_VOLATILITY;
  const testerVolatility = testerData.glicko2Params?.[match.gamemode]?.volatility || GLICKO2_DEFAULT_VOLATILITY;

  // Create player and opponent objects for Glicko-2 calculation
  const playerObj = { rating: playerRating, rd: playerRD, volatility: playerVolatility };
  const testerObj = { rating: testerRating, rd: testerRD, volatility: testerVolatility };

  // Calculate Glicko-2 rating changes
  const playerResult = calculateGlicko2Change(playerObj, testerObj, playerGlicko2Score);
  const testerResult = calculateGlicko2Change(testerObj, playerObj, 1 - playerGlicko2Score);

  // Get title changes
  const playerOldTitle = getAchievementTitle(match.gamemode, playerRating);
  const playerNewTitle = getAchievementTitle(match.gamemode, playerResult.newRating);
  const testerOldTitle = getAchievementTitle(match.gamemode, testerRating);
  const testerNewTitle = getAchievementTitle(match.gamemode, testerResult.newRating);

  const playerTitleChanged = playerOldTitle.title !== playerNewTitle.title;
  const testerTitleChanged = testerOldTitle.title !== testerNewTitle.title;

  // Update ratings using centralized function
  await updatePlayerRating(match.playerId, match.gamemode, playerResult.ratingChange, playerResult.newRating, playerResult.newRD, playerResult.newVolatility);
  await updatePlayerRating(match.testerId, match.gamemode, testerResult.ratingChange, testerResult.newRating, testerResult.newRD, testerResult.newVolatility);

  // Return rating changes and title changes for API response
  return {
    playerRatingChange: playerResult.ratingChange,
    testerRatingChange: testerResult.ratingChange,
    playerNewRating: playerResult.newRating,
    testerNewRating: testerResult.newRating,
    titleChanges: {
      player: playerTitleChanged ? { oldTitle: playerOldTitle, newTitle: playerNewTitle } : null,
      tester: testerTitleChanged ? { oldTitle: testerOldTitle, newTitle: testerNewTitle } : null
    }
  };


  // Set cooldowns for the player after match finalization
  const playerUserRef = db.ref(`users/${match.playerId}`);
  const playerUserSnapshot = await playerUserRef.once('value');
  const playerUserProfile = playerUserSnapshot.val() || {};

  // Regular queue cooldown (existing)
  const lastQueueJoins = playerUserProfile.lastQueueJoins || {};
  lastQueueJoins[match.gamemode] = new Date().toISOString();

  // 30-minute testing cooldown for the tested player
  const lastTestCompletions = playerUserProfile.lastTestCompletions || {};
  lastTestCompletions[match.gamemode] = new Date().toISOString();

  await playerUserRef.update({ lastQueueJoins, lastTestCompletions });
}

/**
 * Calculate overall rating as average of gamemode ratings
 */
function calculateOverallRating(gamemodeRatings) {
  const ratings = Object.values(gamemodeRatings);
  if (ratings.length === 0) return 1000;
  return Math.round(ratings.reduce((sum, rating) => sum + rating, 0) / ratings.length);
}

/**
 * Get achievement title for a gamemode and rating
 */
function getAchievementTitle(gamemode, rating) {
  const titles = CONFIG.TITLES;
  if (!titles) return { title: 'Unknown', color: '#8B8B8B' };

  // Find the highest title the player qualifies for
  for (let i = titles.length - 1; i >= 0; i--) {
    if (rating >= titles[i].minRating) {
      return titles[i];
    }
  }

  // Fallback to first title
  return titles[0] || { title: 'Unknown', color: '#8B8B8B' };
}

/**
 * Handle promotion finalization
 */
async function handlePromotionFinalization(match, promotionTests) {
  // This is a simplified version - full implementation would validate opponent tiers
  // and check promotion paths as specified in the documentation
  
  // Get player's current tier
  const playersRef = db.ref('players');
  const playerSnapshot = await playersRef.orderByChild('username').equalTo(match.playerUsername).once('value');
  
  if (!playerSnapshot.exists()) {
    throw new Error('Player not found');
  }
  
  const players = playerSnapshot.val();
  const playerId = Object.keys(players)[0];
  const player = players[playerId];
  
  const currentTier = player.gamemodeTiers?.[match.gamemode];
  if (!currentTier) {
    throw new Error('Player has no current tier for this gamemode');
  }
  
  // Validate promotion path (simplified - should check opponent tiers match database)
  const promotionPath = CONFIG.PROMOTION_PATHS[currentTier];
  if (!promotionPath) {
    throw new Error('Invalid promotion path');
  }
  
  // Validate tests
  for (const test of promotionTests) {
    if (!test.playerWon || test.matchPoints.player <= test.matchPoints.tester) {
      throw new Error('Player must win both promotion matches');
    }
  }
  
  const targetTier = promotionPath.target;
  const tierPoints = CONFIG.TIERS[targetTier]?.points || 0;
  const currentPoints = CONFIG.TIERS[currentTier]?.points || 0;
  const pointsToAdd = tierPoints - currentPoints;
  
  // Update player
  const playerRef = playersRef.child(playerId);
  await playerRef.update({
    [`gamemodeTiers/${match.gamemode}`]: targetTier,
    [`gamemodePoints/${match.gamemode}`]: (player.gamemodePoints?.[match.gamemode] || 0) + pointsToAdd,
    totalPoints: (player.totalPoints || 0) + pointsToAdd,
    [`lastTested/${match.gamemode}`]: new Date().toISOString()
  });
}

// Note: CONFIG object needs to be defined or imported
// For now, using inline tier definitions

/**
 * POST /api/match/:matchId/abort - Abort match
 */
app.post('/api/match/:matchId/abort', verifyAuth, async (req, res) => {
  try {
    const { matchId } = req.params;
    const matchRef = db.ref(`matches/${matchId}`);
    const snapshot = await matchRef.once('value');
    const match = snapshot.val();
    
    if (!match) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Match not found'
      });
    }
    
    // Verify user is participant
    if (match.playerId !== req.user.uid && match.testerId !== req.user.uid) {
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'You are not a participant in this match'
      });
    }

    // Check if match is still in waiting phase (neither player has joined)
    const pagestats = match.pagestats || {};
    const neitherHasJoined = !pagestats.playerJoined && !pagestats.testerJoined;

    let ratingChanges = null;
    let finalizationData;

    if (neitherHasJoined) {
      // No one has joined yet - abort with no penalties
      finalizationData = {
        type: 'aborted_early',
        playerScore: 0,
        testerScore: 0,
        ratingChanges: null,
        abortedBy: match.playerId === req.user.uid ? 'player' : 'tester',
        reason: 'Match aborted before players joined - no penalties applied'
      };
    } else {
      // Someone has joined - apply normal abort penalties
      const isPlayerAborting = match.playerId === req.user.uid;
      const playerScore = isPlayerAborting ? 0 : 3;
      const testerScore = isPlayerAborting ? 3 : 0;

      // Finalize the match with scores
      ratingChanges = await handleManualFinalization(match, { playerScore, testerScore });

      finalizationData = {
        type: 'forfeit',
        playerScore: playerScore,
        testerScore: testerScore,
        ratingChanges: ratingChanges,
        abortedBy: isPlayerAborting ? 'player' : 'tester',
        reason: `${isPlayerAborting ? 'Player' : 'Tester'} aborted the match`
      };
    }

    await matchRef.update({
      status: 'ended',
      aborted: true,
      abortedAt: new Date().toISOString(),
      finalized: true,
      finalizedAt: new Date().toISOString(),
      finalizationData: finalizationData
    });
    clearAllMatchTimers(matchId);

    // If no penalties were applied, don't update last tested timestamp
    if (!neitherHasJoined) {
      // Update player's last tested timestamp for cooldown (only if penalties applied)
      const userRef = db.ref(`users/${match.playerId}`);
      const userSnapshot = await userRef.once('value');
      const userData = userSnapshot.val() || {};

      const lastTested = userData.lastTested || {};
      lastTested[match.gamemode] = new Date().toISOString();

      await userRef.update({
        lastTested: lastTested
      });
    }
    
    res.json({ success: true, message: 'Match aborted' });
  } catch (error) {
    console.error('Error aborting match:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error aborting match'
    });
  }
});

/**
 * POST /api/match/:matchId/draw-vote - Vote to end match as draw (no scoring)
 */
app.post('/api/match/:matchId/draw-vote', verifyAuth, async (req, res) => {
  try {
    const { matchId } = req.params;
    const agree = req.body?.agree === true;
    const matchRef = db.ref(`matches/${matchId}`);
    const snapshot = await matchRef.once('value');
    const match = snapshot.val();

    if (!match) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Match not found'
      });
    }

    if (match.playerId !== req.user.uid && match.testerId !== req.user.uid) {
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'Only match participants can vote for draw'
      });
    }

    if (match.finalized || match.status === 'ended') {
      return res.status(400).json({
        error: true,
        code: 'ALREADY_FINALIZED',
        message: 'Match has already ended'
      });
    }

    const playerJoined = match.pagestats?.playerJoined === true;
    const testerJoined = match.pagestats?.testerJoined === true;
    if (!playerJoined || !testerJoined) {
      return res.status(400).json({
        error: true,
        code: 'NOT_READY',
        message: 'Both participants must join before draw vote'
      });
    }

    const drawVotes = { ...(match.drawVotes || {}) };
    drawVotes[req.user.uid] = {
      agree,
      at: new Date().toISOString()
    };

    const playerAgreed = drawVotes[match.playerId]?.agree === true;
    const testerAgreed = drawVotes[match.testerId]?.agree === true;

    if (playerAgreed && testerAgreed) {
      const finalizationData = {
        type: 'draw_vote',
        playerScore: 0,
        testerScore: 0,
        ratingChanges: null,
        reason: 'Both participants agreed to end the match without scoring'
      };

      await matchRef.update({
        drawVotes,
        finalized: true,
        finalizedAt: new Date().toISOString(),
        status: 'ended',
        finalizationData
      });
      clearAllMatchTimers(matchId);

      return res.json({
        success: true,
        finalized: true,
        message: 'Match ended as draw',
        votes: { playerAgreed: true, testerAgreed: true },
        finalizationData
      });
    }

    await matchRef.update({
      drawVotes
    });

    res.json({
      success: true,
      finalized: false,
      message: 'Draw vote recorded',
      votes: { playerAgreed, testerAgreed }
    });
  } catch (error) {
    console.error('Error processing draw vote:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error processing draw vote'
    });
  }
});

// ===== Skill Level Management Routes =====

/**
 * POST /api/account/update-skill-levels - Update individual gamemode skill levels (with locking protection)
 */
app.post('/api/account/update-skill-levels', verifyAuth, async (req, res) => {
  try {
    const { gamemodeRatings: newRatings } = req.body;

    if (!newRatings || typeof newRatings !== 'object') {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Invalid gamemode ratings'
      });
    }

    // Get current user profile
    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val() || {};

    // Check for locked skill levels (already set ratings cannot be changed)
    const existingRatings = userProfile.gamemodeRatings || {};
    const lockedGamemodes = Object.keys(newRatings).filter(gamemode =>
      existingRatings[gamemode] !== undefined && existingRatings[gamemode] !== newRatings[gamemode]
    );

    if (lockedGamemodes.length > 0) {
      return res.status(400).json({
        error: true,
        code: 'SKILL_LEVEL_LOCKED',
        message: `Skill levels for the following gamemodes are already locked: ${lockedGamemodes.join(', ')}`
      });
    }

    // Validate new ratings
    for (const [gamemode, rating] of Object.entries(newRatings)) {
      if (typeof rating !== 'number' || rating < 300 || rating > 1300) {
        return res.status(400).json({
          error: true,
          code: 'VALIDATION_ERROR',
          message: `Invalid rating for ${gamemode}: must be 300-1300`
        });
      }
    }

    // Update gamemode ratings
    const updatedRatings = { ...existingRatings, ...newRatings };

    // Calculate overall rating (average of all gamemode ratings)
    const overallRating = Object.keys(updatedRatings).length > 0
      ? Math.round(Object.values(updatedRatings).reduce((sum, rating) => sum + rating, 0) / Object.keys(updatedRatings).length)
      : 1000;

    // Update user profile
    await userRef.update({
      gamemodeRatings: updatedRatings,
      overallRating: overallRating,
      updatedAt: new Date().toISOString()
    });

    res.json({
      success: true,
      message: 'Skill levels updated successfully',
      gamemodeRatings: updatedRatings,
      overallRating
    });
  } catch (error) {
    console.error('Error updating skill levels:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error updating skill levels'
    });
  }
});

// ===== Pre-Authentication Routes =====

// ===== Onboarding Routes =====

/**
 * GET /api/onboarding/status - Get onboarding status and requirements
 */
app.get('/api/onboarding/status', verifyAuth, async (req, res) => {
  try {
    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val();

    if (!userProfile) {
      return res.status(404).json({
        error: true,
        code: 'USER_NOT_FOUND',
        message: 'User profile not found'
      });
    }

    const status = {
      onboardingCompleted: userProfile.onboardingCompleted || false,
      hasMinecraftUsername: !!(userProfile.minecraftUsername && userProfile.minecraftUsername.trim()),
      hasRegion: !!(userProfile.region && userProfile.region.trim()),
      isMinecraftVerified: userProfile.minecraftVerified || false,
      canProceedToStep2: !!(userProfile.minecraftUsername && userProfile.minecraftUsername.trim() &&
                           userProfile.region && userProfile.region.trim()),
      canProceedToStep3: !!(userProfile.minecraftUsername && userProfile.minecraftUsername.trim() &&
                           userProfile.region && userProfile.region.trim() &&
                           userProfile.minecraftVerified === true)
    };

    res.json(status);
  } catch (error) {
    console.error('Error getting onboarding status:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error retrieving onboarding status'
    });
  }
});

/**
 * Attempt immediate matchmaking when player joins queue
 */
async function attemptImmediateMatchmaking(queueEntry, playerProfile) {
  try {
    console.log('Attempting immediate matchmaking for player:', queueEntry.minecraftUsername, 'in gamemode:', queueEntry.gamemode);

    // Check total active matches limit (100 across all gamemodes)
    const activeMatchesRef = db.ref('matches');
    const activeMatchesSnapshot = await activeMatchesRef
      .orderByChild('status')
      .equalTo('active')
      .once('value');

    const activeMatches = activeMatchesSnapshot.val() || {};
    const activeMatchCount = Object.keys(activeMatches).length;
    const activeTesterIds = new Set(
      Object.values(activeMatches)
        .map(match => match?.testerId)
        .filter(Boolean)
    );

    if (activeMatchCount >= 100) {
      console.log(`Match limit reached: ${activeMatchCount}/100 active matches. Cannot create new match.`);
      return null;
    }

    // Get all players to find available testers
    const playersRef = db.ref('players');
    const playersSnapshot = await playersRef.once('value');
    const allPlayers = playersSnapshot.val() || {};
    const testerByUserId = {};
    for (const playerData of Object.values(allPlayers)) {
      if (playerData?.userId) {
        testerByUserId[playerData.userId] = playerData;
      }
    }

    // Get player rating
    const playerRating = playerProfile.gamemodeRatings?.[queueEntry.gamemode] || 1000;

    // Get available testers from testerAvailability database
    const availabilityRef = db.ref('testerAvailability');
    const availabilitySnapshot = await availabilityRef.once('value');
    const testerAvailabilities = availabilitySnapshot.val() || {};

    // Find available testers in this gamemode and region
    const availableTesters = [];

    for (const [userId, availability] of Object.entries(testerAvailabilities)) {
      if (userId === queueEntry.userId) continue; // Skip self
      if (activeTesterIds.has(userId)) continue; // Already busy in another match

      // Check if tester is available for this gamemode
      if (!availability.available || availability.gamemode !== queueEntry.gamemode) {
        continue;
      }

      // Check if availability hasn't expired
      if (availability.timeoutAt && new Date(availability.timeoutAt) < new Date()) {
        continue;
      }

      // Get tester player data
      const testerPlayer = testerByUserId[userId];
      if (!testerPlayer) continue;

      // Check if user has tester role
      try {
        const userRef = db.ref(`users/${userId}`);
        const userSnapshot = await userRef.once('value');
        const userProfile = userSnapshot.val();

        if (userProfile && userProfile.tester) {
          // Check if tester is in the same region
          if (testerPlayer.region === queueEntry.region) {
            availableTesters.push(testerPlayer);
          }
        }
      } catch (error) {
        console.error('Error checking tester status for', userId, ':', error);
      }
    }

    if (availableTesters.length === 0) {
      console.log('No available testers found in region:', queueEntry.region);
      return null;
    }

    // Calculate rating differences and find closest match
    let bestMatch = null;
    let smallestDifference = Infinity;

    for (const tester of availableTesters) {
      // Skip if this is the same player
      if (tester.userId === queueEntry.userId) continue;

      const testerRating = tester.gamemodeRatings?.[queueEntry.gamemode] || 1000;
      const ratingDifference = Math.abs(playerRating - testerRating);

      if (ratingDifference < smallestDifference) {
        smallestDifference = ratingDifference;
        bestMatch = tester;
      }
    }

    if (!bestMatch) {
      console.log('No suitable tester match found');
      return null;
    }

    console.log('Found best tester match:', bestMatch.username, 'rating:', bestMatch.gamemodeRatings?.[queueEntry.gamemode] || 1000, 'difference:', smallestDifference);

    // Create match between player and tester
    const matchesRef = db.ref('matches');
    const newMatchRef = matchesRef.push();
    const matchId = newMatchRef.key;

    const match = {
      matchId,
      playerId: queueEntry.userId,
      playerUsername: queueEntry.minecraftUsername,
      playerEmail: playerProfile.email || '',
      testerId: bestMatch.userId,
      testerUsername: bestMatch.username,
      testerEmail: '', // Could be populated from user profile if needed
      gamemode: queueEntry.gamemode,
      region: queueEntry.region,
      serverIP: queueEntry.serverIP,
      matchType: 'regular',
      testerType: 'matched',
      playerCurrentRating: playerRating,
      testerCurrentRating: bestMatch.gamemodeRatings?.[queueEntry.gamemode] || 1000,
      status: 'active',
      createdAt: new Date().toISOString(),
      finalized: false,
      chat: {},
      participants: {},
      presence: {},
      pagestats: {
        playerJoined: false,
        testerJoined: false,
        lastUpdate: null
      },
      playerJoinTimeout: {
        startedAt: new Date().toISOString(),
        timeoutMinutes: 3,
        autoEndEnabled: true
      },
      testerJoinTimeout: {
        startedAt: new Date().toISOString(),
        timeoutMinutes: 3,
        autoEndEnabled: false // Don't auto-end, just notify player
      }
    };

    await newMatchRef.set(match);

    // Remove player from queue since match was created
    await db.ref(`queue/${queueEntry.queueId}`).remove();

    console.log('Match created successfully:', matchId, 'between', queueEntry.minecraftUsername, 'and', bestMatch.username);

    return {
      matchId,
      testerUsername: bestMatch.username,
      ratingDifference: smallestDifference
    };

  } catch (error) {
    console.error('Error in immediate matchmaking:', error);
    return null;
  }
}

// ===== Onboarding Routes =====

/**
 * POST /api/onboarding/save-preferences - Save gamemode preferences and skill level for Elo-based system
 */
app.post('/api/onboarding/save-preferences', verifyAuth, async (req, res) => {
  try {
    const { selectedGamemodes, gamemodeSkillLevels } = req.body;

    if (!Array.isArray(selectedGamemodes) || selectedGamemodes.length === 0) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'At least one gamemode must be selected'
      });
    }

    // Get current user profile
    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val() || {};

    if (!userProfile.minecraftUsername) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Minecraft username not linked'
      });
    }

    // Check for locked skill levels (already set ratings cannot be changed)
    const existingRatings = userProfile.gamemodeRatings || {};
    const lockedGamemodes = selectedGamemodes.filter(gamemode => existingRatings[gamemode] !== undefined);

    if (lockedGamemodes.length > 0) {
      return res.status(400).json({
        error: true,
        code: 'SKILL_LEVEL_LOCKED',
        message: `Skill levels for the following gamemodes are already locked: ${lockedGamemodes.join(', ')}`
      });
    }

    // Initialize Elo ratings for selected gamemodes
    const gamemodeRatings = { ...existingRatings };
    selectedGamemodes.forEach(gamemode => {
      gamemodeRatings[gamemode] = gamemodeSkillLevels[gamemode];
    });

    // Calculate overall rating (average of gamemode ratings)
    const overallRating = Object.keys(gamemodeRatings).length > 0
      ? Math.round(Object.values(gamemodeRatings).reduce((sum, rating) => sum + rating, 0) / Object.keys(gamemodeRatings).length)
      : 1000;

    // Update user profile
    await userRef.update({
      selectedGamemodes: [...new Set([...(userProfile.selectedGamemodes || []), ...selectedGamemodes])],
      gamemodeRatings: gamemodeRatings,
      overallRating: overallRating,
      updatedAt: new Date().toISOString()
    });

    // Now update or create the player record
    const playersRef = db.ref('players');
    const playerSnapshot = await playersRef.orderByChild('username').equalTo(userProfile.minecraftUsername).once('value');

    let playerRef;
    let playerData;

    if (playerSnapshot.exists()) {
      // Update existing player
      const players = playerSnapshot.val();
      const playerId = Object.keys(players)[0];
      playerRef = playersRef.child(playerId);
      playerData = players[playerId];
    } else {
      // Create new player record
      playerRef = playersRef.push();
      playerData = {
        username: userProfile.minecraftUsername,
        region: userProfile.region || null,
        gamemodeRatings: {},
        overallRating: 0,
        lastTested: {},
        createdAt: new Date().toISOString(),
        createdBy: req.user.uid
      };
    }

    // Update player with Elo ratings
    const playerUpdates = {
      gamemodeRatings: { ...playerData.gamemodeRatings, ...gamemodeRatings },
      overallRating: overallRating,
      updatedAt: new Date().toISOString()
    };

    await playerRef.update(playerUpdates);

    res.json({
      success: true,
      message: 'Elo ratings saved successfully to both profile and leaderboard',
      gamemodeRatings,
      overallRating,
      selectedGamemodes
    });
  } catch (error) {
    console.error('Error saving preferences:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error saving onboarding preferences'
    });
  }
});

/**
 * POST /api/admin/players/:playerId/rating - Admin endpoint to set player rating
 */
app.post('/api/admin/players/:playerId/rating', adminLimiter, verifyAuth, verifyAdminOrTester, async (req, res) => {
  try {

    const { playerId } = req.params;
    const decodedPlayerId = decodeURIComponent(playerId);
    console.log('Original playerId:', playerId, 'Decoded:', decodedPlayerId);

    // Additional security: Sanitize inputs
    const sanitizedPlayerId = decodedPlayerId?.toString().trim();
    const sanitizedGamemode = req.body.gamemode?.toString().trim();
    const sanitizedRating = typeof req.body.rating === 'number' ? req.body.rating : parseFloat(req.body.rating);

    if (!sanitizedPlayerId || sanitizedPlayerId.length > 100) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Invalid player ID'
      });
    }

    const { gamemode, rating } = { gamemode: sanitizedGamemode, rating: sanitizedRating };
    console.log('Setting rating for player:', sanitizedPlayerId, 'gamemode:', gamemode, 'rating:', rating, 'by user:', req.user.email);

    // Security audit log
    console.log(`ADMIN ACTION: Setting rating for player ${sanitizedPlayerId}: ${gamemode}=${rating}`);

    if (!gamemode || !CONFIG.GAMEMODES.find(g => g.id === gamemode)) {
      console.log('Invalid gamemode:', gamemode);
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: `Invalid gamemode: ${gamemode}`
      });
    }

    if (typeof rating !== 'number' || rating < 300 || rating > 3000) {
      console.log('Invalid rating:', rating, typeof rating);
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: `Invalid rating (must be 300-3000): ${rating}`
      });
    }

    // Get player data
    console.log('Looking for player with ID:', decodedPlayerId);
    const playerRef = db.ref(`players/${decodedPlayerId}`);
    const playerSnapshot = await playerRef.once('value');
    console.log('Player snapshot exists:', playerSnapshot.exists());

    if (!playerSnapshot.exists()) {
      console.log('Player not found, checking all players...');
      // Debug: Check if player exists with different ID format
      const allPlayersRef = db.ref('players');
      const allPlayersSnapshot = await allPlayersRef.once('value');
      const allPlayers = allPlayersSnapshot.val() || {};
      console.log('All player keys:', Object.keys(allPlayers));
      console.log('Looking for player with username...');

      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: `Player not found with ID: ${playerId}`
      });
    }

    const playerData = playerSnapshot.val();
    console.log('Player exists:', playerSnapshot.exists());
    console.log('Player data:', playerData);
    const gamemodeRatings = playerData.gamemodeRatings || {};
    console.log('Current gamemode ratings:', gamemodeRatings);

    // Update the specific gamemode rating
    gamemodeRatings[gamemode] = rating;
    console.log('Updated gamemode ratings:', gamemodeRatings);

    // Recalculate overall rating
    const ratings = Object.values(gamemodeRatings);
    console.log('Ratings array:', ratings);
    const overallRating = ratings.length > 0
      ? Math.round(ratings.reduce((sum, r) => sum + r, 0) / ratings.length)
      : rating;
    console.log('Calculated overall rating:', overallRating);

    // Update player
    await playerRef.update({
      gamemodeRatings,
      overallRating,
      updatedAt: new Date().toISOString()
    });

    // Log admin action
    await logAdminAction(req, req.user.uid, 'UPDATE_RATING', decodedPlayerId, {
      gamemode,
      oldRating: gamemodeRatings[gamemode] !== rating ? playerData.gamemodeRatings?.[gamemode] : null,
      newRating: rating,
      overallRating
    });

    res.json({
      success: true,
      message: `Player ${gamemode} rating set to ${rating}`,
      gamemodeRatings,
      overallRating
    });
  } catch (error) {
    console.error('Error setting player rating:', error);
    console.error('Player ID:', playerId);
    console.error('Gamemode:', gamemode);
    console.error('Rating:', rating);
    console.error('User ID:', req.user.uid);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error setting player rating',
      details: error.message
    });
  }
});

/**
 * POST /api/onboarding/complete - Mark onboarding as completed
 */
app.post('/api/onboarding/complete', verifyAuth, async (req, res) => {
  try {
    const userRef = db.ref(`users/${req.user.uid}`);
    await userRef.update({
      onboardingCompleted: true,
      onboardingCompletedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    });

    res.json({ success: true, message: 'Onboarding completed' });
  } catch (error) {
    console.error('Error completing onboarding:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error completing onboarding'
    });
  }
});

// ===== Dashboard Stats Routes =====

/**
 * GET /api/dashboard/stats - Get dashboard statistics
 */
app.get('/api/dashboard/stats', verifyAuthAndNotBanned, async (req, res) => {
  try {
    // Get queue statistics
    const queueRef = db.ref('queue');
    const queueSnapshot = await queueRef.once('value');
    const queueData = queueSnapshot.val() || {};

    // Count players queued by gamemode
    const playersQueued = {};
    const tierTestersQueued = {};

    Object.values(queueData).forEach(entry => {
      if (entry.status === 'waiting') {
        // Check if this is a tier tester (has tierTester role)
        // For now, we'll assume regular players. Tier testers would be identified differently
        const gamemode = entry.gamemode;
        if (!playersQueued[gamemode]) playersQueued[gamemode] = 0;
        playersQueued[gamemode]++;

        // Note: In a full implementation, you'd check user roles to distinguish tier testers
        // For this basic version, we'll show all queued players as regular players
      }
    });

    // Get active matches count
    const matchesRef = db.ref('matches');
    const matchesSnapshot = await matchesRef.orderByChild('status').equalTo('active').once('value');
    const activeMatches = matchesSnapshot.val() || {};
    const activeMatchesCount = Object.keys(activeMatches).length;

    // Get tier tester availability
    const availabilityRef = db.ref('testerAvailability');
    const availabilitySnapshot = await availabilityRef.once('value');
    const availabilityData = availabilitySnapshot.val() || {};

    // Count testers available by gamemode
    const testersAvailable = {};
    Object.values(availabilityData).forEach(availability => {
      if (availability.available) {
        const gamemode = availability.gamemode;
        if (!testersAvailable[gamemode]) testersAvailable[gamemode] = 0;
        testersAvailable[gamemode]++;
      }
    });

    res.json({
      playersQueued,
      testersAvailable,
      activeMatchesCount,
      totalQueuedPlayers: Object.values(playersQueued).reduce((sum, count) => sum + count, 0),
      totalAvailableTierTesters: Object.values(testersAvailable).reduce((sum, count) => sum + count, 0)
    });
  } catch (error) {
    console.error('Error getting dashboard stats:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error retrieving dashboard statistics'
    });
  }
});

// ===== Notification Settings Routes =====

// ===== Account Management Routes =====

/**
 * POST /api/account/reload-badges - Reload account badges and lock username
 */
app.post('/api/account/reload-badges', verifyAuth, verifyAdminOrTester, async (req, res) => {
  try {
    console.log('Reload badges request received');
    console.log('Auth user object:', !!req.user);
    console.log('User UID:', req.user?.uid);

    if (!req.user || !req.user.uid) {
      return res.status(401).json({
        error: true,
        code: 'AUTH_ERROR',
        message: 'User not authenticated'
      });
    }

    console.log('Reload badges request for user:', req.user.uid);
    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val();
    console.log('User profile found:', !!userProfile);

    if (!userProfile) {
      console.error('User profile not found for UID:', req.user.uid);
      return res.status(404).json({
        error: true,
        code: 'USER_NOT_FOUND',
        message: 'User profile not found'
      });
    }

    // Check if user is admin or tier tester
    if (!userProfile.admin && !userProfile.tester) {
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'Only administrators and testers can use this function'
      });
    }

    if (!userProfile.minecraftUsername) {
      return res.status(400).json({
        error: true,
        code: 'USERNAME_NOT_LINKED',
        message: 'Minecraft username must be linked first'
      });
    }

    // Find the player record for this username
    console.log('Looking for player with username:', userProfile.minecraftUsername);
    const playersRef = db.ref('players');
    const playerSnapshot = await playersRef.orderByChild('username').equalTo(userProfile.minecraftUsername).once('value');
    console.log('Player snapshot exists:', playerSnapshot.exists());

    if (!playerSnapshot.exists()) {
      console.error('Player record not found for username:', userProfile.minecraftUsername);
      return res.status(404).json({
        error: true,
        code: 'PLAYER_NOT_FOUND',
        message: 'Player record not found for linked username'
      });
    }

    const players = playerSnapshot.val();
    const playerId = Object.keys(players)[0];
    const playerRef = playersRef.child(playerId);
    const playerData = players[playerId];
    console.log('Found player data:', !!playerData);

    // Check if the account refreshing badges has admin/tiertester roles
    const accountHasAdmin = userProfile.admin || false;
    const accountHasTierTester = userProfile.tester || false;

    // Update player record with account roles if account has them
    console.log('Account has admin:', accountHasAdmin, 'tierTester:', accountHasTierTester);
    if (accountHasAdmin || accountHasTierTester) {
      const playerUpdates = {
        roles: {
          admin: accountHasAdmin,
          tierTester: accountHasTierTester
        },
        updatedAt: new Date().toISOString()
      };
      console.log('Updating player with:', playerUpdates);
      await playerRef.update(playerUpdates);
      console.log('Player update completed');
    }

    // Update user profile to lock username changes (can only be bypassed by admins)
    const updates = {
      // Lock username changes (can only be bypassed by admins)
      usernameLocked: true,
      badgesReloadedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };

    await userRef.update(updates);

    res.json({
      success: true,
      message: 'Account badges reloaded successfully. Username linking has been locked.',
      updates: updates
    });
  } catch (error) {
    console.error('Error reloading account badges:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error reloading account badges'
    });
  }
});

/**
 * POST /api/account/reload-tiers - Reload account tiers to match points
 */
app.post('/api/account/reload-tiers', verifyAuth, verifyAdminOrTester, async (req, res) => {
  try {
    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userProfile = userSnapshot.val();

    if (!userProfile) {
      return res.status(404).json({
        error: true,
        code: 'USER_NOT_FOUND',
        message: 'User profile not found'
      });
    }

    // Check if user is admin or tier tester
    if (!userProfile.admin && !userProfile.tester) {
      return res.status(403).json({
        error: true,
        code: 'PERMISSION_DENIED',
        message: 'Only administrators and testers can use this function'
      });
    }

    // Calculate total points from gamemode points
    const gamemodePoints = userProfile.gamemodePoints || {};
    const totalPoints = Object.values(gamemodePoints).reduce((sum, points) => sum + (points || 0), 0);

    // Update user profile
    const updates = {
      totalPoints: totalPoints,
      tiersReloadedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };

    await userRef.update(updates);

    res.json({
      success: true,
      message: 'Account tiers reloaded successfully.',
      totalPoints: totalPoints
    });
  } catch (error) {
    console.error('Error reloading account tiers:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error reloading account tiers'
    });
  }
});

// ===== Player Management Routes =====

/**
 * POST /api/players/update-region - Update player region
 */
app.post('/api/players/update-region', verifyAuth, async (req, res) => {
  try {
    const { username, region } = req.body;

    if (!username || !region) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Username and region are required'
      });
    }

    // Find player by username
    const playersRef = db.ref('players');
    const playerSnapshot = await playersRef.orderByChild('username').equalTo(username.trim()).once('value');

    if (!playerSnapshot.exists()) {
      return res.status(404).json({
        error: true,
        code: 'PLAYER_NOT_FOUND',
        message: 'Player not found'
      });
    }

    // Get player ID and update region
    const players = playerSnapshot.val();
    const playerId = Object.keys(players)[0];

    await playersRef.child(playerId).update({
      region: region.trim(),
      updatedAt: new Date().toISOString()
    });

    res.json({
      success: true,
      message: 'Player region updated successfully'
    });
  } catch (error) {
    console.error('Error updating player region:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error updating player region'
    });
  }
});

// ===== Player Management Routes =====

/**
 * POST /api/admin/players/:id/roles - Update player roles
 */
app.post('/api/admin/players/:id/roles', adminLimiter, verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { admin, tester } = req.body;

    if (typeof admin !== 'boolean' || typeof tester !== 'boolean') {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'admin and tester must be boolean values'
      });
    }

    // Security audit log
    console.log(`ADMIN ACTION: Updating roles for player ${id}: admin=${admin}, tester=${tester}`);

    const playerRef = db.ref(`players/${id}`);
    const snapshot = await playerRef.once('value');
    const player = snapshot.val();

    if (!player) {
      return res.status(404).json({
        error: true,
        code: 'PLAYER_NOT_FOUND',
        message: 'Player not found'
      });
    }

    const updates = {
      roles: {
        admin: admin,
        tester: tester
      },
      updatedAt: new Date().toISOString()
    };

    await playerRef.update(updates);

    // Log admin action
    await logAdminAction(req, req.user.uid, 'UPDATE_ROLES', id, {
      oldRoles: player.roles || { admin: false, tester: false },
      newRoles: updates.roles
    });

    res.json({
      success: true,
      message: 'Player roles updated successfully',
      playerId: id,
      roles: updates.roles
    });
  } catch (error) {
    console.error('Error updating player roles:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error updating player roles'
    });
  }
});

// ===== Admin Routes =====

/**
 * GET /api/admin/applications - Get all applications
 */
app.get('/api/admin/applications', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const applicationsRef = db.ref('applications');
    const snapshot = await applicationsRef.once('value');
    const applications = snapshot.val() || {};
    
    const applicationsArray = Object.keys(applications).map(key => ({
      id: key,
      ...applications[key]
    }));
    
    res.json({ applications: applicationsArray });
  } catch (error) {
    console.error('Error fetching applications:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching applications'
    });
  }
});

/**
 * POST /api/admin/applications/:id/approve - Approve application
 */
app.post('/api/admin/applications/:id/approve', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const applicationRef = db.ref(`applications/${id}`);
    const snapshot = await applicationRef.once('value');
    const application = snapshot.val();
    
    if (!application) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Application not found'
      });
    }
    
    // Update user to tier tester
    const userRef = db.ref(`users/${application.userId}`);
    await userRef.update({
      tierTester: true,
      tierTesterApprovedAt: new Date().toISOString()
    });
    
    // Update application
    await applicationRef.update({
      status: 'approved',
      approvedAt: new Date().toISOString(),
      approvedBy: req.user.uid
    });
    
    res.json({ success: true, message: 'Application approved' });
  } catch (error) {
    console.error('Error approving application:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error approving application'
    });
  }
});

/**
 * POST /api/admin/applications/:id/deny - Deny application
 */
app.post('/api/admin/applications/:id/deny', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const applicationRef = db.ref(`applications/${id}`);
    const snapshot = await applicationRef.once('value');
    const application = snapshot.val();
    
    if (!application) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Application not found'
      });
    }
    
    await applicationRef.update({
      status: 'denied',
      deniedAt: new Date().toISOString(),
      deniedBy: req.user.uid
    });
    
    res.json({ success: true, message: 'Application denied' });
  } catch (error) {
    console.error('Error denying application:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error denying application'
    });
  }
});

/**
 * GET /api/admin/blacklist - Get blacklist
 */
app.get('/api/admin/blacklist', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const blacklistRef = db.ref('blacklist');
    const snapshot = await blacklistRef.once('value');
    const blacklist = snapshot.val() || {};
    
    const blacklistArray = Object.keys(blacklist).map(key => ({
      id: key,
      ...blacklist[key]
    }));
    
    res.json({ blacklist: blacklistArray });
  } catch (error) {
    console.error('Error fetching blacklist:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching blacklist'
    });
  }
});

/**
 * POST /api/admin/blacklist - Add to blacklist
 */
app.post('/api/admin/blacklist', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { username, reason } = req.body;
    
    if (!username) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Username is required'
      });
    }
    
    const blacklistRef = db.ref('blacklist');
    const newEntryRef = blacklistRef.push();
    await newEntryRef.set({
      username: username.trim(),
      reason: reason || 'No reason provided',
      addedAt: new Date().toISOString(),
      addedBy: req.user.uid
    });
    
    res.json({ success: true, message: 'Added to blacklist', id: newEntryRef.key });
  } catch (error) {
    console.error('Error adding to blacklist:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error adding to blacklist'
    });
  }
});

/**
 * DELETE /api/admin/blacklist/:id - Remove from blacklist
 */
app.delete('/api/admin/blacklist/:id', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const blacklistRef = db.ref(`blacklist/${id}`);
    await blacklistRef.remove();
    
    res.json({ success: true, message: 'Removed from blacklist' });
  } catch (error) {
    console.error('Error removing from blacklist:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error removing from blacklist'
    });
  }
});

/**
 * GET /api/admin/users - Get all users
 */
app.get('/api/admin/users', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const usersRef = db.ref('users');
    const snapshot = await usersRef.once('value');
    const users = snapshot.val() || {};
    
    const usersArray = Object.keys(users).map(key => ({
      id: key,
      ...users[key]
    }));
    
    res.json({ users: usersArray });
  } catch (error) {
    console.error('Error fetching users:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching users'
    });
  }
});

/**
 * POST /api/admin/users/:id/tester - Set tester status
 */
app.post('/api/admin/users/:id/tester', adminLimiter, verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { status } = req.body;
    
    if (typeof status !== 'boolean') {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'status must be a boolean'
      });
    }
    
    const userRef = db.ref(`users/${id}`);
    const oldUserData = await userRef.once('value');
    const oldTesterStatus = oldUserData.val()?.tester || false;

    await userRef.update({
      tester: status,
      updatedAt: new Date().toISOString()
    });

    // Log admin action
    await logAdminAction(req, req.user.uid, 'SET_TESTER_STATUS', id, {
      oldStatus: oldTesterStatus,
      newStatus: status
    });

    res.json({ success: true, message: `Tester status set to ${status}` });
  } catch (error) {
    console.error('Error setting tester status:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error setting tester status'
    });
  }
});

/**
 * POST /api/admin/users/:id/admin - Set admin status
 */
app.post('/api/admin/users/:id/admin', adminLimiter, verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { status } = req.body;
    
    if (typeof status !== 'boolean') {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'status must be a boolean'
      });
    }
    
    // Prevent removing your own admin status
    if (id === req.user.uid && status === false) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'You cannot remove your own admin status'
      });
    }
    
    const userRef = db.ref(`users/${id}`);
    const oldUserData = await userRef.once('value');
    const oldAdminStatus = oldUserData.val()?.admin || false;

    await userRef.update({
      admin: status,
      updatedAt: new Date().toISOString()
    });

    // Log admin action
    await logAdminAction(req, req.user.uid, 'SET_ADMIN_STATUS', id, {
      oldStatus: oldAdminStatus,
      newStatus: status
    });

    res.json({ success: true, message: `Admin status set to ${status}` });
  } catch (error) {
    console.error('Error setting admin status:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error setting admin status'
    });
  }
});

/**
 * POST /api/admin/players/:id/tier - Force set tier for a player
 */
app.post('/api/admin/players/:id/tier', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const { gamemode, tier } = req.body;
    
    if (!gamemode || !tier) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'gamemode and tier are required'
      });
    }
    
    // Validate tier
    const validTiers = ['HT1', 'LT1', 'HT2', 'LT2', 'HT3', 'LT3', 'HT4', 'LT4', 'HT5', 'LT5'];
    if (!validTiers.includes(tier)) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: `tier must be one of: ${validTiers.join(', ')}`
      });
    }
    
    // Get player
    const playerRef = db.ref(`players/${id}`);
    const snapshot = await playerRef.once('value');
    const player = snapshot.val();
    
    if (!player) {
      return res.status(404).json({
        error: true,
        code: 'NOT_FOUND',
        message: 'Player not found'
      });
    }
    
    // Get tier points
    const tierPoints = CONFIG.TIERS[tier]?.points || 0;
    const currentTier = player.gamemodeTiers?.[gamemode];
    const currentPoints = player.gamemodePoints?.[gamemode] || 0;
    const currentTierPoints = currentTier ? (CONFIG.TIERS[currentTier]?.points || 0) : 0;
    
    // Calculate point difference
    const pointsDiff = tierPoints - currentTierPoints;
    const newGamemodePoints = currentPoints + pointsDiff;
    const newTotalPoints = (player.totalPoints || 0) + pointsDiff;
    
    // Update player
    const updates = {};
    updates[`gamemodeTiers/${gamemode}`] = tier;
    updates[`gamemodePoints/${gamemode}`] = newGamemodePoints;
    updates.totalPoints = newTotalPoints;
    updates[`lastTested/${gamemode}`] = new Date().toISOString();
    updates.lastAdminUpdate = new Date().toISOString();
    updates.lastAdminUpdateBy = req.user.uid;
    
    await playerRef.update(updates);
    
    res.json({ 
      success: true, 
      message: `Tier set to ${tier} for ${gamemode}`,
      player: { ...player, ...updates }
    });
  } catch (error) {
    console.error('Error setting tier:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error setting tier'
    });
  }
});

// ===== Ban Management Endpoints =====

// Ban account
app.post('/api/admin/ban', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { identifier, duration, reason } = req.body;

    if (!identifier) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_IDENTIFIER',
        message: 'Account identifier (email or Firebase UID) is required'
      });
    }

    if (!duration) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_DURATION',
        message: 'Ban duration is required'
      });
    }

    // Find user by email or Firebase UID
    let userQuery = db.ref('users').orderByChild('email').equalTo(identifier);
    let userSnapshot = await userQuery.once('value');
    let userData = userSnapshot.val();
    let firebaseUid = null;

    if (!userData) {
      // Try to find by Firebase UID
      const userRef = db.ref(`users/${identifier}`);
      const userSnapshot = await userRef.once('value');
      userData = userSnapshot.val();
      firebaseUid = identifier;
    } else {
      // Get the Firebase UID from the found user
      firebaseUid = Object.keys(userData)[0];
      userData = userData[firebaseUid];
    }

    if (!userData) {
      return res.status(404).json({
        error: true,
        code: 'USER_NOT_FOUND',
        message: 'User not found'
      });
    }

    // Check if already banned
    if (userData.banned) {
      return res.status(400).json({
        error: true,
        code: 'ALREADY_BANNED',
        message: 'User is already banned'
      });
    }

    // Calculate ban expiration
    let banExpires = null;
    if (duration !== 'permanent') {
      const now = new Date();
      const durationMatch = duration.match(/^(\d+)([hd])$/);
      if (durationMatch) {
        const value = parseInt(durationMatch[1]);
        const unit = durationMatch[2];
        if (unit === 'h') {
          banExpires = new Date(now.getTime() + value * 60 * 60 * 1000);
        } else if (unit === 'd') {
          banExpires = new Date(now.getTime() + value * 24 * 60 * 60 * 1000);
        }
      }
    }

    // Update user with ban information
    const banData = {
      banned: true,
      bannedAt: new Date().toISOString(),
      bannedBy: req.user.uid,
      banExpires: banExpires ? banExpires.toISOString() : 'permanent',
      banReason: reason || null
    };

    await db.ref(`users/${firebaseUid}`).update(banData);

    // Log the ban action
    await logAdminAction(req, req.user.uid, 'BAN_USER', firebaseUid, {
      reason: reason || 'No reason provided',
      duration: banExpires ? banExpires.toISOString() : 'permanent',
      isPermanent: duration === 'permanent'
    });

    res.json({
      success: true,
      message: 'Account banned successfully',
      bannedAccount: {
        firebaseUid,
        email: userData.email,
        ...banData
      }
    });

  } catch (error) {
    console.error('Error banning account:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error banning account'
    });
  }
});

// Unban account
/**
 * POST /api/admin/warn/:firebaseUid - Warn a user
 */
app.post('/api/admin/warn/:firebaseUid', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { firebaseUid } = req.params;
    const { reason, severity = 'warning' } = req.body;

    if (!reason || !reason.trim()) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_REASON',
        message: 'Warning reason is required'
      });
    }

    const userRef = db.ref(`users/${firebaseUid}`);
    const userSnapshot = await userRef.once('value');
    const userData = userSnapshot.val();

    if (!userData) {
      return res.status(404).json({
        error: true,
        code: 'USER_NOT_FOUND',
        message: 'User not found'
      });
    }

    // Add warning to user's warnings array
    const warnings = userData.warnings || [];
    const newWarning = {
      id: Date.now().toString(),
      reason: reason.trim(),
      severity: severity,
      issuedBy: req.user.uid,
      issuedAt: new Date().toISOString(),
      acknowledged: false
    };

    warnings.push(newWarning);

    await userRef.update({
      warnings: warnings,
      updatedAt: new Date().toISOString()
    });

    console.log(`ADMIN ACTION: Warning issued to user ${firebaseUid}`);

    res.json({
      success: true,
      message: 'Warning issued successfully',
      warning: newWarning
    });

  } catch (error) {
    console.error('Error issuing warning:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error issuing warning'
    });
  }
});

/**
 * POST /api/auth/acknowledge-warning - Acknowledge a warning
 */
app.post('/api/auth/acknowledge-warning', verifyAuth, async (req, res) => {
  try {
    const { warningId } = req.body;

    if (!warningId) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_WARNING_ID',
        message: 'Warning ID is required'
      });
    }

    const userRef = db.ref(`users/${req.user.uid}`);
    const userSnapshot = await userRef.once('value');
    const userData = userSnapshot.val();

    if (!userData || !userData.warnings) {
      return res.status(404).json({
        error: true,
        code: 'WARNING_NOT_FOUND',
        message: 'Warning not found'
      });
    }

    // Find and acknowledge the warning
    const warnings = userData.warnings.map(warning => {
      if (warning.id === warningId && warning.acknowledged === false) {
        return { ...warning, acknowledged: true, acknowledgedAt: new Date().toISOString() };
      }
      return warning;
    });

    await userRef.update({
      warnings: warnings,
      updatedAt: new Date().toISOString()
    });

    res.json({
      success: true,
      message: 'Warning acknowledged'
    });

  } catch (error) {
    console.error('Error acknowledging warning:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error acknowledging warning'
    });
  }
});

app.post('/api/admin/unban/:firebaseUid', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { firebaseUid } = req.params;

    const userRef = db.ref(`users/${firebaseUid}`);
    const userSnapshot = await userRef.once('value');
    const userData = userSnapshot.val();

    if (!userData) {
      return res.status(404).json({
        error: true,
        code: 'USER_NOT_FOUND',
        message: 'User not found'
      });
    }

    if (!userData.banned) {
      return res.status(400).json({
        error: true,
        code: 'NOT_BANNED',
        message: 'User is not banned'
      });
    }

    // Remove ban fields
    const updates = {
      banned: null,
      bannedAt: null,
      bannedBy: null,
      banExpires: null,
      banReason: null
    };

    await userRef.update(updates);

    // Log the unban action
    await logAdminAction(req, req.user.uid, 'UNBAN_USER', firebaseUid, {
      previousReason: userData.banReason,
      previousExpiry: userData.banExpires
    });

    res.json({
      success: true,
      message: 'Account unbanned successfully',
      unbannedAccount: {
        firebaseUid,
        email: userData.email
      }
    });

  } catch (error) {
    console.error('Error unbanning account:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error unbanning account'
    });
  }
});

// Get all banned accounts
app.get('/api/admin/banned', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const usersRef = db.ref('users');
    const usersSnapshot = await usersRef.once('value');
    const allUsers = usersSnapshot.val() || {};

    const bannedAccounts = [];

    for (const [firebaseUid, userData] of Object.entries(allUsers)) {
      if (userData.banned) {
        bannedAccounts.push({
          firebaseUid,
          email: userData.email,
          bannedAt: userData.bannedAt,
          banExpires: userData.banExpires,
          banReason: userData.banReason,
          bannedBy: userData.bannedBy
        });
      }
    }

    res.json({
      success: true,
      bannedAccounts
    });

  } catch (error) {
    console.error('Error fetching banned accounts:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching banned accounts'
    });
  }
});

// Search banned accounts
app.get('/api/admin/banned/search', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { q: searchTerm } = req.query;

    if (!searchTerm) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_SEARCH_TERM',
        message: 'Search term is required'
      });
    }

    const usersRef = db.ref('users');
    const usersSnapshot = await usersRef.once('value');
    const allUsers = usersSnapshot.val() || {};

    const bannedAccounts = [];

    for (const [firebaseUid, userData] of Object.entries(allUsers)) {
      if (userData.banned) {
        // Check if search term matches email, Firebase UID, or ban reason
        const matchesSearch =
          userData.email?.toLowerCase().includes(searchTerm.toLowerCase()) ||
          firebaseUid.toLowerCase().includes(searchTerm.toLowerCase()) ||
          userData.banReason?.toLowerCase().includes(searchTerm.toLowerCase());

        if (matchesSearch) {
          bannedAccounts.push({
            firebaseUid,
            email: userData.email,
            bannedAt: userData.bannedAt,
            banExpires: userData.banExpires,
            banReason: userData.banReason,
            bannedBy: userData.bannedBy
          });
        }
      }
    }

    res.json({
      success: true,
      bannedAccounts
    });

  } catch (error) {
    console.error('Error searching banned accounts:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error searching banned accounts'
    });
  }
});

// ===== Alt Detection Management Endpoints =====

// Get reported accounts
app.get('/api/admin/alt-reports', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const reportsRef = db.ref('altReports');
    const reportsSnapshot = await reportsRef.once('value');
    const reports = reportsSnapshot.val() || {};

    const reportedAccounts = Object.keys(reports).map(id => ({
      id,
      ...reports[id]
    }));

    res.json({
      success: true,
      reportedAccounts
    });

  } catch (error) {
    console.error('Error fetching alt reports:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching alt reports'
    });
  }
});

// Move report to judgment day (deletes the report)
app.post('/api/admin/alt-reports/:reportId/judgment-day', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { reportId } = req.params;
    const reportRef = db.ref(`altReports/${reportId}`);

    const reportSnapshot = await reportRef.once('value');
    const report = reportSnapshot.val();

    if (!report) {
      return res.status(404).json({
        error: true,
        code: 'REPORT_NOT_FOUND',
        message: 'Report not found'
      });
    }

    if (report.status === 'judgment-day') {
      return res.status(400).json({
        error: true,
        code: 'ALREADY_IN_JUDGMENT',
        message: 'Report is already in judgment day'
      });
    }

    // Create judgment day entry with report data
    const judgmentDayRef = db.ref('judgmentDay').push();
    await judgmentDayRef.set({
      ...report,
      status: 'judgment-day',
      movedToJudgmentBy: req.user.uid,
      movedToJudgmentAt: new Date().toISOString(),
      originalReportId: reportId
    });

    // Delete the original report
    await reportRef.remove();

    res.json({
      success: true,
      message: 'Report moved to judgment day and removed from reports'
    });

  } catch (error) {
    console.error('Error moving report to judgment day:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error moving report to judgment day'
    });
  }
});

// Remove alt report
app.delete('/api/admin/alt-reports/:reportId', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { reportId } = req.params;
    const reportRef = db.ref(`altReports/${reportId}`);

    const reportSnapshot = await reportRef.once('value');
    const report = reportSnapshot.val();

    if (!report) {
      return res.status(404).json({
        error: true,
        code: 'REPORT_NOT_FOUND',
        message: 'Report not found'
      });
    }

    await reportRef.remove();

    res.json({
      success: true,
      message: 'Report removed'
    });

  } catch (error) {
    console.error('Error removing alt report:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error removing alt report'
    });
  }
});

// Get judgment day accounts
app.get('/api/admin/judgment-day', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const judgmentDayRef = db.ref('judgmentDay');
    const judgmentDaySnapshot = await judgmentDayRef.once('value');
    const judgmentDayAccounts = judgmentDaySnapshot.val() || {};

    const accounts = Object.keys(judgmentDayAccounts).map(id => ({
      id,
      ...judgmentDayAccounts[id]
    }));

    res.json({
      success: true,
      judgmentDayAccounts: accounts
    });

  } catch (error) {
    console.error('Error fetching judgment day accounts:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching judgment day accounts'
    });
  }
});

// Execute judgment day - ban all accounts
app.post('/api/admin/judgment-day/execute', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const judgmentDayRef = db.ref('judgmentDay');
    const judgmentDaySnapshot = await judgmentDayRef.once('value');
    const judgmentDayAccounts = judgmentDaySnapshot.val() || {};

    const accounts = Object.keys(judgmentDayAccounts).map(id => ({
      id,
      ...judgmentDayAccounts[id]
    }));

    if (accounts.length === 0) {
      return res.status(400).json({
        error: true,
        code: 'NO_ACCOUNTS',
        message: 'No accounts in judgment day'
      });
    }

    const results = [];
    let bannedCount = 0;
    let blacklistedCount = 0;

    for (const report of accounts) {
      try {
        const processedAccounts = [];

        // Ban primary account
        const primaryRef = db.ref(`users/${report.primaryAccount}`);
        const primarySnapshot = await primaryRef.once('value');
        const primaryData = primarySnapshot.val();

        if (primaryData && !primaryData.banned) {
          await primaryRef.update({
            banned: true,
            bannedAt: new Date().toISOString(),
            bannedBy: req.user.uid,
            banExpires: 'permanent',
            banReason: `Alt account group - ${report.detectionReason} (Flagged ${report.flagCount} times)`
          });
          bannedCount++;
          processedAccounts.push({
            uid: report.primaryAccount,
            email: primaryData.email,
            type: 'primary'
          });
        }

        // Blacklist primary Minecraft username if exists
        if (primaryData?.minecraftUsername) {
          const blacklistRef = db.ref('blacklist').push();
          await blacklistRef.set({
            username: primaryData.minecraftUsername,
            reason: `Alt account group - ${report.detectionReason}`,
            addedAt: new Date().toISOString(),
            addedBy: req.user.uid
          });
          blacklistedCount++;
        }

        // Ban all suspicious accounts
        if (report.suspiciousAccounts) {
          for (const suspicious of report.suspiciousAccounts) {
            const suspiciousRef = db.ref(`users/${suspicious.uid}`);
            const suspiciousSnapshot = await suspiciousRef.once('value');
            const suspiciousData = suspiciousSnapshot.val();

            if (suspiciousData && !suspiciousData.banned) {
              await suspiciousRef.update({
                banned: true,
                bannedAt: new Date().toISOString(),
                bannedBy: req.user.uid,
                banExpires: 'permanent',
                banReason: `Alt account group - ${report.detectionReason} (Flagged ${report.flagCount} times)`
              });
              bannedCount++;
              processedAccounts.push({
                uid: suspicious.uid,
                email: suspicious.email,
                type: 'suspicious'
              });
            }

            // Blacklist suspicious Minecraft username
            if (suspicious.minecraftUsername) {
              const blacklistRef = db.ref('blacklist').push();
              await blacklistRef.set({
                username: suspicious.minecraftUsername,
                reason: `Alt account group - ${report.detectionReason}`,
                addedAt: new Date().toISOString(),
                addedBy: req.user.uid
              });
              blacklistedCount++;
            }
          }
        }

        // Remove from judgment day (delete the entry since it's executed)
        await judgmentDayRef.child(report.id).remove();

        results.push({
          groupId: report.groupId,
          flagCount: report.flagCount,
          accountsProcessed: processedAccounts.length,
          primaryAccount: report.primaryAccount,
          suspiciousAccountsCount: report.suspiciousAccounts?.length || 0,
          status: 'success'
        });

      } catch (error) {
        console.error('Error processing alt group', report.groupId, ':', error);
        results.push({
          groupId: report.groupId,
          status: 'error',
          error: error.message
        });
      }
    }

    res.json({
      success: true,
      message: `Judgment day executed. ${bannedCount} accounts banned, ${blacklistedCount} usernames blacklisted.`,
      stats: {
        totalAccounts: judgmentDayAccounts.length,
        bannedCount,
        blacklistedCount,
        processedCount: results.length
      },
      results
    });

  } catch (error) {
    console.error('Error executing judgment day:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error executing judgment day'
    });
  }
});

/**
 * GET /api/admin/stats - Get system statistics
 */
app.get('/api/admin/stats', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    // Get active matches count
    const activeMatchesRef = db.ref('matches');
    const activeMatchesSnapshot = await activeMatchesRef
      .orderByChild('status')
      .equalTo('active')
      .once('value');

    const activeMatches = activeMatchesSnapshot.val() || {};
    const activeMatchesCount = Object.keys(activeMatches).length;

    // Get total players count
    const playersRef = db.ref('players');
    const playersSnapshot = await playersRef.once('value');
    const players = playersSnapshot.val() || {};
    const totalPlayers = Object.keys(players).length;

    // Get queued players count
    const queueRef = db.ref('queue');
    const queueSnapshot = await queueRef.once('value');
    const queue = queueSnapshot.val() || {};
    const queuedPlayers = Object.keys(queue).length;

    res.json({
      success: true,
      stats: {
        activeMatches: activeMatchesCount,
        totalPlayers: totalPlayers,
        queuedPlayers: queuedPlayers,
        matchCapacity: 100,
        matchUtilizationPercent: Math.round((activeMatchesCount / 100) * 100)
      }
    });

  } catch (error) {
    console.error('Error getting admin stats:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error retrieving system statistics'
    });
  }
});

/**
 * POST /api/admin/manual-rating-update - Manually update player ratings
 */
app.post('/api/admin/manual-rating-update', adminLimiter, verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { player1Username, player2Username, player1Score, player2Score, gamemode } = req.body;

    if (!player1Username || !player2Username || player1Score === undefined || player2Score === undefined || !gamemode) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'player1Username, player2Username, player1Score, player2Score, and gamemode are required'
      });
    }

    if (typeof player1Score !== 'number' || typeof player2Score !== 'number') {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Scores must be numbers'
      });
    }

    if (player1Score < 0 || player2Score < 0) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: 'Scores cannot be negative'
      });
    }

    // Validate gamemode
    const validGamemode = CONFIG.GAMEMODES.find(g => g.id === gamemode);
    if (!validGamemode) {
      return res.status(400).json({
        error: true,
        code: 'VALIDATION_ERROR',
        message: `Invalid gamemode: ${gamemode}`
      });
    }

    // Find players by username
    const playersRef = db.ref('players');
    const playersSnapshot = await playersRef.once('value');
    const allPlayers = playersSnapshot.val() || {};

    let player1Data = null;
    let player2Data = null;
    let player1Id = null;
    let player2Id = null;

    for (const [id, player] of Object.entries(allPlayers)) {
      if (player.username === player1Username) {
        player1Data = player;
        player1Id = id;
      }
      if (player.username === player2Username) {
        player2Data = player;
        player2Id = id;
      }
    }

    // Create players if they don't exist
    if (!player1Data) {
      console.log(`Creating player ${player1Username} for manual rating update`);
      const newPlayerRef = playersRef.push();
      player1Id = newPlayerRef.key;
      const dummyUserId = `manual-${player1Id}`;
      player1Data = {
        username: player1Username,
        userId: dummyUserId,
        gamemodeRatings: {},
        glicko2Params: {},
        createdAt: new Date().toISOString(),
        region: 'unknown' // Default region
      };
      await newPlayerRef.set(player1Data);
    }

    if (!player2Data) {
      console.log(`Creating player ${player2Username} for manual rating update`);
      const newPlayerRef = playersRef.push();
      player2Id = newPlayerRef.key;
      const dummyUserId = `manual-${player2Id}`;
      player2Data = {
        username: player2Username,
        userId: dummyUserId,
        gamemodeRatings: {},
        glicko2Params: {},
        createdAt: new Date().toISOString(),
        region: 'unknown' // Default region
      };
      await newPlayerRef.set(player2Data);
    }

    // Get current ratings and Glicko-2 data
    const player1Rating = player1Data.gamemodeRatings?.[gamemode] || 1000;
    const player2Rating = player2Data.gamemodeRatings?.[gamemode] || 1000;

    const player1Glicko2 = player1Data.glicko2Params?.[gamemode] || {
      rd: GLICKO2_DEFAULT_RD,
      volatility: GLICKO2_DEFAULT_VOLATILITY
    };
    const player2Glicko2 = player2Data.glicko2Params?.[gamemode] || {
      rd: GLICKO2_DEFAULT_RD,
      volatility: GLICKO2_DEFAULT_VOLATILITY
    };

    // Determine Glicko-2 scores
    let player1Glicko2Score, player2Glicko2Score;
    if (player1Score > player2Score) {
      player1Glicko2Score = 1; // Player 1 wins
      player2Glicko2Score = 0; // Player 2 loses
    } else if (player1Score < player2Score) {
      player1Glicko2Score = 0; // Player 1 loses
      player2Glicko2Score = 1; // Player 2 wins
    } else {
      player1Glicko2Score = 0.5; // Draw
      player2Glicko2Score = 0.5; // Draw
    }

    // Calculate Glicko-2 rating changes
    const player1Obj = {
      rating: player1Rating,
      rd: player1Glicko2.rd,
      volatility: player1Glicko2.volatility
    };
    const player2Obj = {
      rating: player2Rating,
      rd: player2Glicko2.rd,
      volatility: player2Glicko2.volatility
    };

    const player1Result = calculateGlicko2Change(player1Obj, player2Obj, player1Glicko2Score);
    const player2Result = calculateGlicko2Change(player2Obj, player1Obj, player2Glicko2Score);

    // Update ratings using centralized function
    await updatePlayerRating(player1Data.userId, gamemode, player1Result.ratingChange, player1Result.newRating, player1Result.newRD, player1Result.newVolatility);
    await updatePlayerRating(player2Data.userId, gamemode, player2Result.ratingChange, player2Result.newRating, player2Result.newRD, player2Result.newVolatility);

    res.json({
      success: true,
      message: 'Player ratings updated successfully',
      results: {
        player1: {
          username: player1Username,
          oldRating: player1Rating,
          newRating: player1Result.newRating,
          ratingChange: player1Result.ratingChange
        },
        player2: {
          username: player2Username,
          oldRating: player2Rating,
          newRating: player2Result.newRating,
          ratingChange: player2Result.ratingChange
        },
        gamemode,
        score: `${player1Score} - ${player2Score}`
      }
    });

  } catch (error) {
    console.error('Error updating player ratings manually:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error updating player ratings'
    });
  }
});

// Add account to alt whitelist
app.post('/api/admin/alt-whitelist', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { identifier } = req.body; // email or firebase UID

    if (!identifier) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_IDENTIFIER',
        message: 'Account identifier (email or Firebase UID) is required'
      });
    }

    // Find user
    const usersRef = db.ref('users');
    const usersSnapshot = await usersRef.once('value');
    const allUsers = usersSnapshot.val() || {};

    let targetUid = null;
    for (const [uid, userData] of Object.entries(allUsers)) {
      if (userData.email === identifier || uid === identifier) {
        targetUid = uid;
        break;
      }
    }

    if (!targetUid) {
      return res.status(404).json({
        error: true,
        code: 'USER_NOT_FOUND',
        message: 'User not found'
      });
    }

    // Add to whitelist
    const whitelistRef = db.ref(`altWhitelist/${targetUid}`);
    await whitelistRef.set({
      whitelistedAt: new Date().toISOString(),
      whitelistedBy: req.user.uid,
      email: allUsers[targetUid].email
    });

    res.json({
      success: true,
      message: 'Account added to alt detection whitelist'
    });

  } catch (error) {
    console.error('Error adding to whitelist:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error adding to whitelist'
    });
  }
});

// Remove account from alt whitelist
app.delete('/api/admin/alt-whitelist/:firebaseUid', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { firebaseUid } = req.params;
    const whitelistRef = db.ref(`altWhitelist/${firebaseUid}`);

    await whitelistRef.remove();

    res.json({
      success: true,
      message: 'Account removed from alt detection whitelist'
    });

  } catch (error) {
    console.error('Error removing from whitelist:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error removing from whitelist'
    });
  }
});

// Search alt reports
app.get('/api/admin/alt-reports/search', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { q: searchTerm } = req.query;

    if (!searchTerm) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_SEARCH_TERM',
        message: 'Search term is required'
      });
    }

    const reportsRef = db.ref('altReports');
    const reportsSnapshot = await reportsRef.once('value');
    const allReports = reportsSnapshot.val() || {};

    const matchingReports = Object.keys(allReports)
      .filter(id => {
        const report = allReports[id];
        const searchLower = searchTerm.toLowerCase();

        // Search in group ID, primary account, emails, UIDs
        return report.groupId?.toLowerCase().includes(searchLower) ||
               report.primaryAccount?.toLowerCase().includes(searchLower) ||
               report.suspiciousAccounts?.some(acc =>
                 acc.email?.toLowerCase().includes(searchLower) ||
                 acc.uid?.toLowerCase().includes(searchLower)
               );
      })
      .map(id => ({
        id,
        ...allReports[id]
      }));

    res.json({
      success: true,
      reportedAccounts: matchingReports
    });

  } catch (error) {
    console.error('Error searching alt reports:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error searching alt reports'
    });
  }
});

// Get alt whitelist
app.get('/api/admin/alt-whitelist', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const whitelistRef = db.ref('altWhitelist');
    const whitelistSnapshot = await whitelistRef.once('value');
    const whitelist = whitelistSnapshot.val() || {};

    const whitelistedAccounts = Object.keys(whitelist).map(uid => ({
      firebaseUid: uid,
      ...whitelist[uid]
    }));

    res.json({
      success: true,
      whitelistedAccounts
    });

  } catch (error) {
    console.error('Error fetching whitelist:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error fetching whitelist'
    });
  }
});

// Search blacklist
app.get('/api/admin/blacklist/search', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { q: searchTerm } = req.query;

    if (!searchTerm) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_SEARCH_TERM',
        message: 'Search term is required'
      });
    }

    const blacklistRef = db.ref('blacklist');
    const blacklistSnapshot = await blacklistRef.once('value');
    const blacklist = blacklistSnapshot.val() || {};

    const matchingEntries = [];

    for (const [id, entry] of Object.entries(blacklist)) {
      if (entry.username?.toLowerCase().includes(searchTerm.toLowerCase())) {
        matchingEntries.push({
          id,
          username: entry.username,
          reason: entry.reason,
          addedAt: entry.addedAt,
          addedBy: entry.addedBy
        });
      }
    }

    res.json({
      success: true,
      blacklist: matchingEntries
    });

  } catch (error) {
    console.error('Error searching blacklist:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error searching blacklist'
    });
  }
});

// Search users
app.get('/api/admin/users/search', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { q: searchTerm } = req.query;

    if (!searchTerm) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_SEARCH_TERM',
        message: 'Search term is required'
      });
    }

    const usersRef = db.ref('users');
    const usersSnapshot = await usersRef.once('value');
    const allUsers = usersSnapshot.val() || {};

    const matchingUsers = [];

    for (const [firebaseUid, userData] of Object.entries(allUsers)) {
      // Check if search term matches email, Firebase UID, or Minecraft username
      const matchesSearch =
        userData.email?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        firebaseUid.toLowerCase().includes(searchTerm.toLowerCase()) ||
        userData.minecraftUsername?.toLowerCase().includes(searchTerm.toLowerCase());

      if (matchesSearch) {
        matchingUsers.push({
          id: firebaseUid,
          email: userData.email,
          minecraftUsername: userData.minecraftUsername,
          admin: userData.admin || false,
          tierTester: userData.tierTester || false,
          banned: userData.banned || false
        });
      }
    }

    res.json({
      success: true,
      users: matchingUsers
    });

  } catch (error) {
    console.error('Error searching users:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error searching users'
    });
  }
});

// Search players
app.get('/api/admin/players/search', verifyAuth, verifyAdmin, async (req, res) => {
  try {
    const { q: searchTerm } = req.query;

    if (!searchTerm) {
      return res.status(400).json({
        error: true,
        code: 'MISSING_SEARCH_TERM',
        message: 'Search term is required'
      });
    }

    const playersRef = db.ref('players');
    const playersSnapshot = await playersRef.once('value');
    const allPlayers = playersSnapshot.val() || {};

    const matchingPlayers = [];

    for (const [playerId, playerData] of Object.entries(allPlayers)) {
      if (playerData.username?.toLowerCase().includes(searchTerm.toLowerCase())) {
        matchingPlayers.push({
          id: playerId,
          username: playerData.username,
          totalPoints: playerData.totalPoints || 0,
          gamemodeTiers: playerData.gamemodeTiers || {},
          blacklisted: playerData.blacklisted || false,
          region: playerData.region
        });
      }
    }

    res.json({
      success: true,
      players: matchingPlayers
    });

  } catch (error) {
    console.error('Error searching players:', error);
    res.status(500).json({
      error: true,
      code: 'SERVER_ERROR',
      message: 'Error searching players'
    });
  }
});

// Error handling middleware (must be last)
let errorHandler, notFoundHandler;
try {
  const errorHandlers = require('./middleware/error-handler');
  errorHandler = errorHandlers.errorHandler;
  notFoundHandler = errorHandlers.notFoundHandler;
} catch (e) {
  // Fallback if middleware file doesn't exist
  notFoundHandler = (req, res) => {
    res.status(404).json({
      error: true,
      code: 'NOT_FOUND',
      message: `Route ${req.method} ${req.path} not found`
    });
  };
  errorHandler = (err, req, res, next) => {
    console.error('Error:', err);
    const status = err.status || err.statusCode || 500;
    const message = err.message || 'Internal Server Error';
    const code = err.code || 'SERVER_ERROR';
    res.status(status).json({
      error: true,
      code,
      message: config.nodeEnv === 'development' ? message : 'An error occurred'
    });
  };
}

app.use(notFoundHandler);
app.use(errorHandler);

// Start server
app.listen(PORT, () => {
  console.log(`MC Leaderboards API server running on port ${PORT}`);
  console.log(`Environment: ${config.nodeEnv}`);
  console.log(`Health check: http://localhost:${PORT}/api/health`);
});

