// MC Leaderboards - Cleanup Old Matches Script
// Removes matches older than 1 week, runs every 48 hours

const admin = require('firebase-admin');
const { loadRuntimeConfig } = require('../config');

const { serviceAccount, config } = loadRuntimeConfig();

// Initialize Firebase Admin
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: config.databaseURL
});

const db = admin.database();

/**
 * Cleanup old matches (older than 1 week)
 */
async function cleanupMatches() {
  console.log('🧹 Starting cleanup of old matches...');

  try {
    const matchesRef = db.ref('matches');
    const snapshot = await matchesRef.once('value');
    const matches = snapshot.val() || {};

    const now = new Date();
    const oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); // 7 days ago

    let cleaned = 0;

    for (const matchId in matches) {
      const match = matches[matchId];

      // Clean up matches that are:
      // 1. Ended status AND older than 1 week, OR
      // 2. Any status AND older than 2 weeks (stuck matches)
      const createdAt = new Date(match.createdAt);
      const shouldClean = (match.status === 'ended' && createdAt < oneWeekAgo) ||
                         (createdAt < new Date(now.getTime() - 14 * 24 * 60 * 60 * 1000)); // 2 weeks

      if (shouldClean) {
        await matchesRef.child(matchId).remove();
        cleaned++;
        console.log(`  ✓ Removed match: ${matchId} (${match.status}) - Created: ${match.createdAt}`);
      }
    }

    console.log(`\n✅ Cleanup completed! Removed ${cleaned} old/stuck matches.`);
    return cleaned;
  } catch (error) {
    console.error('❌ Error cleaning up matches:', error);
    throw error;
  }
}

/**
 * Cleanup expired tier tester availabilities (older than 30 minutes)
 */
async function cleanupExpiredTesterAvailabilities() {
  console.log('🧹 Starting cleanup of expired tier tester availabilities...');

  try {
    const availabilityRef = db.ref('testerAvailability');
    const snapshot = await availabilityRef.once('value');
    const availabilities = snapshot.val() || {};

    const now = new Date();
    let cleaned = 0;

    for (const userId in availabilities) {
      const availability = availabilities[userId];

      if (availability.timeoutAt) {
        const timeoutAt = new Date(availability.timeoutAt);

        if (timeoutAt < now) {
          await availabilityRef.child(userId).remove();
          cleaned++;
          console.log(`  ✓ Removed expired availability for user: ${userId} (timed out at: ${availability.timeoutAt})`);
        }
      }
    }

    console.log(`\n✅ Tester availability cleanup completed! Removed ${cleaned} expired availabilities.`);
    return cleaned;
  } catch (error) {
    console.error('❌ Error cleaning up tester availabilities:', error);
    throw error;
  }
}

/**
 * Run cleanup every 48 hours
 */
async function startScheduledCleanup() {
  console.log('🔄 Starting scheduled match cleanup (runs every 48 hours)...');

  // Run initial cleanup
  try {
    await cleanupMatches();
    await cleanupExpiredTesterAvailabilities();
  } catch (error) {
    console.error('❌ Initial cleanup failed:', error);
  }

  // Schedule to run every 48 hours (48 * 60 * 60 * 1000 ms)
  const INTERVAL_MS = 48 * 60 * 60 * 1000;

  setInterval(async () => {
    console.log(`\n⏰ Running scheduled cleanup at ${new Date().toISOString()}`);
    try {
      await cleanupMatches();
      await cleanupExpiredTesterAvailabilities();
    } catch (error) {
      console.error('❌ Scheduled cleanup failed:', error);
    }
  }, INTERVAL_MS);

  console.log(`✅ Scheduled cleanup initialized. Next run in ${Math.round(INTERVAL_MS / (1000 * 60 * 60))} hours.`);
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n🛑 Received SIGINT, shutting down gracefully...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\n🛑 Received SIGTERM, shutting down gracefully...');
  process.exit(0);
});

// Start the scheduled cleanup
startScheduledCleanup().catch(error => {
  console.error('❌ Failed to start scheduled cleanup:', error);
  process.exit(1);
});

