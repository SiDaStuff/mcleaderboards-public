// MC Leaderboards - Database Restore Script
// Restores database from a backup file

const admin = require('firebase-admin');
const { loadRuntimeConfig } = require('../config');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const { serviceAccount, config } = loadRuntimeConfig();

// Initialize Firebase Admin
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: config.databaseURL
});

const db = admin.database();

/**
 * Restore database from backup
 */
async function restoreDatabase(backupFile) {
  try {
    // Check if backup file exists
    if (!fs.existsSync(backupFile)) {
      console.error(`Error: Backup file not found: ${backupFile}`);
      process.exit(1);
    }
    
    // Read backup file
    console.log(`Reading backup file: ${backupFile}...`);
    const backupData = JSON.parse(fs.readFileSync(backupFile, 'utf8'));
    
    // Confirm restoration
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout
    });
    
    const answer = await new Promise(resolve => {
      rl.question('\n⚠️  WARNING: This will overwrite all existing data!\nAre you sure you want to continue? (yes/no): ', resolve);
    });
    
    rl.close();
    
    if (answer.toLowerCase() !== 'yes') {
      console.log('Restore cancelled.');
      process.exit(0);
    }
    
    // Restore data
    console.log('\nRestoring database...');
    await db.ref('/').set(backupData);
    
    console.log('✓ Database restored successfully!');
    process.exit(0);
  } catch (error) {
    console.error('Error restoring database:', error);
    process.exit(1);
  }
}

// Get backup file from command line argument
const backupFile = process.argv[2];

if (!backupFile) {
  console.error('Usage: node restore-database.js <backup-file-path>');
  console.error('Example: node restore-database.js ../../backups/backup-2024-01-15T10-00-00-000Z.json');
  process.exit(1);
}

// Run restore
restoreDatabase(backupFile);

