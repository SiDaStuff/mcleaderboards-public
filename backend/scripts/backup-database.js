// MC Leaderboards - Database Backup Script
// Creates a backup of the Firebase Realtime Database

const admin = require('firebase-admin');
const { loadRuntimeConfig } = require('../config');
const fs = require('fs');
const path = require('path');

const { serviceAccount, config } = loadRuntimeConfig();

// Initialize Firebase Admin
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: config.databaseURL
});

const db = admin.database();

/**
 * Backup entire database
 */
async function backupDatabase() {
  try {
    console.log('Starting database backup...\n');
    
    // Get all data
    const snapshot = await db.ref('/').once('value');
    const data = snapshot.val();
    
    // Create backup directory if it doesn't exist
    const backupDir = path.join(__dirname, '../../backups');
    if (!fs.existsSync(backupDir)) {
      fs.mkdirSync(backupDir, { recursive: true });
    }
    
    // Create backup file with timestamp
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupFile = path.join(backupDir, `backup-${timestamp}.json`);
    
    // Write backup
    fs.writeFileSync(backupFile, JSON.stringify(data, null, 2));
    
    // Get file size
    const stats = fs.statSync(backupFile);
    const fileSizeInMB = (stats.size / (1024 * 1024)).toFixed(2);
    
    console.log(`✓ Backup created: ${backupFile}`);
    console.log(`✓ File size: ${fileSizeInMB} MB`);
    console.log(`✓ Timestamp: ${new Date().toISOString()}`);
    
    // Clean up old backups (keep last 10)
    const backups = fs.readdirSync(backupDir)
      .filter(file => file.startsWith('backup-') && file.endsWith('.json'))
      .map(file => ({
        name: file,
        path: path.join(backupDir, file),
        time: fs.statSync(path.join(backupDir, file)).mtime
      }))
      .sort((a, b) => b.time - a.time);
    
    if (backups.length > 10) {
      const toDelete = backups.slice(10);
      toDelete.forEach(backup => {
        fs.unlinkSync(backup.path);
        console.log(`  Removed old backup: ${backup.name}`);
      });
    }
    
    console.log('\n✓ Backup process completed successfully!');
    process.exit(0);
  } catch (error) {
    console.error('Error backing up database:', error);
    process.exit(1);
  }
}

// Run backup
backupDatabase();

