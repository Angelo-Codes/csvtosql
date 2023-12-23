
const fs = require('fs');
const path = require('path');
const admin = require('firebase-admin');
const { progressBar } = require('./progress');

const serviceAccount = require('./');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  storageBucket: 'gs://'
});

const bucket = admin.storage().bucket();

async function downloadFile(file) {
  const filePath = path.join(__dirname, file.name);

  try {
    const [metadata] = await bucket.file(file.name).getMetadata();

    const toSqlBar = progressBar(parseInt(metadata.size), 'Downloading csv ' + file);
    if (metadata.size > 0) {
      const stream = bucket.file(file.name).createReadStream();
      const writeStream = fs.createWriteStream(filePath, { flags: 'a' });

      stream.on('data', (chunk) => {
        toSqlBar.tick(chunk.length);
        writeStream.write(chunk);
      });

      stream.on('end', () => {
        console.log(`Downloaded: ${file.name}`);
        writeStream.end();
      });

      stream.on('error', (error) => {
        console.error(`Error reading stream for file ${file.name}: ${error}`);
        progressBar.terminate();
      });
    } else {
      console.log(`Skipped empty file: ${file.name}`);
    }
  } catch (error) {
    console.error(`Error downloading file ${file.name}: ${error}`);
  }
}

async function copyAllCSVFiles() {
  try {
    const [files] = await bucket.getFiles();
    console.log('Copying CSV files...');
    const csvFiles = files.filter((file) => file.name.endsWith('.csv'));

    for (const file of csvFiles) {
      await downloadFile(file);
    }

  } catch (error) {
    console.error('Error copying CSV files:', error);
  }
}

console.log('Calling the function to copy all CSV files');
copyAllCSVFiles();