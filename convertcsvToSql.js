const fastcsv = require('fast-csv');
const fs = require('fs');
const path = require('path');
const { createTableQuery, countdata } = require('./sqlQueries');
const { createTcpPool } = require('./db');

const uploadDataToCloudSql = async (filenames) => {
  try {
    const connection = await createTcpPool();
    await connection.query(createTableQuery);
    //await console.log( await connection.query(countdata));

    for (const filename of filenames) {
      console.log(`Converting CSV file "${filename}"`);
      await processFile(filename, connection);
    }

    connection.end((err) => {
      if (err) {
        console.error('Connection: ', err);
      }
    });

    console.log('All files processed.');
  } catch (err) {
    console.error('Error connecting to MySQL:', err);
  }
};

const processFile = async (filename, connection) => {
  const fileStream = fs.createReadStream(filename);
  const csvStream = fastcsv.parse({ headers: true });
  const rowsArray = [];
  let headers;

  await new Promise((resolve, reject) => {
    fileStream.pipe(csvStream)
      .on('error', reject)
      .on('data', row => {
        if (!headers) {
          headers = Object.keys(row);
        }
        rowsArray.push(Object.values(row));
        if (rowsArray.length >= 10000) {
          insertRows(rowsArray, connection, headers);
          rowsArray.length = 0;
        }
      })
      .on('end', async () => {
        if (rowsArray.length > 0) {
          await insertRows(rowsArray, connection, headers);
        }
        console.log(`CSV file "${filename}" successfully processed.`);
        resolve();
      });
  });
};

const insertRows = async (rowsArray, connection, headers) => {
  try {
    const placeholders = Array(headers.length).fill('?').join(', ');
    const values = rowsArray.map(() => `(${placeholders})`).join(', ');
    const insertQuery = `INSERT INTO mapdata (${headers.join(', ')}) VALUES ${values}`;
    console.log(`${rowsArray.length} rows inserting...`);
    await connection.query(insertQuery, await rowsArray.flat());

    console.log(`${rowsArray.length} rows inserted successfully.`);
    await delay(3000);
  } catch (insertError) {
    console.error('Error inserting data:', insertError.message);
  }
};

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const directoryPath = './database';

fs.readdir(directoryPath, (err, files) => {
  if (err) {
    console.error('Error reading directory:', err);
    return;
  }

  const csvFiles = files
    .filter((file) => path.extname(file).toLowerCase() === '.csv')
    .map((file) => path.join(directoryPath, file));

  uploadDataToCloudSql(csvFiles);
});
