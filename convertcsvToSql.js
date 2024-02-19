
const fastcsv = require('fast-csv');
const fs = require('fs');
const path = require('path');
const { createTableQuery, insertCsvQuery } = require('./sqlQueries');
const { createTcpPool } = require('./db');
const { progressBar } = require('./progress');


const uploadDataToCloudSql = async (filenames) => {
  try {
    const connection = await createTcpPool();

    await connection.query(createTableQuery);
    let rowsArray = [];

    for (const filename of filenames) {
      console.log(`converting CSV file "${filename}"`);
      const totalRows = fs.readFileSync(filename).toString().split('\n').length - 1;
      let rowCount = 0;

      const preparingDataBar = progressBar(totalRows, 'Preparing csv data');
      const toSqlBar = progressBar(totalRows, 'Inserting to Google Cloud SQL');

      await new Promise((resolve) => {
        fs.createReadStream(filename)
          .pipe(fastcsv.parse({ headers: true }))
          .transform((rows) => {
            if (rowCount % 1000 === 0) {
              preparingDataBar.tick(1);
            }

            rowCount++;

            return rows;
          })
          .on('data', (rows) => {
            rowsArray.push(Object.values(rows));
          })
          .on('end', async (data) => {
            console.log(`CSV file "${filename}" successfully processed. ready to inserting to sql.`);

            let num = 0;

            while (num < rowCount) {
              const batch = rowsArray.slice(num, num + 1000);

              try {
                await connection.query(insertCsvQuery, [batch]);
                toSqlBar.tick(batch.length);
              } catch (insertError) {
                const truncateTable = `truncate table mapdata`;
                await connection.query(truncateTable);

                console.error('Error inserting data for row', num + 1, ':', insertError.message, insertError.sqlMessage);
                console.log('note the rows not going to insert to sql check manualy the rows');
              }

              num += 1000;

              await new Promise((resolve) => setTimeout(resolve, 5000)); 
            }

            num = 0;

            rowCount = 0;

            rowsArray.length = 0;

            fs.unlink(filename, (unlinkErr) => {
              if (unlinkErr) {
                console.error('Error deleting file:', unlinkErr);
              }
            });

            resolve();
          });
      });
    }

    connection.end((err) => {
      if (err) {
        console.error('Connection: ', err);
      }
    });
  } catch (err) {
    console.error('Error connecting to MySQL:', err);
  }
};

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
