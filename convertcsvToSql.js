
const fastcsv = require('fast-csv');
//const mysql = require('mysql'); //local
const fs = require('fs');
const path = require('path');
const { createTableQuery, insertCsvQuery } = require('./sqlQueries');
const { createTcpPool } = require('./db');
const { progressBar } = require('./progress');

const uploadDataToCloudSql = async (filenames) => {
  try{
      const connection = await createTcpPool();
      
      await connection.query(createTableQuery); //create table 

      for (const filename of filenames) {
        console.log(`converting CSV file "${filename}"`);
        const totalRows = fs.readFileSync(filename).toString().split('\n').length - 1;
        let rowCount = 0;

        const preparingDataBar = progressBar(totalRows, 'Preparing csv data');
        const toSqlBar = progressBar(totalRows, 'Inserting to Google Cloud SQL');

        const rowsArray = [];
  
        await new Promise((resolve) => {
          fs.createReadStream(filename)
          .pipe(fastcsv.parse({headers: true}))
          .transform((rows) => {
            
            rowCount++;

            return rows;
          })
          .on('data', (rows) => {
            
            rowsArray.push(Object.values(rows)); // Store rows in the array
            preparingDataBar.tick(1); 
            
          })
          .on('end',async (data) => {
            console.log(`CSV file "${filename}" successfully processed. ready to inserting to sql.`);

            let num = 0;

            for (num ; num < rowCount; num++ ) {
              try {
                await connection.query(insertCsvQuery, rowsArray[num]);
                toSqlBar.tick(1);
              } catch (insertError) {
                // for testing porpose
                const truncateTable = `truncate table mapdata`;
                await connection.query(truncateTable);

                console.error('Error inserting data for row', num + 1, ':', insertError.message, insertError.sqlMessage);
                console.log('note the rows not going to insert to sql check manualy the rows');
              }
            }

            // Clear the containers for the next file
            num = 0;

            rowCount = 0;
            
            rowsArray.length = 0;

            //delete the csv file after successful converted
            fs.unlink(filename, (unlinkErr) => {
              if (unlinkErr) {
                console.error('Error deleting file:', unlinkErr);
              }
            });
            resolve();
          });
        });
      };

      //after all files converted pool closed. promise
      connection.end((err) => {
        if (err) {
          console.error('Connection: ', err);
        }
      });
      
    } catch (err){
      console.error('Error connecting to MySQL:', err);
    }
};

//=======CSV FILES SELLECTING=========

const directoryPath = './database';

// Read CSV files from the directory and filter only CSV files
fs.readdir(directoryPath, (err, files) => {
  if (err) {
    console.error('Error reading directory:', err);
    return;
  }

  const csvFiles = files
    .filter((file) => path.extname(file).toLowerCase() === '.csv')
    .map((file) => path.join(directoryPath, file));

  // console.log(csvFiles + "/" + typeof(csvFiles));
  // Process each CSV file
  uploadDataToCloudSql(csvFiles);
});