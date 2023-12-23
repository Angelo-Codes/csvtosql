const express = require('express');
const path = require('path');
const { createTcpPool } = require('./db');

const PORT = process.env.PORT || 8080;
const app = express();


app.use(express.static('public'));

app.get('/data', async (req, res) => {
  try {
    const connection = await createTcpPool();

    const query = 'SELECT * FROM mapdata';
    const rows = await connection.query(query);

    res.json(rows);
  } catch (error) {
    console.error('Error connecting to MySQL:', error);
    res.status(500).send('Internal Server Error');
  }
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});