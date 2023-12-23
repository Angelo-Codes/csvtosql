const sql = require('promise-mysql');
const dotenv = require('dotenv');

// connection via google cloud auth proxy rsa key.
// Use command bellow comment
// cloud_sql_proxy -instances=project-id:region:instance-name=tcp:3306

dotenv.config();

const createTcpPool = async (config) => {
  return sql.createPool({
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    connectTimeout: parseInt(process.env.DB_CONNECT_TIMEOUT) || 30000, // kahit wala na
    acquireTimeout: parseInt(process.env.DB_ACQUIRE_TIMEOUT) || 30000, // kahit wala na
    ...config,
  });
};

module.exports = {
    createTcpPool
}