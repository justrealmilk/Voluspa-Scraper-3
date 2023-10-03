import { fork } from 'child_process';
import dotenv from 'dotenv';
import mysql from 'mysql2';

dotenv.config();

// create the connection to database
const connection = mysql.createConnection({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
});

// make db query async
const pond = connection.promise();

// get a list of members to fetch profile data for
console.log('Querying braytech.members');
const [members] = await pond.query('SELECT id, membershipType, membershipId FROM braytech.members WHERE NOT isPrivate LIMIT 0, 1000000');
console.log('Results received');

const brother = fork('worker.js');
const sister = fork('worker.js');

brother
  .on('message', function (data) {
    console.log('Brother sent data:', data);
  })
  .on('exit', function (code, signal) {
    console.log('Brother process exited with ' + `code ${code} and signal ${signal}`);
  })
  .on('error', function (error) {
    console.log(error);
  });

sister
  .on('message', function (data) {
    console.log('Sister sent data:', data);
  })
  .on('exit', function (code, signal) {
    console.log('Sister process exited with ' + `code ${code} and signal ${signal}`);
  })
  .on('error', function (error) {
    console.log(error);
  });

const half = Math.floor(members.length / 2);

brother.send(members.slice(0, half));
sister.send(members.slice(members.length - half));
