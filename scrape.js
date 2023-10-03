import { fork } from 'child_process';
import dotenv from 'dotenv';
import fs from 'fs';
import mysql from 'mysql2';
import http from 'http';

dotenv.config();

console.log('VOLUSPA');

// create the connection to database
const connection = mysql.createConnection({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  supportBigNumbers: true,
});

// make db query async
const pond = connection.promise();

// get a list of members to fetch profile data for
console.log('Querying braytech.members');
const [members] = await pond.query('SELECT id, membershipType, membershipId FROM braytech.members WHERE NOT isPrivate LIMIT 0, 1000000');
console.log('Results received');

// variables for relaying progress to the console
const completionValue = members.length;
let progress = 0;
let rate = 0;
let successful = 0;
let errors = {};
let metrics = '';

const scrapeStart = new Date();
const scrapeStoreResults = process.env.STORE_JOB_RESULTS === 'true';

const results = [];

// empty objects to hold statistics for later
const StatsTriumphs = {};
const StatsCollections = {};
const StatsParallelProgram = [];

function handleSpawnResponse(data) {
  if (data.Message === 'Results') {
    rate++;
    progress++;

    if (data.Response.result === 'success') {
      successful++;
    } else {
      errors[data.Response.status] = (errors[data.Response.status] ?? 1) + 1;
    }

    results.push(data.Response);

    if (progress === completionValue) {
      finalise();
    }
  } else if (data.Message === 'StatsTriumphs') {
    if (StatsTriumphs[data.Response]) {
      StatsTriumphs[data.Response]++;
    } else {
      StatsTriumphs[data.Response] = 1;
    }
  } else if (data.Message === 'StatsCollections') {
    if (StatsCollections[data.Response]) {
      StatsCollections[data.Response]++;
    } else {
      StatsCollections[data.Response] = 1;
    }
  } else if (data.Message === 'StatsParallelProgram') {
    StatsParallelProgram.push(data.Response);
  } else {
    console.log('Spawn sent data:', data);
  }
}

const brother = fork('worker.js');
const sister = fork('worker.js');

brother
  .on('message', function (data) {
    handleSpawnResponse(data);
  })
  .on('exit', function (code, signal) {
    console.log(`Brother process exited with code ${code} and signal ${signal}`);
  })
  .on('error', function (error) {
    console.log(error);
  });

sister
  .on('message', function (data) {
    handleSpawnResponse(data);
  })
  .on('exit', function (code, signal) {
    console.log(`Sister process exited with code ${code} and signal ${signal}`);
  })
  .on('error', function (error) {
    console.log(error);
  });

const half = Math.floor(members.length / 2);

brother.send({
  Message: 'Members',
  Response: members.slice(0, half),
});
sister.send({
  Message: 'Members',
  Response: members.slice(members.length - half),
});

const requestListener = function (request, response) {
  response.writeHead(200);
  response.end(metrics);
};

const server = http.createServer(requestListener);

server.listen(8181, '0.0.0.0', () => {
  console.log(`HTTP server started`);
});

function report() {
  console.log(progress, completionValue);

  const timeComplete = new Date(Date.now() + ((Date.now() - scrapeStart.getTime()) / Math.max(progress, 1)) * (completionValue - progress));

  metrics = `voluspa_scraper_progress ${Math.floor((progress / completionValue) * 100)}\n\nvoluspa_scraper_job_rate ${rate}\n\nvoluspa_scraper_job_progress ${progress}\n\nvoluspa_scraper_job_completion_value ${completionValue}\n\nvoluspa_scraper_job_parallel_programs ${StatsParallelProgram.length}\n\nvoluspa_scraper_job_time_remaining ${Math.max((timeComplete.getTime() - Date.now()) / 1000, 0)}\n\n${Object.keys(errors)
    .map((key) => `voluspa_scraper_job_error_${key} ${errors[key]}`)
    .join('\n\n')}`;

  rate = 0;
  Object.keys(errors).forEach((key) => {
    errors[key] = 0;
  });

  Promise.all([
    fs.promises.writeFile('./temp/triumphs.temp.json', JSON.stringify(StatsTriumphs)), //
    fs.promises.writeFile('./temp/collections.temp.json', JSON.stringify(StatsCollections)),
    fs.promises.writeFile('./temp/parallel-program.temp.json', JSON.stringify(StatsParallelProgram)),
  ]);
}

const updateIntervalTimer = setInterval(report, 5000);

async function finalise() {
  clearInterval(updateIntervalTimer);
  metrics = '';

  const scrapeEnd = new Date();

  await fs.promises.writeFile(`./logs/job-results.${scrapeEnd.getTime()}.json`, JSON.stringify(results.filter((job) => job.status !== 'success')));
  console.log('Saved job results to disk');

  await fs.promises.copyFile('./temp/triumphs.json', './temp/triumphs.previous.json');
  await fs.promises.writeFile('./temp/triumphs.json', JSON.stringify(StatsTriumphs));
  console.log('Saved Triumphs stats to disk');

  await fs.promises.copyFile('./temp/collections.json', './temp/collections.previous.json');
  await fs.promises.writeFile('./temp/collections.json', JSON.stringify(StatsCollections));
  console.log('Saved Collections stats to disk');

  await fs.promises.copyFile('./temp/parallel-program.json', './temp/parallel-program.previous.json');
  await fs.promises.writeFile('./temp/parallel-program.json', JSON.stringify(StatsParallelProgram));
  console.log('Saved Parallel Program stats to disk');

  if (scrapeStoreResults) {
    const scrapesStatusQuery = mysql.format(`INSERT INTO profiles.scrapes (date, duration, crawled, assessed) VALUES (?, ?, ?, ?);`, [scrapeStart, Math.ceil((scrapeEnd.getTime() - scrapeStart.getTime()) / 60000), completionValue, successful]);
    const rankQuery = mysql.format(
      `SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

      TRUNCATE leaderboards.ranks;
      
      INSERT INTO leaderboards.ranks (
            membershipType,
            membershipId,
            displayName,
            legacyScore,
            activeScore,
            collectionsScore,
            legacyRank,
            activeRank,
            collectionsRank
         ) (
            SELECT membershipType,
               membershipId,
               displayName,
               legacyScore,
               activeScore,
               collectionsScore,
               legacyRank,
               activeRank,
               collectionsRank
            FROM (
                  SELECT *,
                     DENSE_RANK() OVER (
                        ORDER BY legacyScore DESC
                     ) legacyRank,
                     DENSE_RANK() OVER (
                        ORDER BY activeScore DESC
                     ) activeRank,
                     DENSE_RANK() OVER (
                        ORDER BY collectionsScore DESC
                     ) collectionsRank
                  FROM profiles.members
                  WHERE lastUpdated >= ?
                     AND lastPlayed > '2023-02-28 17:00:00'
                  ORDER BY displayName ASC
               ) R
         ) ON DUPLICATE KEY
      UPDATE displayName = R.displayName,
         legacyScore = R.legacyScore,
         activeScore = R.activeScore,
         collectionsScore = R.collectionsScore,
         legacyRank = R.legacyRank,
         activeRank = R.activeRank,
         collectionsRank = R.collectionsRank;
      
      UPDATE leaderboards.ranks r
         INNER JOIN (
            SELECT membershipId,
               ROW_NUMBER() OVER (
                  ORDER BY activeRank,
                     collectionsRank,
                     displayName
               ) AS activePosition,
               ROW_NUMBER() OVER (
                  ORDER BY legacyRank,
                     collectionsRank,
                     displayName
               ) AS legacyPosition,
               ROW_NUMBER() OVER (
                  ORDER BY collectionsRank,
                     activeRank,
                     displayName
               ) AS collectionsPosition
            FROM leaderboards.ranks
         ) p ON p.membershipId = r.membershipId
      SET r.activePosition = p.activePosition,
         r.legacyPosition = p.legacyPosition,
         r.collectionsPosition = p.collectionsPosition;
      
    UPDATE leaderboards.ranks r
         INNER JOIN (
            SELECT membershipId,
               ROUND(
                  PERCENT_RANK() OVER (
                     ORDER BY activeScore DESC
                  ),
                  2
               ) activePercentile,
               ROUND(
                  PERCENT_RANK() OVER (
                     ORDER BY legacyScore DESC
                  ),
                  2
               ) legacyPercentile,
               ROUND(
                  PERCENT_RANK() OVER (
                     ORDER BY collectionsScore DESC
                  ),
                  2
               ) collectionsPercentile
            FROM leaderboards.ranks
         ) p ON p.membershipId = r.membershipId
      SET r.activePercentile = p.activePercentile,
         r.legacyPercentile = p.legacyPercentile,
         r.collectionsPercentile = p.collectionsPercentile;
      
      COMMIT;`,
      [scrapeStart]
    );

    const statsTriumphsQuery = mysql.format(`INSERT INTO profiles.commonality (date, hash, value) VALUES ?;`, [Object.entries(StatsTriumphs).map(([hash, value]) => [scrapeStart, hash, value])]);
    const statsCollectiblesQuery = mysql.format(`INSERT INTO profiles.commonality (date, hash, value) VALUES ?;`, [Object.entries(StatsCollections).map(([hash, value]) => [scrapeStart, hash, value])]);

    await fs.promises.writeFile(`./temp/queries.${Date.now()}.sql`, `${scrapesStatusQuery}\n\n${rankQuery}`);
    await fs.promises.writeFile(`./temp/queries.extended.${Date.now()}.sql`, `${statsTriumphsQuery}\n\n${statsCollectiblesQuery}`);

    console.log('Save scrape to profiles.scrapes...');
    const [status] = await pond.query(scrapesStatusQuery);
    console.log('Saved scrape profiles.scrapes...');

    console.log('Evaluate leaderboards...');
    const ranks = await pond.query(rankQuery);
    console.log(ranks);
    console.log('Evaluated leaderboards...');

    console.log('Save Triumphs stats to database...');
    await pond.query(statsTriumphsQuery);
    console.log('Saved Triumphs stats to database...');

    console.log('Save Collections stats to database...');
    await pond.query(statsCollectiblesQuery);
    console.log('Saved Collections stats to database...');

    console.log('Cache commonality...');
    await fetch(`http://0.0.0.0:8080/Generate/Commonality?id=${status.insertId}`, {
      headers: {
        'x-api-key': process.env.VOLUSPA_API_KEY,
      },
    });
    console.log('Cached commonality...');
  }

  process.exit();
}
