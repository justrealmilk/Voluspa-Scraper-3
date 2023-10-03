import fs from 'fs';
import mysql from 'mysql2';
import pQueue from 'p-queue';
import dotenv from 'dotenv';

import { customFetch } from './utils/request.js';
import { values } from './utils/response.js';

dotenv.config();

const scrapeStoreResults = process.env.STORE_JOB_RESULTS === 'true';

const queue = new pQueue({ concurrency: 150 });

function addToQueue(members) {
  members.forEach((member) => {
    queue.add(() => processJob({ member, retries: 0 }));
  });
}

process.on('message', function (data) {
  if (data.Message === 'Members') {
    addToQueue(data.Response);
    process.send({
      Message: 'Received members - thank you.',
      Response: data.Response.length,
    });
  } else {
    process.send({
      Message: 'Hello',
      Response: null,
    });
  }
});

// setup basic db stuff
const connection = mysql.createPool({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  connectionLimit: 10,
  supportBigNumbers: true,
  multipleStatements: true,
  charset: 'utf8mb4',
});

// make db query async
const pond = connection.promise();

async function processJob({ member, retries }) {
  try {
    const processStart = new Date().toISOString();

    const fetchStart = performance.now();
    const response = await customFetch(`https://www.bungie.net/Platform/Destiny2/${member.membershipType}/Profile/${member.membershipId}/?components=100,800,900`);
    // const response = await fetch(`https://www.bungie.net/Platform/Destiny2/${member.membershipType}/Profile/${member.membershipId}/?components=100,800,900`, {
    //   headers: {
    //     'x-api-key': process.env.BUNGIE_API_KEY,
    //   },
    // }).then((request) => request.json());
    const fetchEnd = performance.now();

    const computeStart = performance.now();
    const result = processResponse(member, response);
    const computeEnd = performance.now();

    if (result !== 'success' && retries < 3) {
      queue.add(() => processJob({ member, retries: retries + 1 }));
    } else {
      process.send({
        Message: 'Results',
        Response: {
          status: result,
          member: {
            membershipType: member.membershipType,
            membershipId: member.membershipId,
          },
          retries,
          performance: {
            start: processStart,
            fetch: fetchEnd - fetchStart,
            compute: computeEnd - computeStart,
          },
        },
      });
    }
  } catch (error) {
    fs.promises.writeFile(`./logs/error.${member.membershipId}.${Date.now()}.txt`, `${JSON.stringify(member)}\n\n${typeof error}\n\n${error.toString()}\n\n${error.message}`);

    if (retries < 3) {
      queue.add(() => processJob({ member, retries: retries + 1 }));
    } else {
      process.send({
        Message: 'Results',
        Response: {
          status: error,
          member: {
            membershipType: member.membershipType,
            membershipId: member.membershipId,
          },
          retries,
          performance: undefined,
        },
      });
    }
  }
}

function processResponse(member, response) {
  if (response && response.ErrorCode !== undefined) {
    if (response.ErrorCode === 1) {
      if (response.Response.profileRecords.data === undefined || Object.keys(response.Response.characterCollectibles.data).length === 0) {
        if (scrapeStoreResults) {
          pond.query(mysql.format(`UPDATE braytech.members SET isPrivate = '1' WHERE membershipId = ?`, [member.membershipId]));
        }

        return 'private_profile';
      }

      const triumphs = [];
      const collections = [];

      // triumphs (profile scope)
      for (const hash in response.Response.profileRecords.data.records) {
        const record = response.Response.profileRecords.data.records[hash];

        if (record.intervalObjectives && record.intervalObjectives.length) {
          if (record.intervalObjectives.some((objective) => objective.complete) && record.intervalObjectives.filter((objective) => objective.complete).length === record.intervalObjectives.length) {
            triumphs.push(hash);
          }
        } else {
          if (!!(record.state & 1) || !!!(record.state & 4)) {
            triumphs.push(hash);
          }
        }
      }

      // triumphs (character scope)
      for (const characterId in response.Response.characterRecords.data) {
        for (const hash in response.Response.characterRecords.data[characterId].records) {
          const record = response.Response.characterRecords.data[characterId].records[hash];

          if (triumphs.indexOf(hash) === -1) {
            if (record.intervalObjectives && record.intervalObjectives.length) {
              if (record.intervalObjectives.some((objective) => objective.complete) && record.intervalObjectives.filter((objective) => objective.complete).length === record.intervalObjectives.length) {
                triumphs.push(hash);
              }
            } else {
              if (!!(record.state & 1) || !!!(record.state & 4)) {
                triumphs.push(hash);
              }
            }
          }
        }
      }

      // collections (profile scope)
      for (const hash in response.Response.profileCollectibles.data.collectibles) {
        if (!!!(response.Response.profileCollectibles.data.collectibles[hash].state & 1)) {
          collections.push(hash);
        }
      }

      // collections (character scope)
      for (const characterId in response.Response.characterCollectibles.data) {
        for (const hash in response.Response.characterCollectibles.data[characterId].collectibles) {
          if (!!!(response.Response.characterCollectibles.data[characterId].collectibles[hash].state & 1)) {
            if (collections.indexOf(hash) === -1) {
              collections.push(hash);
            }
          }
        }
      }

      // for spying 🥸
      if (collections.includes('3316003520')) {
        process.send({
          Message: 'StatsParallelProgram',
          Response: {
            membershipType: member.membershipType,
            membershipId: member.membershipId,
          },
        });
      }

      for (let index = 0; index < triumphs.length; index++) {
        const hash = triumphs[index];

        process.send({
          Message: 'StatsTriumphs',
          Response: hash,
        });
      }

      for (let index = 0; index < collections.length; index++) {
        const hash = collections[index];

        process.send({
          Message: 'StatsCollections',
          Response: hash,
        });
      }

      // values specifically for storing in the database for things like leaderboards
      const PreparedValues = values(response);

      const date = new Date();

      if (scrapeStoreResults) {
        pond.query(
          mysql.format(
            `INSERT INTO profiles.members (
                membershipType,
                membershipId,
                displayName,
                lastUpdated,
                lastPlayed,
                legacyScore,
                activeScore,
                collectionsScore
              )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?
              )
            ON DUPLICATE KEY UPDATE
              displayName = ?,
              lastUpdated = ?,
              lastPlayed = ?,
              legacyScore = ?,
              activeScore = ?,
              collectionsScore = ?`,
            [
              member.membershipType,
              member.membershipId,
              PreparedValues.displayName,
              date,
              PreparedValues.lastPlayed,
              PreparedValues.legacyScore,
              PreparedValues.activeScore,
              collections.length,
              // update...
              PreparedValues.displayName,
              date,
              PreparedValues.lastPlayed,
              PreparedValues.legacyScore,
              PreparedValues.activeScore,
              collections.length,
            ]
          )
        );
      }

      return 'success';
    } else if (response.ErrorCode !== undefined) {
      if (response.ErrorCode === 1601) {
        if (scrapeStoreResults) {
          pond.query(mysql.format(`DELETE FROM braytech.members WHERE membershipId = ?`, [member.membershipId]));
        }
      }

      return `bungie_${response.ErrorStatus}`;
    }
  }

  fs.promises.writeFile(`./logs/error.${member.membershipId}.${Date.now()}.txt`, `${JSON.stringify(member)}\n\n${response}`);

  return 'unknown_error';
}
