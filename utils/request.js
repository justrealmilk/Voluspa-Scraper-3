import https from 'https';
import dotenv from 'dotenv';
import got from 'got';
import { P2cBalancer } from 'load-balancers';

dotenv.config();

const balancer = new P2cBalancer(16);

const instances = {
  0: createInstance('2604:a880:2:d0::1f3e:500f'),
  1: createInstance('2604:a880:2:d0::1f3e:500e'),
  2: createInstance('2604:a880:2:d0::1f3e:500d'),
  3: createInstance('2604:a880:2:d0::1f3e:500c'),
  4: createInstance('2604:a880:2:d0::1f3e:500b'),
  5: createInstance('2604:a880:2:d0::1f3e:500a'),
  6: createInstance('2604:a880:2:d0::1f3e:5009'),
  7: createInstance('2604:a880:2:d0::1f3e:5008'),
  8: createInstance('2604:a880:2:d0::1f3e:5007'),
  9: createInstance('2604:a880:2:d0::1f3e:5006'),
  10: createInstance('2604:a880:2:d0::1f3e:5005'),
  11: createInstance('2604:a880:2:d0::1f3e:5004'),
  12: createInstance('2604:a880:2:d0::1f3e:5003'),
  13: createInstance('2604:a880:2:d0::1f3e:5002'),
  14: createInstance('2604:a880:2:d0::1f3e:5001'),
  15: createInstance('2604:a880:2:d0::1f3e:5000'),
};

function createInstance(localAddress) {
  return got.extend({
    headers: {
      'x-api-key': process.env.BUNGIE_API_KEY,
    },
    agent: {
      https: new https.Agent({
        localAddress,
        family: 6,
        keepAlive: true,
      }),
    },
    timeout: {
      request: 60000,
    },
    throwHttpErrors: false,
  });
}

export async function customFetch(url) {
  const instance = instances[balancer.pick()];

  return instance.get(url, {
    responseType: 'json',
    resolveBodyOnly: true,
  });
}
