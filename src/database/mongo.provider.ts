// import { MongoClient, Db } from 'mongodb';
// import { Provider } from '@nestjs/common';
// import { ConfigService } from '@nestjs/config';

// export const DATABASE_NAME = 'krph_db';

// export const MongoProvider: Provider = {
//   provide: 'MONGO_DB',
//   inject: [ConfigService],
//   useFactory: async (configService: ConfigService): Promise<Db> => {
//     const uri = configService.get<string>('MONGODBPRODURL');
//     const client = new MongoClient(uri);
//     await client.connect();
//     return client.db(DATABASE_NAME);
//   },
// };


import { MongoClient, Db } from 'mongodb';
import { Provider } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import config from '../environment/config'; // import our dynamic config

export const DATABASE_NAME = 'krph_db';

export const MongoProvider: Provider = {
  provide: 'MONGO_DB',
  inject: [ConfigService],
  useFactory: async (configService: ConfigService): Promise<Db> => {
    // Use our config.ts dynamic config
    const mongoUri = config.mongodb; // automatically picks UAT or PROD based on NODE_ENV
    const client = new MongoClient(mongoUri);
    await client.connect();
    return client.db(DATABASE_NAME);
  },
};
