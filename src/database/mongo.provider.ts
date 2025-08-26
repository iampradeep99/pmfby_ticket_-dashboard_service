import { MongoClient, Db } from 'mongodb';
import { Provider } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export const DATABASE_NAME = 'krph_db';

export const MongoProvider: Provider = {
  provide: 'MONGO_DB',
  inject: [ConfigService],
  useFactory: async (configService: ConfigService): Promise<Db> => {
    const uri = configService.get<string>('MONGODBPRODURL');
    const client = new MongoClient(uri);
    await client.connect();
    return client.db(DATABASE_NAME);
  },
};
