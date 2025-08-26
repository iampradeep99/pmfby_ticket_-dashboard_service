// src/database/database.module.ts
import { Module } from '@nestjs/common';
import { MongoProvider } from './mongo.provider';

@Module({
  providers: [MongoProvider],
  exports: [MongoProvider], // export so it can be used in other modules
})
export class DatabaseModule {}
