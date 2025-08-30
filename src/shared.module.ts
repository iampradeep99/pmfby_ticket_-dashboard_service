// shared.module.ts
import { Module } from '@nestjs/common';
import { RedisWrapper } from '../src/commonServices/redisWrapper';

@Module({
  providers: [RedisWrapper],
  exports: [RedisWrapper],
})
export class SharedModule {}
