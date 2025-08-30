import { Module } from '@nestjs/common';
import { RedisWrapper } from './redisWrapper';

@Module({
  providers: [RedisWrapper],
  exports: [RedisWrapper], // 👈 makes it available to other modules
})
export class RedisModule {}