import { Module } from '@nestjs/common';
import { RedisWrapper } from './redisWrapper';
import { QueueBrokerModule } from './queue-broker/queue-broker.module';

@Module({
  providers: [RedisWrapper],
  exports: [RedisWrapper],
  imports: [QueueBrokerModule], // 👈 makes it available to other modules
})
export class RedisModule {}