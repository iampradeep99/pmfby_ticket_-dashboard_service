import { Module, forwardRef } from '@nestjs/common';
import { DatabaseModule } from 'src/database/database.module';
import { RedisModule } from 'src/commonServices/redis.module';
import { UtilModule } from 'src/commonServices/util.module';
import { MailModule } from 'src/mail/mail.module';
import { RabbitMQModule } from 'src/commonServices/rabbitMQ/rabbitmq.module';
import { MysqlModule } from 'src/database/mysql.module';
import { AgentPerformanceController } from './agent-performance.controller';
import { AgentPerformanceService } from './agent-performance.service';
import { AgentPerformanceRwaFileService } from '../agent-performance/AgentPerformanceRwaFileService';  // Correct the name here

@Module({
  imports: [
    DatabaseModule,
    RedisModule,
    UtilModule,
    MailModule,
    forwardRef(() => RabbitMQModule),
    MysqlModule,
  ],
  controllers: [AgentPerformanceController],
  providers: [
    AgentPerformanceService,
    AgentPerformanceRwaFileService,  // Correct the name here
  ],
  exports: [AgentPerformanceService, AgentPerformanceRwaFileService], 
})
export class AgentPerformanceModule {
  constructor() {
    console.log("AgentPerformanceModule loaded");
  }
}

