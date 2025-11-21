import { Module, forwardRef } from '@nestjs/common';
import { DatabaseModule } from 'src/database/database.module';
import { RedisModule } from 'src/commonServices/redis.module';
import { UtilModule } from 'src/commonServices/util.module';
import { MailModule } from 'src/mail/mail.module';
import { RabbitMQModule } from 'src/commonServices/rabbitMQ/rabbitmq.module';
import { MysqlModule } from 'src/database/mysql.module';

import { AgentPerformanceController } from './agent-performance.controller';
import { AgentPerformanceService } from './agent-performance.service';

import { AgentPerformanceRwaFileService } from './AgentPerformanceRwaFileService';
import { ProductiveCallingServices } from './ProductiveCallingServices';
import { HangByAgentService } from './HangByAgentService';
import { FeedbackTransferStatusService } from './FeedbackTransferStatusService';
import { CustomerRatingService } from './CustomerRatingService';
import { CRMTaggingCalcullationService } from './CRMTaggingCalcullationService';
import { CallQualityAssuranceService } from './CallQualityAssuranceService';
import { AverageHandlingTimeService } from './AverageHandlingTimeService';

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
    AgentPerformanceRwaFileService,
    ProductiveCallingServices,
    HangByAgentService,
    FeedbackTransferStatusService,
    CustomerRatingService,
    CRMTaggingCalcullationService,
    CallQualityAssuranceService,
    AverageHandlingTimeService,
  ],

  exports: [
    AgentPerformanceService,
    AgentPerformanceRwaFileService,
    ProductiveCallingServices,
    HangByAgentService,
    FeedbackTransferStatusService,
    CustomerRatingService,
    CRMTaggingCalcullationService,
    CallQualityAssuranceService,
    AverageHandlingTimeService,
  ],
})
export class AgentPerformanceModule {
  constructor() {
    console.log("AgentPerformanceModule loaded");
  }
}
