import { Module, forwardRef } from '@nestjs/common';
import { SlaReportService } from './sla-report.service';
import { SlaReportController } from './sla-report.controller';
import { DatabaseModule } from 'src/database/database.module';
import { RedisModule } from 'src/commonServices/redis.module';
import { UtilModule } from 'src/commonServices/util.module';
import { MailModule } from 'src/mail/mail.module';
import { RabbitMQModule } from 'src/commonServices/rabbitMQ/rabbitmq.module';
import { MysqlModule } from 'src/database/mysql.module';

@Module({
  imports: [
    DatabaseModule,
    RedisModule,
    UtilModule,
    MailModule,
    forwardRef(() => RabbitMQModule),
    MysqlModule,
  ],
  controllers: [SlaReportController],
  providers: [SlaReportService],
  exports: [SlaReportService],
})
export class SlaReportModule {}
