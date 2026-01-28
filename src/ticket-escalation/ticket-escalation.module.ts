import { Module, forwardRef } from '@nestjs/common';
import { TicketEscalationService } from './ticket-escalation.service';
import { TicketEscalationController } from './ticket-escalation.controller';
import { TicketDashboardModule } from '../ticket-dashboard/ticket-dashboard.module'; // if needed
import { DatabaseModule } from 'src/database/database.module';
import { RedisModule } from 'src/commonServices/redis.module';
import { UtilModule } from 'src/commonServices/util.module';
import { MailModule } from 'src/mail/mail.module';
import { RabbitMQModule } from 'src/commonServices/rabbitMQ/rabbitmq.module';
import { MysqlModule } from 'src/database/mysql.module';
import { CronModule } from 'src/cron/cron.module';

@Module({
  imports: [
    // forwardRef(() => TicketEscalationModule), 
      DatabaseModule,
        RedisModule,
        UtilModule,
        MailModule,
        forwardRef(() => RabbitMQModule), 
        MysqlModule, // Use forwardRef here
        CronModule
  ],
  controllers: [TicketEscalationController],
  providers: [TicketEscalationService],
  exports: [TicketEscalationService],
})
export class TicketEscalationModule {}
