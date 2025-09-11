import { Module, forwardRef } from '@nestjs/common';
import { TicketDashboardController } from './ticket-dashboard.controller';
import { TicketDashboardService } from './ticket-dashboard.service';
import { DatabaseModule } from '../database/database.module';
import { RedisModule } from '../commonServices/redis.module';
import { UtilModule } from 'src/commonServices/util.module';
import { MailModule } from 'src/mail/mail.module';
import { RabbitMQModule } from 'src/commonServices/rabbitMQ/rabbitmq.module';

@Module({
  imports: [
    DatabaseModule,
    RedisModule,
    UtilModule,
    MailModule,
    forwardRef(() => RabbitMQModule),  // Use forwardRef here
  ],
  controllers: [TicketDashboardController],
  providers: [TicketDashboardService],
  exports: [TicketDashboardService],  // Ensure it's exported for RabbitMQService
})
export class TicketDashboardModule {}
