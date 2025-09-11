import { Module, forwardRef } from '@nestjs/common';
import { RabbitMQService } from './rabbitMQ.service';
import { TicketDashboardModule } from 'src/ticket-dashboard/ticket-dashboard.module';

@Module({
  imports: [forwardRef(() => TicketDashboardModule)],  // Use forwardRef here
  providers: [RabbitMQService],
  exports: [RabbitMQService],
})
export class RabbitMQModule {}
