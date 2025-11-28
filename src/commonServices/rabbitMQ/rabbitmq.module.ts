import { Module, forwardRef } from '@nestjs/common';
import { RabbitMQService } from './rabbitmq.service';
import { TicketDashboardModule } from 'src/ticket-dashboard/ticket-dashboard.module';
import { NewTestBroker } from './NewTestBroker';

@Module({
  imports: [forwardRef(() => TicketDashboardModule)],  // Use forwardRef here
  providers: [RabbitMQService,NewTestBroker],
  exports: [RabbitMQService,NewTestBroker],
})
export class RabbitMQModule {}
