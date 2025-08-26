import { Module } from '@nestjs/common';
import { TicketDashboardController } from './ticket-dashboard.controller';
import { TicketDashboardService } from './ticket-dashboard.service';
import { DatabaseModule } from '../database/database.module';

@Module({
  imports:[DatabaseModule],
  controllers: [TicketDashboardController],
  providers: [TicketDashboardService]
})
export class TicketDashboardModule {}
