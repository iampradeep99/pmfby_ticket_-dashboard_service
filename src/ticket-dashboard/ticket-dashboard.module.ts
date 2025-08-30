import { Module } from '@nestjs/common';
import { TicketDashboardController } from './ticket-dashboard.controller';
import { TicketDashboardService } from './ticket-dashboard.service';
import { DatabaseModule } from '../database/database.module';
import { RedisModule } from '../commonServices/redis.module';
import { UtilModule } from 'src/commonServices/util.module';
@Module({
  imports:[DatabaseModule,RedisModule,UtilModule],
  controllers: [TicketDashboardController],
  providers: [TicketDashboardService]
})
export class TicketDashboardModule {}
