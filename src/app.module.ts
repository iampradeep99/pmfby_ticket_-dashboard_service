import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { TicketDashboardModule } from './ticket-dashboard/ticket-dashboard.module';
import { ConfigModule } from '@nestjs/config';
import { MysqlModule } from './database/mysql.module';
import { CronModule } from './cron/cron.module';
import { TicketEscalationModule } from './ticket-escalation/ticket-escalation.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true, // This makes ConfigService available in all modules without needing to import it again
    }),
    TicketDashboardModule,
    MysqlModule,
    CronModule,
    TicketEscalationModule
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
