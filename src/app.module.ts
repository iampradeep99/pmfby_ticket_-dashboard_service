import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { TicketDashboardModule } from './ticket-dashboard/ticket-dashboard.module';
import { ConfigModule } from '@nestjs/config';
import { MysqlModule } from './database/mysql.module';
import { CronModule } from './cron/cron.module';
import { TicketEscalationModule } from './ticket-escalation/ticket-escalation.module';
import { SlaReportModule } from './sla-report/sla-report.module';
import { AgentPerformanceModule } from './agent-performance/agent-performance.module';
import { AuthModule } from './auth/auth.module';
import { BillingDashboardModule } from './billing-dashboard/billing-dashboard.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true, // This makes ConfigService available in all modules without needing to import it again
    }),
    TicketDashboardModule,
    MysqlModule,
    CronModule,
    TicketEscalationModule,
    SlaReportModule,
    AgentPerformanceModule,
    AuthModule,
    BillingDashboardModule, // This is where the module is imported
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
