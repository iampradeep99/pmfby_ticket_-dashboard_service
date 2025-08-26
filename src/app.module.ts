import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { TicketDashboardModule } from './ticket-dashboard/ticket-dashboard.module';
import { ConfigModule } from '@nestjs/config';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true, // This makes ConfigService available in all modules without needing to import it again
    }),
    TicketDashboardModule
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
