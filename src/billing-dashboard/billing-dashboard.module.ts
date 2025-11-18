import { forwardRef, Module } from '@nestjs/common';
import { BillingDashboardController } from './billing-dashboard.controller';
import { BillingDashboardService } from './billing-dashboard.service';
import { MysqlModule } from 'src/database/mysql.module';
import { RabbitMQModule } from 'src/commonServices/rabbitMQ/rabbitmq.module';
import { MailModule } from 'src/mail/mail.module';
import { UtilModule } from 'src/commonServices/util.module';
import { RedisModule } from 'src/commonServices/redis.module';
import { DatabaseModule } from 'src/database/database.module';


@Module({
  imports: [
      DatabaseModule,
      RedisModule,
      UtilModule,
      MailModule,
      forwardRef(() => RabbitMQModule),
      MysqlModule,
    ],
  controllers: [BillingDashboardController],
  providers: [BillingDashboardService]
})
export class BillingDashboardModule {}
