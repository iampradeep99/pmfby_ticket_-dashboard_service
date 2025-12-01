import { forwardRef, Module } from '@nestjs/common';
import { KrphFarmerPdfGenerationService } from './krph-farmer-pdf-generation.service';
import { KrphFarmerPdfGenerationController } from './krph-farmer-pdf-generation.controller';
import { DatabaseModule } from 'src/database/database.module';
import { RedisModule } from 'src/commonServices/redis.module';
import { UtilModule } from 'src/commonServices/util.module';
import { MailModule } from 'src/mail/mail.module';
import { MysqlModule } from 'src/database/mysql.module';
import { RabbitMQModule } from 'src/commonServices/rabbitMQ/rabbitmq.module';
import { QueueBrokerModule } from 'src/commonServices/queue-broker/queue-broker.module';

@Module({
   imports: [
      DatabaseModule,
      RedisModule,
      UtilModule,
      MailModule,
      forwardRef(() => RabbitMQModule),
      forwardRef(() => QueueBrokerModule),

      MysqlModule,
    ],
  providers: [KrphFarmerPdfGenerationService],
  controllers: [KrphFarmerPdfGenerationController]
})
export class KrphFarmerPdfGenerationModule {}
