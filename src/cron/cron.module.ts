    import { Module } from '@nestjs/common';
    import { ScheduleModule } from '@nestjs/schedule';
    import { CronService } from './cron.service';
    import { MysqlModule } from 'src/database/mysql.module';
    import { DatabaseModule } from 'src/database/database.module'; 
import { MailModule } from 'src/mail/mail.module';
import { TicketEscalationCron } from './ticketEscalationCron';
import { UtilModule } from 'src/commonServices/util.module';


    @Module({
        imports: [
        ScheduleModule.forRoot(),
        MysqlModule, 
        DatabaseModule,
        MailModule,
        UtilModule
    ],
    providers: [CronService,TicketEscalationCron],
     exports: [TicketEscalationCron,TicketEscalationCron]
    })
    export class CronModule {}
