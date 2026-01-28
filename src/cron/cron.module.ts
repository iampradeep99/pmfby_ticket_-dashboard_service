//     import { Module } from '@nestjs/common';
//     import { ScheduleModule } from '@nestjs/schedule';
//     import { CronService } from './cron.service';
//     import { MysqlModule } from 'src/database/mysql.module';
//     import { DatabaseModule } from 'src/database/database.module'; 
// import { MailModule } from 'src/mail/mail.module';
// import { TicketEscalationCron } from './ticketEscalationCron';
// import { UtilModule } from 'src/commonServices/util.module';
// import { DocketUpdateCron } from './docketUpdateCron';


//     @Module({
//         imports: [
//         ScheduleModule.forRoot(),
//         MysqlModule, 
//         DatabaseModule,
//         MailModule,
//         UtilModule
//     ],
//     providers: [CronService,TicketEscalationCron],
//      exports: [DocketUpdateCron,TicketEscalationCron]
//     })
//     export class CronModule {}


import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';
import { CronService } from './cron.service';
import { MysqlModule } from 'src/database/mysql.module';
import { DatabaseModule } from 'src/database/database.module'; 
import { MailModule } from 'src/mail/mail.module';
import { TicketEscalationCron } from './ticketEscalationCron';
import { UtilModule } from 'src/commonServices/util.module';
import { DocketUpdateCron } from './docketUpdateCron';

@Module({
  imports: [
    ScheduleModule.forRoot(),
    MysqlModule,
    DatabaseModule,
    MailModule,
    UtilModule,
  ],
  providers: [
    CronService,
    TicketEscalationCron,
    DocketUpdateCron, // ✅ MUST be here
  ],
  exports: [
    TicketEscalationCron,
    DocketUpdateCron, // ✅ now valid
  ],
})
export class CronModule {}
