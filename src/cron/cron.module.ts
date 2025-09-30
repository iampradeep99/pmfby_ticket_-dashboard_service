    import { Module } from '@nestjs/common';
    import { ScheduleModule } from '@nestjs/schedule';
    import { CronService } from './cron.service';
    import { MysqlModule } from 'src/database/mysql.module';
    import { DatabaseModule } from 'src/database/database.module'; 
import { MailModule } from 'src/mail/mail.module';

    @Module({
        imports: [
        ScheduleModule.forRoot(),
        MysqlModule, 
        DatabaseModule,
        MailModule
    ],
    providers: [CronService]
    })
    export class CronModule {}
