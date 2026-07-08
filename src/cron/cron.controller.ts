import { Controller, Post } from '@nestjs/common';
import { CronService } from './cron.service';

@Controller('cron')
export class CronController {
  constructor(private readonly cronService: CronService) {}

  @Post('sync-ticket-comments')
  async syncTicketComments() {
    const message = await this.cronService.syncTicketComments();

    return {
      code: 1,
      message,
    };
  }
}
