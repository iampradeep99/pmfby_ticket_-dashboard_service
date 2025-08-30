import {
  Body,
  Controller,
  Post,
  Req,
  Res,
} from '@nestjs/common';
import { Request, Response } from 'express';

import { TicketDashboardService } from './ticket-dashboard.service';
import { CreateTicketDto } from 'src/DTOs/createTicket.dto';
import { UtilService } from '../commonServices/utilService';
import {
  jsonErrorHandler,
  jsonResponseHandler,
} from '../commonServices/responseHandler';

@Controller('ticket-dashboard')
export class TicketDashboardController {
  constructor(
    private readonly dashboardService: TicketDashboardService,
    private readonly utilService: UtilService
  ) {}

  @Post('myticket')
  async createTicket(@Body() ticketData: CreateTicketDto) {
    return await this.dashboardService.createTicket(ticketData);
  }

  @Post()
  async fetchDashboard(
    @Body() ticketInfo: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.dashboardService.fetchTickets(ticketInfo);

      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => {});
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => {});
    }
  }

  @Post('getSupportTicketHistory')
  async fetchSupportTicketHistory(@Body() ticketPayload: any) {
    await this.dashboardService.getSupportTicketHistotReportDownload(ticketPayload);
    return { rcode: 1, rmessage: 'Request received. Processing in background.' };
  }
}
