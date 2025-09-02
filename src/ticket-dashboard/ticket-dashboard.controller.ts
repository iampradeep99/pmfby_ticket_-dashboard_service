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
  jsonResponseHandler,jsonResponseHandlerCopy
} from '../commonServices/responseHandler';

@Controller('ticket-dashboard')
export class TicketDashboardController {
  constructor(
    private readonly dashboardService: TicketDashboardService,
    private readonly utilService: UtilService,

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
    const userEmail = ticketPayload?.userEmail?.trim();

    if (!userEmail) {
      return {
        rcode: 0,
        rmessage: 'User Email is required',
      };
    }

    await this.dashboardService.getSupportTicketHistotReportDownload(ticketPayload);

    return {
      rcode: 1,
      rmessage: `Your download request has been received. You will receive an email at ${userEmail} with the support ticket data shortly.`

    };
  }

@Post('getSupportTicketHistoryReportView')
async fetchSupportTicketHistoryReportView(
  @Body() ticketPayload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const userEmail = ticketPayload?.userEmail?.trim();

    if (!userEmail) {
      // Call the response handler with optional parameters
      return jsonResponseHandlerCopy(
        null,
        'User Email is required',
        undefined, // pagination is optional
        req,
        res
      );
    }

    const result: any = await this.dashboardService.getSupportTicketHistotReport(ticketPayload);

    let { data, message, pagination } = result;

    if (data) {
      data = await this.utilService.GZip(data);
    }

    return jsonResponseHandlerCopy(
      data,
      message || 'Report generated successfully.',
      pagination, // optional
      req,
      res
    );
  } catch (err) {
   return jsonErrorHandler(err, req, res, () => {});
  }
}


 



}

