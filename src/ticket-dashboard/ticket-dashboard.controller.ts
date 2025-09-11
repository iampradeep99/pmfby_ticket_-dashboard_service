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
import { RabbitMQService } from '../commonServices/rabbitMQ/rabbitmq.service';
import {
  jsonErrorHandler,
  jsonResponseHandler, jsonResponseHandlerCopy,jsonResponseHandlerReport
} from '../commonServices/responseHandler';

@Controller('ticket-dashboard')
export class TicketDashboardController {
  constructor(
    private readonly dashboardService: TicketDashboardService,
    private readonly utilService: UtilService,private readonly rabbitMQService: RabbitMQService

  ) { }

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

      return jsonResponseHandler(data, message, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }

/*   @Post('getSupportTicketHistory')
  async fetchSupportTicketHistory(@Body() ticketPayload: any, @Req() req: Request,
    @Res({ passthrough: false }) res: Response) {

    const userEmail = ticketPayload?.userEmail?.trim();

    if (!userEmail) {
      return {
        rcode: 0,
        rmessage: 'User Email is required',
      };
    }
     await this.rabbitMQService.sendToQueue(ticketPayload);
    // await this.dashboardService.getSupportTicketHistotReportDownload(ticketPayload);
    let rmessage = 'Your request has been accepted and is being processed in the background. You will soon see the download link in the list section.'
    return jsonResponseHandler([], rmessage, req, res, () => { });

  } */


    @Post('getSupportTicketHistory')
async fetchSupportTicketHistory(
  @Body() ticketPayload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  const userEmail = ticketPayload?.userEmail?.trim();

  if (!userEmail) {
    return {
      rcode: 0,
      rmessage: 'User Email is required',
    };
  }

  // Push job to RabbitMQ with proper job type
  await this.rabbitMQService.sendToQueue({
    type: 'ticket_history',
    payload: ticketPayload,
  });

  const rmessage =
    'Your request has been accepted and is being processed in the background. You will soon see the download link in the list section.';
  
  return jsonResponseHandler([], rmessage, req, res, () => {});
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
        return jsonResponseHandlerCopy(
          null,
          'User Email is required',
          undefined,
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
      return jsonErrorHandler(err, req, res, () => { });
    }
  }



  @Post('getRequestDownloadHistory')
  async getDownloadHistory(
    @Body() payload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response,
  ) {
    try {
      const { data: resultArray, message } = await this.dashboardService.downloadHistory(payload);

      let gzippedData = null;
      if (resultArray && resultArray.length > 0) {
        const stringifiedData: any = resultArray;
        console.log(stringifiedData)
        gzippedData = await this.utilService.GZip(stringifiedData);
      }

      return jsonResponseHandler(gzippedData, message, req, res, () => { });

    } catch (err) {
      console.error(err);
      return res.status(500).json({ message: 'Internal Server Error' });
    }
  }


@Post('FarmerSelectCallingHistory')
async FarmerSelectCallingHistoryRoute(
  @Body() payload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response,
) {
  try {
    const responsePayload = await this.dashboardService.FarmerSelectCallingHistoryService(payload);

    const { data: resultArray, pagination } = responsePayload;

    let gzippedData = null;
    if (resultArray && resultArray.length > 0) {
      const stringifiedData: any = resultArray;
      gzippedData = await this.utilService.GZip(stringifiedData); // ✅ Make sure this returns a Buffer
    }

    return jsonResponseHandlerReport(
      gzippedData,
      "✅ Data fetched successfully",
      pagination,
      req,
      res,
      () => {}
    );

  } catch (err) {
    console.error(err);
    return jsonResponseHandler(
      null,
      { msg: '❌ Internal Server Error', code: 0 },
      req,
      res,
      () => {}
    );
  }
}







@Post('FarmerSelectCallingHistoryDownload')
async downloadFarmerCallingReport(
  @Body() ticketPayload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const userEmail = ticketPayload?.userEmail?.trim();

    if (!userEmail) {
      return jsonResponseHandlerCopy(
        null,
        'User Email is required',
        undefined,
        req,
        res
      );
    }

    // Push job to RabbitMQ instead of running the service directly
    await this.rabbitMQService.sendToQueue({
      type: 'farmer_calling_history',
      payload: ticketPayload,
    });

    const rmessage = 'Your request has been accepted and is being processed in the background. You will soon see the download link in the list section.';
    return jsonResponseHandler([], rmessage, req, res, () => {});
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}



 @Post('assignAllIndexed')
  async createIndexesAll(@Body() ticketPayload: any, @Req() req: Request,
    @Res({ passthrough: false }) res: Response) {


    await this.dashboardService.assignIndexes(ticketPayload);
    let rmessage = 'Your request has been accepted and is being processed in the background. You will soon see the download link in the list section.'
    return jsonResponseHandler([], rmessage, req, res, () => { });

  }


  }





