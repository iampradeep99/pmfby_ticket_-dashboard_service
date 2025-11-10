import {
  Body,
  Controller,
  Get,
  Post,
  Req,
  Res,
  UploadedFile,
  UseInterceptors,
} from '@nestjs/common';
import { Request, Response } from 'express';
import { diskStorage } from 'multer';
import { TicketEscalationService } from './ticket-escalation.service';
import { CreateTicketDto } from 'src/DTOs/createTicket.dto';
import { UtilService } from '../commonServices/utilService';
import { RabbitMQService } from '../commonServices/rabbitMQ/rabbitmq.service';
import {
  jsonErrorHandler,
  jsonResponseHandler, jsonResponseHandlerCopy,jsonResponseHandlerReport
} from '../commonServices/responseHandler';
import { FileInterceptor } from '@nestjs/platform-express';

@Controller('ticket-escalation')
export class TicketEscalationController {
  constructor(
    private readonly dashboardService: TicketEscalationService,
    private readonly utilService: UtilService,private readonly rabbitMQService: RabbitMQService

  ) { }

  
  @Post('roles')
  async fetchRoles(
    @Body() payload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.dashboardService.fetchRoles(payload);

      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }


  @Post('InsuranceTicketList')
async InsuranceTicketListing(
  @Body() payload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const { obj, message }: any = await this.dashboardService.insuracneTicketListingService(payload);

    const compressedData = obj ? await this.utilService.GZip(obj) : null;
    console.log("test")
    return jsonResponseHandler(compressedData, message, req, res, () => {});
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}

@Post('AssignTickets')
async AssignTickets(
  @Body() payload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const result: any = await this.dashboardService.AssignTicketServie(payload);

    const responseData = {
      summary: result.summary,
      details: result.details, 
    };

    const compressedData = responseData ? await this.utilService.GZip(responseData) : null;

    return jsonResponseHandler(compressedData, result.summary.message, req, res, () => {});
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}




  
}

 


  





