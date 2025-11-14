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
import { SlaReportService } from './sla-report.service';
import {
  jsonErrorHandler,
  jsonResponseHandler, jsonResponseHandlerCopy,jsonResponseHandlerReport
} from '../commonServices/responseHandler';
import { UtilService } from '../commonServices/utilService';
import { RabbitMQService } from 'src/commonServices/rabbitMQ/rabbitmq.service';


@Controller('sla-report')
export class SlaReportController {
  constructor(private readonly slaReportService: SlaReportService, 
      private readonly utilService: UtilService,private readonly rabbitMQService: RabbitMQService) {}

  
 
 
@Post('calculateSLA')
async calculateSLA(
    @Body() payload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.slaReportService.startSlaCalculation(payload);

      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }


}
