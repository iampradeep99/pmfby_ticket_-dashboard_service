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
import { AgentPerformanceService } from './agent-performance.service';
import {
  jsonErrorHandler,
  jsonResponseHandler, jsonResponseHandlerCopy,jsonResponseHandlerReport
} from '../commonServices/responseHandler';
import { UtilService } from '../commonServices/utilService';
import { RabbitMQService } from 'src/commonServices/rabbitMQ/rabbitmq.service';
import { FileInterceptor } from '@nestjs/platform-express';


@Controller('agent-performance')
export class AgentPerformanceController {

      constructor(private readonly agentPerformance: AgentPerformanceService, 
          private readonly utilService: UtilService,private readonly rabbitMQService: RabbitMQService) {}



@Post('calculate')
async calculateSLA(
    @Body() payload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.agentPerformance.AgentPerformanceCalculateService(payload)

      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }

}
