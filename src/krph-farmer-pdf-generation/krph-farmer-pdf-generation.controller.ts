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
import { KrphFarmerPdfGenerationService } from './krph-farmer-pdf-generation.service';
import {
  jsonErrorHandler,
  jsonResponseHandler, jsonResponseHandlerCopy,jsonResponseHandlerReport
} from '../commonServices/responseHandler';
import { UtilService } from '../commonServices/utilService';
import { RabbitMQService } from 'src/commonServices/rabbitMQ/rabbitmq.service';
import { FileInterceptor } from '@nestjs/platform-express';
import {PdfGenerationPayload} from "./pdfGeneration.dto"


@Controller('farmerpdf')
export class KrphFarmerPdfGenerationController {

constructor(private readonly krphPDFGeneration: KrphFarmerPdfGenerationService, 
          private readonly utilService: UtilService,private readonly rabbitMQService: RabbitMQService) {}


@Post('generate')
async generatePDF(
    @Body() payload: PdfGenerationPayload,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.krphPDFGeneration.generatePDFService(payload);


      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }



}
