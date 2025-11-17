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
import { FileInterceptor } from '@nestjs/platform-express';


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


/* @Post('UploadCallQualityRecords')
@UseInterceptors(FileInterceptor('file'))
async uploadCallQualityFile(
  @UploadedFile() file: Express.Multer.File,
  @Body('year_month') yearMonth: string, // Capture the year_month from formData
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    // Pass both file and yearMonth to the service
    const { data, message } =
      await this.slaReportService.uploadCallQualityFileService(file, yearMonth);

    // GZip the data if it exists
    const zipped = data ? await this.utilService.GZip(data) : null;

    return jsonResponseHandler(zipped, message, req, res, () => {});
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
} */

/* @Post('UploadCallQualityRecords')
@UseInterceptors(FileInterceptor('file'))
async uploadCallQualityFile(
  @UploadedFile() file: Express.Multer.File,
  @Body('year_month') yearMonth: string, // Capture the year_month from formData
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    // Immediately respond with an acknowledgment to the client
    res.status(200).json({ message: 'File upload started. Processing in the background.' });

    // Start background processing without blocking the response
    setImmediate(async () => {
      try {
        // Pass both file and yearMonth to the service for processing
        const { data, message } = await this.slaReportService.uploadCallQualityFileService(file, yearMonth);

        // GZip the data if it exists
        const zipped = data ? await this.utilService.GZip(data) : null;

        // Optionally, handle the zipped data (store, email, etc.)
        // For example, saving to a file or database, sending an email, etc.
        // Your post-processing logic can go here
        console.log('File processed successfully:', message);
      } catch (err) {
        console.error('Error in background processing:', err);
      }
    });
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}
 */

@Post('UploadCallQualityRecords')
@UseInterceptors(FileInterceptor('file'))
async uploadCallQualityFile(
  @UploadedFile() file: Express.Multer.File,
  @Body('year_month') yearMonth: string,
  @Req() req: Request,
  @Res() res: Response
) {
  try {
    const response = await this.slaReportService.startCallQualityFileUploading(file, yearMonth);

    return res.status(200).json(response);
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}




}
