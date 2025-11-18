import {
  Body,
  Controller,
  Post,
  Req,
  Res,
  UploadedFile,
  UseInterceptors,
  Headers,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { Request, Response } from 'express';
import { BillingDashboardService } from './billing-dashboard.service';
import { jsonErrorHandler, jsonResponseHandler } from '../commonServices/responseHandler';
import { UtilService } from '../commonServices/utilService';
import { RabbitMQService } from 'src/commonServices/rabbitMQ/rabbitmq.service';

@Controller('billing-dashboard/import/')
export class BillingDashboardController {
  constructor(
    private readonly billingDashbaord: BillingDashboardService,
    private readonly utilService: UtilService,
    private readonly rabbitMQService: RabbitMQService,
  ) {}



  @Post('inbound')
@UseInterceptors(FileInterceptor('file'))
async uploadCallQualityFile(
  @UploadedFile() file: Express.Multer.File,
  @Body('year_month') yearMonth: string,
  @Req() req: Request,
  @Res() res: Response
) {
  try {
    const response = await this.billingDashbaord.ImportInboundRecordService(file, yearMonth);

    return res.status(200).json(response);
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}



    @Post('getInbound')
    async FetchInboundRecords(
        @Body() payload: any,
        @Req() req: Request,
        @Res({ passthrough: false }) res: Response
    ) {
        try {
        let { data, message } = await this.billingDashbaord.FetchInboundRecordService(payload);

        if (data) data = await this.utilService.GZip(data);

        return jsonResponseHandler(data, message, req, res, () => { });
        } catch (err) {
        return jsonErrorHandler(err, req, res, () => { });
        }
    }



}
