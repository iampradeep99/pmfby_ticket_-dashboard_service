import {
  Body,
  Controller,
  Post,
  Req,
  Res,
  UploadedFile,
  UseInterceptors,
} from '@nestjs/common';
import { Request, Response } from 'express';
import { diskStorage } from 'multer';
import { TicketDashboardService } from './ticket-dashboard.service';
import { CreateTicketDto } from 'src/DTOs/createTicket.dto';
import { UtilService } from '../commonServices/utilService';
import { RabbitMQService } from '../commonServices/rabbitMQ/rabbitmq.service';
import {
  jsonErrorHandler,
  jsonResponseHandler, jsonResponseHandlerCopy,jsonResponseHandlerReport
} from '../commonServices/responseHandler';
import { FileInterceptor } from '@nestjs/platform-express';

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

 @Post('getSupportTicketHistory')
async fetchSupportTicketHistory(@Body() ticketPayload: any, @Req() req: Request, @Res() res: Response) {
  const userEmail = ticketPayload?.userEmail?.trim();
  if (!userEmail) {
    return { rcode: 0, rmessage: 'User Email is required' };
  }

  // Send request to RabbitMQ queue
  await this.rabbitMQService.sendToQueue(ticketPayload);

  return jsonResponseHandler(
    [],
    'Your request has been accepted and is being processed in the background. You will soon see the download link in the list section.',
    req,
    res,
    () => {}
  );
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

    const { data: resultArray, pagination, message } = responsePayload;

    let gzippedData = null;
    if (resultArray && resultArray.length > 0) {
      const stringifiedData: any = resultArray;
      gzippedData = await this.utilService.GZip(stringifiedData); // ✅ Make sure this returns a Buffer
    }

    return jsonResponseHandlerReport(
      gzippedData,
      message,
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






  @Post('getSupportTicketHistory')
  async FarmerSelectCallingHistoryDownload(@Body() ticketPayload: any, @Req() req: Request,
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

  }

 @Post('FarmerSelectCallingHistoryDownload')
  async downloadFarmerCallingReport(
    @Body() ticketPayload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      const userEmail = ticketPayload?.userEmail?.trim();

     

    await this.dashboardService.downloadFarmerCallingReportService(ticketPayload);


    let rmessage = 'Your request has been accepted and is being processed in the background. You will soon see the download link in the list section.'
    return jsonResponseHandler([], rmessage, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }


 @Post('assignAllIndexed')
  async createIndexesAll(@Body() ticketPayload: any, @Req() req: Request,
    @Res({ passthrough: false }) res: Response) {


    await this.dashboardService.assignIndexes(ticketPayload);
    let rmessage = 'Your request has been accepted and is being processed in the background. You will soon see the download link in the list section.'
    return jsonResponseHandler([], rmessage, req, res, () => { });

  }


@Post('SupportTicketListing')
async supportTicketListing(
  @Body() payload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const { obj, message }: any = await this.dashboardService.fetchTicketListing(payload);

    const compressedData = obj ? await this.utilService.GZip(obj) : null;
    console.log("test")
    return jsonResponseHandler(compressedData, message, req, res, () => {});
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}

@Post('GrievanceTicketDashboard')
async GrievanceTicketDashboard(
  @Body() payload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const { obj, message }: any = await this.dashboardService.GrievanceTicketDashboard(payload);

    const compressedData = obj ? await this.utilService.GZip(obj) : null;
    console.log("test")
    return jsonResponseHandler(compressedData, message, req, res, () => {});
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}




@Post('SupportTicketListingDownload')
async supportTicketListingDownload(
  @Body() payload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const { obj, message }: any = await this.dashboardService.fetchTicketListingDownload(payload);

    const compressedData = obj ? await this.utilService.GZip(obj) : null;
    console.log("test")
    return jsonResponseHandler(compressedData, message, req, res, () => {});
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}

@Post('UpdateNCIPDocket')
async UpdateNCIPDocket(
  @Body() ticketPayload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const userEmail = ticketPayload?.userEmail?.trim();

    const rmessage =
      'Your request has been accepted and is being processed in the background. You will soon see the download link in the list section.';

    res.status(200).json({
      success: true,
      message: rmessage,
      data: [],
    });

    setImmediate(async () => {
      try {
        await this.dashboardService.supportTicketSyncingUpdateForDocketNumberForTicketHistory();
        console.log('✅ Docket update background process completed successfully.');
      } catch (backgroundErr) {
        console.error('❌ Background docket update failed:', backgroundErr);
      }
    });

  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}
 

 @Post('UpdateCallingRecords')
  @UseInterceptors(
    FileInterceptor('file', {
      storage: diskStorage({
        destination: 'uploads/',
        filename: (req, file, cb) => {
          const uniqueName = `${Date.now()}-${file.originalname}`;
          cb(null, uniqueName);
        },
      }),
    }),
  )
  async UpdateCallingRecords(
    @UploadedFile() file: Express.Multer.File,
    @Req() req: Request,
    @Res() res: Response,
  ) {
    try {
      // ✅ Send response immediately
      res.status(202).json({
        success: true,
        message: 'Your file has been uploaded and will be processed in the background.',
      });

      // ✅ Continue background processing
      setImmediate(async () => {
        try {
          console.log(`🚀 Background processing started for file: ${file?.filename}`);
          await this.dashboardService.updateCallingRecords(file);
          console.log('✅ Background processing completed successfully.');
        } catch (backgroundErr) {
          console.error('❌ Background processing failed:', backgroundErr);
        }
      });
    } catch (err) {
      console.error('Immediate error in UpdateCallingRecords:', err);
      return jsonErrorHandler(err, req, res, () => {});
    }
  }





  @Post('UpdateTicketStatus')
async updateTicketStatus(
  @Body() ticketPayload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const userEmail = ticketPayload?.userEmail?.trim();

    const rmessage =
      'Your request has been accepted and is being processed in the background. You will soon see the download link in the list section.';

    res.status(200).json({
      success: true,
      message: rmessage,
      data: [],
    });

    setImmediate(async () => {
      try {
        await this.dashboardService.updateStatusOfAllTickets(ticketPayload?.fromDate, ticketPayload?.toDate);
        console.log('✅ UpdateTicketStatus background process completed successfully.');
      } catch (backgroundErr) {
        console.error('❌ UpdateTicketStatus update failed:', backgroundErr);
      }
    });

  } catch (err) {
    // Handle any immediate (synchronous) errors
    return jsonErrorHandler(err, req, res, () => {});
  }
}

@Post('copyTicket')
async copyTicket(
  @Body() ticketPayload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const userEmail = ticketPayload?.userEmail?.trim();
    const prodCollectionName = ticketPayload?.prodCollectionName?.trim();
    const uatCollectionName = ticketPayload?.uatCollectionName?.trim();
    const fromDate = ticketPayload?.fromDate?.trim();
    const toDate = ticketPayload?.toDate?.trim();
    const chunkSize = Number(ticketPayload?.chunkSize) || 1000;

    if (!prodCollectionName || !uatCollectionName || !fromDate || !toDate) {
      return res.status(400).json({
        success: false,
        message:
          "Missing required fields. Please provide prodCollectionName, uatCollectionName, fromDate, and toDate.",
      });
    }

    const rmessage =
      "Your request has been accepted and is being processed in the background. You will soon see the result logs in the system.";

    res.status(200).json({
      success: true,
      message: rmessage,
      data: [],
    });

    setImmediate(async () => {
      try {
        console.log("🚀 Background Job: Copying from Prod to UAT...");
        console.log("📁 Source:", prodCollectionName);
        console.log("📁 Destination:", uatCollectionName);
        console.log("📅 Date Range:", fromDate, "→", toDate);
        console.log("⚙️ Chunk Size:", chunkSize);

        const resultMessage = await this.dashboardService.copyTicketListingFromProdToUAT(
          prodCollectionName,
          uatCollectionName,
          fromDate,
          toDate,
          chunkSize
        );

        console.log("✅ Copy operation completed successfully:", resultMessage);
      } catch (backgroundErr) {
        console.error("❌ CopyTicket background process failed:", backgroundErr);
      }
    });
  } catch (err) {
    // Handle any immediate (synchronous) errors
    return jsonErrorHandler(err, req, res, () => {});
  }
}



@Post('SaveCDRFilePath')
async CDRFilePath(
  @Body() payload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const { obj, message }: any = await this.dashboardService.saveCDRFilesPaths(payload);

    const compressedData = obj ? await this.utilService.GZip(obj) : null;
    console.log("test")
    return jsonResponseHandler(compressedData, message, req, res, () => {});
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}

@Post('GetNCIPUserRole')
async GetNCIPUserRole(
  @Body() payload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const { obj, message }: any = await this.dashboardService.GetNCIPUserRole(payload);

    const compressedData = obj ? await this.utilService.GZip(obj) : null;
    console.log("test")
    return jsonResponseHandler(compressedData, message, req, res, () => {});
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}


 @Post('excelImport')
@UseInterceptors(FileInterceptor('file')) 
async excelImport(
  @UploadedFile() file: Express.Multer.File,
  @Body() body: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response,
) {
  try {
    if (!file) return jsonErrorHandler({ message: "No file uploaded" }, req, res, () => {});

    const payload = {
      file: file.buffer,                 
      collectionName: body.collectionName,
      insertedBy: body.insertedBy || 'system',
    };

    jsonResponseHandler(
      { message: 'Excel import started' },
      'File received successfully',
      req,
      res,
      () => {},
    );

    this.dashboardService.csvImportService(payload)
      .then((result) => {
        console.log('✅ Excel import completed:', result.message);
      })
      .catch((err) => {
        console.error('❌ Excel import failed:', err?.message || err);
      });

  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}

}

 


  





