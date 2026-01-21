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
import { DocketUpdateCron } from 'src/cron/docketUpdateCron';


@Controller('ticket-escalation')

export class TicketEscalationController {

  constructor(
    private readonly dashboardService: TicketEscalationService,
// private readonly docketUpdateCron: DocketUpdateCron,

    private readonly utilService: UtilService,private readonly rabbitMQService: RabbitMQService

  ) { 

  }

  
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
   @Post('getBank')
  async getRoles(
    @Body() payload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.dashboardService.getRole(payload);

      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }


  @Post('getRoles')
  async getRolesForGovt(
    @Body() payload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.dashboardService.getRolesForGovt(payload);

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
    const {data, message} :any = await this.dashboardService.AssignTicketService(payload);
console.log(message)
    const responseData = {
      summary: data.summary,
      details: data.details, 
    };

    const compressedData = responseData ? await this.utilService.GZip(data) : null;

    return jsonResponseHandler(compressedData, message, req, res, () => {});
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}


  @Post('UserWiseState')
async UserWiseState(
  @Body() payload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const { data, message }: any = await this.dashboardService.UserWiseState(payload);
    

    const compressedData = data ? await this.utilService.GZip(data) : null;
    return jsonResponseHandler(compressedData, message, req, res, () => {});
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}


@Post('RoleWiseAssignedTickets')
async RoleWiseAssignedTicketList(
  @Body() payload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {
    const { data, message }: any = await this.dashboardService.RoleWiseAssignedTickets(payload);

    const compressedData = data?.length ? await this.utilService.GZip(data) : null;

    return jsonResponseHandler(compressedData, message, req, res, () => {});
  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}



 @Post('uploadTicketPDF')
  @UseInterceptors(FileInterceptor('file')) 
  async uploadTicketPDF(
    @UploadedFile() file: Express.Multer.File,
    @Body() body: any, 
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response,
  ) {
    try {
      if (!file) {
        throw new Error('No file uploaded');
      }

      const { buffer, originalname, mimetype } = file;

      const {
        SupportTicketID,
        SupportTicketNo,
        TicketHistoryID,
        TicketStatusID,
        LastTicketStatusID,
        RequestorMobileNo,
        UpdatedByID,
        UpdatedBy,
        UpdateDateTime,
      } = body;

      let { data, message } = await this.dashboardService.uploadTicketPDFService({
        SupportTicketID,
        SupportTicketNo,
        TicketHistoryID,
        TicketStatusID,
        LastTicketStatusID,
        RequestorMobileNo,
        UpdatedByID,
        UpdatedBy,
        UpdateDateTime,
        fileBuffer: buffer,
        fileName: originalname,
        mimeType: mimetype,
      });
      
      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => {});
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => {});
    }
  }



 @Post('SyncAudio')
async syncAudio(
  @Body() payload: any,
  @Req() req: Request,
  @Res({ passthrough: false }) res: Response
) {
  try {

    jsonResponseHandler(
      { status: "processing", requestId: Date.now() },
      { msg: "Sync started in background", code: 1 },
      req,
      res,
      () => {}
    );

    setImmediate(async () => {
      try {
        console.log("🚀 Background sync started...");
        await this.dashboardService.syncAudioFiles(payload);
        console.log("🎉 Background sync completed!");
      } catch (err) {
        console.log("❌ Background sync failed:", err);
      }
    });

  } catch (err) {
    return jsonErrorHandler(err, req, res, () => {});
  }
}



 @Post('getPhoto')
  async getphoto(
    @Body() payload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.dashboardService.getPhotoServie(payload);

      // if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }


  @Post('insuranceWiseTicket')
  async InsuranceWiseTickets(
    @Body() payload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.dashboardService.insuranceWiseTicketList(payload);

      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }

    @Post('BucketAssignedTicketsCount')
  async getBucketTicketCount(
    @Body() payload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.dashboardService.getBucketTicketCount(payload);

      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }

    @Post('BucketAssignedTickets')
  async getBucketTicket(
    @Body() payload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.dashboardService.getBucketTicket(payload);

      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }



  @Post('assigedTicketByInsurance')
  async AssignedTicketByInsurance(
    @Body() payload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.dashboardService.AssignedTicketByInsuranceService(payload);

      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }



  @Post('escalationHistoryTrail')
  async EscalationHistoryTrail(
    @Body() payload: any,
    @Req() req: Request,
    @Res({ passthrough: false }) res: Response
  ) {
    try {
      let { data, message } = await this.dashboardService.EscalationHistoryTrailService(payload);

      if (data) data = await this.utilService.GZip(data);

      return jsonResponseHandler(data, message, req, res, () => { });
    } catch (err) {
      return jsonErrorHandler(err, req, res, () => { });
    }
  }

  // @Post('updateDocketMissing')
  // async updateDocket(
  //   @Body() payload: any,
  //   @Req() req: Request,
  //   @Res({ passthrough: false }) res: Response
  // ) {
  //   try {
  //     await this.docketUpdateCron.docketUpdateTickets();
  //     let data = {};
  //     let message = {}
  //     if (data) data = await this.utilService.GZip(data);

  //     return jsonResponseHandler(data, message, req, res, () => { });
  //   } catch (err) {
  //     return jsonErrorHandler(err, req, res, () => { });
  //   }
  // }

  
  



}

 

  


 


  





