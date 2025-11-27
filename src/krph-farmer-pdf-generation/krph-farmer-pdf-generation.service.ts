import { Injectable, Inject } from '@nestjs/common';
import * as streamBuffers from 'stream-buffers';
import { Db, Collection, MongoClient } from 'mongodb';
import * as NodeCache from 'node-cache';
import axios from 'axios';
import { UtilService } from "../commonServices/utilService";
import * as fs from 'fs-extra';
import * as path from 'path';
import * as archiver from 'archiver';
import { RedisWrapper } from '../commonServices/redisWrapper';
import * as ExcelJS from 'exceljs';
import { MailService } from '../mail/mail.service';
import { generateSupportTicketEmailHTML, getCurrentFormattedDateTime } from '../templates/mailTemplates';
import { GCPServices } from '../commonServices/GCSFileUpload';
import { format } from '@fast-csv/format';
import { pipe } from 'rxjs';
import { Sequelize } from 'sequelize-typescript';
import { QueryTypes } from 'sequelize';
import * as moment from "moment";
import { parse } from "csv-parse/sync";
import config from '../environment/config';
import { randomBytes } from 'crypto';
import * as FormData from 'form-data';
import { gunzipSync } from 'zlib';
const Logger = require("../commonServices/logger");
import { Responses, ResponseCodes } from '../agent-performance/constant';
import { QueueBrokerModule } from 'src/commonServices/queue-broker/queue-broker.module';
import { PdfGenerationService } from 'src/commonServices/queue-broker/pdf-generation/pdf-generation.service';
const XLSX = require('xlsx');

@Injectable()
export class KrphFarmerPdfGenerationService {
  private logger: InstanceType<typeof Logger>;

  constructor(
    @Inject('MONGO_DB') private readonly db: Db,
    @Inject('SEQUELIZE') private readonly sequelize: Sequelize,
    private readonly redisWrapper: RedisWrapper,
    private readonly mailService: MailService,
    private readonly utilServices: UtilService,
    private readonly pdfGenerationBroker: PdfGenerationService
  ) {
    this.logger = new Logger('agentPerformance.log');
    this.logger.info('agentPerformance initialized');
  }

 async generatePDFService(payload: any) {
  const db = this.db;

  try {
    const response = {
      data: {},
      message: {
        msg: Responses.PDF_GENERATION_CLIENT_MESSAGE,
        code: ResponseCodes.SUCCESS_CODE
      }
    };

    setImmediate(() => {
      this.pdfGenerationBroker.sendToQueue(payload);
    });

    return response;

  } catch (err) {
    const errorDetails = typeof err === "object" ? JSON.stringify(err) : String(err);

    this.logger.error(`AgentPerformanceCalculateService failed: ${errorDetails}`);

    return {
      data: null,
      message: {
        msg: Responses.INTERNAL_ERROR,
        code: ResponseCodes.ERROR_CODE
      }
    };
  }
}

}
