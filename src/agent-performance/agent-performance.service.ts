import { Injectable, Inject } from '@nestjs/common';
import * as streamBuffers from 'stream-buffers';
import { Db, Collection } from 'mongodb';
import * as NodeCache from 'node-cache';
import axios, { AxiosResponse } from 'axios';
import { UtilService } from "../commonServices/utilService";
import * as fs from 'fs-extra';
import * as path from 'path';
import * as archiver from 'archiver';
import { RedisWrapper } from '../commonServices/redisWrapper';
const XLSX = require('xlsx');
import * as ExcelJS from 'exceljs';
import { MailService } from '../mail/mail.service';
import { generateSupportTicketEmailHTML, getCurrentFormattedDateTime } from '../templates/mailTemplates';
import { GCPServices } from '../commonServices/GCSFileUpload';
import { format } from '@fast-csv/format';
import { pipe } from 'rxjs';
import { Sequelize } from 'sequelize-typescript';
import { QueryTypes } from 'sequelize';
import { MongoClient } from 'mongodb';
import * as moment from "moment";
import { parse } from "csv-parse/sync";
import config from '../environment/config';
import { randomBytes } from 'crypto';
import * as FormData from 'form-data';
import { gunzipSync } from 'zlib';
const Logger = require("../commonServices/logger");
import {Responses,ResponseCodes} from './constant'
import { AgentPerformanceRwaFileService } from './AgentPerformanceRwaFileService';


@Injectable()
export class AgentPerformanceService {
  private logger: InstanceType<typeof Logger>;

  constructor(
    @Inject('MONGO_DB') private readonly db: Db,
    @Inject('SEQUELIZE') private readonly sequelize: Sequelize,
    private readonly redisWrapper: RedisWrapper,
    private readonly mailService: MailService,
    private readonly utilServices: UtilService,
    private readonly agentPerformanceRawFileService: AgentPerformanceRwaFileService,


  ) {
    this.logger = new Logger('agentPerformance.log');
    this.logger.info('agentPerformance initialized');


    
  }




async AgentPerformanceCalculateService(payload: any) {
  try {
    const db =  this.db;

    // Validate payload
    if (!payload || typeof payload !== "object" || Object.keys(payload).length === 0) {
      const reason = Responses.INVALID_PAYLOAD;
      this.logger.error(`Invalid payload received. Reason: ${reason}`);
      return {
        data: null,
        message: {
          msg: reason,
          code: ResponseCodes.ERROR_CODE
        }
      };
    }

    const { year_month } = payload;
    if (!year_month) {
      const reason = Responses.YEAR_MONTH_MSG;
      this.logger.error(`Payload missing year_month. Reason: ${reason}`);
      return {
        data: null,
        message: {
          msg: reason,
          code: ResponseCodes.ERROR_CODE
        }
      };
    }

    const [yearStr, monthStr] = year_month.split("-");
    const year = Number(yearStr);
    const month = Number(monthStr);

    if (isNaN(year) || isNaN(month) || month < 1 || month > 12) {
      const reason = "Invalid year or month values";
      this.logger.error(`Payload has invalid year_month. Reason: ${reason}`);
      return {
        data: {},
        message: { msg: reason, code: ResponseCodes.ERROR_CODE }
      };
    }

    this.logger.info(Responses.BACKGROUND_TASK_STARTED);

    const performanceData = await this.fetchRawFilesInformation(db, year, month);

    this.logger.info(Responses.BACKGROUND_TASK_COMPLETED);

    return {
      data: performanceData,
      message: {
        msg: Responses.AGENT_PERFORMANCE_QUICK_RES_MESSAGE,
        code: ResponseCodes.SUCCESS_CODE
      }
    };

  } catch (err) {
    this.logger.error(
      `AgentPerformanceCalculateService failed. Reason: ${err?.message || err}`,
      err
    );
    return {
      data: null,
      message: {
        msg: Responses.INTERNAL_ERROR,
        code: ResponseCodes.ERROR_CODE
      }
    };
  }
}



async fetchRawFilesInformation(db: any, year: any, month: any) {
    try {
        this.logger.info(`Fetching raw data for ${year}-${month}`);
      
        
        await this.agentPerformanceRawFileService.fetchInboundRawData(db, year, month);
    } catch (err) {
      console.log(err)
        this.logger.error(`Error in fetchRawFilesInformation: ${err?.message || err}`);
    }
}








}
