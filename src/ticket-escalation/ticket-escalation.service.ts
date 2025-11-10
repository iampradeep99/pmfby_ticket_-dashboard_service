import { Injectable, Inject } from '@nestjs/common';
import * as streamBuffers from 'stream-buffers';
import { Db, Collection } from 'mongodb';
import * as NodeCache from 'node-cache';
// import axios from 'axios';
import axios, { AxiosResponse } from 'axios';
import { UtilService } from "../commonServices/utilService";
import * as fs from 'fs-extra';
import * as path from 'path';
import * as archiver from 'archiver';
import { RedisWrapper } from '../commonServices/redisWrapper';
const XLSX = require('xlsx');
// const ExcelJS = require('exceljs');
import * as ExcelJS from 'exceljs';
import { MailService } from '../mail/mail.service';
import { generateSupportTicketEmailHTML, getCurrentFormattedDateTime } from '../templates/mailTemplates';
import { GCPServices } from '../commonServices/GCSFileUpload';
import { format } from '@fast-csv/format';
import { pipe } from 'rxjs';
import { Sequelize } from 'sequelize-typescript';
import { QueryTypes } from 'sequelize'; // ✅ import QueryTypes
import { MongoClient } from 'mongodb';
import * as moment from "moment";
import { parse } from "csv-parse/sync";

// import * as ExcelJS from 'exceljs';

@Injectable()
export class TicketEscalationService {
  private ticketCollection: Collection;
  private ticketDbCollection: Collection;
  public gcp = new GCPServices();
  logDir = path.join(__dirname, '..', 'logs');

  constructor(
    @Inject('MONGO_DB') private readonly db: Db,
    @Inject('SEQUELIZE') private readonly sequelize: Sequelize,
    private readonly redisWrapper: RedisWrapper,
    private readonly mailService: MailService,
  ) {
    this.ticketCollection = this.db.collection('tickets');
    this.ticketDbCollection = this.db.collection('SLA_KRPH_SupportTickets_Records');
  }

  

   
  async fetchRoles(payload: any): Promise<{ data: any; message: { msg: string; code: number } }> {
    console.log("🚀 Entering fetchRoles process");

    try {
      const response: AxiosResponse<any> = await axios.get(process.env.pmfby_ROLE_URL, {
        params: payload || {},
        timeout: 10000, 
        headers: {
          'Content-Type': 'application/json'
        }
      });

      if (!response || !response.data) {
        return {
          data: null,
          message: { msg: '❌ No data received from API', code: 0 }
        };
      }

      return {
        data: response.data.data,
        message: { msg: '✅ Data fetched successfully', code: 1 }
      };

    } catch (error: any) {
      let errorMsg = '❌ Failed to fetch roles';

      if (error.response) {
        errorMsg = `❌ API responded with status ${error.response.status}`;
      } else if (error.request) {
        errorMsg = '❌ No response received from API';
      } else if (error.message) {
        errorMsg = `❌ Error: ${error.message}`;
      }

      console.error('❌ Error in fetchRoles:', error);

      return {
        data: null,
        message: { msg: errorMsg, code: 0 }
      };
    }
  }


}
