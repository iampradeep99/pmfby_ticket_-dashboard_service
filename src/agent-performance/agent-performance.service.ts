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
import { AverageHandlingTimeService } from './AverageHandlingTimeService';
import { CRMTaggingCalcullationService } from './CRMTaggingCalcullationService';
import { CustomerRatingService } from './CustomerRatingService';
import { FeedbackTransferStatusService } from './FeedbackTransferStatusService';
import { ProductiveCallingServices } from './ProductiveCallingServices';
import { HangByAgentService } from './HangByAgentService';
import { CallQualityAssuranceService } from './CallQualityAssuranceService';


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
    private readonly averageHandlingTime: AverageHandlingTimeService,
    private readonly crmTaggingCalculation: CRMTaggingCalcullationService,
    private readonly customerRating: CustomerRatingService,
    private readonly feedbackTransfer:FeedbackTransferStatusService ,
    private readonly productionCalling:ProductiveCallingServices ,
    private readonly hangByAgent:HangByAgentService ,
    private readonly callQuality:CallQualityAssuranceService ,










  ) {
    this.logger = new Logger('agentPerformance.log');
    this.logger.info('agentPerformance initialized');


    
  }




/* async AgentPerformanceCalculateService(payload: any) {
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

await this.fetchRawFilesInformation(db, year, month);
this.logger.info(`Processing raw files for ${year_month}`);

await this.DiffecenceBetweenLoginAndActiveTime(db, year, month);
this.logger.info(`Calculating difference between login and active time for ${year_month}`);

    await this.processMonthWiseDifference(db, year, month);
    this.logger.info(`Calculating agent performance completed for ${year_month}`);

  

    this.logger.info(Responses.BACKGROUND_TASK_COMPLETED);

    return {
      data: {},
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
} */


  async AgentPerformanceCalculateServicePrevious(payload: any) {
  const db = this.db;

  try {
    this.logger.info("AgentPerformanceCalculateService invoked");

    if (!payload || typeof payload !== "object" || !Object.keys(payload).length) {
      const msg = Responses.INVALID_PAYLOAD;
      this.logger.error(`Invalid payload received: ${JSON.stringify(payload)}`);
      return {
        data: null,
        message: { msg, code: ResponseCodes.ERROR_CODE }
      };
    }

    const { year_month } = payload;

    if (!year_month || typeof year_month !== "string") {
      const msg = Responses.YEAR_MONTH_MSG;
      this.logger.error(`Missing or invalid year_month: ${year_month}`);
      return {
        data: null,
        message: { msg, code: ResponseCodes.ERROR_CODE }
      };
    }

    const parts = year_month.split("-");
    if (parts.length !== 2) {
      const msg = "Invalid year_month format. Expected YYYY-MM";
      this.logger.error(`Invalid year_month format: ${year_month}`);
      return {
        data: null,
        message: { msg, code: ResponseCodes.ERROR_CODE }
      };
    }

    const [yearStr, monthStr] = parts;
    const year = Number(yearStr);
    const month = Number(monthStr);

    if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
      const msg = "Invalid year or month value";
      this.logger.error(`Invalid year or month: year=${year}, month=${month}`);
      return {
        data: null,
        message: { msg, code: ResponseCodes.ERROR_CODE }
      };
    }

    this.logger.info(`Task started for year_month=${year_month}`);

    this.logger.info("Step 1: Fetching raw file information");
    await this.fetchRawFilesInformation(db, year, month);

    this.logger.info("Step 2: Calculating login vs active time difference");
    await this.DiffecenceBetweenLoginAndActiveTime(db, year, month);

    this.logger.info("Step 3: Processing month-wise agent performance");
    await this.processMonthWiseDifference(db, year, month);

    this.logger.info("Step 4: Processing Day Wise Average Handling Time");
    await this.averageHandlingTime.agentAverageHandlingTime(db, year, month);
     
    this.logger.info("Step 5: Processing Month Wise Average Handling Time");
    await this.averageHandlingTime.logic_agentAvegrageHandlingTime_monthWise(db, year, month);

    this.logger.info("Step 6: Processing CRM Tagging Day Wise");
    await this.crmTaggingCalculation.dayWiseCRMTaggingCalculation(db,year,month);

    this.logger.info("Step 7: Processing CRM Tagging Month Wise");
    await this.crmTaggingCalculation.monthWiseCRMTaggingCalculation(db,year,month);

    this.logger.info("Step 8: Processing Customer Rating Day Wise");
    await this.customerRating.dayWiseCustomerRatingForAgent(db,year,month);

     this.logger.info("Step 9: Processing Customer Rating Month Wise");
    await this.customerRating.monthWiseCustomerRatingForAgent(db,year,month);

    this.logger.info("Step 9: Processing Feedback Transfer Day Wise");
    await this.feedbackTransfer.feedbackTransferStatusDayWise(db,year,month);

    this.logger.info("Step 9: Processing Feedback Transfer Month Wise");
    await this.feedbackTransfer.feedbackTransferStatusMonthWise(db,year,month);

    this.logger.info("Step 10: Processing Productive Calling Day Wise");
    await this.productionCalling.productiveCallingDayWise(db,year,month);
    
    this.logger.info("Step 11: Processing Productive Calling Month Wise");
    await this.productionCalling.productiveCallingMonthWise(db,year,month);

    this.logger.info("Step 12: Hang By Agent Day Wise");
    await this.hangByAgent.hangByAgentDayWSise(db,year,month);

    this.logger.info("Step 13: Hang By Agent Month Wise");
    await this.hangByAgent.hangByAgentMonthWise(db,year,month);

    this.logger.info("Step 14: Call Quality Scrore Day Wise");
    await this.callQuality.callQualityAssuranceDayWise(db,year,month);

    this.logger.info("Step 14: Call Quality Scrore Month Wise");
    await this.callQuality.callQualityAssuranceMonthWise(db,year,month);
    

    this.logger.info(`Task completed for year_month=${year_month}`);

    return {
      data: {},
      message: {
        msg: Responses.AGENT_PERFORMANCE_QUICK_RES_MESSAGE,
        code: ResponseCodes.SUCCESS_CODE
      }
    };

  } catch (err) {
    const errorDetails =
      typeof err === "object" ? JSON.stringify(err) : String(err);

    this.logger.error(
      `AgentPerformanceCalculateService failed: ${errorDetails}`
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

async AgentPerformanceCalculateService(payload: any) {
  const db = this.db;
  const log = this.logger;

  try {
    log.info("AgentPerformanceCalculateService invoked");

    if (!payload || typeof payload !== "object" || !Object.keys(payload).length) {
      log.error("Invalid payload received");
      return {
        data: null,
        message: { msg: Responses.INVALID_PAYLOAD, code: ResponseCodes.ERROR_CODE }
      };
    }

    const { year_month } = payload;

    if (typeof year_month !== "string" || !year_month.includes("-")) {
      log.error("Invalid or missing year_month");
      return {
        data: null,
        message: { msg: Responses.YEAR_MONTH_MSG, code: ResponseCodes.ERROR_CODE }
      };
    }

    const [yearStr, monthStr] = year_month.split("-");
    const year = Number(yearStr);
    const month = Number(monthStr);

    if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
      log.error(`Invalid year or month: year=${year}, month=${month}`);
      return {
        data: null,
        message: { msg: "Invalid year or month value", code: ResponseCodes.ERROR_CODE }
      };
    }

    /**
     * --------------------------------------------------
     * RETURN RESPONSE TO CLIENT **IMMEDIATELY**
     * --------------------------------------------------
     */
    setTimeout(() => this._processAgentPerformance(db, year, month, year_month), 0);

    return {
      data: {},
      message: {
        msg: "Agent performance calculations started in background",
        code: ResponseCodes.SUCCESS_CODE
      }
    };

  } catch (err) {
    const details = typeof err === "object" ? JSON.stringify(err) : String(err);
    log.error(`AgentPerformanceCalculateService failed: ${details}`);

    return {
      data: null,
      message: {
        msg: Responses.INTERNAL_ERROR,
        code: ResponseCodes.ERROR_CODE
      }
    };
  }
}


/**
 * --------------------------------------------------
 * BACKGROUND PROCESSOR FUNCTION
 * --------------------------------------------------
 */
private async _processAgentPerformance(db: any, year: number, month: number, year_month: string) {
  const log = this.logger;

  try {
    log.info(`Background Task started for ${year_month}`);

    const steps = [
      { name: "Fetching raw file information", fn: this.fetchRawFilesInformation },
      { name: "Calculating login vs active time difference", fn: this.DiffecenceBetweenLoginAndActiveTime },
      { name: "Processing month-wise agent performance", fn: this.processMonthWiseDifference },
      { name: "Processing Day Wise AHT", fn: this.averageHandlingTime.agentAverageHandlingTime },
      { name: "Processing Month Wise AHT", fn: this.averageHandlingTime.logic_agentAvegrageHandlingTime_monthWise },
      { name: "Processing CRM Tagging Day Wise", fn: this.crmTaggingCalculation.dayWiseCRMTaggingCalculation },
      { name: "Processing CRM Tagging Month Wise", fn: this.crmTaggingCalculation.monthWiseCRMTaggingCalculation },
      { name: "Processing Customer Rating Day Wise", fn: this.customerRating.dayWiseCustomerRatingForAgent },
      { name: "Processing Customer Rating Month Wise", fn: this.customerRating.monthWiseCustomerRatingForAgent },
      { name: "Processing Feedback Transfer Day Wise", fn: this.feedbackTransfer.feedbackTransferStatusDayWise },
      { name: "Processing Feedback Transfer Month Wise", fn: this.feedbackTransfer.feedbackTransferStatusMonthWise },
      { name: "Processing Productive Calling Day Wise", fn: this.productionCalling.productiveCallingDayWise },
      { name: "Processing Productive Calling Month Wise", fn: this.productionCalling.productiveCallingMonthWise },
      { name: "Hang By Agent Day Wise", fn: this.hangByAgent.hangByAgentDayWSise },
      { name: "Hang By Agent Month Wise", fn: this.hangByAgent.hangByAgentMonthWise },
      { name: "Call Quality Score Day Wise", fn: this.callQuality.callQualityAssuranceDayWise },
      { name: "Call Quality Score Month Wise", fn: this.callQuality.callQualityAssuranceMonthWise }
    ];

    for (const step of steps) {
      log.info(step.name);
      try {
        await step.fn.call(this, db, year, month);
      } catch (err) {
        log.error(`Background step failed: ${step.name} | ${err}`);
      }
    }

    log.info(`Background Task completed for ${year_month}`);

  } catch (err) {
    log.error(`Background processing error: ${err}`);
  }
}


async fetchRawFilesInformation(db: any, year: any, month: any) {
    try {
        this.logger.info(`Fetching raw data for ${year}-${month}`);
        await this.agentPerformanceRawFileService.fetchInboundRawData(db, year, month);
        await this.agentPerformanceRawFileService.fetchCallQualityQAData(db, year, month);
        await this.agentPerformanceRawFileService.fetchFramerCallingData(db, year, month);
        await this.agentPerformanceRawFileService.fetchAgentAcitivityData(db, year, month);

    } catch (err) {
      console.log(err)
        this.logger.error(`Error in fetchRawFilesInformation: ${err?.message || err}`);
    }
}


async processRawFilesForAgentPerformance(db: any, month: number, year: number) {
    this.logger.info(`Process start for month=${month}, year=${year}`);

    try {
        if (!db) throw new Error("Database instance is required");
        if (!month || month < 1 || month > 12) throw new Error(`Invalid month: ${month}`);
        if (!year || year.toString().length !== 4) throw new Error(`Invalid year: ${year}`);

        this.logger.info(`Cleanup start for target month=${month}, year=${year}`);

        await this.utilServices.cleanupCollection(
            db,
            'krph_agent_activity_month_performance',
            { year, month }
        );

        await this.utilServices.cleanupCollection(
            db,
            'krph_agent_activity_daily_performance',
            { year, month }
        );

        const start = new Date(Date.UTC(year, month - 1, 1));
        const end = new Date(Date.UTC(year, month, 1));

        await this.utilServices.cleanupCollection(
            db,
            'krph_agent_login_active_diff_daywise',
            { tc_date: { $gte: start, $lt: end } }
        );

        await this.utilServices.cleanupCollection(
            db,
            'krph_agent_login_active_diff_monthWise',
            { year, month }
        );

        this.logger.info(`Cleanup complete for month=${month}, year=${year}`);
        this.logger.info(`Raw processing start for month=${month}, year=${year}`);

        const processingResult = await this.processingTheFile(db, month, year);

        this.logger.info(`Raw processing complete for month=${month}, year=${year}`);
        this.logger.info(`Starting aggregation calculations for month=${month}, year=${year}`);

        const aggregationResult = await this.dayWisetoMonthCalculation(db, year, month);

        this.logger.info(`Aggregation complete for month=${month}, year=${year}`);
        this.logger.info(`Process completed successfully for month=${month}, year=${year}`);

        return {
            success: true,
            processingResult,
            aggregationResult
        };
    } catch (err: any) {
        this.logger.error(`Process failed for month=${month}, year=${year}, error=${err.message}`);
        throw err;
    }
}


async processingTheFile(db: any, month: number, year: number) {
    this.logger.info(`processingTheFile start for month=${month}, year=${year}`);

    try {
        if (!db) throw new Error("Database instance is required");
        if (!month || month < 1 || month > 12) throw new Error(`Invalid month: ${month}`);
        if (!year || year.toString().length !== 4) throw new Error(`Invalid year: ${year}`);

        const pipeline = [
            {
                $match: {
                    $expr: {
                        $and: [
                            { $eq: [{ $year: "$tc_date" }, year] },
                            { $eq: [{ $month: "$tc_date" }, month] }
                        ]
                    }
                }
            },
            {
                $addFields: {
                    active_seconds: {
                        $cond: [
                            { $and: [{ $isArray: { $split: ["$ActiveHour", ":"] } }] },
                            {
                                $let: {
                                    vars: { parts: { $split: ["$ActiveHour", ":"] } },
                                    in: {
                                        $add: [
                                            {
                                                $multiply: [
                                                    { $toInt: { $ifNull: [{ $arrayElemAt: ["$$parts", 0] }, 0] } },
                                                    3600
                                                ]
                                            },
                                            {
                                                $multiply: [
                                                    { $toInt: { $ifNull: [{ $arrayElemAt: ["$$parts", 1] }, 0] } },
                                                    60
                                                ]
                                            },
                                            {
                                                $toInt: { $ifNull: [{ $arrayElemAt: ["$$parts", 2] }, 0] }
                                            }
                                        ]
                                    }
                                }
                            },
                            0
                        ]
                    },
                    year: { $year: "$tc_date" },
                    month: { $month: "$tc_date" },
                    day: { $dayOfMonth: "$tc_date" }
                }
            },
            {
                $group: {
                    _id: {
                        user: "$user",
                        year: "$year",
                        month: "$month",
                        day: "$day"
                    },
                    full_name: { $first: "$full_name" },
                    daily_active_seconds: { $sum: "$active_seconds" }
                }
            },
            {
                $addFields: {
                    daily_active_hour: {
                        $let: {
                            vars: {
                                hours: { $floor: { $divide: ["$daily_active_seconds", 3600] } },
                                minutes: {
                                    $floor: {
                                        $divide: [
                                            { $mod: ["$daily_active_seconds", 3600] },
                                            60
                                        ]
                                    }
                                },
                                seconds: { $mod: ["$daily_active_seconds", 60] }
                            },
                            in: {
                                $concat: [
                                    { $toString: "$$hours" },
                                    ":",
                                    {
                                        $cond: [
                                            { $lt: ["$$minutes", 10] },
                                            { $concat: ["0", { $toString: "$$minutes" }] },
                                            { $toString: "$$minutes" }
                                        ]
                                    },
                                    ":",
                                    {
                                        $cond: [
                                            { $lt: ["$$seconds", 10] },
                                            { $concat: ["0", { $toString: "$$seconds" }] },
                                            { $toString: "$$seconds" }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            },
            {
                $project: {
                    _id: 0,
                    user: "$_id.user",
                    full_name: 1,
                    year: "$_id.year",
                    month: "$_id.month",
                    day: "$_id.day",
                    daily_active_seconds: 1,
                    daily_active_hour: 1
                }
            },
            {
                $merge: {
                    into: "krph_agent_activity_daily_performance",
                    whenMatched: "merge",
                    whenNotMatched: "insert"
                }
            }
        ];

        this.logger.info(`Aggregation pipeline execution started for month=${month}, year=${year}`);

        const result = await db
            .collection("all_agent_activity_records")
            .aggregate(pipeline)
            .toArray();

        this.logger.info(`Aggregation executed successfully, processed=${result.length} records`);
        this.logger.info(`processingTheFile completed for month=${month}, year=${year}`);

        return { success: true, count: result.length, data: result };
    } catch (err: any) {
        this.logger.error(`processingTheFile failed for month=${month}, year=${year}, error=${err.message}`);
        throw err;
    }
}


async dayWisetoMonthCalculation(db: any, year: number, month: number) {
    this.logger.info(`dayWisetoMonthCalculation start for month=${month}, year=${year}`);

    try {
        if (!db) throw new Error("Database instance is required");
        if (!month || month < 1 || month > 12) throw new Error(`Invalid month: ${month}`);
        if (!year || year.toString().length !== 4) throw new Error(`Invalid year: ${year}`);

        const pipeline = [
            {
                $match: { year, month }
            },
            {
                $group: {
                    _id: "$user",
                    full_name: { $first: "$full_name" },
                    total_active_seconds: { $sum: "$daily_active_seconds" },
                    total_days: { $sum: 1 },
                    year: { $first: "$year" },
                    month: { $first: "$month" }
                }
            },
            {
                $addFields: {
                    avg_active_seconds: {
                        $cond: [
                            { $eq: ["$total_days", 0] },
                            0,
                            { $divide: ["$total_active_seconds", "$total_days"] }
                        ]
                    }
                }
            },
            {
                $addFields: {
                    avg_active_hour: {
                        $let: {
                            vars: {
                                hours: { $floor: { $divide: ["$avg_active_seconds", 3600] } },
                                minutes: {
                                    $floor: {
                                        $divide: [
                                            { $mod: ["$avg_active_seconds", 3600] },
                                            60
                                        ]
                                    }
                                },
                                seconds: { $mod: ["$avg_active_seconds", 60] }
                            },
                            in: {
                                $concat: [
                                    { $toString: "$$hours" },
                                    ":",
                                    {
                                        $cond: [
                                            { $lt: ["$$minutes", 10] },
                                            { $concat: ["0", { $toString: "$$minutes" }] },
                                            { $toString: "$$minutes" }
                                        ]
                                    },
                                    ":",
                                    {
                                        $cond: [
                                            { $lt: ["$$seconds", 10] },
                                            { $concat: ["0", { $toString: "$$seconds" }] },
                                            { $toString: "$$seconds" }
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    performanceFlag: {
                        $switch: {
                            branches: [
                                { case: { $gt: ["$avg_active_seconds", 8 * 3600] }, then: 4 },
                                { case: { $eq: ["$avg_active_seconds", 8 * 3600] }, then: 3 },
                                { case: { $gt: ["$avg_active_seconds", 7 * 3600 + 45 * 60] }, then: 2 },
                                { case: { $lt: ["$avg_active_seconds", 7 * 3600 + 45 * 60] }, then: 1 }
                            ],
                            default: 0
                        }
                    }
                }
            },
            {
                $project: {
                    _id: 0,
                    user: "$_id",
                    full_name: 1,
                    total_active_seconds: 1,
                    total_days: 1,
                    avg_active_seconds: 1,
                    avg_active_hour: 1,
                    performanceFlag: 1,
                    year: 1,
                    month: 1
                }
            },
            {
                $merge: {
                    into: "krph_agent_activity_month_performance",
                    whenMatched: "merge",
                    whenNotMatched: "insert"
                }
            }
        ];

        this.logger.info(`Executing monthly aggregation pipeline for month=${month}, year=${year}`);

        const result = await db
            .collection("krph_agent_activity_daily_performance")
            .aggregate(pipeline)
            .toArray();

        this.logger.info(
            `Monthly aggregation completed for month=${month}, year=${year}, processed=${result.length} records`
        );

        this.logger.info(`dayWisetoMonthCalculation finished successfully for month=${month}, year=${year}`);

        return { success: true, count: result.length, data: result };
    } catch (err: any) {
        this.logger.error(
            `dayWisetoMonthCalculation failed for month=${month}, year=${year}, error=${err.message}`
        );
        throw err;
    }
}



async DiffecenceBetweenLoginAndActiveTime(db: any, year: number, month: number) {
    this.logger.info(`DiffecenceBetweenLoginAndActiveTime start for month=${month}, year=${year}`)
    try {
        if (!db) throw new Error("Database instance is required");
        if (!month || month < 1 || month > 12) throw new Error(`Invalid month: ${month}`);
        if (!year || year.toString().length !== 4) throw new Error(`Invalid year: ${year}`);

        this.logger.info(`Calling processDayWiseDifference for month=${month}, year=${year}`);

        const result = await this.processDayWiseDifference(db, year, month);

        this.logger.info(
            `DiffecenceBetweenLoginAndActiveTime completed for month=${month}, year=${year}, resultCount=${Array.isArray(result) ? result.length : 0}`
        );

        return { success: true, data: result };
    } catch (err: any) {
        this.logger.error(
            `Error in DiffecenceBetweenLoginAndActiveTime for month=${month}, year=${year}, error=${err.message}`
        );
        throw err;
    }
}

async processDayWiseDifference(db: any, year: number, month: number) {
    this.logger.info(`processDayWiseDifference start for month=${month}, year=${year}`);

    try {
        if (!db) throw new Error("Database instance is required");
        if (!month || month < 1 || month > 12) throw new Error(`Invalid month: ${month}`);
        if (!year || year.toString().length !== 4) throw new Error(`Invalid year: ${year}`);

        const pipeline = [
            {
                $match: {
                    $expr: {
                        $and: [
                            { $eq: [{ $year: "$tc_date" }, year] },
                            { $eq: [{ $month: "$tc_date" }, month] }
                        ]
                    }
                }
            },
            {
                $addFields: {
                    loginTimeParsed: {
                        $dateFromString: {
                            dateString: "$first_login_time",
                            format: "%Y-%m-%d %H:%M:%S",
                            onError: null
                        }
                    },
                    activeTimeParsed: {
                        $dateFromString: {
                            dateString: "$first_active_time",
                            format: "%Y-%m-%d %H:%M:%S",
                            onError: null
                        }
                    }
                }
            },
            {
                $addFields: {
                    diff_seconds: {
                        $cond: [
                            {
                                $and: [
                                    { $ne: ["$loginTimeParsed", null] },
                                    { $ne: ["$activeTimeParsed", null] }
                                ]
                            },
                            {
                                $divide: [
                                    { $subtract: ["$activeTimeParsed", "$loginTimeParsed"] },
                                    1000
                                ]
                            },
                            null
                        ]
                    }
                }
            },
            {
                $addFields: {
                    activity_diff_category: {
                        $cond: [
                            { $eq: ["$diff_seconds", null] },
                            "No",
                            {
                                $switch: {
                                    branches: [
                                        { case: { $lt: ["$diff_seconds", 180] }, then: "Below 3 Min" },
                                        { case: { $lt: ["$diff_seconds", 360] }, then: "Below 6 Mins" },
                                        { case: { $lt: ["$diff_seconds", 600] }, then: "Below 10 Mins" },
                                        { case: { $gt: ["$diff_seconds", 900] }, then: "Above 15" }
                                    ],
                                    default: "No"
                                }
                            }
                        ]
                    }
                }
            },
            {
                $project: {
                    _id: 0,
                    user: 1,
                    full_name: 1,
                    location: 1,
                    tc_date: 1,
                    first_login_time: 1,
                    first_active_time: 1,
                    diff_seconds: 1,
                    activity_diff_category: 1
                }
            },
            { $sort: { tc_date: 1, user: 1 } },
            {
                $merge: {
                    into: "krph_agent_login_active_diff_daywise",
                    whenMatched: "merge",
                    whenNotMatched: "insert"
                }
            }
        ];

        this.logger.info(`Executing day-wise login vs active difference pipeline for month=${month}, year=${year}`);

        const result = await db
            .collection("all_agent_activity_records")
            .aggregate(pipeline)
            .toArray();

        this.logger.info(
            `processDayWiseDifference completed for month=${month}, year=${year}, processed=${result.length} records`
        );

        return { success: true, count: result.length, data: result };
    } catch (err: any) {
        this.logger.error(
            `processDayWiseDifference failed for month=${month}, year=${year}, error=${err.message}`
        );
        throw err;
    }
}



async processMonthWiseDifference(db, year, month) {
    try {
        const pipeline = [
            {
                $match: {
                    $expr: {
                        $and: [
                            { $eq: [{ $year: "$tc_date" }, year] },
                            { $eq: [{ $month: "$tc_date" }, month] }
                        ]
                    }
                }
            },
            {
                $group: {
                    _id: "$user",
                    full_name: { $first: "$full_name" },
                    total_diff_seconds: { $sum: "$diff_seconds" },
                    total_days: { $sum: 1 },
                    year: { $first: { $year: "$tc_date" } },
                    month: { $first: { $month: "$tc_date" } }
                }
            },
            {
                $addFields: {
                    avg_diff_seconds: {
                        $divide: ["$total_diff_seconds", "$total_days"]
                    }
                }
            },
            {
                $addFields: {
                    performanceFlag: {
                        $switch: {
                            branches: [
                                { case: { $lt: ["$avg_diff_seconds", 180] }, then: 4 },
                                { case: { $lt: ["$avg_diff_seconds", 360] }, then: 3 },
                                { case: { $lt: ["$avg_diff_seconds", 600] }, then: 2 },
                                { case: { $gte: ["$avg_diff_seconds", 900] }, then: 1 },
                            ],
                            default: 0
                        }
                    }
                }
            },
            {
                $project: {
                    _id: 0,
                    user: "$_id",
                    full_name: 1,
                    total_diff_seconds: 1,
                    total_diff_minutes: { $divide: ["$total_diff_seconds", 60] },
                    total_days: 1,
                    avg_diff_seconds: 1,
                    avg_diff_minutes: { $divide: ["$avg_diff_seconds", 60] },
                    performanceFlag: 1,
                    year: 1,
                    month: 1
                }
            },
            {
                $merge: {
                    into: "krph_agent_login_active_diff_monthWise",
                    whenMatched: "merge",
                    whenNotMatched: "insert"
                }
            }
        ];

        const result = await db
            .collection("krph_agent_login_active_diff_daywise")
            .aggregate(pipeline)
            .toArray();

        this.logger.info(`processMonthWiseDifference executed for ${year}-${month}`);

        return result;

    } catch (err) {
        this.logger.error(`processMonthWiseDifference error: ${err}`);
        throw err;
    }
}
















}
