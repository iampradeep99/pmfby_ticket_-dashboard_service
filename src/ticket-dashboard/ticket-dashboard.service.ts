import { Injectable, Inject } from '@nestjs/common';
import * as streamBuffers from 'stream-buffers';
import { Db, Collection } from 'mongodb';
import * as NodeCache from 'node-cache';
import axios from 'axios'
import { UtilService } from "../commonServices/utilService";
import * as fs from 'fs-extra';
import * as path from 'path';
import * as archiver from 'archiver';
import { RedisWrapper } from '../commonServices/redisWrapper';
const XLSX = require('xlsx');
// const ExcelJS = require('exceljs');
import * as ExcelJS from 'exceljs'
import { MailService } from '../mail/mail.service';
import { generateSupportTicketEmailHTML, getCurrentFormattedDateTime } from '../templates/mailTemplates'
import { GCPServices } from '../commonServices/GCSFileUpload'
import { format } from '@fast-csv/format';
import { pipe } from 'rxjs';


// import * as ExcelJS from 'exceljs';
@Injectable()
export class TicketDashboardService {
  private ticketCollection: Collection;
  private ticketDbCollection: Collection;
  public gcp = new GCPServices();

  // private redisWrapper: RedisWrapper;

  constructor(@Inject('MONGO_DB') private readonly db: Db, private readonly redisWrapper: RedisWrapper, private readonly mailService: MailService,) {
    this.ticketCollection = this.db.collection('tickets');
    this.ticketDbCollection = this.db.collection('SLA_KRPH_SupportTickets_Records');

  }

  async createTicket(ticketData: any): Promise<any> {

    const result = await this.ticketCollection.insertOne(ticketData);
    return {
      message: 'Ticket created successfully',
      ticketId: result.insertedId
    };
  }


  async createOptimizedIndex(db: any): Promise<void> {
    try {
      const collection = db.collection('SLA_KRPH_SupportTickets_Records');

      const indexes = await collection.indexes();

      if (indexes.some(idx => idx.name === 'TicketQuery_Compound_Index')) {
        await collection.dropIndex('TicketQuery_Compound_Index');
        console.log('✅ Dropped existing index: TicketQuery_Compound_Index');
      }

      // Create new indexes
      await collection.createIndex(
        {
          TicketHeaderID: 1,
          InsertDateTime: 1,
          InsuranceCompanyID: 1,
          FilterStateID: 1,
          FilterDistrictRequestorID: 1,
        },
        {
          name: 'MainFilterIndex',
          background: true,
        }
      );

      await collection.createIndex(
        {
          TicketStatus: 1,
          TicketHeadName: 1,
          BMCGCode: 1,
        },
        {
          name: 'AggregationFieldsIndex',
          background: true,
        }
      );

      console.log("✅ Indexes created successfully: MainFilterIndex, AggregationFieldsIndex");
    } catch (error) {
      console.error("❌ Failed to create indexes:", error);
    }
  }







  async fetchTickets(ticketInfo: any): Promise<{ data: any; message: { msg: string; code: number } }> {

    console.log("🚀 Entering fetchTickets process");
    try {

      console.log(ticketInfo?.userID)
      const Delta = await this.getSupportTicketUserDetail(ticketInfo?.userID);
      const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
      const item = (responseInfo.data as any)?.user?.[0];
      console.log(JSON.stringify(item))
      const userDetail = {
        InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
        StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
        BRHeadTypeID: item.BRHeadTypeID,
        LocationTypeID: item.LocationTypeID,
        DistrictIDs: item.DistrictIDs || []
      };
      console.log(userDetail, "userDetail")

      const { InsuranceCompanyID, StateMasterID, LocationTypeID, DistrictIDs } = userDetail;

      const fromDate = new Date(`${ticketInfo.fromDate}T00:00:00.000Z`);

      const toDate = new Date(`${ticketInfo.toDate}T23:59:59.999Z`);


      toDate.setDate(toDate.getDate() - 1);
      if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
        return {
          data: null,
          message: { msg: "❌ Invalid date format", code: 0 }
        };
      }

      const cacheKey = `ticket-stats-${ticketInfo.fromDate}-to-${ticketInfo.toDate}-user-${ticketInfo.userID}`;
      const cachedData = await this.redisWrapper.getRedisCache(cacheKey);
      if (cachedData) {
        console.log('✅ Redis cache hit:', cacheKey);
        return {
          data: cachedData,
          message: { msg: '✅ Data fetched from cache', code: 1 }
        };
      }

      const match: any = {
        InsertDateTime: { $gte: fromDate, $lte: toDate }
      };

      if (InsuranceCompanyID?.length) {
        match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
      }

      if (LocationTypeID === 1 && StateMasterID?.length) {
        match.FilterStateID = { $in: StateMasterID.map(Number) };
      } else if (LocationTypeID === 2 && DistrictIDs?.length) {
        match.FilterDistrictRequestorID = { $in: DistrictIDs.map(Number) };
      }

      const grievancePipeline = [
        { $match: { ...match, TicketHeaderID: 1 } },
        { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
        { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } }
      ];

      const informationPipeline = [
        { $match: { ...match, TicketHeaderID: 2 } },
        {
          $group: {
            _id: {
              status: "$TicketStatus",
              head: "$TicketHeadName",
              code: "$BMCGCode"
            },
            Total: { $sum: 1 }
          }
        },
        {
          $project: {
            _id: 0,
            TicketStatus: {
              $cond: [
                { $eq: ["$_id.code", 109025] },
                { $concat: ["$_id.status", " (", "$_id.head", ")"] },
                "$_id.status"
              ]
            },
            Total: 1
          }
        }
      ];

      const cropLossPipeline = [
        { $match: { ...match, TicketHeaderID: 4 } },
        { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
        { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } }
      ];

      const [grievance, information, cropLoss] = await Promise.all([
        this.ticketDbCollection.aggregate(grievancePipeline, { allowDiskUse: true }).toArray(),
        this.ticketDbCollection.aggregate(informationPipeline, { allowDiskUse: true }).toArray(),
        this.ticketDbCollection.aggregate(cropLossPipeline, { allowDiskUse: true }).toArray()
      ]);

      const addTotalRow = (arr: any[]) => {
        const total = arr.reduce((sum, item) => sum + item.Total, 0);
        return [...arr, { TicketStatus: "Total", Total: total }];
      };

      const response = {
        Grievance: addTotalRow(grievance),
        Information: addTotalRow(information),
        CropLoss: addTotalRow(cropLoss)
      };

      await this.redisWrapper.setRedisCache(cacheKey, response, 3600);

      return {
        data: response,
        message: { msg: '✅ Data fetched successfully', code: 1 }
      };

    } catch (error) {
      console.error('❌ Error in fetchTickets:', error);
      return {
        data: null,
        message: { msg: '❌ Failed to fetch ticket data', code: 0 }
      };
    }
  }



  async getSupportTicketUserDetail(userID) {
    const data = { userID };
    const TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHBpcmVzSW4iOiIyMDI0LTEwLTA5VDE4OjA4OjA4LjAyOFoiLCJpYXQiOjE3Mjg0NjEyODguMDI4LCJpZCI6NzA5LCJ1c2VybmFtZSI6InJhamVzaF9iYWcifQ.niMU8WnJCK5SOCpNOCXMBeDrsr2ZqC96LUzQ5Z9MoBk'

    const url = 'https://pmfby.gov.in/krphapi/FGMS/GetSupportTicketUserDetail'
    return axios.post(url, data, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': TOKEN
      }
    })
      .then(response => {
        return response.data;
      })
      .catch(error => {
        console.error('Error:', error);
        throw error;
      });
  };

  async convertStringToArray(str) {
    return str.split(",").map(Number);
  }











  async getSupportTicketHistotReportDownload(ticketPayload: any): Promise<void> {

    setImmediate(async () => {
      try {
        await this.processTicketHistoryAndGenerateZip(ticketPayload);
      } catch (err) {
        console.error('Background processing failed:', err);
      }
    });


  }

  async getSupportTicketHistotReport(ticketPayload: any): Promise<{ data: any[], message: {}, pagination: any }> {
    const result = await this.processTicketHistoryView(ticketPayload);
    return {
      data: result.data,
      message: result.rmessage || 'Success',
      pagination: result?.pagination
    };
  }




  async downloadFarmerCallingReportService(ticketPayload: any): Promise<void> {

    setImmediate(async () => {
      try {
        await this.farmerCallingHistoryDownloadReportAndZip(ticketPayload);
      } catch (err) {
        console.error('Background processing failed:', err);
      }
    });


  }




  async processTicketHistoryView(ticketPayload: any) {
    let {
      SPFROMDATE,
      SPTODATE,
      SPInsuranceCompanyID,
      SPStateID,
      SPTicketHeaderID,
      SPUserID,
      page = 1,
      limit = 20,
    } = ticketPayload;

    SPTicketHeaderID = Number(SPTicketHeaderID)

    const db = this.db;
    // this.AddIndex(db);

    if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
    if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };

    // const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
    // const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;
    // if (cachedData) {
    //   return {
    //     rcode: 1,
    //     rmessage: 'Success',
    //     data: cachedData.data,
    //     pagination: cachedData.pagination,
    //   };
    // }

    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);

    const item = (responseInfo.data as any)?.user?.[0];
    if (!item) return { rcode: 0, rmessage: 'User details not found.' };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };

    const { InsuranceCompanyID, StateMasterID, LocationTypeID, BRHeadTypeID } = userDetail;

    let locationFilter: any = {};

    if (LocationTypeID === 1 && StateMasterID?.length) {
      locationFilter = {
        FilterStateID: { $in: StateMasterID },
      };
    } else if (LocationTypeID === 2 && item.DistrictIDs?.length) {
      locationFilter = {
        FilterDistrictRequestorID: { $in: item.DistrictIDs },
      };
    } else {
      locationFilter = {};
    }

    const match: any = {
      ...locationFilter,
    };

    if (SPTicketHeaderID && SPTicketHeaderID !== 0) {
      match.TicketHeaderID = SPTicketHeaderID;
    }



    if (SPInsuranceCompanyID && SPInsuranceCompanyID !== '#ALL') {
      const requestedInsuranceIDs = SPInsuranceCompanyID
        .split(',')
        .map(id => Number(id.trim()));

      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);

      const validInsuranceIDs = requestedInsuranceIDs.filter(id =>
        allowedInsuranceIDs.includes(id)
      );

      if (validInsuranceIDs.length === 0) {
        return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
      }

      match.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else {

      if (InsuranceCompanyID?.length) {
        match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) }; // force integers
      }
    }



    if (SPStateID && SPStateID !== '#ALL') {
      const requestedStateIDs = SPStateID
        .split(',')
        .map(id => Number(id.trim()));

      const validStateIDs = requestedStateIDs.filter(id =>
        StateMasterID.map(Number).includes(id)
      );

      if (validStateIDs.length === 0) {
        return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
      }

      match.FilterStateID = { $in: validStateIDs };
    } else if (StateMasterID?.length && LocationTypeID !== 2) {
      match.FilterStateID = { $in: StateMasterID.map(Number) };
    }


    // DATE FILTER
    if (SPFROMDATE || SPTODATE) {
      match.InsertDateTime = {};

      if (SPFROMDATE) {
        match.InsertDateTime.$gte = new Date(`${SPFROMDATE}T00:00:00.000Z`);
      }

      if (SPTODATE) {
        match.InsertDateTime.$lte = new Date(`${SPTODATE}T23:59:59.999Z`);
      }
    }



    const totalCount = await db.collection('SLA_Ticket_listing').countDocuments(match);
    const totalPages = Math.ceil(totalCount / limit);
    const pipeline: any[] = [
      { $match: match },

      { $sort: { InsertDateTime: -1 } },
      {
        $group: {
          _id: "$SupportTicketNo",
          doc: { $first: "$$ROOT" }
        }
      },
      { $replaceRoot: { newRoot: "$doc" } },

      {
        $lookup: {
          from: 'SLA_KRPH_SupportTicketsHistory_Records',
          let: { ticketId: '$SupportTicketID' },
          pipeline: [
            {
              $match: {
                $expr: {
                  $and: [
                    { $eq: ['$SupportTicketID', '$$ticketId'] },
                    { $eq: ['$TicketStatusID', 109304] }
                  ]
                }
              }
            },
            { $sort: { TicketHistoryID: -1 } },
            { $limit: 1 }
          ],
          as: 'ticketHistory'
        }
      },
      { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },

      {
        $lookup: {
          from: 'support_ticket_claim_intimation_report_history',
          let: { ticketNo: '$SupportTicketNo' },
          pipeline: [
            { $match: { $expr: { $eq: ['$SupportTicketNo', '$$ticketNo'] } } },
            { $sort: { InsertDateTime: -1 } },
            { $limit: 1 }
          ],
          as: 'claimInfo'
        }
      },
      { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },

      {
        $lookup: {
          from: 'csc_agent_master',
          let: { userLoginId: '$InsertUserID' },
          pipeline: [
            { $match: { $expr: { $eq: ['$UserLoginID', '$$userLoginId'] } } },
            { $limit: 1 }
          ],
          as: 'agentInfo'
        }
      },
      { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
      {
        "$lookup": {
          "from": "ticket_comment_journey",
          "localField": "SupportTicketNo",
          "foreignField": "SupportTicketNo",
          "as": "ticket_comment_journey",
          "pipeline": [
            { "$sort": { "CreatedDate": -1 } },
            {
              "$group": {
                "_id": "$ResolvedComment",
                "unique_comments": { "$first": "$$ROOT" }
              }
            },
            { "$replaceRoot": { "newRoot": "$unique_comments" } }
          ]
        }
      },
      

      {
        $project: {
          SupportTicketID: 1,
          SupportTicketNo: 1,
          InsertUserID: 1,
          Created: 1,
          StatusUpdateTime: 1,
          TicketStatusID: 1,
          TicketStatus: 1,
          ApplicationNo: 1,
          InsurancePolicyNo: 1,
          CallerContactNumber: 1,
          RequestorName: 1,
          RequestorMobileNo: 1,
          StateMasterName: 1,
          DistrictMasterName: 1,
          SubDistrictName: 1,
          TicketHeadName: 1,
          TicketCategoryName: 1,
          RequestSeason: 1,
          RequestYear: 1,
          ApplicationCropName: 1,
          Relation: 1,
          RelativeName: 1,
          PolicyPremium: 1,
          PolicyArea: 1,
          PolicyType: 1,
          LandSurveyNumber: 1,
          LandDivisionNumber: 1,
          IsSos: 1,
          PlotStateName: 1,
          PlotDistrictName: 1,
          PlotVillageName: 1,
          ApplicationSource: 1,
          CropShare: 1,
          IFSCCode: 1,
          FarmerShare: 1,
          SowingDate: 1,
          LossDate: 1,
          CreatedBY: 1,
          InsertDateTime: 1,
          Sos: 1,
          TicketNCIPDocketNo: 1,
          TicketDescription: 1,
          CallingUniqueID: 1,
          TicketTypeName: 1,
          TicketReOpenDate: 1,
          InsuranceCompany: 1,
          SchemeName: 1,
          // Unwinded lookups
          ticketHistory: 1,
          claimInfo: 1,
          agentInfo: 1,
          ticket_comment_journey: 1
        }
      },

      {
        $project: {
          _id: 0,
          "Agent ID": "$agentInfo.UserID",
          "Calling ID": "$CallingUniqueID",
          "NCIP Docket No": "$TicketNCIPDocketNo",
          "Ticket No": "$SupportTicketNo",
          "Creation Date": "$InsertDateTime",
          "Re-Open Date": "$TicketReOpenDate",
          "Ticket Status": "$TicketStatus",
          "Status Date": "$StatusUpdateTime",
          "State": "$StateMasterName",
          "District": "$DistrictMasterName",
          "Type": "$TicketHeadName",
          "Category": "$TicketTypeName",
          "Sub Category": "$TicketCategoryName",
          "Season": "$RequestSeason",
          "Year": "$RequestYear",
          "Insurance Company": "$InsuranceCompany",
          "Application No": "$ApplicationNo",
          "Policy No": "$InsurancePolicyNo",
          "Caller Mobile No": "$CallerContactNumber",
          "Farmer Name": "$RequestorName",
          "Mobile No": "$RequestorMobileNo",
          "Created By": "$CreatedBY",
          "Description": "$TicketDescription",
          "ticket_comment_journey": "$ticket_comment_journey"
        }
      },
      {$sort:{
        "Creation Date":-1
      }},

      { $skip: (page - 1) * limit },
      { $limit: limit }
    ];

    let results = await db.collection('SLA_Ticket_listing')
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray();
    if (results.length === 0) {
      let data = {
        msg:"No Record Found",
        code:0
      }
      return {
        data: results,
         rmessage: data,
        pagination: null,
         code: 0,
      };
    }

    results = Array.isArray(results) ? results : [results];

results.forEach(doc => {
  if (Array.isArray(doc.ticket_comment_journey) && doc.ticket_comment_journey.length > 0) {
    const journey = doc.ticket_comment_journey;

    journey.forEach((commentObj) => {
      if (commentObj.InprogressDate && commentObj.InprogressComment) {
        const inProgressDate = this.formatToDDMMYYYY(commentObj.InprogressDate);
        const rawInProgressComment = commentObj.InprogressComment || '';
        const cleanInProgressComment = rawInProgressComment.replace(/<\/?[^>]+(>|$)/g, '').trim() || 'NA';

        doc['In-Progress Date'] = inProgressDate;
        doc['In-Progress Comment'] = cleanInProgressComment === "" ? "NA" : cleanInProgressComment;
      } else {
        doc['In-Progress Date'] = "NA";
        doc['In-Progress Comment'] = "NA";
      }

      // Handle Resolved Date and Comment
      if (commentObj.ResolvedDate && commentObj.ResolvedComment) {
        const resolvedDate = this.formatToDDMMYYYY(commentObj.ResolvedDate);
        const rawResolvedComment = commentObj.ResolvedComment || '';
        const cleanResolvedComment = rawResolvedComment.replace(/<\/?[^>]+(>|$)/g, '').trim() || 'NA';

        doc['Resolved-Date'] = resolvedDate;
        doc['Resolved Comment'] = cleanResolvedComment === "" ? "NA" : cleanResolvedComment;
      } else {
        doc['Resolved-Date'] = "NA";
        doc['Resolved Comment'] = "NA";
      }

      // Handle ReOpen Date and Comment
      if (commentObj.ReOpenDate && commentObj.ReOpenComment) {
        const reOpenDate = this.formatToDDMMYYYY(commentObj.ReOpenDate);
        const rawReOpenComment = commentObj.ReOpenComment || '';
        const cleanReOpenComment = rawReOpenComment.replace(/<\/?[^>]+(>|$)/g, '').trim() || 'NA';

        doc['Re-Open-Date'] = reOpenDate;
        doc['Re-Open Comment'] = cleanReOpenComment === "" ? "NA" : cleanReOpenComment;
      } else {
        doc['Re-Open-Date'] = "NA";
        doc['Re-Open Comment'] = "NA";
      }

      if (commentObj.Inprogress1Date && commentObj.Inprogress1Comment) {
        const inProgress1Date = this.formatToDDMMYYYY(commentObj.Inprogress1Date);
        const rawInProgress1Comment = commentObj.Inprogress1Comment || '';
        const cleanInProgress1Comment = rawInProgress1Comment.replace(/<\/?[^>]+(>|$)/g, '').trim() || 'NA';

        doc['In-Progress Date 1'] = inProgress1Date;
        doc['In-Progress Comment 1'] = cleanInProgress1Comment === "" ? "NA" : cleanInProgress1Comment;
      } else {
        doc['In-Progress Date 1'] = "NA";
        doc['In-Progress Comment 1'] = "NA";
      }

      if (commentObj.Resolved1Date && commentObj.Resolved1Comment) {
        const resolved1Date = this.formatToDDMMYYYY(commentObj.Resolved1Date);
        const rawResolved1Comment = commentObj.Resolved1Comment || '';
        const cleanResolved1Comment = rawResolved1Comment.replace(/<\/?[^>]+(>|$)/g, '').trim() || 'NA';

        doc['Resolved-Date 1'] = resolved1Date;
        doc['Resolved Comment 1'] = cleanResolved1Comment === "" ? "NA" : cleanResolved1Comment;
      } else {
        doc['Resolved-Date 1'] = "NA";
        doc['Resolved Comment 1'] = "NA";
      }

      if (commentObj.ReOpen1Date && commentObj.ReOpen1Comment) {
        const reOpen1Date = this.formatToDDMMYYYY(commentObj.ReOpen1Date);
        const rawReOpen1Comment = commentObj.ReOpen1Comment || '';
        const cleanReOpen1Comment = rawReOpen1Comment.replace(/<\/?[^>]+(>|$)/g, '').trim() || 'NA';

        doc['Re-Open-Date 1'] = reOpen1Date;
        doc['Re-Open Comment 1'] = cleanReOpen1Comment === "" ? "NA" : cleanReOpen1Comment;
      } else {
        doc['Re-Open-Date 1'] = "NA";
        doc['Re-Open Comment 1'] = "NA";
      }

      if (commentObj.Inprogress2Date && commentObj.Inprogress2Comment) {
        const inProgress2Date = this.formatToDDMMYYYY(commentObj.Inprogress2Date);
        const rawInProgress2Comment = commentObj.Inprogress2Comment || '';
        const cleanInProgress2Comment = rawInProgress2Comment.replace(/<\/?[^>]+(>|$)/g, '').trim() || 'NA';

        doc['In-Progress Date 2'] = inProgress2Date;
        doc['In-Progress Comment 2'] = cleanInProgress2Comment === "" ? "NA" : cleanInProgress2Comment;
      } else {
        doc['In-Progress Date 2'] = "NA";
        doc['In-Progress Comment 2'] = "NA";
      }

      if (commentObj.Resolved2Date && commentObj.Resolved2Comment) {
        const resolved2Date = this.formatToDDMMYYYY(commentObj.Resolved2Date);
        const rawResolved2Comment = commentObj.Resolved2Comment || '';
        const cleanResolved2Comment = rawResolved2Comment.replace(/<\/?[^>]+(>|$)/g, '').trim() || 'NA';

        doc['Resolved-Date 2'] = resolved2Date;
        doc['Resolved Comment 2'] = cleanResolved2Comment === "" ? "NA" : cleanResolved2Comment;
      } else {
        doc['Resolved-Date 2'] = "NA";
        doc['Resolved Comment 2'] = "NA";
      }

      if (commentObj.ReOpen2Date && commentObj.ReOpen2Comment) {
        const reOpen2Date = this.formatToDDMMYYYY(commentObj.ReOpen2Date);
        const rawReOpen2Comment = commentObj.ReOpen2Comment || '';
        const cleanReOpen2Comment = rawReOpen2Comment.replace(/<\/?[^>]+(>|$)/g, '').trim() || 'NA';

        doc['Re-Open-Date 2'] = reOpen2Date;
        doc['Re-Open Comment 2'] = cleanReOpen2Comment === "" ? "NA" : cleanReOpen2Comment;
      } else {
        doc['Re-Open-Date 2'] = "NA";
        doc['Re-Open Comment 2'] = "NA";
      }
    });

    delete doc.ticket_comment_journey;
  } else {
    doc['In-Progress Date'] = "NA";
    doc['In-Progress Comment'] = "NA";
    doc['Resolved-Date'] = "NA";
    doc['Resolved Comment'] = "NA";
    doc['Re-Open-Date'] = "NA";
    doc['Re-Open Comment'] = "NA";
    doc['In-Progress Date 1'] = "NA";
    doc['In-Progress Comment 1'] = "NA";
    doc['Resolved-Date 1'] = "NA";
    doc['Resolved Comment 1'] = "NA";
    doc['Re-Open-Date 1'] = "NA";
    doc['Re-Open Comment 1'] = "NA";
    doc['In-Progress Date 2'] = "NA";
    doc['In-Progress Comment 2'] = "NA";
    doc['Resolved-Date 2'] = "NA";
    doc['Resolved Comment 2'] = "NA";
    doc['Re-Open-Date 2'] = "NA";
    doc['Re-Open Comment 2'] = "NA";
  }
});


    const responsePayload = {
      data: results,
      pagination: {
        total: totalCount,
        page,
        limit,
        totalPages,
        hasNextPage: page < totalPages,
        hasPrevPage: page > 1
      },
    };

    // await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);

    // if(results.length === 0){
    //    return {
    //   code: 0,
    //   rmessage: 'No Record Found',
    //   data: [],
    //   pagination: null,
    // };

    // }
let message = {
  msg:"Success",
  code:1
}
    return {
      data: results,
      rmessage:message,
      pagination: responsePayload.pagination,
      code: 1,
    };
  }







  async processTicketHistoryAndGenerateZipccPradeep(ticketPayload: any) {
    let {
      SPFROMDATE,
      SPTODATE,
      SPInsuranceCompanyID,
      SPStateID,
      SPTicketHeaderID,
      SPUserID,
      page = 1,
      limit = 1000000000,
      userEmail,
    } = ticketPayload;

    const db = this.db;
    SPTicketHeaderID = Number(SPTicketHeaderID);

    if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
    if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };

    const RequestDateTime = await getCurrentFormattedDateTime();
    const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;
    if (cachedData) {
      console.log('Using cached data');
      return cachedData;
    }

    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];
    if (!item) return { rcode: 0, rmessage: 'User details not found.' };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };
    const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length)
      locationFilter = { FilterStateID: { $in: StateMasterID } };
    else if (LocationTypeID === 2 && item.DistrictIDs?.length)
      locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };

    const baseMatch: any = { ...locationFilter };
    if (SPTicketHeaderID && SPTicketHeaderID !== 0) baseMatch.TicketHeaderID = SPTicketHeaderID;

    if (SPInsuranceCompanyID && SPInsuranceCompanyID !== '#ALL') {
      const requestedInsuranceIDs = SPInsuranceCompanyID.split(',').map((id) => Number(id.trim()));
      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter((id) => allowedInsuranceIDs.includes(id));
      if (!validInsuranceIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
      baseMatch.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else if (InsuranceCompanyID?.length)
      baseMatch.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };

    if (SPStateID && SPStateID !== '#ALL') {
      const requestedStateIDs = SPStateID.split(',').map((id) => id.trim());
      const validStateIDs = requestedStateIDs.filter((id) => StateMasterID.includes(id));
      if (!validStateIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
      baseMatch.FilterStateID = { $in: validStateIDs };
    } else if (StateMasterID?.length && LocationTypeID !== 2)
      baseMatch.FilterStateID = { $in: StateMasterID };

    const folderPath = path.join(process.cwd(), 'downloads');
    await fs.promises.mkdir(folderPath, { recursive: true });

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Support Tickets');

    worksheet.columns = [
      { header: 'Agent ID', key: 'AgentID', width: 20 },
      { header: 'Calling ID', key: 'CallingUniqueID', width: 25 },
      { header: 'NCIP Docket No', key: 'TicketNCIPDocketNo', width: 30 },
      { header: 'Ticket No', key: 'SupportTicketNo', width: 30 },
      { header: 'Creation Date', key: 'Created', width: 25 },
      { header: 'Re-Open Date', key: 'TicketReOpenDate', width: 25 },
      { header: 'Ticket Status', key: 'TicketStatus', width: 20 },
      { header: 'Status Date', key: 'StatusUpdateTime', width: 25 },
      { header: 'State', key: 'StateMasterName', width: 20 },
      { header: 'District', key: 'DistrictMasterName', width: 20 },
      { header: 'Sub District', key: 'SubDistrictName', width: 20 },
      { header: 'Type', key: 'TicketHeadName', width: 20 },
      { header: 'Category', key: 'TicketTypeName', width: 20 },
      { header: 'Sub Category', key: 'TicketCategoryName', width: 20 },
      { header: 'Season', key: 'CropSeasonName', width: 15 },
      { header: 'Year', key: 'RequestYear', width: 10 },
      { header: 'Insurance Company', key: 'InsuranceCompany', width: 30 },
      { header: 'Application No', key: 'ApplicationNo', width: 25 },
      { header: 'Policy No', key: 'InsurancePolicyNo', width: 25 },
      { header: 'Caller Mobile No', key: 'CallerContactNumber', width: 20 },
      { header: 'Farmer Name', key: 'RequestorName', width: 25 },
      { header: 'Mobile No', key: 'RequestorMobileNo', width: 20 },
      { header: 'Relation', key: 'Relation', width: 15 },
      { header: 'Relative Name', key: 'RelativeName', width: 25 },
      { header: 'Policy Premium', key: 'PolicyPremium', width: 15 },
      { header: 'Policy Area', key: 'PolicyArea', width: 15 },
      { header: 'Policy Type', key: 'PolicyType', width: 20 },
      { header: 'Land Survey Number', key: 'LandSurveyNumber', width: 25 },
      { header: 'Land Division Number', key: 'LandDivisionNumber', width: 25 },
      { header: 'Plot State', key: 'PlotStateName', width: 20 },
      { header: 'Plot District', key: 'PlotDistrictName', width: 20 },
      { header: 'Plot Village', key: 'PlotVillageName', width: 25 },
      { header: 'Application Source', key: 'ApplicationSource', width: 20 },
      { header: 'Crop Share', key: 'CropShare', width: 15 },
      { header: 'IFSC Code', key: 'IFSCCode', width: 20 },
      { header: 'Farmer Share', key: 'FarmerShare', width: 15 },
      { header: 'Sowing Date', key: 'SowingDate', width: 20 },
      { header: 'Created By', key: 'CreatedBY', width: 20 },
      { header: 'Description', key: 'TicketDescription', width: 50 },
    ];

    await this.insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, "", "", this.db)


    const CHUNK_SIZE = 10000;

    async function processDateWithChunking(currentDate: Date, endDate: Date) {
      if (currentDate > endDate) return;

      const startOfDay = new Date(currentDate);
      startOfDay.setUTCHours(0, 0, 0, 0);
      const endOfDay = new Date(currentDate);
      endOfDay.setUTCHours(23, 59, 59, 999);

      let skip = 0;
      let hasMore = true;

      while (hasMore) {
        console.log(CHUNK_SIZE, "CHUNK_SIZE")
        const dailyMatch = { ...baseMatch, InsertDateTime: { $gte: startOfDay, $lte: endOfDay } };

        const pipeline: any[] = [
          { $match: dailyMatch },
          {
            $lookup: {
              from: 'SLA_KRPH_SupportTicketsHistory_Records',
              let: { ticketId: '$SupportTicketID' },
              pipeline: [
                { $match: { $expr: { $and: [{ $eq: ['$SupportTicketID', '$$ticketId'] }, { $eq: ['$TicketStatusID', 109304] }] } } },
                { $sort: { TicketHistoryID: -1 } },
                { $limit: 1 }
              ],
              as: 'ticketHistory',
            }
          },
          {
            $lookup: {
              from: 'support_ticket_claim_intimation_report_history',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'claimInfo',
            }
          },
          {
            $lookup: {
              from: 'csc_agent_master',
              localField: 'InsertUserID',
              foreignField: 'UserLoginID',
              as: 'agentInfo',
            }
          },
          {
            $lookup: {
              from: 'ticket_comment_journey',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'ticket_comment_journey',
            },
          },

          { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
          { $skip: skip },
          { $limit: CHUNK_SIZE },
          {
            $project: {
              agentInfo: 1,
              TicketComments: {
                $arrayToObject: {
                  $map: {
                    input: '$ticket_comment_journey',
                    as: 'comment',
                    in: {
                      k: {
                        $concat: [
                          'Comment (',
                          {
                            $dateToString: {
                              format: '%Y-%m-%d',
                              date: '$$comment.ResolvedDate',
                            },
                          },
                          ')',
                        ],
                      },
                      v: '$$comment.ResolvedComment',
                    },
                  },
                },
              },
              CallingUniqueID: 1,
              TicketNCIPDocketNo: 1,
              SupportTicketNo: 1,
              Created: 1,
              TicketReOpenDate: 1,
              TicketStatus: 1,
              StatusUpdateTime: 1,
              StateMasterName: 1,
              DistrictMasterName: 1,
              SubDistrictName: 1,
              TicketHeadName: 1,
              TicketTypeName: 1,
              TicketCategoryName: 1,
              CropSeasonName: 1,
              RequestYear: 1,
              InsuranceCompany: 1,
              ApplicationNo: 1,
              InsurancePolicyNo: 1,
              CallerContactNumber: 1,
              RequestorName: 1,
              RequestorMobileNo: 1,
              Relation: 1,
              RelativeName: 1,
              PolicyPremium: 1,
              PolicyArea: 1,
              PolicyType: 1,
              LandSurveyNumber: 1,
              LandDivisionNumber: 1,
              PlotStateName: 1,
              PlotDistrictName: 1,
              PlotVillageName: 1,
              ApplicationSource: 1,
              CropShare: 1,
              IFSCCode: 1,
              FarmerShare: 1,
              SowingDate: 1,
              CreatedBY: 1,
              TicketDescription: 1
            }
          },
          {
            $project: {
              "Agent ID": "$AgentID",
              "Calling ID": "$CallingUniqueID",
              "NCIP Docket No": "$TicketNCIPDocketNo",
              "Ticket No": "$SupportTicketNo",
              "Creation Date": "$Created",
              "Re-Open Date": "$TicketReOpenDate",
              "Ticket Status": "$TicketStatus",
              "Status Date": "$StatusUpdateTime",
              "State": "$StateMasterName",
              "District": "$DistrictMasterName",
              "Sub District": "$SubDistrictName",
              "Type": "$TicketHeadName",
              "Category": "$TicketTypeName",
              "Sub Category": "$TicketCategoryName",
              "Season": "$CropSeasonName",
              "Year": "$RequestYear",
              "Insurance Company": "$InsuranceCompany",
              "Application No": "$ApplicationNo",
              "Policy No": "$InsurancePolicyNo",
              "Caller Mobile No": "$CallerContactNumber",
              "Farmer Name": "$RequestorName",
              "Mobile No": "$RequestorMobileNo",
              "Relation": "$Relation",
              "Relative Name": "$RelativeName",
              "Policy Premium": "$PolicyPremium",
              "Policy Area": "$PolicyArea",
              "Policy Type": "$PolicyType",
              "Land Survey Number": "$LandSurveyNumber",
              "Land Division Number": "$LandDivisionNumber",
              "Plot State": "$PlotStateName",
              "Plot District": "$PlotDistrictName",
              "Plot Village": "$PlotVillageName",
              "Application Source": "$ApplicationSource",
              "Crop Share": "$CropShare",
              "IFSC Code": "$IFSCCode",
              "Farmer Share": "$FarmerShare",
              "Sowing Date": "$SowingDate",
              "Created By": "$CreatedBY",
              "Description": "$TicketDescription",
              "TicketComments": "$TicketComments"
            }


          }
        ];

        const docs = await db.collection('SLA_KRPH_SupportTickets_Records')
          .aggregate(pipeline, { allowDiskUse: true })
          .toArray();

        docs.forEach(doc => {
          if (doc.TicketComments) {
            for (const [key, value] of Object.entries(doc.TicketComments)) {
              doc[key] = value;
            }
            delete doc.TicketComments;
          }
        });


        if (docs.length === 0) {
          hasMore = false;
        } else {
          docs.forEach(doc => {

            worksheet.addRow({
              AgentID: doc.agentInfo?.UserID?.toString() || '',
              CallingUniqueID: doc.CallingUniqueID || '',
              TicketNCIPDocketNo: doc.TicketNCIPDocketNo || '',
              SupportTicketNo: doc.SupportTicketNo ? doc.SupportTicketNo.toString() : '',
              Created: doc.Created ? new Date(doc.Created).toISOString() : '',
              TicketReOpenDate: doc.TicketReOpenDate || '',
              TicketStatus: doc.TicketStatus || '',
              StatusUpdateTime: doc.StatusUpdateTime ? new Date(doc.StatusUpdateTime).toISOString() : '',
              StateMasterName: doc.StateMasterName || '',
              DistrictMasterName: doc.DistrictMasterName || '',
              SubDistrictName: doc.SubDistrictName || '',
              TicketHeadName: doc.TicketHeadName || '',
              TicketTypeName: doc.TicketTypeName || '',
              TicketCategoryName: doc.TicketCategoryName || '',
              CropSeasonName: doc.CropSeasonName || '',
              RequestYear: doc.RequestYear || '',
              InsuranceCompany: doc.InsuranceCompany || '',
              ApplicationNo: doc.ApplicationNo || '',
              InsurancePolicyNo: doc.InsurancePolicyNo || '',
              CallerContactNumber: doc.CallerContactNumber || '',
              RequestorName: doc.RequestorName || '',
              RequestorMobileNo: doc.RequestorMobileNo || '',
              Relation: doc.Relation || '',
              RelativeName: doc.RelativeName || '',
              PolicyPremium: doc.PolicyPremium || '',
              PolicyArea: doc.PolicyArea || '',
              PolicyType: doc.PolicyType || '',
              LandSurveyNumber: doc.LandSurveyNumber || '',
              LandDivisionNumber: doc.LandDivisionNumber || '',
              PlotStateName: doc.PlotStateName || '',
              PlotDistrictName: doc.PlotDistrictName || '',
              PlotVillageName: doc.PlotVillageName || '',
              ApplicationSource: doc.ApplicationSource || '',
              CropShare: doc.CropShare || '',
              IFSCCode: doc.IFSCCode || '',
              FarmerShare: doc.FarmerShare || '',
              SowingDate: doc.SowingDate || '',
              CreatedBY: doc.CreatedBY || '',
              TicketDescription: doc.TicketDescription || ''
            });
          });





          skip += CHUNK_SIZE;
        }
      }

      const nextDate = new Date(currentDate);
      nextDate.setDate(nextDate.getDate() + 1);
      await processDateWithChunking(nextDate, endDate);
    }

    await processDateWithChunking(new Date(SPFROMDATE), new Date(SPTODATE));



    const excelFileName = `support_ticket_data_${Date.now()}.xlsx`;
    const excelFilePath = path.join(folderPath, excelFileName);

    await workbook.xlsx.writeFile(excelFilePath);
    console.log(`Excel file created at: ${excelFilePath}`);

    const zipFileName = excelFileName.replace('.xlsx', '.zip');
    const zipFilePath = path.join(folderPath, zipFileName);
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(output);
    archive.file(excelFilePath, { name: excelFileName });
    await archive.finalize();
    await new Promise<void>((resolve, reject) => {
      output.on('close', resolve);
      output.on('error', reject);
    });
    await fs.promises.unlink(excelFilePath).catch(console.error);

    const gcpService = new GCPServices();
    const fileBuffer = await fs.promises.readFile(zipFilePath);
    const uploadResult = await gcpService.uploadFileToGCP({
      filePath: 'krph/reports/',
      uploadedBy: 'KRPH',
      file: { buffer: fileBuffer, originalname: zipFileName },
    });
    const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
    if (gcpDownloadUrl) await fs.promises.unlink(zipFilePath).catch(console.error);

    // await db.collection('support_ticket_download_logs').insertOne({
    //   userId: SPUserID,
    //   insuranceCompanyId: SPInsuranceCompanyID,
    //   stateId: SPStateID,
    //   ticketHeaderId: SPTicketHeaderID,
    //   fromDate: SPFROMDATE,
    //   toDate: SPTODATE,
    //   zipFileName,
    //   downloadUrl: gcpDownloadUrl,
    //   createdAt: new Date(),
    // });
    await this.insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, zipFileName, gcpDownloadUrl, this.db)

    const responsePayload = {
      data: [], pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
      downloadUrl: gcpDownloadUrl
    };

    const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);
    try {
      await this.mailService.sendMail({ to: userEmail, subject: 'Support Ticket History Report Download Service', text: 'Support Ticket History Report', html: supportTicketTemplate });
      console.log("Mail sent successfully");
    } catch (err) {
      console.error(`Failed to send email to ${userEmail}:`, err);
    }

    await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);

    // return responsePayload;
  }

  async processTicketHistoryAndGenerateZipLastWorking(ticketPayload: any) {
    let {
      SPFROMDATE,
      SPTODATE,
      SPInsuranceCompanyID,
      SPStateID,
      SPTicketHeaderID,
      SPUserID,
      page = 1,
      limit = 1000000000,
      userEmail,
    } = ticketPayload;

    const db = this.db;
    SPTicketHeaderID = Number(SPTicketHeaderID);

    if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
    if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };

    const RequestDateTime = await getCurrentFormattedDateTime();
    const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;
    if (cachedData) {
      console.log('Using cached data');
      await this.db.collection('support_ticket_download_logs').updateOne(
        { SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE },
        {
          $set: {
            downloadUrl: cachedData.downloadUrl || '',
            zipFileName: cachedData.zipFileName || '',
            updatedAt: new Date()
          },
          $setOnInsert: { createdAt: new Date() }
        },
        { upsert: true }
      );



      // return cachedData;
    }

    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];
    if (!item) return { rcode: 0, rmessage: 'User details not found.' };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };
    const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length)
      locationFilter = { FilterStateID: { $in: StateMasterID } };
    else if (LocationTypeID === 2 && item.DistrictIDs?.length)
      locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };

    const baseMatch: any = { ...locationFilter };
    if (SPTicketHeaderID && SPTicketHeaderID !== 0) baseMatch.TicketHeaderID = SPTicketHeaderID;

    if (SPInsuranceCompanyID && SPInsuranceCompanyID !== '#ALL') {
      const requestedInsuranceIDs = SPInsuranceCompanyID.split(',').map((id) => Number(id.trim()));
      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter((id) => allowedInsuranceIDs.includes(id));
      if (!validInsuranceIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
      baseMatch.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else if (InsuranceCompanyID?.length)
      baseMatch.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };

    if (SPStateID && SPStateID !== '#ALL') {
      const requestedStateIDs = SPStateID.split(',').map((id) => id.trim());
      const validStateIDs = requestedStateIDs.filter((id) => StateMasterID.includes(id));
      if (!validStateIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
      baseMatch.FilterStateID = { $in: validStateIDs };
    } else if (StateMasterID?.length && LocationTypeID !== 2)
      baseMatch.FilterStateID = { $in: StateMasterID };

    const folderPath = path.join(process.cwd(), 'downloads');
    await fs.promises.mkdir(folderPath, { recursive: true });

    // =========================
    // ExcelJS streaming workbook
    // =========================
    const excelFileName = `support_ticket_data_${Date.now()}.xlsx`;
    const excelFilePath = path.join(folderPath, excelFileName);
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
    const worksheet = workbook.addWorksheet('Support Tickets');

    worksheet.columns = [
      { header: 'Agent ID', key: 'AgentID', width: 20 },
      { header: 'Calling ID', key: 'CallingUniqueID', width: 25 },
      { header: 'NCIP Docket No', key: 'TicketNCIPDocketNo', width: 30 },
      { header: 'Ticket No', key: 'SupportTicketNo', width: 30 },
      { header: 'Creation Date', key: 'Created', width: 25 },
      { header: 'Re-Open Date', key: 'TicketReOpenDate', width: 25 },
      { header: 'Ticket Status', key: 'TicketStatus', width: 20 },
      { header: 'Status Date', key: 'StatusUpdateTime', width: 25 },
      { header: 'State', key: 'StateMasterName', width: 20 },
      { header: 'District', key: 'DistrictMasterName', width: 20 },
      { header: 'Sub District', key: 'SubDistrictName', width: 20 },
      { header: 'Type', key: 'TicketHeadName', width: 20 },
      { header: 'Category', key: 'TicketTypeName', width: 20 },
      { header: 'Sub Category', key: 'TicketCategoryName', width: 20 },
      { header: 'Season', key: 'CropSeasonName', width: 15 },
      { header: 'Year', key: 'RequestYear', width: 10 },
      { header: 'Insurance Company', key: 'InsuranceCompany', width: 30 },
      { header: 'Application No', key: 'ApplicationNo', width: 25 },
      { header: 'Policy No', key: 'InsurancePolicyNo', width: 25 },
      { header: 'Caller Mobile No', key: 'CallerContactNumber', width: 20 },
      { header: 'Farmer Name', key: 'RequestorName', width: 25 },
      { header: 'Mobile No', key: 'RequestorMobileNo', width: 20 },
      { header: 'Relation', key: 'Relation', width: 15 },
      { header: 'Relative Name', key: 'RelativeName', width: 25 },
      { header: 'Policy Premium', key: 'PolicyPremium', width: 15 },
      { header: 'Policy Area', key: 'PolicyArea', width: 15 },
      { header: 'Policy Type', key: 'PolicyType', width: 20 },
      { header: 'Land Survey Number', key: 'LandSurveyNumber', width: 25 },
      { header: 'Land Division Number', key: 'LandDivisionNumber', width: 25 },
      { header: 'Plot State', key: 'PlotStateName', width: 20 },
      { header: 'Plot District', key: 'PlotDistrictName', width: 20 },
      { header: 'Plot Village', key: 'PlotVillageName', width: 25 },
      { header: 'Application Source', key: 'ApplicationSource', width: 20 },
      { header: 'Crop Share', key: 'CropShare', width: 15 },
      { header: 'IFSC Code', key: 'IFSCCode', width: 20 },
      { header: 'Farmer Share', key: 'FarmerShare', width: 15 },
      { header: 'Sowing Date', key: 'SowingDate', width: 20 },
      { header: 'Created By', key: 'CreatedBY', width: 20 },
      { header: 'Description', key: 'TicketDescription', width: 50 },
    ];

    await this.insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, "", "", this.db);

    const CHUNK_SIZE = 10000;

    async function processDateWithChunking(currentDate: Date, endDate: Date) {
      if (currentDate > endDate) return;

      const startOfDay = new Date(currentDate);
      startOfDay.setUTCHours(0, 0, 0, 0);
      const endOfDay = new Date(currentDate);
      endOfDay.setUTCHours(23, 59, 59, 999);

      let skip = 0;
      let hasMore = true;

      while (hasMore) {
        const dailyMatch = { ...baseMatch, InsertDateTime: { $gte: startOfDay, $lte: endOfDay } };
        const pipeline: any[] = [
          { $match: dailyMatch },
          {
            $lookup: {
              from: 'SLA_KRPH_SupportTicketsHistory_Records',
              let: { ticketId: '$SupportTicketID' },
              pipeline: [
                { $match: { $expr: { $and: [{ $eq: ['$SupportTicketID', '$$ticketId'] }, { $eq: ['$TicketStatusID', 109304] }] } } },
                { $sort: { TicketHistoryID: -1 } },
                { $limit: 1 }
              ],
              as: 'ticketHistory',
            }
          },
          {
            $lookup: {
              from: 'support_ticket_claim_intimation_report_history',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'claimInfo',
            }
          },
          {
            $lookup: {
              from: 'csc_agent_master',
              localField: 'InsertUserID',
              foreignField: 'UserLoginID',
              as: 'agentInfo',
            }
          },
          {
            $lookup: {
              from: 'ticket_comment_journey',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'ticket_comment_journey',
            },
          },

          { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
          { $skip: skip },
          { $limit: CHUNK_SIZE },
          {
            $project: {
              agentInfo: 1,
              TicketComments: {
                $arrayToObject: {
                  $map: {
                    input: {
                      $filter: {
                        input: '$ticket_comment_journey',
                        as: 'comment',
                        cond: {
                          $and: [
                            { $ne: ['$$comment.ResolvedDate', null] },
                            { $ne: ['$$comment.ResolvedComment', null] }
                          ]
                        }
                      }
                    },
                    as: 'comment',
                    in: {
                      k: {
                        $concat: [
                          'Comment (',
                          {
                            $dateToString: {
                              format: '%Y-%m-%d',
                              date: '$$comment.ResolvedDate',
                            },
                          },
                          ')',
                        ],
                      },
                      v: '$$comment.ResolvedComment',
                    },
                  }
                }
              },

              CallingUniqueID: 1,
              TicketNCIPDocketNo: 1,
              SupportTicketNo: 1,
              Created: 1,
              TicketReOpenDate: 1,
              TicketStatus: 1,
              StatusUpdateTime: 1,
              StateMasterName: 1,
              DistrictMasterName: 1,
              SubDistrictName: 1,
              TicketHeadName: 1,
              TicketTypeName: 1,
              TicketCategoryName: 1,
              CropSeasonName: 1,
              RequestYear: 1,
              InsuranceCompany: 1,
              ApplicationNo: 1,
              InsurancePolicyNo: 1,
              CallerContactNumber: 1,
              RequestorName: 1,
              RequestorMobileNo: 1,
              Relation: 1,
              RelativeName: 1,
              PolicyPremium: 1,
              PolicyArea: 1,
              PolicyType: 1,
              LandSurveyNumber: 1,
              LandDivisionNumber: 1,
              PlotStateName: 1,
              PlotDistrictName: 1,
              PlotVillageName: 1,
              ApplicationSource: 1,
              CropShare: 1,
              IFSCCode: 1,
              FarmerShare: 1,
              SowingDate: 1,
              CreatedBY: 1,
              TicketDescription: 1
            }
          },

        ];

        const cursor = db.collection('SLA_KRPH_SupportTickets_Records').aggregate(pipeline, { allowDiskUse: true });
        const docs = await cursor.toArray();

        docs.forEach(doc => {
          if (doc.TicketComments) {
            for (const [key, value] of Object.entries(doc.TicketComments)) {
              doc[key] = value;
            }
            delete doc.TicketComments;
          }

          // streaming addRow
          worksheet.addRow({
            AgentID: doc.agentInfo?.UserID?.toString() || '',
            CallingUniqueID: doc.CallingUniqueID || '',
            TicketNCIPDocketNo: doc.TicketNCIPDocketNo || '',
            SupportTicketNo: doc.SupportTicketNo?.toString() || '',
            Created: doc.Created ? new Date(doc.Created).toISOString() : '',
            TicketReOpenDate: doc.TicketReOpenDate || '',
            TicketStatus: doc.TicketStatus || '',
            StatusUpdateTime: doc.StatusUpdateTime ? new Date(doc.StatusUpdateTime).toISOString() : '',
            StateMasterName: doc.StateMasterName || '',
            DistrictMasterName: doc.DistrictMasterName || '',
            SubDistrictName: doc.SubDistrictName || '',
            TicketHeadName: doc.TicketHeadName || '',
            TicketTypeName: doc.TicketTypeName || '',
            TicketCategoryName: doc.TicketCategoryName || '',
            CropSeasonName: doc.CropSeasonName || '',
            RequestYear: doc.RequestYear || '',
            InsuranceCompany: doc.InsuranceCompany || '',
            ApplicationNo: doc.ApplicationNo || '',
            InsurancePolicyNo: doc.InsurancePolicyNo || '',
            CallerContactNumber: doc.CallerContactNumber || '',
            RequestorName: doc.RequestorName || '',
            RequestorMobileNo: doc.RequestorMobileNo || '',
            Relation: doc.Relation || '',
            RelativeName: doc.RelativeName || '',
            PolicyPremium: doc.PolicyPremium || '',
            PolicyArea: doc.PolicyArea || '',
            PolicyType: doc.PolicyType || '',
            LandSurveyNumber: doc.LandSurveyNumber || '',
            LandDivisionNumber: doc.LandDivisionNumber || '',
            PlotStateName: doc.PlotStateName || '',
            PlotDistrictName: doc.PlotDistrictName || '',
            PlotVillageName: doc.PlotVillageName || '',
            ApplicationSource: doc.ApplicationSource || '',
            CropShare: doc.CropShare || '',
            IFSCCode: doc.IFSCCode || '',
            FarmerShare: doc.FarmerShare || '',
            SowingDate: doc.SowingDate || '',
            CreatedBY: doc.CreatedBY || '',
            TicketDescription: doc.TicketDescription || ''
          }).commit();
        });

        if (docs.length < CHUNK_SIZE) hasMore = false;
        else skip += CHUNK_SIZE;
      }

      const nextDate = new Date(currentDate);
      nextDate.setDate(nextDate.getDate() + 1);
      await processDateWithChunking(nextDate, endDate);
    }

    await processDateWithChunking(new Date(SPFROMDATE), new Date(SPTODATE));

    await workbook.commit();
    console.log(`Excel file created at: ${excelFilePath}`);

    const zipFileName = excelFileName.replace('.xlsx', '.zip');
    const zipFilePath = path.join(folderPath, zipFileName);
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(output);
    archive.file(excelFilePath, { name: excelFileName });
    await archive.finalize();
    await new Promise<void>((resolve, reject) => {
      output.on('close', resolve);
      output.on('error', reject);
    });
    await fs.promises.unlink(excelFilePath).catch(console.error);

    const gcpService = new GCPServices();
    const fileBuffer = await fs.promises.readFile(zipFilePath);
    const uploadResult = await gcpService.uploadFileToGCP({
      filePath: 'krph/reports/',
      uploadedBy: 'KRPH',
      file: { buffer: fileBuffer, originalname: zipFileName },
    });
    const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
    if (gcpDownloadUrl) await fs.promises.unlink(zipFilePath).catch(console.error);

    await this.insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, zipFileName, gcpDownloadUrl, this.db);

    const responsePayload = {
      data: [],
      pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
      downloadUrl: gcpDownloadUrl,
      zipFileName: zipFileName
    };

    const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);
    try {
      await this.mailService.sendMail({
        to: userEmail,
        subject: 'Support Ticket History Report Download Service',
        text: 'Support Ticket History Report',
        html: supportTicketTemplate
      });
      console.log("Mail sent successfully");
    } catch (err) {
      console.error(`Failed to send email to ${userEmail}:`, err);
    }

    await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);

    // return responsePayload;
  }

  async processTicketHistoryAndGenerateZipPreviousWorkingWothoutComment(ticketPayload: any) {
    let {
      SPFROMDATE,
      SPTODATE,
      SPInsuranceCompanyID,
      SPStateID,
      SPTicketHeaderID,
      SPUserID,
      page = 1,
      limit = 1000000000,
      userEmail,
    } = ticketPayload;

    const db = this.db;
    SPTicketHeaderID = Number(SPTicketHeaderID);

    if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
    if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };

    const RequestDateTime = await getCurrentFormattedDateTime();
    const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;
    if (cachedData) {
      console.log('Using cached data');
      await this.db.collection('support_ticket_download_logs').updateOne(
        { SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE },
        {
          $set: {
            downloadUrl: cachedData.downloadUrl || '',
            zipFileName: cachedData.zipFileName || '',
            updatedAt: new Date()
          },
          $setOnInsert: { createdAt: new Date() }
        },
        { upsert: true }
      );
      // Maybe return cachedData? You had commented that out.
    }

    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];
    if (!item) return { rcode: 0, rmessage: 'User details not found.' };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };
    const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length)
      locationFilter = { FilterStateID: { $in: StateMasterID } };
    else if (LocationTypeID === 2 && item.DistrictIDs?.length)
      locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };

    const baseMatch: any = { ...locationFilter };
    if (SPTicketHeaderID && SPTicketHeaderID !== 0) baseMatch.TicketHeaderID = SPTicketHeaderID;

    if (SPInsuranceCompanyID && SPInsuranceCompanyID !== '#ALL') {
      const requestedInsuranceIDs = SPInsuranceCompanyID.split(',').map((id) => Number(id.trim()));
      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter((id) => allowedInsuranceIDs.includes(id));
      if (!validInsuranceIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
      baseMatch.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else if (InsuranceCompanyID?.length) {
      baseMatch.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
    }

    if (SPStateID && SPStateID !== '#ALL') {
      const requestedStateIDs = SPStateID.split(',').map((id) => id.trim());
      const validStateIDs = requestedStateIDs.filter((id) => StateMasterID.includes(id));
      if (!validStateIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
      baseMatch.FilterStateID = { $in: validStateIDs };
    } else if (StateMasterID?.length && LocationTypeID !== 2) {
      baseMatch.FilterStateID = { $in: StateMasterID };
    }

    const folderPath = path.join(process.cwd(), 'downloads');
    await fs.promises.mkdir(folderPath, { recursive: true });

    // ====== Filename Logic Based on SPTicketHeaderID ======

    const headerTypeMap: Record<number, string> = {
      1: 'Grievance',
      2: 'Information',
      4: 'Crop_Loss',
    };
    const ticketTypeName = headerTypeMap[SPTicketHeaderID] || 'General';
    const currentDateStr = new Date().toLocaleDateString('en-GB').split('/').join('_');  // "dd_mm_yyyy"

    const excelFileName = `Support_ticket_data_${ticketTypeName}_${currentDateStr}.xlsx`;
    const excelFilePath = path.join(folderPath, excelFileName);

    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
    const worksheet = workbook.addWorksheet('Support Tickets');

    worksheet.columns = [
      { header: 'Agent ID', key: 'AgentID', width: 20 },
      { header: 'Calling ID', key: 'CallingUniqueID', width: 25 },
      { header: 'NCIP Docket No', key: 'TicketNCIPDocketNo', width: 30 },
      { header: 'Ticket No', key: 'SupportTicketNo', width: 30 },
      { header: 'Creation Date', key: 'Created', width: 25 },
      { header: 'Re-Open Date', key: 'TicketReOpenDate', width: 25 },
      { header: 'Ticket Status', key: 'TicketStatus', width: 20 },
      { header: 'Status Date', key: 'StatusUpdateTime', width: 25 },
      { header: 'State', key: 'StateMasterName', width: 20 },
      { header: 'District', key: 'DistrictMasterName', width: 20 },
      { header: 'Sub District', key: 'SubDistrictName', width: 20 },
      { header: 'Type', key: 'TicketHeadName', width: 20 },
      { header: 'Category', key: 'TicketTypeName', width: 20 },
      { header: 'Sub Category', key: 'TicketCategoryName', width: 20 },
      { header: 'Season', key: 'CropSeasonName', width: 15 },
      { header: 'Year', key: 'RequestYear', width: 10 },
      { header: 'Insurance Company', key: 'InsuranceCompany', width: 30 },
      { header: 'Application No', key: 'ApplicationNo', width: 25 },
      { header: 'Policy No', key: 'InsurancePolicyNo', width: 25 },
      { header: 'Caller Mobile No', key: 'CallerContactNumber', width: 20 },
      { header: 'Farmer Name', key: 'RequestorName', width: 25 },
      { header: 'Mobile No', key: 'RequestorMobileNo', width: 20 },
      { header: 'Relation', key: 'Relation', width: 15 },
      { header: 'Relative Name', key: 'RelativeName', width: 25 },
      { header: 'Policy Premium', key: 'PolicyPremium', width: 15 },
      { header: 'Policy Area', key: 'PolicyArea', width: 15 },
      { header: 'Policy Type', key: 'PolicyType', width: 20 },
      { header: 'Land Survey Number', key: 'LandSurveyNumber', width: 25 },
      { header: 'Land Division Number', key: 'LandDivisionNumber', width: 25 },
      { header: 'Plot State', key: 'PlotStateName', width: 20 },
      { header: 'Plot District', key: 'PlotDistrictName', width: 20 },
      { header: 'Plot Village', key: 'PlotVillageName', width: 25 },
      { header: 'Application Source', key: 'ApplicationSource', width: 20 },
      { header: 'Crop Share', key: 'CropShare', width: 15 },
      { header: 'IFSC Code', key: 'IFSCCode', width: 20 },
      { header: 'Farmer Share', key: 'FarmerShare', width: 15 },
      { header: 'Sowing Date', key: 'SowingDate', width: 20 },
      { header: 'Created By', key: 'CreatedBY', width: 20 },
      { header: 'Description', key: 'TicketDescription', width: 50 },
    ];

    await this.insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, "", "", this.db);

    const CHUNK_SIZE = 10000;

    async function processDateWithChunking(currentDate: Date, endDate: Date) {
      if (currentDate > endDate) return;

      const startOfDay = new Date(currentDate);
      startOfDay.setUTCHours(0, 0, 0, 0);
      const endOfDay = new Date(currentDate);
      endOfDay.setUTCHours(23, 59, 59, 999);

      let skip = 0;
      let hasMore = true;

      while (hasMore) {
        const dailyMatch = { ...baseMatch, InsertDateTime: { $gte: startOfDay, $lte: endOfDay } };
        const pipeline: any[] = [
          { $match: dailyMatch },
          {
            $lookup: {
              from: 'SLA_KRPH_SupportTicketsHistory_Records',
              let: { ticketId: '$SupportTicketID' },
              pipeline: [
                { $match: { $expr: { $and: [{ $eq: ['$SupportTicketID', '$$ticketId'] }, { $eq: ['$TicketStatusID', 109304] }] } } },
                { $sort: { TicketHistoryID: -1 } },
                { $limit: 1 }
              ],
              as: 'ticketHistory',
            }
          },
          {
            $lookup: {
              from: 'support_ticket_claim_intimation_report_history',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'claimInfo',
            }
          },
          {
            $lookup: {
              from: 'csc_agent_master',
              localField: 'InsertUserID',
              foreignField: 'UserLoginID',
              as: 'agentInfo',
            }
          },
          {
            $lookup: {
              from: 'ticket_comment_journey',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'ticket_comment_journey',
            },
          },

          { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
          { $skip: skip },
          { $limit: CHUNK_SIZE },
          {
            $project: {
              agentInfo: 1,
              TicketComments: {
                $arrayToObject: {
                  $map: {
                    input: {
                      $filter: {
                        input: '$ticket_comment_journey',
                        as: 'comment',
                        cond: {
                          $and: [
                            { $ne: ['$$comment.ResolvedDate', null] },
                            { $ne: ['$$comment.ResolvedComment', null] }
                          ]
                        }
                      }
                    },
                    as: 'comment',
                    in: {
                      k: {
                        $concat: [
                          'Comment (',
                          {
                            $dateToString: {
                              format: '%Y-%m-%d',
                              date: '$$comment.ResolvedDate',
                            },
                          },
                          ')',
                        ],
                      },
                      v: '$$comment.ResolvedComment',
                    },
                  }
                }
              },

              CallingUniqueID: 1,
              TicketNCIPDocketNo: 1,
              SupportTicketNo: 1,
              Created: 1,
              TicketReOpenDate: 1,
              TicketStatus: 1,
              StatusUpdateTime: 1,
              StateMasterName: 1,
              DistrictMasterName: 1,
              SubDistrictName: 1,
              TicketHeadName: 1,
              TicketTypeName: 1,
              TicketCategoryName: 1,
              CropSeasonName: 1,
              RequestYear: 1,
              InsuranceCompany: 1,
              ApplicationNo: 1,
              InsurancePolicyNo: 1,
              CallerContactNumber: 1,
              RequestorName: 1,
              RequestorMobileNo: 1,
              Relation: 1,
              RelativeName: 1,
              PolicyPremium: 1,
              PolicyArea: 1,
              PolicyType: 1,
              LandSurveyNumber: 1,
              LandDivisionNumber: 1,
              PlotStateName: 1,
              PlotDistrictName: 1,
              PlotVillageName: 1,
              ApplicationSource: 1,
              CropShare: 1,
              IFSCCode: 1,
              FarmerShare: 1,
              SowingDate: 1,
              CreatedBY: 1,
              TicketDescription: 1
            }
          },
        ];

        const cursor = db.collection('SLA_KRPH_SupportTickets_Records').aggregate(pipeline, { allowDiskUse: true });
        const docs = await cursor.toArray();

        docs.forEach(doc => {
          if (doc.TicketComments) {
            for (const [key, value] of Object.entries(doc.TicketComments)) {
              doc[key] = value;
            }
            delete doc.TicketComments;
          }

          worksheet.addRow({
            AgentID: doc.agentInfo?.UserID?.toString() || '',
            CallingUniqueID: doc.CallingUniqueID || '',
            TicketNCIPDocketNo: doc.TicketNCIPDocketNo || '',
            SupportTicketNo: doc.SupportTicketNo?.toString() || '',
            Created: doc.Created ? new Date(doc.Created).toISOString() : '',
            TicketReOpenDate: doc.TicketReOpenDate || '',
            TicketStatus: doc.TicketStatus || '',
            StatusUpdateTime: doc.StatusUpdateTime ? new Date(doc.StatusUpdateTime).toISOString() : '',
            StateMasterName: doc.StateMasterName || '',
            DistrictMasterName: doc.DistrictMasterName || '',
            SubDistrictName: doc.SubDistrictName || '',
            TicketHeadName: doc.TicketHeadName || '',
            TicketTypeName: doc.TicketTypeName || '',
            TicketCategoryName: doc.TicketCategoryName || '',
            CropSeasonName: doc.CropSeasonName || '',
            RequestYear: doc.RequestYear || '',
            InsuranceCompany: doc.InsuranceCompany || '',
            ApplicationNo: doc.ApplicationNo || '',
            InsurancePolicyNo: doc.InsurancePolicyNo || '',
            CallerContactNumber: doc.CallerContactNumber || '',
            RequestorName: doc.RequestorName || '',
            RequestorMobileNo: doc.RequestorMobileNo || '',
            Relation: doc.Relation || '',
            RelativeName: doc.RelativeName || '',
            PolicyPremium: doc.PolicyPremium || '',
            PolicyArea: doc.PolicyArea || '',
            PolicyType: doc.PolicyType || '',
            LandSurveyNumber: doc.LandSurveyNumber || '',
            LandDivisionNumber: doc.LandDivisionNumber || '',
            PlotStateName: doc.PlotStateName || '',
            PlotDistrictName: doc.PlotDistrictName || '',
            PlotVillageName: doc.PlotVillageName || '',
            ApplicationSource: doc.ApplicationSource || '',
            CropShare: doc.CropShare || '',
            IFSCCode: doc.IFSCCode || '',
            FarmerShare: doc.FarmerShare || '',
            SowingDate: doc.SowingDate || '',
            CreatedBY: doc.CreatedBY || '',
            TicketDescription: doc.TicketDescription || ''
          }).commit();
        });

        if (docs.length < CHUNK_SIZE) hasMore = false;
        else skip += CHUNK_SIZE;
      }

      const nextDate = new Date(currentDate);
      nextDate.setDate(nextDate.getDate() + 1);
      await processDateWithChunking(nextDate, endDate);
    }

    await processDateWithChunking(new Date(SPFROMDATE), new Date(SPTODATE));

    await workbook.commit();
    console.log(`Excel file created at: ${excelFilePath}`);

    // Create ZIP
    const zipFileName = excelFileName.replace('.xlsx', '.zip');
    const zipFilePath = path.join(folderPath, zipFileName);
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(output);
    archive.file(excelFilePath, { name: excelFileName });
    await archive.finalize();
    await new Promise<void>((resolve, reject) => {
      output.on('close', resolve);
      output.on('error', reject);
    });
    await fs.promises.unlink(excelFilePath).catch(console.error);

    // Upload to GCP
    const gcpService = new GCPServices();
    const fileBuffer = await fs.promises.readFile(zipFilePath);
    const uploadResult = await gcpService.uploadFileToGCP({
      filePath: 'krph/reports/',
      uploadedBy: 'KRPH',
      file: { buffer: fileBuffer, originalname: zipFileName },
    });
    const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
    if (gcpDownloadUrl) await fs.promises.unlink(zipFilePath).catch(console.error);

    await this.insertOrUpdateDownloadLog(
      SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID,
      SPFROMDATE, SPTODATE, zipFileName, gcpDownloadUrl, this.db
    );

    const responsePayload = {
      data: [],
      pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
      downloadUrl: gcpDownloadUrl,
      zipFileName: zipFileName
    };

    const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);
    try {
      await this.mailService.sendMail({
        to: userEmail,
        subject: 'Support Ticket History Report Download Service',
        text: 'Support Ticket History Report',
        html: supportTicketTemplate
      });
      console.log("Mail sent successfully");
    } catch (err) {
      console.error(`Failed to send email to ${userEmail}:`, err);
    }

    await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);

    // return responsePayload;   <-- optionally return
  }

  //Upgraded code Previoud
  /* async processTicketHistoryAndGenerateZip(ticketPayload: any) {
    let {
      SPFROMDATE,
      SPTODATE,
      SPInsuranceCompanyID,
      SPStateID,
      SPTicketHeaderID,
      SPUserID,
      page = 1,
      limit = 1000000000,
      userEmail,
    } = ticketPayload;
  
    const db = this.db;
    SPTicketHeaderID = Number(SPTicketHeaderID);
  
    if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
    if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };
  
    const RequestDateTime = await getCurrentFormattedDateTime();
    const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;
    if (cachedData) {
      console.log('Using cached data');
      await this.db.collection('support_ticket_download_logs').updateOne(
        { SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE },
        {
          $set: {
            downloadUrl: cachedData.downloadUrl || '',
            zipFileName: cachedData.zipFileName || '',
            updatedAt: new Date()
          },
          $setOnInsert: { createdAt: new Date() }
        },
        { upsert: true }
      );
      return cachedData;
    }
  
    // ===== User detail auth =====
    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];
    if (!item) return { rcode: 0, rmessage: 'User details not found.' };
  
    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };
    const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;
  
    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length)
      locationFilter = { FilterStateID: { $in: StateMasterID } };
    else if (LocationTypeID === 2 && item.DistrictIDs?.length)
      locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };
  
    const baseMatch: any = { ...locationFilter };
    if (SPTicketHeaderID && SPTicketHeaderID !== 0) baseMatch.TicketHeaderID = SPTicketHeaderID;
  
    if (SPInsuranceCompanyID && SPInsuranceCompanyID !== '#ALL') {
      const requestedInsuranceIDs = SPInsuranceCompanyID.split(',').map((id) => Number(id.trim()));
      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter((id) => allowedInsuranceIDs.includes(id));
      if (!validInsuranceIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
      baseMatch.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else if (InsuranceCompanyID?.length) {
      baseMatch.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
    }
  
    if (SPStateID && SPStateID !== '#ALL') {
      const requestedStateIDs = SPStateID.split(',').map((id) => id.trim());
      const validStateIDs = requestedStateIDs.filter((id) => StateMasterID.includes(id));
      if (!validStateIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
      baseMatch.FilterStateID = { $in: validStateIDs };
    } else if (StateMasterID?.length && LocationTypeID !== 2) {
      baseMatch.FilterStateID = { $in: StateMasterID };
    }
  
    const folderPath = path.join(process.cwd(), 'downloads');
    await fs.promises.mkdir(folderPath, { recursive: true });
  
    // ===== Filename =====
    const headerTypeMap: Record<number, string> = {
      1: 'Grievance',
      2: 'Information',
      4: 'Crop_Loss',
    };
    const ticketTypeName = headerTypeMap[SPTicketHeaderID] || 'General';
    const currentDateStr = new Date().toLocaleDateString('en-GB').split('/').join('_');
  
    const excelFileName = `Support_ticket_data_${ticketTypeName}_${currentDateStr}.xlsx`;
    const excelFilePath = path.join(folderPath, excelFileName);
  
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
    const worksheet = workbook.addWorksheet('Support Tickets');
  
    await this.insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, "", "", this.db);
  
    const CHUNK_SIZE = 10000;
  
    // Keep global set of dynamic columns
    const dynamicColumns = new Set<string>();
    const allDocs: any[] = []; // collect all docs first
  
    function formatToDDMMYYYY(dateString) {
      if (!dateString) return '';
      const date = new Date(dateString);
      if (isNaN(date.getTime())) return '';
      const day = String(date.getDate()).padStart(2, '0');
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const year = date.getFullYear();
      const hours = String(date.getHours()).padStart(2, '0');
      const minutes = String(date.getMinutes()).padStart(2, '0');
      return `${day}-${month}-${year} ${hours}:${minutes}`;
    }
  
    async function processDateWithChunking(currentDate: Date, endDate: Date) {
      if (currentDate > endDate) return;
  
      const startOfDay = new Date(currentDate);
      startOfDay.setUTCHours(0, 0, 0, 0);
      const endOfDay = new Date(currentDate);
      endOfDay.setUTCHours(23, 59, 59, 999);
  
      let skip = 0;
      let hasMore = true;
  
      while (hasMore) {
        const dailyMatch = { ...baseMatch, InsertDateTime: { $gte: startOfDay, $lte: endOfDay } };
        const pipeline: any[] = [
          { $match: dailyMatch },
          {
            $lookup: {
              from: 'SLA_KRPH_SupportTicketsHistory_Records',
              let: { ticketId: '$SupportTicketID' },
              pipeline: [
                { $match: { $expr: { $and: [{ $eq: ['$SupportTicketID', '$$ticketId'] }, { $eq: ['$TicketStatusID', 109304] }] } } },
                { $sort: { TicketHistoryID: -1 } },
                { $limit: 1 }
              ],
              as: 'ticketHistory',
            }
          },
          {
            $lookup: {
              from: 'support_ticket_claim_intimation_report_history',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'claimInfo',
            }
          },
          {
            $lookup: {
              from: 'csc_agent_master',
              localField: 'InsertUserID',
              foreignField: 'UserLoginID',
              as: 'agentInfo',
            }
          },
          {
            $lookup: {
              from: 'ticket_comment_journey',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'ticket_comment_journey',
            },
          },
          { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
          { $skip: skip },
          { $limit: CHUNK_SIZE },
          { $addFields: { ticket_comment_journey: { $ifNull: ['$ticket_comment_journey', []] } } }
        ];
  
        const cursor = db.collection('SLA_KRPH_SupportTickets_Records').aggregate(pipeline, { allowDiskUse: true });
        const docs = await cursor.toArray();
  
        docs.forEach(doc => {
          if (Array.isArray(doc.ticket_comment_journey)) {
            const seenComments = new Set();
            let commentIndex = 1;
  
            doc.ticket_comment_journey.forEach((commentObj) => {
              const rawComment = (commentObj.ResolvedComment || '').replace(/<\/?[^>]+(>|$)/g, '').trim();
              const commentDate = formatToDDMMYYYY(commentObj.ResolvedDate);
  
              const uniqueKey = `${commentDate}__${rawComment}`;
              if (!seenComments.has(uniqueKey)) {
                doc[`Date ${commentIndex}`] = commentDate;
                doc[`Comment ${commentIndex}`] = rawComment;
                dynamicColumns.add(`Date ${commentIndex}`);
                dynamicColumns.add(`Comment ${commentIndex}`);
                seenComments.add(uniqueKey);
                commentIndex++;
              }
            });
  
            delete doc.ticket_comment_journey;
          }
  
          allDocs.push(doc);
        });
  
        if (docs.length < CHUNK_SIZE) hasMore = false;
        else skip += CHUNK_SIZE;
      }
  
      const nextDate = new Date(currentDate);
      nextDate.setDate(nextDate.getDate() + 1);
      await processDateWithChunking(nextDate, endDate);
    }
  
    await processDateWithChunking(new Date(SPFROMDATE), new Date(SPTODATE));
  
    // ===== Set worksheet columns BEFORE adding rows =====
    const staticColumns = [
      { header: 'Agent ID', key: 'AgentID', width: 20 },
      { header: 'Calling ID', key: 'CallingUniqueID', width: 25 },
      { header: 'Ticket NCIP Docket No', key: 'TicketNCIPDocketNo', width: 25 },
      { header: 'Ticket No', key: 'SupportTicketNo', width: 30 },
      { header: 'Creation Date', key: 'Created', width: 25 },
      { header: 'Ticket ReOpen Date', key: 'TicketReOpenDate', width: 25 },
      { header: 'Ticket Status', key: 'TicketStatus', width: 20 },
      { header: 'Status Update Time', key: 'StatusUpdateTime', width: 25 },
      { header: 'State', key: 'StateMasterName', width: 20 },
      { header: 'District', key: 'DistrictMasterName', width: 20 },
      { header: 'Sub District', key: 'SubDistrictName', width: 20 },
      { header: 'Ticket Head', key: 'TicketHeadName', width: 20 },
      { header: 'Ticket Type', key: 'TicketTypeName', width: 20 },
      { header: 'Ticket Category', key: 'TicketCategoryName', width: 20 },
      { header: 'Crop Season', key: 'CropSeasonName', width: 20 },
      { header: 'Request Year', key: 'RequestYear', width: 20 },
      { header: 'Insurance Company', key: 'InsuranceCompany', width: 30 },
      { header: 'Application No', key: 'ApplicationNo', width: 30 },
      { header: 'Policy No', key: 'InsurancePolicyNo', width: 30 },
      { header: 'Caller Contact No', key: 'CallerContactNumber', width: 20 },
      { header: 'Requestor Name', key: 'RequestorName', width: 20 },
      { header: 'Requestor Mobile No', key: 'RequestorMobileNo', width: 20 },
      { header: 'Relation', key: 'Relation', width: 20 },
      { header: 'Relative Name', key: 'RelativeName', width: 20 },
      { header: 'Policy Premium', key: 'PolicyPremium', width: 20 },
      { header: 'Policy Area', key: 'PolicyArea', width: 20 },
      { header: 'Policy Type', key: 'PolicyType', width: 20 },
      { header: 'Land Survey No', key: 'LandSurveyNumber', width: 20 },
      { header: 'Land Division No', key: 'LandDivisionNumber', width: 20 },
      { header: 'Plot State', key: 'PlotStateName', width: 20 },
      { header: 'Plot District', key: 'PlotDistrictName', width: 20 },
      { header: 'Plot Village', key: 'PlotVillageName', width: 20 },
      { header: 'Application Source', key: 'ApplicationSource', width: 20 },
      { header: 'Crop Share', key: 'CropShare', width: 20 },
      { header: 'IFSC Code', key: 'IFSCCode', width: 20 },
      { header: 'Farmer Share', key: 'FarmerShare', width: 20 },
      { header: 'Sowing Date', key: 'SowingDate', width: 20 },
      { header: 'Created By', key: 'CreatedBY', width: 20 },
      { header: 'Ticket Description', key: 'TicketDescription', width: 50 },
    ];
  
    const dynamicColumnDefs = Array.from(dynamicColumns).map(col => ({
      header: col,
      key: col,
      width: 40
    }));
  
    worksheet.columns = [...staticColumns, ...dynamicColumnDefs];
  
    // ===== Now add rows safely =====
    allDocs.forEach(doc => {
      worksheet.addRow({
        AgentID: doc.agentInfo?.UserID?.toString() || '',
        CallingUniqueID: doc.CallingUniqueID || '',
        TicketNCIPDocketNo: doc.TicketNCIPDocketNo || '',
        SupportTicketNo: doc.SupportTicketNo?.toString() || '',
        Created: doc.Created ? new Date(doc.Created).toISOString() : '',
        TicketReOpenDate: doc.TicketReOpenDate || '',
        TicketStatus: doc.TicketStatus || '',
        StatusUpdateTime: doc.StatusUpdateTime ? new Date(doc.StatusUpdateTime).toISOString() : '',
        StateMasterName: doc.StateMasterName || '',
        DistrictMasterName: doc.DistrictMasterName || '',
        SubDistrictName: doc.SubDistrictName || '',
        TicketHeadName: doc.TicketHeadName || '',
        TicketTypeName: doc.TicketTypeName || '',
        TicketCategoryName: doc.TicketCategoryName || '',
        CropSeasonName: doc.CropSeasonName || '',
        RequestYear: doc.RequestYear || '',
        InsuranceCompany: doc.InsuranceCompany || '',
        ApplicationNo: doc.ApplicationNo || '',
        InsurancePolicyNo: doc.InsurancePolicyNo || '',
        CallerContactNumber: doc.CallerContactNumber || '',
        RequestorName: doc.RequestorName || '',
        RequestorMobileNo: doc.RequestorMobileNo || '',
        Relation: doc.Relation || '',
        RelativeName: doc.RelativeName || '',
        PolicyPremium: doc.PolicyPremium || '',
        PolicyArea: doc.PolicyArea || '',
        PolicyType: doc.PolicyType || '',
        LandSurveyNumber: doc.LandSurveyNumber || '',
        LandDivisionNumber: doc.LandDivisionNumber || '',
        PlotStateName: doc.PlotStateName || '',
        PlotDistrictName: doc.PlotDistrictName || '',
        PlotVillageName: doc.PlotVillageName || '',
        ApplicationSource: doc.ApplicationSource || '',
        CropShare: doc.CropShare || '',
        IFSCCode: doc.IFSCCode || '',
        FarmerShare: doc.FarmerShare || '',
        SowingDate: doc.SowingDate || '',
        CreatedBY: doc.CreatedBY || '',
        TicketDescription: doc.TicketDescription || '',
        // include dynamic cols
        ...Object.fromEntries(Object.entries(doc).filter(([k]) => k.startsWith('Date') || k.startsWith('Comment')))
      }).commit();
    });
  
    await workbook.commit();
    console.log(`Excel file created at: ${excelFilePath}`);
  
    // ===== Create ZIP =====
    const zipFileName = excelFileName.replace('.xlsx', '.zip');
    const zipFilePath = path.join(folderPath, zipFileName);
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(output);
    archive.file(excelFilePath, { name: excelFileName });
    await archive.finalize();
    await new Promise<void>((resolve, reject) => {
      output.on('close', resolve);
      output.on('error', reject);
    });
    await fs.promises.unlink(excelFilePath).catch(console.error);
  
    // ===== Upload to GCP =====
    const gcpService = new GCPServices();
    const fileBuffer = await fs.promises.readFile(zipFilePath);
    const uploadResult = await gcpService.uploadFileToGCP({
      filePath: 'krph/reports/',
      uploadedBy: 'KRPH',
      file: { buffer: fileBuffer, originalname: zipFileName },
    });
    const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
    if (gcpDownloadUrl) await fs.promises.unlink(zipFilePath).catch(console.error);
  
    await this.insertOrUpdateDownloadLog(
      SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID,
      SPFROMDATE, SPTODATE, zipFileName, gcpDownloadUrl, this.db
    );
  
    const responsePayload = {
      data: [],
      pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
      downloadUrl: gcpDownloadUrl,
      zipFileName: zipFileName
    };
  
  
    const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);
    try {
      await this.mailService.sendMail({
        to: userEmail,
        subject: 'Support Ticket History Report Download Service',
        text: 'Support Ticket History Report',
        html: supportTicketTemplate
      });
      console.log("Mail sent successfully");
    } catch (err) {
      console.error(`Failed to send email to ${userEmail}:`, err);
    }
  
    await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);
  
    } */


  async processTicketHistoryAndGenerateZipchabged(ticketPayload: any) {
    let {
      SPFROMDATE,
      SPTODATE,
      SPInsuranceCompanyID,
      SPStateID,
      SPTicketHeaderID,
      SPUserID,
      page = 1,
      limit = 1000000000,
      userEmail,
    } = ticketPayload;

    const db = this.db;
    SPTicketHeaderID = Number(SPTicketHeaderID);

    if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
    if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };

    const RequestDateTime = await getCurrentFormattedDateTime();
    const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;
    if (cachedData) {
      console.log('Using cached data');
      await this.db.collection('support_ticket_download_logs').updateOne(
        { SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE },
        {
          $set: {
            downloadUrl: cachedData.downloadUrl || '',
            zipFileName: cachedData.zipFileName || '',
            updatedAt: new Date()
          },
          $setOnInsert: { createdAt: new Date() }
        },
        { upsert: true }
      );
      return cachedData;
    }

    // ===== User detail auth =====
    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];
    if (!item) return { rcode: 0, rmessage: 'User details not found.' };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };
    const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length)
      locationFilter = { FilterStateID: { $in: StateMasterID } };
    else if (LocationTypeID === 2 && item.DistrictIDs?.length)
      locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };

    const baseMatch: any = { ...locationFilter };
    if (SPTicketHeaderID && SPTicketHeaderID !== 0) baseMatch.TicketHeaderID = SPTicketHeaderID;

    if (SPInsuranceCompanyID && SPInsuranceCompanyID !== '#ALL') {
      const requestedInsuranceIDs = SPInsuranceCompanyID.split(',').map((id) => Number(id.trim()));
      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter((id) => allowedInsuranceIDs.includes(id));
      if (!validInsuranceIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
      baseMatch.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else if (InsuranceCompanyID?.length) {
      baseMatch.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
    }

    if (SPStateID && SPStateID !== '#ALL') {
      const requestedStateIDs = SPStateID.split(',').map((id) => id.trim());
      const validStateIDs = requestedStateIDs.filter((id) => StateMasterID.includes(id));
      if (!validStateIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
      baseMatch.FilterStateID = { $in: validStateIDs };
    } else if (StateMasterID?.length && LocationTypeID !== 2) {
      baseMatch.FilterStateID = { $in: StateMasterID };
    }

    const folderPath = path.join(process.cwd(), 'downloads');
    await fs.promises.mkdir(folderPath, { recursive: true });

    // ===== Filename =====
    const headerTypeMap: Record<number, string> = {
      1: 'Grievance',
      2: 'Information',
      4: 'Crop_Loss',
    };
    const ticketTypeName = headerTypeMap[SPTicketHeaderID] || 'General';
    const currentDateStr = new Date().toLocaleDateString('en-GB').split('/').join('_');

    const excelFileName = `Support_ticket_data_${ticketTypeName}_${currentDateStr}.xlsx`;
    const excelFilePath = path.join(folderPath, excelFileName);

    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
    const worksheet = workbook.addWorksheet('Support Tickets');

    await this.insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, "", "", this.db);

    const CHUNK_SIZE = 1000;

    const staticColumns = [
      { header: 'Agent ID', key: 'AgentID', width: 20 },
      { header: 'Calling ID', key: 'CallingUniqueID', width: 25 },
      { header: 'Ticket NCIP Docket No', key: 'TicketNCIPDocketNo', width: 25 },
      { header: 'Ticket No', key: 'SupportTicketNo', width: 30 },
      { header: 'Creation Date', key: 'Created', width: 25 },
      { header: 'Ticket ReOpen Date', key: 'TicketReOpenDate', width: 25 },
      { header: 'Ticket Status', key: 'TicketStatus', width: 20 },
      { header: 'Status Update Time', key: 'StatusUpdateTime', width: 25 },
      { header: 'State', key: 'StateMasterName', width: 20 },
      { header: 'District', key: 'DistrictMasterName', width: 20 },
      { header: 'Sub District', key: 'SubDistrictName', width: 20 },
      { header: 'Ticket Head', key: 'TicketHeadName', width: 20 },
      { header: 'Ticket Type', key: 'TicketTypeName', width: 20 },
      { header: 'Ticket Category', key: 'TicketCategoryName', width: 20 },
      { header: 'Crop Season', key: 'CropSeasonName', width: 20 },
      { header: 'Request Year', key: 'RequestYear', width: 20 },
      { header: 'Insurance Company', key: 'InsuranceCompany', width: 30 },
      { header: 'Application No', key: 'ApplicationNo', width: 30 },
      { header: 'Policy No', key: 'InsurancePolicyNo', width: 30 },
      { header: 'Caller Contact No', key: 'CallerContactNumber', width: 20 },
      { header: 'Requestor Name', key: 'RequestorName', width: 20 },
      { header: 'Requestor Mobile No', key: 'RequestorMobileNo', width: 20 },
      { header: 'Relation', key: 'Relation', width: 20 },
      { header: 'Relative Name', key: 'RelativeName', width: 20 },
      { header: 'Policy Premium', key: 'PolicyPremium', width: 20 },
      { header: 'Policy Area', key: 'PolicyArea', width: 20 },
      { header: 'Policy Type', key: 'PolicyType', width: 20 },
      { header: 'Land Survey No', key: 'LandSurveyNumber', width: 20 },
      { header: 'Land Division No', key: 'LandDivisionNumber', width: 20 },
      { header: 'Plot State', key: 'PlotStateName', width: 20 },
      { header: 'Plot District', key: 'PlotDistrictName', width: 20 },
      { header: 'Plot Village', key: 'PlotVillageName', width: 20 },
      { header: 'Application Source', key: 'ApplicationSource', width: 20 },
      { header: 'Crop Share', key: 'CropShare', width: 20 },
      { header: 'IFSC Code', key: 'IFSCCode', width: 20 },
      { header: 'Farmer Share', key: 'FarmerShare', width: 20 },
      { header: 'Sowing Date', key: 'SowingDate', width: 20 },
      { header: 'Created By', key: 'CreatedBY', width: 20 },
      { header: 'Ticket Description', key: 'TicketDescription', width: 50 },
    ];

    const dynamicColumns = new Set<string>();
    let headersSet = false;

    function formatToDDMMYYYY(dateString) {
      if (!dateString) return '';
      const date = new Date(dateString);
      if (isNaN(date.getTime())) return '';
      const day = String(date.getDate()).padStart(2, '0');
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const year = date.getFullYear();
      const hours = String(date.getHours()).padStart(2, '0');
      const minutes = String(date.getMinutes()).padStart(2, '0');
      return `${day}-${month}-${year} ${hours}:${minutes}`;
    }

    async function processDateWithChunking(currentDate: Date, endDate: Date) {
      if (currentDate > endDate) return;

      const startOfDay = new Date(currentDate);
      startOfDay.setUTCHours(0, 0, 0, 0);
      const endOfDay = new Date(currentDate);
      endOfDay.setUTCHours(23, 59, 59, 999);

      let skip = 0, hasMore = true;

      while (hasMore) {
        const dailyMatch = { ...baseMatch, InsertDateTime: { $gte: startOfDay, $lte: endOfDay } };
        const pipeline: any[] = [
          { $match: dailyMatch },
          {
            $lookup: {
              from: 'SLA_KRPH_SupportTicketsHistory_Records',
              let: { ticketId: '$SupportTicketID' },
              pipeline: [
                { $match: { $expr: { $and: [{ $eq: ['$SupportTicketID', '$$ticketId'] }, { $eq: ['$TicketStatusID', 109304] }] } } },
                { $sort: { TicketHistoryID: -1 } },
                { $limit: 1 }
              ],
              as: 'ticketHistory',
            }
          },
          {
            $lookup: {
              from: 'support_ticket_claim_intimation_report_history',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'claimInfo',
            }
          },
          {
            $lookup: {
              from: 'csc_agent_master',
              localField: 'InsertUserID',
              foreignField: 'UserLoginID',
              as: 'agentInfo',
            }
          },
          {
            $lookup: {
              from: 'ticket_comment_journey',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'ticket_comment_journey',
            },
          },
          { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
          { $skip: skip },
          { $limit: CHUNK_SIZE },
          { $addFields: { ticket_comment_journey: { $ifNull: ['$ticket_comment_journey', []] } } }
        ];

        const cursor = db.collection('SLA_KRPH_SupportTickets_Records').aggregate(pipeline, { allowDiskUse: true });
        const docs = await cursor.toArray();

        for (const doc of docs) {
          // --- dynamic comment handling ---
          if (Array.isArray(doc.ticket_comment_journey)) {
            const seen = new Set();
            let idx = 1;

            for (const c of doc.ticket_comment_journey) {
              const raw = (c.ResolvedComment || '').replace(/<\/?[^>]+>/g, '').trim();
              const date = formatToDDMMYYYY(c.ResolvedDate);
              const key = `${date}__${raw}`;
              if (!seen.has(key)) {
                doc[`Date ${idx}`] = date;
                doc[`Comment ${idx}`] = raw;
                dynamicColumns.add(`Date ${idx}`);
                dynamicColumns.add(`Comment ${idx}`);
                seen.add(key);
                idx++;
              }
            }
            delete doc.ticket_comment_journey;
          }

          if (!headersSet) {
            const dynamicDefs = Array.from(dynamicColumns).map(col => ({ header: col, key: col, width: 40 }));
            worksheet.columns = [...staticColumns, ...dynamicDefs];
            headersSet = true;
          }

          worksheet.addRow({
            AgentID: doc.agentInfo?.UserID?.toString() || '',
            CallingUniqueID: doc.CallingUniqueID || '',
            TicketNCIPDocketNo: doc.TicketNCIPDocketNo || '',
            SupportTicketNo: doc.SupportTicketNo?.toString() || '',
            Created: doc.Created ? new Date(doc.Created).toISOString() : '',
            TicketReOpenDate: doc.TicketReOpenDate || '',
            TicketStatus: doc.TicketStatus || '',
            StatusUpdateTime: doc.StatusUpdateTime ? new Date(doc.StatusUpdateTime).toISOString() : '',
            StateMasterName: doc.StateMasterName || '',
            DistrictMasterName: doc.DistrictMasterName || '',
            SubDistrictName: doc.SubDistrictName || '',
            TicketHeadName: doc.TicketHeadName || '',
            TicketTypeName: doc.TicketTypeName || '',
            TicketCategoryName: doc.TicketCategoryName || '',
            CropSeasonName: doc.CropSeasonName || '',
            RequestYear: doc.RequestYear || '',
            InsuranceCompany: doc.InsuranceCompany || '',
            ApplicationNo: doc.ApplicationNo || '',
            InsurancePolicyNo: doc.InsurancePolicyNo || '',
            CallerContactNumber: doc.CallerContactNumber || '',
            RequestorName: doc.RequestorName || '',
            RequestorMobileNo: doc.RequestorMobileNo || '',
            Relation: doc.Relation || '',
            RelativeName: doc.RelativeName || '',
            PolicyPremium: doc.PolicyPremium || '',
            PolicyArea: doc.PolicyArea || '',
            PolicyType: doc.PolicyType || '',
            LandSurveyNumber: doc.LandSurveyNumber || '',
            LandDivisionNumber: doc.LandDivisionNumber || '',
            PlotStateName: doc.PlotStateName || '',
            PlotDistrictName: doc.PlotDistrictName || '',
            PlotVillageName: doc.PlotVillageName || '',
            ApplicationSource: doc.ApplicationSource || '',
            CropShare: doc.CropShare || '',
            IFSCCode: doc.IFSCCode || '',
            FarmerShare: doc.FarmerShare || '',
            SowingDate: doc.SowingDate || '',
            CreatedBY: doc.CreatedBY || '',
            TicketDescription: doc.TicketDescription || '',
            ...Object.fromEntries(Object.entries(doc).filter(([k]) => k.startsWith('Date') || k.startsWith('Comment')))
          }).commit();
        }

        hasMore = docs.length === CHUNK_SIZE;
        skip += CHUNK_SIZE;
      }

      const nextDate = new Date(currentDate);
      nextDate.setDate(nextDate.getDate() + 1);
      await processDateWithChunking(nextDate, endDate);
    }

    await processDateWithChunking(new Date(SPFROMDATE), new Date(SPTODATE));
    await workbook.commit();
    console.log(`Excel file created at: ${excelFilePath}`);

    // ===== Create ZIP =====
    const zipFileName = excelFileName.replace('.xlsx', '.zip');
    const zipFilePath = path.join(folderPath, zipFileName);
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(output);
    archive.file(excelFilePath, { name: excelFileName });
    await archive.finalize();
    await new Promise<void>((resolve, reject) => {
      output.on('close', resolve);
      output.on('error', reject);
    });
    await fs.promises.unlink(excelFilePath).catch(console.error);

    // ===== Upload to GCP =====
    const gcpService = new GCPServices();
    const fileBuffer = await fs.promises.readFile(zipFilePath);
    const uploadResult = await gcpService.uploadFileToGCP({
      filePath: 'krph/reports/',
      uploadedBy: 'KRPH',
      file: { buffer: fileBuffer, originalname: zipFileName },
    });
    const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
    if (gcpDownloadUrl) await fs.promises.unlink(zipFilePath).catch(console.error);

    await this.insertOrUpdateDownloadLog(
      SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID,
      SPFROMDATE, SPTODATE, zipFileName, gcpDownloadUrl, this.db
    );

    const responsePayload = {
      data: [],
      pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
      downloadUrl: gcpDownloadUrl,
      zipFileName: zipFileName
    };

    const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);
    try {
      await this.mailService.sendMail({
        to: userEmail,
        subject: 'Support Ticket History Report Download Service',
        text: 'Support Ticket History Report',
        html: supportTicketTemplate
      });
      console.log("Mail sent successfully");
    } catch (err) {
      console.error(`Failed to send email to ${userEmail}:`, err);
    }

    await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);

    // return responsePayload;
  }


  async processTicketHistoryAndGenerateZip(ticketPayload: any) {
    let {
      SPFROMDATE,
      SPTODATE,
      SPInsuranceCompanyID,
      SPStateID,
      SPTicketHeaderID,
      SPUserID,
      page = 1,
      limit = 1000000000,
      userEmail,
    } = ticketPayload;

    const db = this.db;
    SPTicketHeaderID = Number(SPTicketHeaderID);

    if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
    if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };

    const RequestDateTime = await getCurrentFormattedDateTime();
    const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;
    if (cachedData) {
      console.log('Using cached data');
      await this.db.collection('support_ticket_download_logs').updateOne(
        { SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE },
        {
          $set: {
            downloadUrl: cachedData.downloadUrl || '',
            zipFileName: cachedData.zipFileName || '',
            updatedAt: new Date()
          },
          $setOnInsert: { createdAt: new Date() }
        },
        { upsert: true }
      );
      return cachedData;
    }

    // ===== User detail auth =====
    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];
    if (!item) return { rcode: 0, rmessage: 'User details not found.' };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };
    const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length)
      locationFilter = { FilterStateID: { $in: StateMasterID } };
    else if (LocationTypeID === 2 && item.DistrictIDs?.length)
      locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };

    const baseMatch: any = { ...locationFilter };
    if (SPTicketHeaderID && SPTicketHeaderID !== 0) baseMatch.TicketHeaderID = SPTicketHeaderID;

    if (SPInsuranceCompanyID && SPInsuranceCompanyID !== '#ALL') {
      const requestedInsuranceIDs = SPInsuranceCompanyID.split(',').map((id) => Number(id.trim()));
      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter((id) => allowedInsuranceIDs.includes(id));
      if (!validInsuranceIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
      baseMatch.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else if (InsuranceCompanyID?.length) {
      baseMatch.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
    }

    if (SPStateID && SPStateID !== '#ALL') {
      const requestedStateIDs = SPStateID.split(',').map((id) => id.trim());
      const validStateIDs = requestedStateIDs.filter((id) => StateMasterID.includes(id));
      if (!validStateIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
      baseMatch.FilterStateID = { $in: validStateIDs };
    } else if (StateMasterID?.length && LocationTypeID !== 2) {
      baseMatch.FilterStateID = { $in: StateMasterID };
    }

    const folderPath = path.join(process.cwd(), 'downloads');
    await fs.promises.mkdir(folderPath, { recursive: true });

    // ===== Filename =====
    const headerTypeMap: Record<number, string> = {
      1: 'Grievance',
      2: 'Information',
      4: 'Crop_Loss',
    };
    const ticketTypeName = headerTypeMap[SPTicketHeaderID] || 'General';
    const currentDateStr = new Date().toLocaleDateString('en-GB').split('/').join('_');

    const excelFileName = `Support_ticket_data_${ticketTypeName}_${currentDateStr}.xlsx`;
    const excelFilePath = path.join(folderPath, excelFileName);

    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
    const worksheet = workbook.addWorksheet('Support Tickets');

    await this.insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, "", "", this.db);

    const CHUNK_SIZE = 1000;

    const staticColumns = [
      { header: 'Agent ID', key: 'AgentID', width: 20 },
      { header: 'Calling ID', key: 'CallingUniqueID', width: 25 },
      { header: 'Ticket NCIP Docket No', key: 'TicketNCIPDocketNo', width: 25 },
      { header: 'Ticket No', key: 'SupportTicketNo', width: 30 },
      { header: 'Creation Date', key: 'Created', width: 25 },
      { header: 'Ticket ReOpen Date', key: 'TicketReOpenDate', width: 25 },
      { header: 'Ticket Status', key: 'TicketStatus', width: 20 },
      { header: 'Status Update Time', key: 'StatusUpdateTime', width: 25 },
      { header: 'State', key: 'StateMasterName', width: 20 },
      { header: 'District', key: 'DistrictMasterName', width: 20 },
      { header: 'Sub District', key: 'SubDistrictName', width: 20 },
      { header: 'Ticket Head', key: 'TicketHeadName', width: 20 },
      { header: 'Ticket Type', key: 'TicketTypeName', width: 20 },
      { header: 'Ticket Category', key: 'TicketCategoryName', width: 20 },
      { header: 'Crop Season', key: 'CropSeasonName', width: 20 },
      { header: 'Request Year', key: 'RequestYear', width: 20 },
      { header: 'Insurance Company', key: 'InsuranceCompany', width: 30 },
      { header: 'Application No', key: 'ApplicationNo', width: 30 },
      { header: 'Policy No', key: 'InsurancePolicyNo', width: 30 },
      { header: 'Caller Contact No', key: 'CallerContactNumber', width: 20 },
      { header: 'Requestor Name', key: 'RequestorName', width: 20 },
      { header: 'Requestor Mobile No', key: 'RequestorMobileNo', width: 20 },
      { header: 'Relation', key: 'Relation', width: 20 },
      { header: 'Relative Name', key: 'RelativeName', width: 20 },
      { header: 'Policy Premium', key: 'PolicyPremium', width: 20 },
      { header: 'Policy Area', key: 'PolicyArea', width: 20 },
      { header: 'Policy Type', key: 'PolicyType', width: 20 },
      { header: 'Land Survey No', key: 'LandSurveyNumber', width: 20 },
      { header: 'Land Division No', key: 'LandDivisionNumber', width: 20 },
      { header: 'Plot State', key: 'PlotStateName', width: 20 },
      { header: 'Plot District', key: 'PlotDistrictName', width: 20 },
      { header: 'Plot Village', key: 'PlotVillageName', width: 20 },
      { header: 'Application Source', key: 'ApplicationSource', width: 20 },
      { header: 'Crop Share', key: 'CropShare', width: 20 },
      { header: 'IFSC Code', key: 'IFSCCode', width: 20 },
      { header: 'Farmer Share', key: 'FarmerShare', width: 20 },
      { header: 'Sowing Date', key: 'SowingDate', width: 20 },
      { header: 'Created By', key: 'CreatedBY', width: 20 },
      { header: 'Ticket Description', key: 'TicketDescription', width: 50 },
    ];

    worksheet.columns = staticColumns;

    function formatToDDMMYYYY(dateString) {
      if (!dateString) return '';
      const date = new Date(dateString);
      if (isNaN(date.getTime())) return '';
      const day = String(date.getDate()).padStart(2, '0');
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const year = date.getFullYear();
      const hours = String(date.getHours()).padStart(2, '0');
      const minutes = String(date.getMinutes()).padStart(2, '0');
      return `${day}-${month}-${year} ${hours}:${minutes}`;
    }

    async function processDateWithChunking(currentDate: Date, endDate: Date) {
      if (currentDate > endDate) return;

      const startOfDay = new Date(currentDate);
      startOfDay.setUTCHours(0, 0, 0, 0);
      const endOfDay = new Date(currentDate);
      endOfDay.setUTCHours(23, 59, 59, 999);

      let skip = 0, hasMore = true;

      while (hasMore) {
        const dailyMatch = { ...baseMatch, InsertDateTime: { $gte: startOfDay, $lte: endOfDay } };
        const pipeline: any[] = [
          { $match: dailyMatch },
          {
            $lookup: {
              from: 'SLA_KRPH_SupportTicketsHistory_Records',
              let: { ticketId: '$SupportTicketID' },
              pipeline: [
                { $match: { $expr: { $and: [{ $eq: ['$SupportTicketID', '$$ticketId'] }, { $eq: ['$TicketStatusID', 109304] }] } } },
                { $sort: { TicketHistoryID: -1 } },
                { $limit: 1 }
              ],
              as: 'ticketHistory',
            }
          },
          {
            $lookup: {
              from: 'support_ticket_claim_intimation_report_history',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'claimInfo',
            }
          },
          {
            $lookup: {
              from: 'csc_agent_master',
              localField: 'InsertUserID',
              foreignField: 'UserLoginID',
              as: 'agentInfo',
            }
          },
          {
            $lookup: {
              from: 'ticket_comment_journey',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'ticket_comment_journey',
            },
          },
          { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
          { $skip: skip },
          { $limit: CHUNK_SIZE },
          { $addFields: { ticket_comment_journey: { $ifNull: ['$ticket_comment_journey', []] } } }
        ];

        const cursor = db.collection('SLA_KRPH_SupportTickets_Records').aggregate(pipeline, { allowDiskUse: true });
        const docs = await cursor.toArray();

        for (const doc of docs) {
          const dynamicColumnsBatch: any = {};
          if (Array.isArray(doc.ticket_comment_journey)) {
            const seen = new Set();
            let idx = 1;
            for (const c of doc.ticket_comment_journey) {
              const raw = (c.ResolvedComment || '').replace(/<\/?[^>]+>/g, '').trim();
              const date = formatToDDMMYYYY(c.ResolvedDate);
              const key = `${date}__${raw}`;
              if (!seen.has(key)) {
                dynamicColumnsBatch[`Date ${idx}`] = date;
                dynamicColumnsBatch[`Comment ${idx}`] = raw;
                seen.add(key);
                idx++;
              }
            }
          }

          worksheet.addRow({
            AgentID: doc.agentInfo?.UserID?.toString() || '',
            CallingUniqueID: doc.CallingUniqueID || '',
            TicketNCIPDocketNo: doc.TicketNCIPDocketNo || '',
            SupportTicketNo: doc.SupportTicketNo?.toString() || '',
            Created: doc.Created ? new Date(doc.Created).toISOString() : '',
            TicketReOpenDate: doc.TicketReOpenDate || '',
            TicketStatus: doc.TicketStatus || '',
            StatusUpdateTime: doc.StatusUpdateTime ? new Date(doc.StatusUpdateTime).toISOString() : '',
            StateMasterName: doc.StateMasterName || '',
            DistrictMasterName: doc.DistrictMasterName || '',
            SubDistrictName: doc.SubDistrictName || '',
            TicketHeadName: doc.TicketHeadName || '',
            TicketTypeName: doc.TicketTypeName || '',
            TicketCategoryName: doc.TicketCategoryName || '',
            CropSeasonName: doc.CropSeasonName || '',
            RequestYear: doc.RequestYear || '',
            InsuranceCompany: doc.InsuranceCompany || '',
            ApplicationNo: doc.ApplicationNo || '',
            InsurancePolicyNo: doc.InsurancePolicyNo || '',
            CallerContactNumber: doc.CallerContactNumber || '',
            RequestorName: doc.RequestorName || '',
            RequestorMobileNo: doc.RequestorMobileNo || '',
            Relation: doc.Relation || '',
            RelativeName: doc.RelativeName || '',
            PolicyPremium: doc.PolicyPremium || '',
            PolicyArea: doc.PolicyArea || '',
            PolicyType: doc.PolicyType || '',
            LandSurveyNumber: doc.LandSurveyNumber || '',
            LandDivisionNumber: doc.LandDivisionNumber || '',
            PlotStateName: doc.PlotStateName || '',
            PlotDistrictName: doc.PlotDistrictName || '',
            PlotVillageName: doc.PlotVillageName || '',
            ApplicationSource: doc.ApplicationSource || '',
            CropShare: doc.CropShare || '',
            IFSCCode: doc.IFSCCode || '',
            FarmerShare: doc.FarmerShare || '',
            SowingDate: doc.SowingDate || '',
            CreatedBY: doc.CreatedBY || '',
            TicketDescription: doc.TicketDescription || '',
            ...dynamicColumnsBatch
          }).commit();
        }

        hasMore = docs.length === CHUNK_SIZE;
        skip += CHUNK_SIZE;
      }

      const nextDate = new Date(currentDate);
      nextDate.setDate(nextDate.getDate() + 1);
      await processDateWithChunking(nextDate, endDate);
    }

    await processDateWithChunking(new Date(SPFROMDATE), new Date(SPTODATE));
    await workbook.commit();
    console.log(`Excel file created at: ${excelFilePath}`);

    // ===== Create ZIP =====
    const zipFileName = excelFileName.replace('.xlsx', '.zip');
    const zipFilePath = path.join(folderPath, zipFileName);
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(output);
    archive.file(excelFilePath, { name: excelFileName });
    await archive.finalize();
    await new Promise<void>((resolve, reject) => {
      output.on('close', resolve);
      output.on('error', reject);
    });
    await fs.promises.unlink(excelFilePath).catch(console.error);

    // ===== Upload to GCP =====
    const gcpService = new GCPServices();
    const fileBuffer = await fs.promises.readFile(zipFilePath);
    const uploadResult = await gcpService.uploadFileToGCP({
      filePath: 'krph/reports/',
      uploadedBy: 'KRPH',
      file: { buffer: fileBuffer, originalname: zipFileName },
    });
    const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
    if (gcpDownloadUrl) await fs.promises.unlink(zipFilePath).catch(console.error);

    await this.insertOrUpdateDownloadLog(
      SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID,
      SPFROMDATE, SPTODATE, zipFileName, gcpDownloadUrl, this.db
    );

    const responsePayload = {
      data: [],
      pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
      downloadUrl: gcpDownloadUrl,
      zipFileName: zipFileName
    };

    const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);
    try {
      await this.mailService.sendMail({
        to: userEmail,
        subject: 'Support Ticket History Report Download Service',
        text: 'Support Ticket History Report',
        html: supportTicketTemplate
      });
      console.log("Mail sent successfully");
    } catch (err) {
      console.error(`Failed to send email to ${userEmail}:`, err);
    }

    await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);
  }




  async processTicketHistoryAndGenerateZipWithlogDetails(ticketPayload: any) {
    let {
      SPFROMDATE,
      SPTODATE,
      SPInsuranceCompanyID,
      SPStateID,
      SPTicketHeaderID,
      SPUserID,
      page = 1,
      limit = 1000000000,
      userEmail,
    } = ticketPayload;

    const db = this.db;
    SPTicketHeaderID = Number(SPTicketHeaderID);

    if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
    if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };

    const RequestDateTime = await getCurrentFormattedDateTime();
    const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;

    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];
    if (!item) return { rcode: 0, rmessage: 'User details not found.' };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };
    const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length)
      locationFilter = { FilterStateID: { $in: StateMasterID } };
    else if (LocationTypeID === 2 && item.DistrictIDs?.length)
      locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };

    const baseMatch: any = { ...locationFilter };
    if (SPTicketHeaderID && SPTicketHeaderID !== 0) baseMatch.TicketHeaderID = SPTicketHeaderID;

    if (SPInsuranceCompanyID && SPInsuranceCompanyID !== '#ALL') {
      const requestedInsuranceIDs = SPInsuranceCompanyID.split(',').map((id) => Number(id.trim()));
      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter((id) => allowedInsuranceIDs.includes(id));
      if (!validInsuranceIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
      baseMatch.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else if (InsuranceCompanyID?.length)
      baseMatch.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };

    if (SPStateID && SPStateID !== '#ALL') {
      const requestedStateIDs = SPStateID.split(',').map((id) => id.trim());
      const validStateIDs = requestedStateIDs.filter((id) => StateMasterID.includes(id));
      if (!validStateIDs.length)
        return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
      baseMatch.FilterStateID = { $in: validStateIDs };
    } else if (StateMasterID?.length && LocationTypeID !== 2)
      baseMatch.FilterStateID = { $in: StateMasterID };

    const folderPath = path.join(process.cwd(), 'downloads');
    await fs.promises.mkdir(folderPath, { recursive: true });

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Support Tickets');

    worksheet.columns = [
      { header: 'Agent ID', key: 'AgentID', width: 20 },
      { header: 'Calling ID', key: 'CallingUniqueID', width: 25 },
      { header: 'NCIP Docket No', key: 'TicketNCIPDocketNo', width: 30 },
      { header: 'Ticket No', key: 'SupportTicketNo', width: 30 },
      { header: 'Creation Date', key: 'Created', width: 25 },
      { header: 'Re-Open Date', key: 'TicketReOpenDate', width: 25 },
      { header: 'Ticket Status', key: 'TicketStatus', width: 20 },
      { header: 'Status Date', key: 'StatusUpdateTime', width: 25 },
      { header: 'State', key: 'StateMasterName', width: 20 },
      { header: 'District', key: 'DistrictMasterName', width: 20 },
      { header: 'Sub District', key: 'SubDistrictName', width: 20 },
      { header: 'Type', key: 'TicketHeadName', width: 20 },
      { header: 'Category', key: 'TicketTypeName', width: 20 },
      { header: 'Sub Category', key: 'TicketCategoryName', width: 20 },
      { header: 'Season', key: 'CropSeasonName', width: 15 },
      { header: 'Year', key: 'RequestYear', width: 10 },
      { header: 'Insurance Company', key: 'InsuranceCompany', width: 30 },
      { header: 'Application No', key: 'ApplicationNo', width: 25 },
      { header: 'Policy No', key: 'InsurancePolicyNo', width: 25 },
      { header: 'Caller Mobile No', key: 'CallerContactNumber', width: 20 },
      { header: 'Farmer Name', key: 'RequestorName', width: 25 },
      { header: 'Mobile No', key: 'RequestorMobileNo', width: 20 },
      { header: 'Relation', key: 'Relation', width: 15 },
      { header: 'Relative Name', key: 'RelativeName', width: 25 },
      { header: 'Policy Premium', key: 'PolicyPremium', width: 15 },
      { header: 'Policy Area', key: 'PolicyArea', width: 15 },
      { header: 'Policy Type', key: 'PolicyType', width: 20 },
      { header: 'Land Survey Number', key: 'LandSurveyNumber', width: 25 },
      { header: 'Land Division Number', key: 'LandDivisionNumber', width: 25 },
      { header: 'Plot State', key: 'PlotStateName', width: 20 },
      { header: 'Plot District', key: 'PlotDistrictName', width: 20 },
      { header: 'Plot Village', key: 'PlotVillageName', width: 25 },
      { header: 'Application Source', key: 'ApplicationSource', width: 20 },
      { header: 'Crop Share', key: 'CropShare', width: 15 },
      { header: 'IFSC Code', key: 'IFSCCode', width: 20 },
      { header: 'Farmer Share', key: 'FarmerShare', width: 15 },
      { header: 'Sowing Date', key: 'SowingDate', width: 20 },
      { header: 'Created By', key: 'CreatedBY', width: 20 },
      { header: 'Description', key: 'TicketDescription', width: 50 },
    ];

    await this.insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, "", "", this.db);

    const CHUNK_SIZE = 10000;
    let dataToProcess: any[] = [];

    async function processDateWithChunking(currentDate: Date, endDate: Date) {
      if (currentDate > endDate) return;

      const startOfDay = new Date(currentDate);
      startOfDay.setUTCHours(0, 0, 0, 0);
      const endOfDay = new Date(currentDate);
      endOfDay.setUTCHours(23, 59, 59, 999);

      let skip = 0;
      let hasMore = true;

      while (hasMore) {
        const dailyMatch = { ...baseMatch, InsertDateTime: { $gte: startOfDay, $lte: endOfDay } };

        const pipeline: any[] = [
          { $match: dailyMatch },
          {
            $lookup: {
              from: 'SLA_KRPH_SupportTicketsHistory_Records',
              let: { ticketId: '$SupportTicketID' },
              pipeline: [
                { $match: { $expr: { $and: [{ $eq: ['$SupportTicketID', '$$ticketId'] }, { $eq: ['$TicketStatusID', 109304] }] } } },
                { $sort: { TicketHistoryID: -1 } },
                { $limit: 1 }
              ],
              as: 'ticketHistory',
            }
          },
          {
            $lookup: {
              from: 'support_ticket_claim_intimation_report_history',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'claimInfo',
            }
          },
          {
            $lookup: {
              from: 'csc_agent_master',
              localField: 'InsertUserID',
              foreignField: 'UserLoginID',
              as: 'agentInfo',
            }
          },
          {
            $lookup: {
              from: 'ticket_comment_journey',
              localField: 'SupportTicketNo',
              foreignField: 'SupportTicketNo',
              as: 'ticket_comment_journey',
            }
          },
          { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
          { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
          { $skip: skip },
          { $limit: CHUNK_SIZE },
          {
            $project: {
              agentInfo: 1,
              TicketComments: {
                $arrayToObject: {
                  $map: {
                    input: '$ticket_comment_journey',
                    as: 'comment',
                    in: {
                      k: {
                        $concat: [
                          'Comment (',
                          {
                            $dateToString: {
                              format: '%Y-%m-%d',
                              date: '$$comment.ResolvedDate',
                            },
                          },
                          ')',
                        ],
                      },
                      v: '$$comment.ResolvedComment',
                    },
                  },
                },
              },
              CallingUniqueID: 1,
              TicketNCIPDocketNo: 1,
              SupportTicketNo: 1,
              Created: 1,
              TicketReOpenDate: 1,
              TicketStatus: 1,
              StatusUpdateTime: 1,
              StateMasterName: 1,
              DistrictMasterName: 1,
              SubDistrictName: 1,
              TicketHeadName: 1,
              TicketTypeName: 1,
              TicketCategoryName: 1,
              CropSeasonName: 1,
              RequestYear: 1,
              InsuranceCompany: 1,
              ApplicationNo: 1,
              InsurancePolicyNo: 1,
              CallerContactNumber: 1,
              RequestorName: 1,
              RequestorMobileNo: 1,
              Relation: 1,
              RelativeName: 1,
              PolicyPremium: 1,
              PolicyArea: 1,
              PolicyType: 1,
              LandSurveyNumber: 1,
              LandDivisionNumber: 1,
              PlotStateName: 1,
              PlotDistrictName: 1,
              PlotVillageName: 1,
              ApplicationSource: 1,
              CropShare: 1,
              IFSCCode: 1,
              FarmerShare: 1,
              SowingDate: 1,
              CreatedBY: 1,
              TicketDescription: 1
            }
          },
          {
            $project: {
              "Agent ID": "$AgentID",
              "Calling ID": "$CallingUniqueID",
              "NCIP Docket No": "$TicketNCIPDocketNo",
              "Ticket No": "$SupportTicketNo",
              "Creation Date": "$Created",
              "Re-Open Date": "$TicketReOpenDate",
              "Ticket Status": "$TicketStatus",
              "Status Date": "$StatusUpdateTime",
              "State": "$StateMasterName",
              "District": "$DistrictMasterName",
              "Sub District": "$SubDistrictName",
              "Type": "$TicketHeadName",
              "Category": "$TicketTypeName",
              "Sub Category": "$TicketCategoryName",
              "Season": "$CropSeasonName",
              "Year": "$RequestYear",
              "Insurance Company": "$InsuranceCompany",
              "Application No": "$ApplicationNo",
              "Policy No": "$InsurancePolicyNo",
              "Caller Mobile No": "$CallerContactNumber",
              "Farmer Name": "$RequestorName",
              "Mobile No": "$RequestorMobileNo",
              "Relation": "$Relation",
              "Relative Name": "$RelativeName",
              "Policy Premium": "$PolicyPremium",
              "Policy Area": "$PolicyArea",
              "Policy Type": "$PolicyType",
              "Land Survey Number": "$LandSurveyNumber",
              "Land Division Number": "$LandDivisionNumber",
              "Plot State": "$PlotStateName",
              "Plot District": "$PlotDistrictName",
              "Plot Village": "$PlotVillageName",
              "Application Source": "$ApplicationSource",
              "Crop Share": "$CropShare",
              "IFSC Code": "$IFSCCode",
              "Farmer Share": "$FarmerShare",
              "Sowing Date": "$SowingDate",
              "Created By": "$CreatedBY",
              "Description": "$TicketDescription",
              "TicketComments": "$TicketComments"
            }
          }
        ];

        const docs = await db.collection('SLA_KRPH_SupportTickets_Records')
          .aggregate(pipeline, { allowDiskUse: true })
          .toArray();

        docs.forEach(doc => {
          if (doc.TicketComments) {
            for (const [key, value] of Object.entries(doc.TicketComments)) {
              doc[key] = value;
            }
            delete doc.TicketComments;
          }
        });

        if (docs.length === 0) {
          hasMore = false;
        } else {
          docs.forEach(doc => {
            worksheet.addRow({
              AgentID: doc.agentInfo?.UserID?.toString() || '',
              CallingUniqueID: doc.CallingUniqueID || '',
              TicketNCIPDocketNo: doc.TicketNCIPDocketNo || '',
              SupportTicketNo: doc.SupportTicketNo?.toString() || '',
              Created: doc.Created ? new Date(doc.Created).toISOString() : '',
              TicketReOpenDate: doc.TicketReOpenDate || '',
              TicketStatus: doc.TicketStatus || '',
              StatusUpdateTime: doc.StatusUpdateTime ? new Date(doc.StatusUpdateTime).toISOString() : '',
              StateMasterName: doc.StateMasterName || '',
              DistrictMasterName: doc.DistrictMasterName || '',
              SubDistrictName: doc.SubDistrictName || '',
              TicketHeadName: doc.TicketHeadName || '',
              TicketTypeName: doc.TicketTypeName || '',
              TicketCategoryName: doc.TicketCategoryName || '',
              CropSeasonName: doc.CropSeasonName || '',
              RequestYear: doc.RequestYear || '',
              InsuranceCompany: doc.InsuranceCompany || '',
              ApplicationNo: doc.ApplicationNo || '',
              InsurancePolicyNo: doc.InsurancePolicyNo || '',
              CallerContactNumber: doc.CallerContactNumber || '',
              RequestorName: doc.RequestorName || '',
              RequestorMobileNo: doc.RequestorMobileNo || '',
              Relation: doc.Relation || '',
              RelativeName: doc.RelativeName || '',
              PolicyPremium: doc.PolicyPremium || '',
              PolicyArea: doc.PolicyArea || '',
              PolicyType: doc.PolicyType || '',
              LandSurveyNumber: doc.LandSurveyNumber || '',
              LandDivisionNumber: doc.LandDivisionNumber || '',
              PlotStateName: doc.PlotStateName || '',
              PlotDistrictName: doc.PlotDistrictName || '',
              PlotVillageName: doc.PlotVillageName || '',
              ApplicationSource: doc.ApplicationSource || '',
              CropShare: doc.CropShare || '',
              IFSCCode: doc.IFSCCode || '',
              FarmerShare: doc.FarmerShare || '',
              SowingDate: doc.SowingDate || '',
              CreatedBY: doc.CreatedBY || '',
              TicketDescription: doc.TicketDescription || ''
            });

            dataToProcess.push(doc);
          });

          skip += CHUNK_SIZE;
        }
      }

      const nextDate = new Date(currentDate);
      nextDate.setDate(nextDate.getDate() + 1);
      await processDateWithChunking(nextDate, endDate);
    }

    if (!cachedData) {
      await processDateWithChunking(new Date(SPFROMDATE), new Date(SPTODATE));
    } else {
      dataToProcess = cachedData.data;
    }

    const excelFileName = `support_ticket_data_${Date.now()}.xlsx`;
    const excelFilePath = path.join(folderPath, excelFileName);
    await workbook.xlsx.writeFile(excelFilePath);

    const zipFileName = excelFileName.replace('.xlsx', '.zip');
    const zipFilePath = path.join(folderPath, zipFileName);
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(output);
    archive.file(excelFilePath, { name: excelFileName });
    await archive.finalize();
    await new Promise<void>((resolve, reject) => {
      output.on('close', resolve);
      output.on('error', reject);
    });
    await fs.promises.unlink(excelFilePath).catch(console.error);

    const gcpService = new GCPServices();
    const fileBuffer = await fs.promises.readFile(zipFilePath);
    const uploadResult = await gcpService.uploadFileToGCP({
      filePath: 'krph/reports/',
      uploadedBy: 'KRPH',
      file: { buffer: fileBuffer, originalname: zipFileName },
    });
    const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
    if (gcpDownloadUrl) await fs.promises.unlink(zipFilePath).catch(console.error);

    await this.insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, zipFileName, gcpDownloadUrl, this.db);

    const responsePayload = {
      data: dataToProcess,
      pagination: { total: dataToProcess.length, page, limit, totalPages: 1, hasNextPage: false, hasPrevPage: false },
      downloadUrl: gcpDownloadUrl
    };

    await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);

    return responsePayload;
  }


  async insertOrUpdateDownloadLog(
    userId,
    insuranceCompanyId,
    stateId,
    ticketHeaderId,
    fromDate,
    toDate,
    zipFileName,
    downloadUrl,
    db
  ) {
    await db.collection('support_ticket_download_logs').updateOne(
      {
        userId,
        insuranceCompanyId,
        stateId,
        ticketHeaderId,
        fromDate,
        toDate
      },
      {
        $set: {
          zipFileName,
          downloadUrl,
          createdAt: new Date()
        }
      },
      { upsert: true } // Insert if not found, update if exists
    );
  }


  // async processTicketHistoryAndGenerateZip(ticketPayload: any) {
  //   let {
  //     SPFROMDATE,
  //     SPTODATE,
  //     SPInsuranceCompanyID,
  //     SPStateID,
  //     SPTicketHeaderID,
  //     SPUserID,
  //     page = 1,
  //     limit = 1000000000,
  //     userEmail,
  //   } = ticketPayload;

  //   const db = this.db;
  //   SPTicketHeaderID = Number(SPTicketHeaderID);

  //   if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
  //   if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };

  //   const RequestDateTime = await getCurrentFormattedDateTime();
  //   const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
  //   const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;

  //   const logId = await this.insertOrGetDownloadLog(
  //     SPUserID,
  //     SPInsuranceCompanyID,
  //     SPStateID,
  //     SPTicketHeaderID,
  //     SPFROMDATE,
  //     SPTODATE,
  //     this.db
  //   );

  //   const Delta = await this.getSupportTicketUserDetail(SPUserID);
  //   const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
  //   const item = (responseInfo.data as any)?.user?.[0];
  //   if (!item) return { rcode: 0, rmessage: 'User details not found.' };

  //   const userDetail = {
  //     InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
  //     StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
  //     BRHeadTypeID: item.BRHeadTypeID,
  //     LocationTypeID: item.LocationTypeID,
  //   };
  //   const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

  //   let locationFilter: any = {};
  //   if (LocationTypeID === 1 && StateMasterID?.length)
  //     locationFilter = { FilterStateID: { $in: StateMasterID } };
  //   else if (LocationTypeID === 2 && item.DistrictIDs?.length)
  //     locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };

  //   const baseMatch: any = { ...locationFilter };
  //   if (SPTicketHeaderID && SPTicketHeaderID !== 0) baseMatch.TicketHeaderID = SPTicketHeaderID;

  //   if (SPInsuranceCompanyID && SPInsuranceCompanyID !== '#ALL') {
  //     const requestedInsuranceIDs = SPInsuranceCompanyID.split(',').map((id) => Number(id.trim()));
  //     const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
  //     const validInsuranceIDs = requestedInsuranceIDs.filter((id) => allowedInsuranceIDs.includes(id));
  //     if (!validInsuranceIDs.length)
  //       return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
  //     baseMatch.InsuranceCompanyID = { $in: validInsuranceIDs };
  //   } else if (InsuranceCompanyID?.length)
  //     baseMatch.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };

  //   if (SPStateID && SPStateID !== '#ALL') {
  //     const requestedStateIDs = SPStateID.split(',').map((id) => id.trim());
  //     const validStateIDs = requestedStateIDs.filter((id) => StateMasterID.includes(id));
  //     if (!validStateIDs.length)
  //       return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
  //     baseMatch.FilterStateID = { $in: validStateIDs };
  //   } else if (StateMasterID?.length && LocationTypeID !== 2)
  //     baseMatch.FilterStateID = { $in: StateMasterID };

  //   const folderPath = path.join(process.cwd(), 'downloads');
  //   await fs.promises.mkdir(folderPath, { recursive: true });

  //   const workbook = new ExcelJS.Workbook();
  //   const worksheet = workbook.addWorksheet('Support Tickets');

  //   worksheet.columns = [
  //     { header: 'Agent ID', key: 'AgentID', width: 20 },
  //     { header: 'Calling ID', key: 'CallingUniqueID', width: 25 },
  //     { header: 'NCIP Docket No', key: 'TicketNCIPDocketNo', width: 30 },
  //     { header: 'Ticket No', key: 'SupportTicketNo', width: 30 },
  //     { header: 'Creation Date', key: 'Created', width: 25 },
  //     { header: 'Re-Open Date', key: 'TicketReOpenDate', width: 25 },
  //     { header: 'Ticket Status', key: 'TicketStatus', width: 20 },
  //     { header: 'Status Date', key: 'StatusUpdateTime', width: 25 },
  //     { header: 'State', key: 'StateMasterName', width: 20 },
  //     { header: 'District', key: 'DistrictMasterName', width: 20 },
  //     { header: 'Sub District', key: 'SubDistrictName', width: 20 },
  //     { header: 'Type', key: 'TicketHeadName', width: 20 },
  //     { header: 'Category', key: 'TicketTypeName', width: 20 },
  //     { header: 'Sub Category', key: 'TicketCategoryName', width: 20 },
  //     { header: 'Season', key: 'CropSeasonName', width: 15 },
  //     { header: 'Year', key: 'RequestYear', width: 10 },
  //     { header: 'Insurance Company', key: 'InsuranceCompany', width: 30 },
  //     { header: 'Application No', key: 'ApplicationNo', width: 25 },
  //     { header: 'Policy No', key: 'InsurancePolicyNo', width: 25 },
  //     { header: 'Caller Mobile No', key: 'CallerContactNumber', width: 20 },
  //     { header: 'Farmer Name', key: 'RequestorName', width: 25 },
  //     { header: 'Mobile No', key: 'RequestorMobileNo', width: 20 },
  //     { header: 'Relation', key: 'Relation', width: 15 },
  //     { header: 'Relative Name', key: 'RelativeName', width: 25 },
  //     { header: 'Policy Premium', key: 'PolicyPremium', width: 15 },
  //     { header: 'Policy Area', key: 'PolicyArea', width: 15 },
  //     { header: 'Policy Type', key: 'PolicyType', width: 20 },
  //     { header: 'Land Survey Number', key: 'LandSurveyNumber', width: 25 },
  //     { header: 'Land Division Number', key: 'LandDivisionNumber', width: 25 },
  //     { header: 'Plot State', key: 'PlotStateName', width: 20 },
  //     { header: 'Plot District', key: 'PlotDistrictName', width: 20 },
  //     { header: 'Plot Village', key: 'PlotVillageName', width: 25 },
  //     { header: 'Application Source', key: 'ApplicationSource', width: 20 },
  //     { header: 'Crop Share', key: 'CropShare', width: 15 },
  //     { header: 'IFSC Code', key: 'IFSCCode', width: 20 },
  //     { header: 'Farmer Share', key: 'FarmerShare', width: 15 },
  //     { header: 'Sowing Date', key: 'SowingDate', width: 20 },
  //     { header: 'Created By', key: 'CreatedBY', width: 20 },
  //     { header: 'Description', key: 'TicketDescription', width: 50 },
  //   ];

  //   const CHUNK_SIZE = 10000;
  //   const dataToProcess = [];

  //   async function processDateWithChunking(currentDate: Date, endDate: Date) {
  //     if (currentDate > endDate) return;

  //     const startOfDay = new Date(currentDate);
  //     startOfDay.setUTCHours(0, 0, 0, 0);
  //     const endOfDay = new Date(currentDate);
  //     endOfDay.setUTCHours(23, 59, 59, 999);

  //     let skip = 0;
  //     let hasMore = true;

  //     while (hasMore) {
  //       const dailyMatch = { ...baseMatch, InsertDateTime: { $gte: startOfDay, $lte: endOfDay } };

  //       const pipeline = [
  //         { $match: dailyMatch },
  //         {
  //           $lookup: {
  //             from: 'SLA_KRPH_SupportTicketsHistory_Records',
  //             let: { ticketId: '$SupportTicketID' },
  //             pipeline: [
  //               { $match: { $expr: { $and: [{ $eq: ['$SupportTicketID', '$$ticketId'] }, { $eq: ['$TicketStatusID', 109304] }] } } },
  //               { $sort: { TicketHistoryID: -1 } },
  //               { $limit: 1 }
  //             ],
  //             as: 'ticketHistory',
  //           }
  //         },
  //         {
  //           $lookup: {
  //             from: 'support_ticket_claim_intimation_report_history',
  //             localField: 'SupportTicketNo',
  //             foreignField: 'SupportTicketNo',
  //             as: 'claimInfo',
  //           }
  //         },
  //         {
  //           $lookup: {
  //             from: 'csc_agent_master',
  //             localField: 'InsertUserID',
  //             foreignField: 'UserLoginID',
  //             as: 'agentInfo',
  //           }
  //         },
  //         {
  //           $lookup: {
  //             from: 'ticket_comment_journey',
  //             localField: 'SupportTicketNo',
  //             foreignField: 'SupportTicketNo',
  //             as: 'ticket_comment_journey',
  //           }
  //         },
  //         { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
  //         { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
  //         { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
  //         { $skip: skip },
  //         { $limit: CHUNK_SIZE },
  //         {
  //           $project: {
  //             agentInfo: 1,
  //             TicketComments: {
  //               $arrayToObject: {
  //                 $map: {
  //                   input: '$ticket_comment_journey',
  //                   as: 'comment',
  //                   in: {
  //                     k: {
  //                       $concat: [
  //                         'Comment (',
  //                         { $dateToString: { format: '%Y-%m-%d', date: '$$comment.ResolvedDate' } },
  //                         ')',
  //                       ],
  //                     },
  //                     v: '$$comment.ResolvedComment',
  //                   },
  //                 },
  //               },
  //             },
  //             CallingUniqueID: 1,
  //             TicketNCIPDocketNo: 1,
  //             SupportTicketNo: 1,
  //             Created: 1,
  //             TicketReOpenDate: 1,
  //             TicketStatus: 1,
  //             StatusUpdateTime: 1,
  //             StateMasterName: 1,
  //             DistrictMasterName: 1,
  //             SubDistrictName: 1,
  //             TicketHeadName: 1,
  //             TicketTypeName: 1,
  //             TicketCategoryName: 1,
  //             CropSeasonName: 1,
  //             RequestYear: 1,
  //             InsuranceCompany: 1,
  //             ApplicationNo: 1,
  //             InsurancePolicyNo: 1,
  //             CallerContactNumber: 1,
  //             RequestorName: 1,
  //             RequestorMobileNo: 1,
  //             Relation: 1,
  //             RelativeName: 1,
  //             PolicyPremium: 1,
  //             PolicyArea: 1,
  //             PolicyType: 1,
  //             LandSurveyNumber: 1,
  //             LandDivisionNumber: 1,
  //             PlotStateName: 1,
  //             PlotDistrictName: 1,
  //             PlotVillageName: 1,
  //             ApplicationSource: 1,
  //             CropShare: 1,
  //             IFSCCode: 1,
  //             FarmerShare: 1,
  //             SowingDate: 1,
  //             CreatedBY: 1,
  //             TicketDescription: 1
  //           }
  //         }
  //       ];

  //       const docs = await db.collection('SLA_KRPH_SupportTickets_Records')
  //         .aggregate(pipeline, { allowDiskUse: true })
  //         .toArray();

  //       docs.forEach(doc => {
  //         if (doc.TicketComments) {
  //           for (const [key, value] of Object.entries(doc.TicketComments)) {
  //             doc[key] = value;
  //           }
  //           delete doc.TicketComments;
  //         }
  //       });

  //       if (docs.length === 0) {
  //         hasMore = false;
  //       } else {
  //         docs.forEach(doc => {
  //           worksheet.addRow({
  //             AgentID: doc.agentInfo?.UserID?.toString() || '',
  //             CallingUniqueID: doc.CallingUniqueID || '',
  //             TicketNCIPDocketNo: doc.TicketNCIPDocketNo || '',
  //             SupportTicketNo: doc.SupportTicketNo ? doc.SupportTicketNo.toString() : '',
  //             Created: doc.Created ? new Date(doc.Created).toISOString() : '',
  //             TicketReOpenDate: doc.TicketReOpenDate || '',
  //             TicketStatus: doc.TicketStatus || '',
  //             StatusUpdateTime: doc.StatusUpdateTime ? new Date(doc.StatusUpdateTime).toISOString() : '',
  //             StateMasterName: doc.StateMasterName || '',
  //             DistrictMasterName: doc.DistrictMasterName || '',
  //             SubDistrictName: doc.SubDistrictName || '',
  //             TicketHeadName: doc.TicketHeadName || '',
  //             TicketTypeName: doc.TicketTypeName || '',
  //             TicketCategoryName: doc.TicketCategoryName || '',
  //             CropSeasonName: doc.CropSeasonName || '',
  //             RequestYear: doc.RequestYear || '',
  //             InsuranceCompany: doc.InsuranceCompany || '',
  //             ApplicationNo: doc.ApplicationNo || '',
  //             InsurancePolicyNo: doc.InsurancePolicyNo || '',
  //             CallerContactNumber: doc.CallerContactNumber || '',
  //             RequestorName: doc.RequestorName || '',
  //             RequestorMobileNo: doc.RequestorMobileNo || '',
  //             Relation: doc.Relation || '',
  //             RelativeName: doc.RelativeName || '',
  //             PolicyPremium: doc.PolicyPremium || '',
  //             PolicyArea: doc.PolicyArea || '',
  //             PolicyType: doc.PolicyType || '',
  //             LandSurveyNumber: doc.LandSurveyNumber || '',
  //             LandDivisionNumber: doc.LandDivisionNumber || '',
  //             PlotStateName: doc.PlotStateName || '',
  //             PlotDistrictName: doc.PlotDistrictName || '',
  //             PlotVillageName: doc.PlotVillageName || '',
  //             ApplicationSource: doc.ApplicationSource || '',
  //             CropShare: doc.CropShare || '',
  //             IFSCCode: doc.IFSCCode || '',
  //             FarmerShare: doc.FarmerShare || '',
  //             SowingDate: doc.SowingDate || '',
  //             CreatedBY: doc.CreatedBY || '',
  //             TicketDescription: doc.TicketDescription || ''
  //           });
  //         });

  //         skip += CHUNK_SIZE;
  //       }
  //     }

  //     const nextDate = new Date(currentDate);
  //     nextDate.setDate(nextDate.getDate() + 1);
  //     await processDateWithChunking(nextDate, endDate);
  //   }

  //   await processDateWithChunking(new Date(SPFROMDATE), new Date(SPTODATE));

  //   const excelFileName = `support_ticket_data_${Date.now()}.xlsx`;
  //   const excelFilePath = path.join(folderPath, excelFileName);

  //   await workbook.xlsx.writeFile(excelFilePath);

  //   const zipFileName = excelFileName.replace('.xlsx', '.zip');
  //   const zipFilePath = path.join(folderPath, zipFileName);
  //   const output = fs.createWriteStream(zipFilePath);
  //   const archive = archiver('zip', { zlib: { level: 9 } });
  //   archive.pipe(output);
  //   archive.file(excelFilePath, { name: excelFileName });
  //   await archive.finalize();
  //   await new Promise<void>((resolve, reject) => {
  //     output.on('close', resolve);
  //     output.on('error', reject);
  //   });
  //   await fs.promises.unlink(excelFilePath).catch(console.error);

  //   const gcpService = new GCPServices();
  //   const fileBuffer = await fs.promises.readFile(zipFilePath);
  //   const uploadResult = await gcpService.uploadFileToGCP({
  //     filePath: 'krph/reports/',
  //     uploadedBy: 'KRPH',
  //     file: { buffer: fileBuffer, originalname: zipFileName },
  //   });
  //   const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
  //   if (gcpDownloadUrl) await fs.promises.unlink(zipFilePath).catch(console.error);

  //   await this.updateDownloadLogFileInfo(logId, zipFileName, gcpDownloadUrl, this.db);

  //   const responsePayload = {
  //     data: [], pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
  //     downloadUrl: gcpDownloadUrl
  //   };

  //   const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);
  //   try {
  //     await this.mailService.sendMail({ to: userEmail, subject: 'Support Ticket History Report Download Service', text: 'Support Ticket History Report', html: supportTicketTemplate });
  //   } catch (err) {
  //     console.error(`Failed to send email to ${userEmail}:`, err);
  //   }

  //   await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);

  //   // return responsePayload;
  // }









  formatToDDMMYYYY(dateString) {
    if (!dateString) return '';

    const date = new Date(dateString);
    if (isNaN(date.getTime())) return '';

    const day = String(date.getDate()).padStart(2, '0');
    const month = String(date.getMonth() + 1).padStart(2, '0'); // Months are 0-based
    const year = date.getFullYear();

    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');

    return `${day}-${month}-${year} ${hours}:${minutes}`;
  }










  async processTicketHistoryAndGenerateZipOldWithBatch(ticketPayload: any) {
    const {
      SPFROMDATE,
      SPTODATE,
      SPInsuranceCompanyID,
      SPStateID,
      SPTicketHeaderID,
      SPUserID,
      limit = 50000, // batch size
      userEmail,
    } = ticketPayload;

    const db = this.db;

    if (!SPInsuranceCompanyID || !SPStateID) {
      return { rcode: 0, rmessage: `${!SPInsuranceCompanyID ? 'InsuranceCompanyID' : 'StateID'} Missing!` };
    }

    const RequestDateTime = await getCurrentFormattedDateTime();

    // Get user details
    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];
    if (!item) return { rcode: 0, rmessage: 'User details not found.' };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };
    const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

    // Build location filter
    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length) locationFilter = { FilterStateID: { $in: StateMasterID } };
    else if (LocationTypeID === 2 && item.DistrictIDs?.length) locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };

    // Main match filter
    const match: any = {
      ...locationFilter,
      ...(SPStateID !== '#ALL' && { FilterStateID: { $in: SPStateID.split(',') } }),
      ...(SPInsuranceCompanyID !== '#ALL' && { InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',') } }),
      ...(SPTicketHeaderID && SPTicketHeaderID !== 0 && { TicketHeaderID: SPTicketHeaderID }),
      ...(InsuranceCompanyID?.length && { InsuranceCompanyID: { $in: InsuranceCompanyID } }),
      ...(StateMasterID?.length && LocationTypeID !== 2 && { FilterStateID: { $in: StateMasterID } }),
    };

    if (SPFROMDATE || SPTODATE) {
      match.InsertDateTime = {};
      if (SPFROMDATE) match.InsertDateTime.$gte = new Date(`${SPFROMDATE}T00:00:00.000Z`);
      if (SPTODATE) match.InsertDateTime.$lte = new Date(`${SPTODATE}T23:59:59.999Z`);
    }

    // Count total documents
    const totalCount = await db.collection('SLA_KRPH_SupportTickets_Records').countDocuments(match);
    const totalPages = Math.ceil(totalCount / limit);
    console.log(totalCount, "totalCount");

    // Prepare Excel file paths
    const folderPath = path.join(process.cwd(), 'downloads');
    await fs.ensureDir(folderPath);
    const timestamp = Date.now();
    const excelFileName = `support_ticket_data_${timestamp}.xlsx`;
    const excelFilePath = path.join(folderPath, excelFileName);

    // ExcelJS streaming
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
    const worksheet = workbook.addWorksheet('Support Ticket Data');

    const headers = [
      "SupportTicketID", "ApplicationNo", "InsurancePolicyNo", "TicketStatusID", "TicketStatus",
      "CallerContactNumber", "RequestorName", "RequestorMobileNo", "StateMasterName", "DistrictMasterName",
      "SubDistrictName", "TicketHeadName", "TicketCategoryName", "RequestSeason", "RequestYear",
      "ApplicationCropName", "Relation", "RelativeName", "PolicyPremium", "PolicyArea",
      "PolicyType", "LandSurveyNumber", "LandDivisionNumber", "IsSos", "PlotStateName",
      "PlotDistrictName", "PlotVillageName", "ApplicationSource", "CropShare", "IFSCCode",
      "FarmerShare", "SowingDate", "LossDate", "CreatedBY", "CreatedAt", "Sos",
      "NCIPDocketNo", "TicketDescription", "CallingUniqueID", "TicketDate", "StatusDate",
      "SupportTicketTypeName", "SupportTicketNo", "InsuranceMasterName", "ReOpenDate", "CallingUserID", "SchemeName"
    ];
    worksheet.addRow(headers).commit();

    // Process in batches
    for (let currentPage = 0; currentPage < totalPages; currentPage++) {
      const pipeline: any[] = [
        { $match: match },
        {
          $lookup: {
            from: 'SLA_KRPH_SupportTicketsHistory_Records',
            let: { ticketId: '$SupportTicketID' },
            pipeline: [
              { $match: { $expr: { $and: [{ $eq: ['$SupportTicketID', '$$ticketId'] }, { $eq: ['$TicketStatusID', 109304] }] } } },
              { $sort: { TicketHistoryID: -1 } },
              { $limit: 1 },
              { $project: { TicketHistoryDate: 1 } }
            ],
            as: 'ticketHistory'
          }
        },
        {
          $lookup: {
            from: 'support_ticket_claim_intimation_report_history',
            localField: 'SupportTicketNo',
            foreignField: 'SupportTicketNo',
            pipeline: [{ $project: { ClaimReportNo: 1 } }],
            as: 'claimInfo'
          }
        },
        {
          $lookup: {
            from: 'csc_agent_master',
            localField: 'InsertUserID',
            foreignField: 'UserLoginID',
            pipeline: [{ $project: { UserID: 1 } }],
            as: 'agentInfo'
          }
        },
        { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
        { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
        { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
        { $skip: currentPage * limit },
        { $limit: limit },
        {
          $project: {
            _id: 0,
            SupportTicketID: 1,
            ApplicationNo: 1,
            InsurancePolicyNo: 1,
            TicketStatusID: 1,
            TicketStatus: 1,
            CallerContactNumber: 1,
            RequestorName: 1,
            RequestorMobileNo: 1,
            StateMasterName: 1,
            DistrictMasterName: 1,
            SubDistrictName: 1,
            TicketHeadName: 1,
            TicketCategoryName: 1,
            RequestSeason: 1,
            RequestYear: 1,
            ApplicationCropName: 1,
            Relation: 1,
            RelativeName: 1,
            PolicyPremium: 1,
            PolicyArea: 1,
            PolicyType: 1,
            LandSurveyNumber: 1,
            LandDivisionNumber: 1,
            IsSos: 1,
            PlotStateName: 1,
            PlotDistrictName: 1,
            PlotVillageName: 1,
            ApplicationSource: 1,
            CropShare: 1,
            IFSCCode: 1,
            FarmerShare: 1,
            SowingDate: 1,
            LossDate: 1,
            CreatedBY: 1,
            CreatedAt: "$InsertDateTime",
            Sos: 1,
            NCIPDocketNo: "$TicketNCIPDocketNo",
            TicketDescription: 1,
            CallingUniqueID: 1,
            TicketDate: { $dateToString: { format: "%Y-%m-%d %H:%M:%S", date: "$Created" } },
            StatusDate: { $dateToString: { format: "%Y-%m-%d %H:%M:%S", date: "$StatusUpdateTime" } },
            SupportTicketTypeName: "$TicketTypeName",
            SupportTicketNo: 1,
            InsuranceMasterName: "$InsuranceCompany",
            ReOpenDate: "$TicketReOpenDate",
            CallingUserID: "$agentInfo.UserID",
            SchemeName: 1,
          }
        }
      ];

      const batchResults = await db.collection('SLA_KRPH_SupportTickets_Records')
        .aggregate(pipeline, { allowDiskUse: true })
        .toArray();

      for (const row of batchResults) {
        worksheet.addRow(Object.values(row)).commit();
      }

      console.log(`Processed batch ${currentPage + 1} / ${totalPages}`);
    }

    await workbook.commit();
    console.log(`Excel file created at: ${excelFilePath}`);

    // ZIP the Excel
    const zipFileName = excelFileName.replace('.xlsx', '.zip');
    const zipFilePath = path.join(folderPath, zipFileName);
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(output);
    archive.file(excelFilePath, { name: excelFileName });
    await archive.finalize();
    await fs.remove(excelFilePath);

    // Upload to GCP
    const gcpService = new GCPServices();
    const fileBuffer = await fs.readFile(zipFilePath);
    const uploadResult = await gcpService.uploadFileToGCP({
      filePath: 'krph/reports/',
      uploadedBy: "KRPH",
      file: { buffer: fileBuffer, originalname: zipFileName }
    });
    const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
    await fs.remove(zipFilePath);

    // Log download
    await db.collection('support_ticket_download_logs').insertOne({
      userId: SPUserID,
      insuranceCompanyId: SPInsuranceCompanyID,
      stateId: SPStateID,
      ticketHeaderId: SPTicketHeaderID,
      fromDate: SPFROMDATE,
      toDate: SPTODATE,
      zipFileName,
      downloadUrl: gcpDownloadUrl,
      createdAt: new Date(),
    });

    // Send email
    const supportTicketTemplate = await generateSupportTicketEmailHTML(
      'Portal User',
      RequestDateTime,
      gcpDownloadUrl
    );
    await this.mailService.sendMail({
      to: userEmail,
      subject: 'Support Ticket History Report Download Service',
      text: 'Support Ticket History Report',
      html: supportTicketTemplate,
    });

    return { rcode: 1, rmessage: 'Success', downloadUrl: gcpDownloadUrl, totalCount };
  }













  async downloadHistory(payload) {
    console.log(payload)
    let collectionName = 'support_ticket_download_logs'
    let pipeline = [
      {
        $match: {
          userId: payload.userID
        }
      },
      {
        $lookup: {
          from: "bm_app_access",
          localField: "userId",
          foreignField: "AppAccessID",
          as: "data"
        }
      },
      {
        $unwind: {
          path: "$data",
          preserveNullAndEmptyArrays: true
        }
      },
      {
        $project: {
          ReqestorUserID: "$userId",
          RequestedParamsTicketHeaderID: "$ticketHeaderId",
          RequestedParamsInsuranceCompany: "$insuranceCompanyId",
          RequestedParamsStateID: "$stateId",
          RequestedParamsFromDate: "$fromDate",
          RequestedParamsToDate: "$toDate",
          ZippedFileName: "$zipFileName",
          DownloadURL: "$downloadUrl",
          RequestCreationDate: "$createdAt",
          RequestorUserName: "$data.AppAccessUserName",
          RequestorRole: "$data.BRHeadTypeID"

        }
      }, {
        $sort: { RequestCreationDate: -1 }
      }
    ]

    let result = await this.db.collection(collectionName).aggregate(pipeline).toArray()


    return {
      data: result,
      message: { msg: '✅ Data fetched successfully', code: 1 }
    };
  }


  async downloadFarmerCallingHistory(payload) {
    console.log(payload)
    let collectionName = 'report_logs'
    let pipeline = [
      {
        $match: {
          userId: payload.userID
        }
      },
      {
        $lookup: {
          from: "bm_app_access",
          localField: "userId",
          foreignField: "AppAccessID",
          as: "data"
        }
      },
      {
        $unwind: {
          path: "$data",
          preserveNullAndEmptyArrays: true
        }
      },
      {
        $project: {
          ReqestorUserID: "$userId",
          RequestedParamsTicketHeaderID: "$ticketHeaderId",
          RequestedParamsInsuranceCompany: "$insuranceCompanyId",
          RequestedParamsStateID: "$stateId",
          RequestedParamsFromDate: "$fromDate",
          RequestedParamsToDate: "$toDate",
          ZippedFileName: "$zipFileName",
          DownloadURL: "$downloadUrl",
          RequestCreationDate: "$createdAt",
          RequestorUserName: "$data.AppAccessUserName",
          RequestorRole: "$data.BRHeadTypeID"

        }
      }, {
        $sort: { RequestCreationDate: -1 }
      }
    ]

    let result = await this.db.collection(collectionName).aggregate(pipeline).toArray()


    return {
      data: result,
      message: { msg: '✅ Data fetched successfully', code: 1 }
    };
  }
  /* 
  async FarmerSelectCallingHistoryService(payload: any) {
    let { fromDate, toDate, stateCodeAlpha, page = 1, limit = 1000, objCommon } = payload;
  
    page = parseInt(page);
    limit = parseInt(limit);
    const skip = (page - 1) * limit;
  
    this.createIndexes(this.db).catch(err => {
      console.error('❌ Index creation failed:', err);
    });
  
    let matchStage: Record<string, any> = {
      InsertDateTime: {}
    };
  
    if (fromDate) {
      matchStage.InsertDateTime.$gte = new Date(`${fromDate}T00:00:00.000Z`);
    }
  
    if (toDate) {
      matchStage.InsertDateTime.$lte = new Date(`${toDate}T23:59:59.999Z`);
    }
  
    if (Object.keys(matchStage.InsertDateTime).length === 0) {
      delete matchStage.InsertDateTime;
    }
  
    if (stateCodeAlpha && stateCodeAlpha.trim() !== '') {
      matchStage.StateCodeAlpha = stateCodeAlpha;
    }
  
    const collectionName = 'SLA_KRPH_Farmer_Calling_Master';
  
    const totalCount = await this.db.collection(collectionName).countDocuments(matchStage);
  
    const pipeline = [
      { $match: matchStage },
  
      // Lookup bm_app_access based on InsertUserID -> AppAccessID
      {
        $lookup: {
          from: 'bm_app_access',
          localField: 'InsertUserID',
          foreignField: 'AppAccessID',
          as: 'appAccess'
        }
      },
      { $unwind: { path: '$appAccess', preserveNullAndEmptyArrays: true } },
  
      // Lookup csc_agent_master where UserLoginID = AppAccessID and Status = 'Y'
      {
        $lookup: {
          from: 'csc_agent_master',
          let: { appAccessId: '$appAccess.AppAccessID' },
          pipeline: [
            { $match: { $expr: { $and: [ { $eq: ['$UserLoginID', '$$appAccessId'] }, { $eq: ['$Status', 'Y'] } ] } } }
          ],
          as: 'agentMaster'
        }
      },
      { $unwind: { path: '$agentMaster', preserveNullAndEmptyArrays: true } },
  
      { $sort: { InsertDateTime: 1 } },
      { $skip: skip },
      { $limit: limit }
    ];
  
    console.log('🧱 Aggregation Pipeline:', JSON.stringify(pipeline));
  
    const results = await this.db.collection(collectionName)
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray();
  
    const totalPages = Math.ceil(totalCount / limit);
  
    const responsePayload = {
      data: results || [],
      pagination: {
        total: totalCount,
        page,
        limit,
        totalPages,
        hasNextPage: page < totalPages,
        hasPrevPage: page > 1
      },
    };
  
    return responsePayload;
  }
   */

  async FarmerSelectCallingHistoryService(payload: any) {
    let { fromDate, toDate, stateCodeAlpha, page = 1, limit, objCommon } = payload;

    page = parseInt(page);
    limit = parseInt(limit);
    const skip = (page - 1) * limit;

    let matchStage: Record<string, any> = {
      InsertDateTime: {}
    };

    if (fromDate) {
      matchStage.InsertDateTime.$gte = new Date(`${fromDate}T00:00:00.000Z`);
    }

    if (toDate) {
      matchStage.InsertDateTime.$lte = new Date(`${toDate}T23:59:59.999Z`);
    }

    if (Object.keys(matchStage.InsertDateTime).length === 0) {
      delete matchStage.InsertDateTime;
    }

    if (stateCodeAlpha && stateCodeAlpha.trim() !== '') {
      matchStage.StateCodeAlpha = stateCodeAlpha;
    }

    const collectionName = 'SLA_KRPH_Farmer_Calling_Master';

    const totalCount = await this.db.collection(collectionName).countDocuments(matchStage);

    const pipeline = [
      { $match: matchStage },
      { $sort: { InsertDateTime: 1 } },
      { $skip: skip },
      { $limit: limit },
      {
        $lookup: {
          from: 'bm_app_access',
          localField: 'InsertUserID',
          foreignField: 'AppAccessID',
          as: 'appAccess'
        }
      },
      { $unwind: { path: '$appAccess', preserveNullAndEmptyArrays: true } },
      {
        $lookup: {
          from: 'csc_agent_master',
          let: { appAccessId: '$appAccess.AppAccessID' },
          pipeline: [
            {
              $match: {
                $expr: {
                  $and: [
                    { $eq: ['$UserLoginID', '$$appAccessId'] },
                    { $eq: ['$Status', 'Y'] }
                  ]
                }
              }
            },
            {
              $project: {
                CSCAgentMasterID: 1,
                UserID: 1,
                DisplayName: 1,
                Status: 1,
                Location: 1
              }
            }
          ],
          as: 'agentMaster'
        }
      },
      { $unwind: { path: '$agentMaster', preserveNullAndEmptyArrays: true } },
      {
        $project: {
          UserID: '$agentMaster.UserID',
          CallingUniqueID: '$CallingUniqueID',
          CallerMobileNumber: '$CallerMobileNumber',
          CallStatus: '$CallStatus',
          CallPurpose: '$CallPurpose',
          FarmerName: '$FarmerName',
          StateMasterName: '$FarmerStateName',
          DistrictMasterName: '$FarmerDistrictName',
          IsRegistered: '$IsRegistered',
          Reason: '$Reason',
          InsertDateTime: '$InsertDateTime'
        }
      }
    ];

    const results = await this.db.collection(collectionName)
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray();

    const totalPages = Math.ceil(totalCount / limit);

    if (results.length === 0) {
      return {
        data: [],
        message: {
          msg: 'Fetched SuccessFully',
          code: 0
        },
        pagination: {
          total: totalCount,
          page,
          limit,
          totalPages,
          hasNextPage: false,
          hasPrevPage: false
        }
      };
    }

    return {
      data: results,
      message: {
        msg: 'Fetched SuccessFully',
        code: 1
      },
      pagination: {
        total: totalCount,
        page,
        limit,
        totalPages,
        hasNextPage: page < totalPages,
        hasPrevPage: page > 1
      }
    };
  }





  async getUserDetails(userId: any): Promise<any> {
    const Delta = await this.getSupportTicketUserDetail(userId);

    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);

    const item = (responseInfo.data as any)?.user?.[0];
    console.log(JSON.stringify(item));

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID
        ? await this.convertStringToArray(item.InsuranceCompanyID)
        : [],
      StateMasterID: item.StateMasterID
        ? await this.convertStringToArray(item.StateMasterID)
        : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
      DistrictIDs: item.DistrictIDs || [],
    };

    console.log(userDetail, "userDetail");

    return userDetail;
  }


  async createIndexes(db) {
    try {
      await db.collection('SLA_KRPH_Farmer_Calling_Master').createIndex(
        { InsertDateTime: 1, StateCodeAlpha: 1 },
        { name: 'idx_InsertDateTime_StateCodeAlpha' }
      );
      console.log('✅ Index created: InsertDateTime + StateCodeAlpha');

      await db.collection('bm_app_access').createIndex(
        { AppAccessID: 1 },
        { name: 'idx_AppAccessID' }
      );
      console.log('✅ Index created: bm_app_access.AppAccessID');

      await db.collection('csc_agent_master').createIndex(
        { UserLoginID: 1, Status: 1 },
        { name: 'idx_UserLoginID_Status' }
      );
      console.log('✅ Index created: csc_agent_master.UserLoginID + Status');

    } catch (error) {
      console.error('❌ Failed to create indexes:', error);
    }
  }


  async AddIndex(db) {
    await db.collection('SLA_KRPH_SupportTickets_Records').createIndex({
      FilterStateID: 1,
      InsuranceCompanyID: 1,
      TicketHeaderID: 1,
      InsertDateTime: 1,
      SupportTicketID: 1,
      SupportTicketNo: 1,
      InsertUserID: 1
    });

    await db.collection('SLA_KRPH_SupportTicketsHistory_Records').createIndex({
      SupportTicketID: 1,
      TicketStatusID: 1,
      TicketHistoryID: -1
    });

    await db.collection('support_ticket_claim_intimation_report_history').createIndex({
      SupportTicketNo: 1
    });

    await db.collection('csc_agent_master').createIndex({
      UserLoginID: 1
    });

  }


  async assignIndexes(payload) {
    let database = this.db
    // this.AddIndex(database)
    // this.createIndexes(database)
    this.AddIndexss(database)
  }


  async AddIndexss(db) {
    // Drop indexes before recreating (excluding _id index)
    const collections = [
      'SLA_KRPH_SupportTickets_Records',
      'SLA_KRPH_SupportTicketsHistory_Records',
      'support_ticket_claim_intimation_report_history',
      'csc_agent_master',
      'ticket_comment_journey',
    ];

    for (const collName of collections) {
      const coll = db.collection(collName);
      const indexes = await coll.indexes();

      for (const index of indexes) {
        if (index.name !== '_id_') {
          await coll.dropIndex(index.name);
        }
      }
    }

    await db.collection('SLA_KRPH_SupportTickets_Records').createIndex({
      InsuranceCompanyID: 1,
      FilterStateID: 1,
      TicketHeaderID: 1,
      InsertDateTime: 1,
    });

    await db.collection('SLA_KRPH_SupportTickets_Records').createIndex({ SupportTicketID: 1 });
    await db.collection('SLA_KRPH_SupportTickets_Records').createIndex({ SupportTicketNo: 1 });
    await db.collection('SLA_KRPH_SupportTickets_Records').createIndex({ InsertUserID: 1 });

    await db.collection('SLA_KRPH_SupportTicketsHistory_Records').createIndex({
      SupportTicketID: 1,
      TicketStatusID: 1,
      TicketHistoryID: -1,
    });

    await db.collection('support_ticket_claim_intimation_report_history').createIndex({
      SupportTicketNo: 1,
    });

    await db.collection('csc_agent_master').createIndex({
      UserLoginID: 1,
    });

    await db.collection('ticket_comment_journey').createIndex({
      SupportTicketNo: 1,
    });

    console.log('Indexes dropped and recreated successfully.');
  }



  /* async farmerCallingHistoryDownloadReportAndZip(payload: any) {
    const {
      fromDate,
      toDate,
      stateCodeAlpha,
      userEmail,
      page = 1,
      limit = 1000000000,
    } = payload;
  
    const db = this.db;
    const cacheKey = `farmerCallingHistory:${fromDate}:${toDate}:${stateCodeAlpha}:${page}:${limit}`;
    const RequestDateTime = await getCurrentFormattedDateTime();
  
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey);
    if (cachedData) {
      console.log('✅ Using cached data');
      return cachedData;
    }
  
    const todayFolder = new Date().toISOString().split('T')[0];
    const folderPath = path.join(process.cwd(), 'downloads', todayFolder);
    try {
      await fs.promises.mkdir(folderPath, { recursive: true });
    } catch (err) {
      console.error(`❌ Failed to create folder ${folderPath}`, err);
    }
  
    const excelFileName = `farmer_calling_history_${Date.now()}.xlsx`;
    const excelFilePath = path.join(folderPath, excelFileName);
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
    const worksheet = workbook.addWorksheet('Farmer Calling History');
  
    worksheet.columns = [
      { header: 'User ID', key: 'UserID', width: 15 },
      { header: 'Calling ID', key: 'CallingUniqueID', width: 25 },
      { header: 'Caller Mobile No', key: 'CallerMobileNumber', width: 20 },
      { header: 'Call Status', key: 'CallStatus', width: 20 },
      { header: 'Call Purpose', key: 'CallPurpose', width: 30 },
      { header: 'Farmer Name', key: 'FarmerName', width: 25 },
      { header: 'State', key: 'StateMasterName', width: 20 },
      { header: 'District', key: 'DistrictMasterName', width: 20 },
      { header: 'Is Registered', key: 'IsRegistered', width: 15 },
      { header: 'Reason', key: 'Reason', width: 30 },
      { header: 'Insert Date', key: 'InsertDateTime', width: 25 }
    ];
  
    const CHUNK_SIZE = 10000;
  
    async function processDateChunk(currentDate: Date, endDate: Date) {
      if (currentDate > endDate) return;
  
      const startOfDay = new Date(currentDate);
      startOfDay.setUTCHours(0, 0, 0, 0);
  
      const endOfDay = new Date(currentDate);
      endOfDay.setUTCHours(23, 59, 59, 999);
  
      let skip = 0;
      let hasMore = true;
  
      while (hasMore) {
        const matchStage: Record<string, any> = {
          InsertDateTime: { $gte: startOfDay, $lte: endOfDay }
        };
  
        if (stateCodeAlpha && stateCodeAlpha.trim() !== '') {
          matchStage.StateCodeAlpha = stateCodeAlpha;
        }
  
        const pipeline = [
          { $match: matchStage },
          { $sort: { InsertDateTime: 1 } },
          { $skip: skip },
          { $limit: CHUNK_SIZE },
          {
            $lookup: {
              from: 'bm_app_access',
              localField: 'InsertUserID',
              foreignField: 'AppAccessID',
              as: 'appAccess'
            }
          },
          { $unwind: { path: '$appAccess', preserveNullAndEmptyArrays: true } },
          {
            $lookup: {
              from: 'csc_agent_master',
              let: { appAccessId: '$appAccess.AppAccessID' },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $and: [
                        { $eq: ['$UserLoginID', '$$appAccessId'] },
                        { $eq: ['$Status', 'Y'] }
                      ]
                    }
                  }
                },
                {
                  $project: {
                    UserID: 1,
                    DisplayName: 1
                  }
                }
              ],
              as: 'agentMaster'
            }
          },
          { $unwind: { path: '$agentMaster', preserveNullAndEmptyArrays: true } },
          {
            $project: {
              UserID: '$agentMaster.UserID',
              CallingUniqueID: 1,
              CallerMobileNumber: 1,
              CallStatus: 1,
              CallPurpose: 1,
              FarmerName: 1,
              StateMasterName: '$FarmerStateName',
              DistrictMasterName: '$FarmerDistrictName',
              IsRegistered: 1,
              Reason: 1,
              InsertDateTime: 1
            }
          }
        ];
  
        let results: any[] = [];
        try {
          results = await db.collection('SLA_KRPH_Farmer_Calling_Master').aggregate(pipeline, { allowDiskUse: true }).toArray();
        } catch (err) {
          console.error('❌ Error while querying DB:', err);
        }
  
        results.forEach(row => {
          worksheet.addRow({
            UserID: row.UserID || '',
            CallingUniqueID: row.CallingUniqueID || '',
            CallerMobileNumber: row.CallerMobileNumber || '',
            CallStatus: row.CallStatus || '',
            CallPurpose: row.CallPurpose || '',
            FarmerName: row.FarmerName || '',
            StateMasterName: row.StateMasterName || '',
            DistrictMasterName: row.DistrictMasterName || '',
            IsRegistered: row.IsRegistered || '',
            Reason: row.Reason || '',
            InsertDateTime: row.InsertDateTime ? new Date(row.InsertDateTime).toISOString() : ''
          }).commit();
        });
  
        if (results.length < CHUNK_SIZE) {
          hasMore = false;
        } else {
          skip += CHUNK_SIZE;
        }
      }
  
      const nextDate = new Date(currentDate);
      nextDate.setDate(currentDate.getDate() + 1);
      await processDateChunk(nextDate, endDate);
    }
  
    try {
      await processDateChunk(new Date(fromDate), new Date(toDate));
    } catch (err) {
      console.error('❌ Error while processing date chunks:', err);
    }
  
    try {
      await workbook.commit();
    } catch (err) {
      console.error('❌ Failed to commit workbook:', err);
    }
  
    const zipFileName = excelFileName.replace('.xlsx', '.zip');
    const zipFilePath = path.join(folderPath, zipFileName);
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver('zip', { zlib: { level: 9 } });
  
    archive.pipe(output);
    archive.file(excelFilePath, { name: excelFileName });
  
    try {
      await archive.finalize();
      await new Promise<void>((resolve, reject) => {
        output.on('close', resolve);
        output.on('error', reject);
      });
    } catch (err) {
      console.error('❌ Failed to create ZIP archive:', err);
    }
  
    const gcpService = new GCPServices();
    let gcpDownloadUrl = '';
    const MAX_RETRIES = 3;
    let attempt = 0;
  
    while (attempt < MAX_RETRIES) {
      try {
        const fileBuffer = await fs.promises.readFile(zipFilePath);
        const uploadResult = await gcpService.uploadFileToGCP({
          filePath: 'krph/reports/',
          uploadedBy: 'KRPH',
          file: { buffer: fileBuffer, originalname: zipFileName },
        });
        gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
        if (gcpDownloadUrl) break;
      } catch (err) {
        console.error(`❌ GCP upload attempt ${attempt + 1} failed:`, err);
      }
      attempt++;
      await new Promise(res => setTimeout(res, 1000 * Math.pow(2, attempt)));
    }
  
    if (gcpDownloadUrl) {
      await Promise.all([
        fs.promises.unlink(zipFilePath).catch(err => console.error('❌ Failed to delete zip file:', err)),
        fs.promises.unlink(excelFilePath).catch(err => console.error('❌ Failed to delete excel file:', err))
      ]);
    } else {
      console.error('❌ All GCP upload attempts failed. File not sent.');
      console.log('📄 File not uploaded. Skipping email and cache.');
      return;
    }
  
    const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);
  
    try {
      await this.mailService.sendMail({
        to: userEmail,
        subject: 'Farmer Calling History Report Download',
        text: 'Farmer Calling History Report',
        html: supportTicketTemplate
      });
      console.log('📧 Mail sent successfully');
    } catch (err) {
      console.error(`❌ Failed to send email to ${userEmail}:`, err);
    }
  
    const responsePayload = {
      data: [],
      pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
      downloadUrl: gcpDownloadUrl,
      zipFileName
    };
  
    try {
      await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);
    } catch (err) {
      console.error('❌ Failed to cache the response:', err);
    }
  } */

  async farmerCallingHistoryDownloadReportAndZip(payload: any) {
    const {
      fromDate,
      toDate,
      stateCodeAlpha,
      userEmail,
      page = 1,
      limit = 1000000000,
    } = payload;

    const db = this.db;
    const cacheKey = `farmerCallingHistory:${fromDate}:${toDate}:${stateCodeAlpha}:${page}:${limit}`;
    const RequestDateTime = await getCurrentFormattedDateTime();

    const reportLogCollection = db.collection('report_logs');
    let logDocId = null;

    try {
      const logDoc = await reportLogCollection.insertOne({
        userEmail,
        stateCodeAlpha,
        fromDate,
        toDate,
        createdAt: new Date(),
        status: 'Processing',
        zipFileName: null,
        gcpDownloadUrl: null
      });
      logDocId = logDoc.insertedId;
    } catch (err) {
      console.error('❌ Failed to create report log document:', err);
    }

    const cachedData = await this.redisWrapper.getRedisCache(cacheKey);
    if (cachedData) {
      console.log('✅ Using cached data');
      return cachedData;
    }

    const todayFolder = new Date().toISOString().split('T')[0];
    const folderPath = path.join(process.cwd(), 'downloads', todayFolder);
    try {
      await fs.promises.mkdir(folderPath, { recursive: true });
    } catch (err) {
      console.error(`❌ Failed to create folder ${folderPath}`, err);
    }

    const excelFileName = `farmer_calling_history_${Date.now()}.xlsx`;
    const excelFilePath = path.join(folderPath, excelFileName);
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
    const worksheet = workbook.addWorksheet('Farmer Calling History');

    worksheet.columns = [
      { header: 'User ID', key: 'UserID', width: 15 },
      { header: 'Calling ID', key: 'CallingUniqueID', width: 25 },
      { header: 'Caller Mobile No', key: 'CallerMobileNumber', width: 20 },
      { header: 'Call Status', key: 'CallStatus', width: 20 },
      { header: 'Call Purpose', key: 'CallPurpose', width: 30 },
      { header: 'Farmer Name', key: 'FarmerName', width: 25 },
      { header: 'State', key: 'StateMasterName', width: 20 },
      { header: 'District', key: 'DistrictMasterName', width: 20 },
      { header: 'Is Registered', key: 'IsRegistered', width: 15 },
      { header: 'Reason', key: 'Reason', width: 30 },
      { header: 'Created At', key: 'InsertDateTime', width: 25 }
    ];

    const CHUNK_SIZE = 10000;

    async function processDateChunk(currentDate: Date, endDate: Date) {
      if (currentDate > endDate) return;

      const startOfDay = new Date(currentDate);
      startOfDay.setUTCHours(0, 0, 0, 0);

      const endOfDay = new Date(currentDate);
      endOfDay.setUTCHours(23, 59, 59, 999);

      let skip = 0;
      let hasMore = true;

      while (hasMore) {
        const matchStage: Record<string, any> = {
          InsertDateTime: { $gte: startOfDay, $lte: endOfDay }
        };

        if (stateCodeAlpha && stateCodeAlpha.trim() !== '') {
          matchStage.StateCodeAlpha = stateCodeAlpha;
        }

        const pipeline = [
          { $match: matchStage },
          { $sort: { InsertDateTime: 1 } },
          { $skip: skip },
          { $limit: CHUNK_SIZE },
          {
            $lookup: {
              from: 'bm_app_access',
              localField: 'InsertUserID',
              foreignField: 'AppAccessID',
              as: 'appAccess'
            }
          },
          { $unwind: { path: '$appAccess', preserveNullAndEmptyArrays: true } },
          {
            $lookup: {
              from: 'csc_agent_master',
              let: { appAccessId: '$appAccess.AppAccessID' },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $and: [
                        { $eq: ['$UserLoginID', '$$appAccessId'] },
                        { $eq: ['$Status', 'Y'] }
                      ]
                    }
                  }
                },
                {
                  $project: {
                    UserID: 1,
                    DisplayName: 1
                  }
                }
              ],
              as: 'agentMaster'
            }
          },
          { $unwind: { path: '$agentMaster', preserveNullAndEmptyArrays: true } },
          {
            $project: {
              UserID: '$agentMaster.UserID',
              CallingUniqueID: 1,
              CallerMobileNumber: 1,
              CallStatus: 1,
              CallPurpose: 1,
              FarmerName: 1,
              StateMasterName: '$FarmerStateName',
              DistrictMasterName: '$FarmerDistrictName',
              IsRegistered: 1,
              Reason: 1,
              InsertDateTime: 1
            }
          }, {
            $project: {
              UserID: "$UserID",
              CallingUniqueID: "$CallingUniqueID",
              CallerMobileNumber: "$CallerMobileNumber",
              CallStatus: "$CallStatus",
              CallPurpose: "$CallPurpose",
              FarmerName: "$FarmerName",
              StateMasterName: "$FarmerStateName",
              DistrictMasterName: "$DistrictMasterName",
              IsRegistered: "$IsRegistered",
              Reason: "$Reason",
              InsertDateTime: "$InsertDateTime"


            }
          }
        ];

        let results: any[] = [];
        try {
          results = await db.collection('SLA_KRPH_Farmer_Calling_Master').aggregate(pipeline, { allowDiskUse: true }).toArray();
        } catch (err) {
          console.error('❌ Error while querying DB:', err);
        }

        // results.forEach(row => {
        //   worksheet.addRow({
        //     UserID: row.UserID || '',
        //     CallingUniqueID: row.CallingUniqueID || '',
        //     CallerMobileNumber: row.CallerMobileNumber || '',
        //     CallStatus: row.CallStatus || '',
        //     CallPurpose: row.CallPurpose || '',
        //     FarmerName: row.FarmerName || '',
        //     StateMasterName: row.StateMasterName || '',
        //     DistrictMasterName: row.DistrictMasterName || '',
        //     IsRegistered: row.IsRegistered || '',
        //     Reason: row.Reason || '',
        //     // InsertDateTime: row.InsertDateTime ? new Date(row.InsertDateTime).toISOString() : ''
        //     InsertDateTime: row.InsertDateTime ? this.formatTimestamp(row.InsertDateTime) : ''
        //   }).commit();
        // });

        results.forEach(row => {
          const insertDateTime = row.InsertDateTime
            ? (() => {
              const date = new Date(row.InsertDateTime);
              const day = String(date.getUTCDate()).padStart(2, '0');
              const month = String(date.getUTCMonth() + 1).padStart(2, '0');
              const year = date.getUTCFullYear();
              const hours = String(date.getUTCHours()).padStart(2, '0');
              const minutes = String(date.getUTCMinutes()).padStart(2, '0');
              return `${day}-${month}-${year} ${hours}:${minutes}`;
            })()
            : '';

          worksheet.addRow({
            UserID: row.UserID || '',
            CallingUniqueID: row.CallingUniqueID || '',
            CallerMobileNumber: row.CallerMobileNumber || '',
            CallStatus: row.CallStatus || '',
            CallPurpose: row.CallPurpose || '',
            FarmerName: row.FarmerName || '',
            StateMasterName: row.StateMasterName || '',
            DistrictMasterName: row.DistrictMasterName || '',
            IsRegistered: row.IsRegistered || '',
            Reason: row.Reason || '',
            InsertDateTime: insertDateTime,
          }).commit();
        });


        if (results.length < CHUNK_SIZE) {
          hasMore = false;
        } else {
          skip += CHUNK_SIZE;
        }
      }

      const nextDate = new Date(currentDate);
      nextDate.setDate(currentDate.getDate() + 1);
      await processDateChunk(nextDate, endDate);
    }

    try {
      await processDateChunk(new Date(fromDate), new Date(toDate));
    } catch (err) {
      console.error('❌ Error while processing date chunks:', err);
    }

    try {
      await workbook.commit();
    } catch (err) {
      console.error('❌ Failed to commit workbook:', err);
    }

    const zipFileName = excelFileName.replace('.xlsx', '.zip');
    const zipFilePath = path.join(folderPath, zipFileName);
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver('zip', { zlib: { level: 9 } });

    archive.pipe(output);
    archive.file(excelFilePath, { name: excelFileName });

    try {
      await archive.finalize();
      await new Promise<void>((resolve, reject) => {
        output.on('close', resolve);
        output.on('error', reject);
      });
    } catch (err) {
      console.error('❌ Failed to create ZIP archive:', err);
    }

    const gcpService = new GCPServices();
    let gcpDownloadUrl = '';
    const MAX_RETRIES = 3;
    let attempt = 0;

    while (attempt < MAX_RETRIES) {
      try {
        const fileBuffer = await fs.promises.readFile(zipFilePath);
        const uploadResult = await gcpService.uploadFileToGCP({
          filePath: 'krph/reports/',
          uploadedBy: 'KRPH',
          file: { buffer: fileBuffer, originalname: zipFileName },
        });
        gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
        if (gcpDownloadUrl) break;
      } catch (err) {
        console.error(`❌ GCP upload attempt ${attempt + 1} failed:`, err);
      }
      attempt++;
      await new Promise(res => setTimeout(res, 1000 * Math.pow(2, attempt)));
    }

    if (!gcpDownloadUrl) {
      console.error('❌ All GCP upload attempts failed. File not sent.');
      console.log('📄 File not uploaded. Skipping email and cache.');
      if (logDocId) {
        try {
          await reportLogCollection.updateOne(
            { _id: logDocId },
            {
              $set: {
                status: 'Failed',
                updatedAt: new Date()
              }
            }
          );
        } catch (err) {
          console.error('❌ Failed to update log document on failure:', err);
        }
      }
      return;
    }

    await Promise.all([
      fs.promises.unlink(zipFilePath).catch(err => console.error('❌ Failed to delete zip file:', err)),
      fs.promises.unlink(excelFilePath).catch(err => console.error('❌ Failed to delete excel file:', err))
    ]);

    if (logDocId) {
      try {
        await reportLogCollection.updateOne(
          { _id: logDocId },
          {
            $set: {
              status: 'Completed',
              zipFileName,
              gcpDownloadUrl,
              updatedAt: new Date()
            }
          }
        );
      } catch (err) {
        console.error('❌ Failed to update report log document:', err);
      }
    }

    const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);

    try {
      await this.mailService.sendMail({
        to: userEmail,
        subject: 'Farmer Calling History Report Download',
        text: 'Farmer Calling History Report',
        html: supportTicketTemplate
      });
      console.log('📧 Mail sent successfully');
    } catch (err) {
      console.error(`❌ Failed to send email to ${userEmail}:`, err);
    }

    const responsePayload = {
      data: [],
      pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
      downloadUrl: gcpDownloadUrl,
      zipFileName
    };

    try {
      await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);
    } catch (err) {
      console.error('❌ Failed to cache the response:', err);
    }
  }



async fetchTicketListingWorking(payload: any) {
  try {
    const db = this.db;
    const {
      fromdate,
      toDate,
      viewTYP,
      supportTicketID,
      ticketCategoryID,
      ticketSourceID,
      supportTicketTypeID,
      supportTicketNo,
      applicationNo,
      docketNo,
      statusID,
      RequestorMobileNo,
      schemeID,
      ticketHeaderID,
      stateID,
      districtID,
      insuranceCompanyID,
      pageIndex = 1,
      pageSize = 100,
      objCommon
    } = payload;

    let pipeline: any[] = [];
    let message = {};
    let data: any = '';

    // Get user detail
    const Delta = await this.getSupportTicketUserDetail(objCommon.insertedUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];

    if (!item) return { rcode: 0, rmessage: 'User details not found.' };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };

    const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

    // Location-based filter
    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length) {
      locationFilter = { FilterStateID: { $in: StateMasterID } };
    } else if (LocationTypeID === 2 && item.DistrictIDs?.length) {
      locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };
    }

    const match: any = { ...locationFilter };

    if (ticketHeaderID && ticketHeaderID !== 0) {
      match.TicketHeaderID = ticketHeaderID;
    }

    if (insuranceCompanyID && insuranceCompanyID !== 0) {
      const requestedInsuranceIDs = insuranceCompanyID
        .split(',')
        .map(id => Number(id.trim()));
      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter(id =>
        allowedInsuranceIDs.includes(id)
      );

      if (validInsuranceIDs.length === 0) {
        return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
      }

      match.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else {
      if (InsuranceCompanyID?.length) {
        match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
      }
    }

    if (stateID && stateID !== '') {
      const requestedStateIDs = stateID
        .split(',')
        .map(id => Number(id.trim()));
      const validStateIDs = requestedStateIDs.filter(id =>
        StateMasterID.map(Number).includes(id)
      );

      if (validStateIDs.length === 0) {
        return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
      }

      match.FilterStateID = { $in: validStateIDs };
    } else if (StateMasterID?.length && LocationTypeID !== 2) {
      match.FilterStateID = { $in: StateMasterID.map(Number) };
    }

    console.log(JSON.stringify(match), 'Initial match filters');

    if (viewTYP === 'FILTER') {
      if (fromdate && toDate) {
        match.Created = {
          $gte: new Date(`${fromdate}T00:00:00.000Z`),
          $lte: new Date(`${toDate}T23:59:59.999Z`)
        };
      }

      if (supportTicketID) match.SupportTicketID = supportTicketID;
      if (ticketCategoryID) match.TicketCategoryID = ticketCategoryID;
      if (ticketSourceID) match.TicketSourceID = ticketSourceID;
      if (supportTicketTypeID) match.SupportTicketTypeID = supportTicketTypeID;
      if (statusID) match.TicketStatusID = statusID;
      if (schemeID) match.SchemeID = schemeID;
      if (ticketHeaderID) match.TicketHeaderID = ticketHeaderID;
      if (stateID) match.StateMasterID = parseInt(stateID);
      if (districtID) match.DistrictMasterID = districtID;
      if (insuranceCompanyID) match.InsuranceCompanyID = insuranceCompanyID;

      if (supportTicketNo) match.SupportTicketNo = supportTicketNo;
      if (applicationNo) match.ApplicationNo = applicationNo;
      if (docketNo) match.TicketNCIPDocketNo = docketNo;
      if (RequestorMobileNo) match.RequestorMobileNo = RequestorMobileNo;

      pipeline.push({ $match: match });

      const skipCount = (pageIndex - 1) * pageSize;
      pipeline.push({ $skip: skipCount });
      pipeline.push({ $limit: pageSize });

      pipeline.push({
        $project: {
          _id: 0,
          SupportTicketID: 1,
          CallerContactNumber: 1,
          CallingAudioFile: 1,
          TicketRequestorID: 1,
          StateCodeAlpha: 1,
          StateMasterID: 1,
          DistrictMasterID: 1,
          VillageRequestorID: 1,
          NyayPanchayatID: 1,
          NyayPanchayat: 1,
          GramPanchayatID: 1,
          GramPanchayat: 1,
          CallerID: 1,
          CreationMode: 1,
          SupportTicketNo: 1,
          RequestorUniqueNo: 1,
          RequestorName: 1,
          RequestorMobileNo: 1,
          RequestorAccountNo: 1,
          RequestorAadharNo: 1,
          TicketCategoryID: 1,
          CropCategoryOthers: 1,
          CropStageMaster: 1,
          CropStageMasterID: 1,
          TicketHeaderID: 1,
          SupportTicketTypeID: 1,
          RequestYear: 1,
          RequestSeason: 1,
          TicketSourceID: 1,
          TicketDescription: 1,
          LossDate: 1,
          LossTime: 1,
          OnTimeIntimationFlag: 1,
          VillageName: 1,
          ApplicationCropName: 1,
          CropName: 1,
          AREA: 1,
          DistrictRequestorID: 1,
          PostHarvestDate: 1,
          TicketStatusID: 1,
          StatusUpdateTime: 1,
          StatusUpdateUserID: 1,
          ApplicationNo: 1,
          InsuranceCompanyCode: 1,
          InsuranceCompanyID: 1,
          InsurancePolicyNo: 1,
          InsurancePolicyDate: 1,
          InsuranceExpiryDate: 1,
          BankMasterID: 1,
          AgentUserID: 1,
          SchemeID: 1,
          AttachmentPath: 1,
          HasDocument: 1,
          Relation: 1,
          RelativeName: 1,
          SubDistrictID: 1,
          SubDistrictName: 1,
          PolicyPremium: 1,
          PolicyArea: 1,
          PolicyType: 1,
          LandSurveyNumber: 1,
          LandDivisionNumber: 1,
          PlotVillageName: 1,
          PlotDistrictName: 1,
          PlotStateName: 1,
          ApplicationSource: 1,
          CropShare: 1,
          IFSCCode: 1,
          FarmerShare: 1,
          SowingDate: 1,
          CropSeasonName: 1,
          TicketSourceName: 1,
          TicketCategoryName: 1,
          TicketStatus: 1,
          InsuranceCompany: 1,
          Created: 1,
          TicketTypeName: 1,
          StateMasterName: 1,
          DistrictMasterName: 1,
          TicketHeadName: 1,
          BMCGCode: 1,
          BusinessRelationName: 1,
          CropLossDetailID: 1,
          CallingUniqueID: 1,
          CallingInsertUserID: 1,
          CropStage: 1,
          CategoryHeadID: 1,
          TicketReOpenDate: 1,
          Sos: 1,
          IsSos: 1,
          TicketNCIPDocketNo: 1,
          FilterDistrictRequestorID: 1,
          FilterStateID: 1,
          SchemeName: 1,
          InsertUserID: 1,
          InsertDateTime: 1,
          InsertIPAddress: 1,
          UpdateUserID: 1,
          AgentName: 1,
          CreatedBY: 1,
          CallingUserID: 1,
          UpdateDateTime: 1,
          UpdateIPAddress: 1,
          CreatedAt: 1
        }
      });
    }

      console.log('📦 Aggregation Pipeline:', JSON.stringify(pipeline, null, 2));
    try {
      data = await db
        .collection('SLA_Ticket_listing')
        .aggregate(pipeline, { allowDiskUse: true })
        .toArray();
    } catch (err) {
      console.error('❌ Error while querying DB:', err);

    }
    if(data.length == 0){
      data = []
      let message = {
        msg :"Record Not Found",
        code: "0"
      }
       return { data, message };
    }
    
          let obj = {
      supportTicket: data
    };
    message = {
      msg:"Fetched Success",
      code: "1"
    }
    return { obj, message };
  } catch (err) {
    console.log('❌ Top-level error:', err);
    return { data: [], message: 'Unexpected error' };
  }
}

async fetchTicketListingLastWOrking(payload: any) {
  try {
    const db = this.db;
    const {
      fromdate,
      toDate,
      viewTYP,
      supportTicketID,
      ticketCategoryID,
      ticketSourceID,
      supportTicketTypeID,
      supportTicketNo,
      applicationNo,
      docketNo,
      statusID,
      RequestorMobileNo,
      schemeID,
      ticketHeaderID,
      stateID,
      districtID,
      insuranceCompanyID,
      pageIndex = 1,
      pageSize = 100,
      objCommon
    } = payload;
    // this.createIndexesForTicketListing(this.db)
    let pipeline: any[] = [];
    let message = {};
    let data: any = '';

    const Delta = await this.getSupportTicketUserDetail(objCommon.insertedUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];

    if (!item) return { rcode: 0, rmessage: 'User details not found.' };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };

    const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length) {
      locationFilter = { FilterStateID: { $in: StateMasterID } };
    } else if (LocationTypeID === 2 && item.DistrictIDs?.length) {
      locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };
    }

    const match: any = { ...locationFilter };

    if (ticketHeaderID && ticketHeaderID !== 0) {
      match.TicketHeaderID = ticketHeaderID;
    }

    if (insuranceCompanyID && insuranceCompanyID !== 0) {
      const requestedInsuranceIDs = insuranceCompanyID
        .split(',')
        .map(id => Number(id.trim()));
      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter(id =>
        allowedInsuranceIDs.includes(id)
      );

      if (validInsuranceIDs.length === 0) {
        return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
      }

      match.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else {
      if (InsuranceCompanyID?.length) {
        match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
      }
    }

    if (stateID && stateID !== '') {
      const requestedStateIDs = stateID
        .split(',')
        .map(id => Number(id.trim()));
      const validStateIDs = requestedStateIDs.filter(id =>
        StateMasterID.map(Number).includes(id)
      );

      if (validStateIDs.length === 0) {
        return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
      }

      match.FilterStateID = { $in: validStateIDs };
    } else if (StateMasterID?.length && LocationTypeID !== 2) {
      match.FilterStateID = { $in: StateMasterID.map(Number) };
    }

    if (viewTYP === 'FILTER') {
      if (fromdate && toDate) {
        match.Created = {
          $gte: new Date(`${fromdate}T00:00:00.000Z`),
          $lte: new Date(`${toDate}T23:59:59.999Z`)
        };
      }

      if (supportTicketID) match.SupportTicketID = supportTicketID;
      if (ticketCategoryID) match.TicketCategoryID = ticketCategoryID;
      if (ticketSourceID) match.TicketSourceID = ticketSourceID;
      if (supportTicketTypeID) match.SupportTicketTypeID = supportTicketTypeID;
      if (statusID) match.TicketStatusID = statusID;
      if (schemeID) match.SchemeID = schemeID;
      if (ticketHeaderID) match.TicketHeaderID = ticketHeaderID;
      if (stateID) match.StateMasterID = parseInt(stateID);
      if (districtID) match.DistrictMasterID = districtID;
      if (insuranceCompanyID) match.InsuranceCompanyID = insuranceCompanyID;
      if (supportTicketNo) match.SupportTicketNo = supportTicketNo;
      if (applicationNo) match.ApplicationNo = applicationNo;
      if (docketNo) match.TicketNCIPDocketNo = docketNo;
      if (RequestorMobileNo) match.RequestorMobileNo = RequestorMobileNo;
    }

    const totalCount = await db.collection('SLA_Ticket_listing').countDocuments(match);

    pipeline.push({ $match: match });

    const skipCount = (pageIndex - 1) * pageSize;
    pipeline.push({ $skip: skipCount });
    pipeline.push({ $limit: pageSize });

    pipeline.push({
      $project: {
        _id: 0,
        SupportTicketID: 1,
        CallerContactNumber: 1,
        CallingAudioFile: 1,
        TicketRequestorID: 1,
        StateCodeAlpha: 1,
        StateMasterID: 1,
        DistrictMasterID: 1,
        VillageRequestorID: 1,
        NyayPanchayatID: 1,
        NyayPanchayat: 1,
        GramPanchayatID: 1,
        GramPanchayat: 1,
        CallerID: 1,
        CreationMode: 1,
        SupportTicketNo: 1,
        RequestorUniqueNo: 1,
        RequestorName: 1,
        RequestorMobileNo: 1,
        RequestorAccountNo: 1,
        RequestorAadharNo: 1,
        TicketCategoryID: 1,
        CropCategoryOthers: 1,
        CropStageMaster: 1,
        CropStageMasterID: 1,
        TicketHeaderID: 1,
        SupportTicketTypeID: 1,
        RequestYear: 1,
        RequestSeason: 1,
        TicketSourceID: 1,
        TicketDescription: 1,
        LossDate: 1,
        LossTime: 1,
        OnTimeIntimationFlag: 1,
        VillageName: 1,
        ApplicationCropName: 1,
        CropName: 1,
        AREA: 1,
        DistrictRequestorID: 1,
        PostHarvestDate: 1,
        TicketStatusID: 1,
        StatusUpdateTime: 1,
        StatusUpdateUserID: 1,
        ApplicationNo: 1,
        InsuranceCompanyCode: 1,
        InsuranceCompanyID: 1,
        InsurancePolicyNo: 1,
        InsurancePolicyDate: 1,
        InsuranceExpiryDate: 1,
        BankMasterID: 1,
        AgentUserID: 1,
        SchemeID: 1,
        AttachmentPath: 1,
        HasDocument: 1,
        Relation: 1,
        RelativeName: 1,
        SubDistrictID: 1,
        SubDistrictName: 1,
        PolicyPremium: 1,
        PolicyArea: 1,
        PolicyType: 1,
        LandSurveyNumber: 1,
        LandDivisionNumber: 1,
        PlotVillageName: 1,
        PlotDistrictName: 1,
        PlotStateName: 1,
        ApplicationSource: 1,
        CropShare: 1,
        IFSCCode: 1,
        FarmerShare: 1,
        SowingDate: 1,
        CropSeasonName: 1,
        TicketSourceName: 1,
        TicketCategoryName: 1,
        TicketStatus: 1,
        InsuranceCompany: 1,
        Created: 1,
        TicketTypeName: 1,
        StateMasterName: 1,
        DistrictMasterName: 1,
        TicketHeadName: 1,
        BMCGCode: 1,
        BusinessRelationName: 1,
        CropLossDetailID: 1,
        CallingUniqueID: 1,
        CallingInsertUserID: 1,
        CropStage: 1,
        CategoryHeadID: 1,
        TicketReOpenDate: 1,
        Sos: 1,
        IsSos: 1,
        TicketNCIPDocketNo: 1,
        FilterDistrictRequestorID: 1,
        FilterStateID: 1,
        SchemeName: 1,
        InsertUserID: 1,
        InsertDateTime: 1,
        InsertIPAddress: 1,
        UpdateUserID: 1,
        AgentName: 1,
        CreatedBY: 1,
        CallingUserID: 1,
        UpdateDateTime: 1,
        UpdateIPAddress: 1,
        CreatedAt: 1
      }
    });

    data = await db.collection('SLA_Ticket_listing').aggregate(pipeline, { allowDiskUse: true }).toArray();

    if (data.length === 0) {
      return {
        data: [],
        message: {
          msg: "Record Not Found",
          code: "0"
        },
        totalCount: 0,
        totalPages: 0
      };
    }

    const totalPages = Math.ceil(totalCount / pageSize);

    return {
      obj: {
        status:status,
        supportTicket: data,
        
      },
      message: {
        msg: "Fetched Success",
        code: "1"
      }
    };
  } catch (err) {
    console.log('❌ Top-level error:', err);
    return { data: [], message: 'Unexpected error' };
  }
}

async fetchTicketListingQithResolvedHeader(payload: any) {
  try {
    const db = this.db;
    const {
      fromdate,
      toDate,
      viewTYP,
      supportTicketID,
      ticketCategoryID,
      ticketSourceID,
      supportTicketTypeID,
      supportTicketNo,
      applicationNo,
      docketNo,
      statusID,
      RequestorMobileNo,
      schemeID,
      ticketHeaderID,
      stateID,
      districtID,
      insuranceCompanyID,
      pageIndex = 1,
      pageSize = 100,
      objCommon
    } = payload;

    let pipeline: any[] = [];
    let message = {};
    let data: any = '';

    // Get user details
    const Delta = await this.getSupportTicketUserDetail(objCommon.insertedUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];

    if (!item) return { rcode: 0, rmessage: 'User details not found.' };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };

    const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length) {
      locationFilter = { FilterStateID: { $in: StateMasterID } };
    } else if (LocationTypeID === 2 && item.DistrictIDs?.length) {
      locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };
    }

    const match: any = { ...locationFilter };

    if (ticketHeaderID && ticketHeaderID !== 0) {
      match.TicketHeaderID = ticketHeaderID;
    }

    if (insuranceCompanyID && insuranceCompanyID !== 0) {
      const requestedInsuranceIDs = insuranceCompanyID
        .split(',')
        .map(id => Number(id.trim()));
      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter(id =>
        allowedInsuranceIDs.includes(id)
      );

      if (validInsuranceIDs.length === 0) {
        return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
      }

      match.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else {
      if (InsuranceCompanyID?.length) {
        match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
      }
    }

    if (stateID && stateID !== '') {
      const requestedStateIDs = stateID
        .split(',')
        .map(id => Number(id.trim()));
      const validStateIDs = requestedStateIDs.filter(id =>
        StateMasterID.map(Number).includes(id)
      );

      if (validStateIDs.length === 0) {
        return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
      }

      match.FilterStateID = { $in: validStateIDs };
    } else if (StateMasterID?.length && LocationTypeID !== 2) {
      match.FilterStateID = { $in: StateMasterID.map(Number) };
    }

    if (viewTYP === 'FILTER') {
      if (fromdate && toDate) {
        match.Created = {
          $gte: new Date(`${fromdate}T00:00:00.000Z`),
          $lte: new Date(`${toDate}T23:59:59.999Z`)
        };
      }

      if (supportTicketID) match.SupportTicketID = supportTicketID;
      if (ticketCategoryID) match.TicketCategoryID = ticketCategoryID;
      if (ticketSourceID) match.TicketSourceID = ticketSourceID;
      if (supportTicketTypeID) match.SupportTicketTypeID = supportTicketTypeID;
      if (statusID) match.TicketStatusID = statusID;
      if (schemeID) match.SchemeID = schemeID;
      if (ticketHeaderID) match.TicketHeaderID = ticketHeaderID;
      if (stateID) match.StateMasterID = parseInt(stateID);
      if (districtID) match.DistrictMasterID = districtID;
      if (insuranceCompanyID) match.InsuranceCompanyID = insuranceCompanyID;
      if (supportTicketNo) match.SupportTicketNo = supportTicketNo;
      if (applicationNo) match.ApplicationNo = applicationNo;
      if (docketNo) match.TicketNCIPDocketNo = docketNo;
      if (RequestorMobileNo) match.RequestorMobileNo = RequestorMobileNo;
    }

    // Total count for pagination
    const totalCount = await db.collection('SLA_Ticket_listing').countDocuments(match);

    // Prepare the data listing pipeline
    pipeline.push({ $match: match });

    const skipCount = (pageIndex - 1) * pageSize;
    pipeline.push({ $skip: skipCount });
    pipeline.push({ $limit: pageSize });

    pipeline.push({
      $project: {
        _id: 0,
        SupportTicketID: 1,
        CallerContactNumber: 1,
        CallingAudioFile: 1,
        TicketRequestorID: 1,
        StateCodeAlpha: 1,
        StateMasterID: 1,
        DistrictMasterID: 1,
        VillageRequestorID: 1,
        NyayPanchayatID: 1,
        NyayPanchayat: 1,
        GramPanchayatID: 1,
        GramPanchayat: 1,
        CallerID: 1,
        CreationMode: 1,
        SupportTicketNo: 1,
        RequestorUniqueNo: 1,
        RequestorName: 1,
        RequestorMobileNo: 1,
        RequestorAccountNo: 1,
        RequestorAadharNo: 1,
        TicketCategoryID: 1,
        CropCategoryOthers: 1,
        CropStageMaster: 1,
        CropStageMasterID: 1,
        TicketHeaderID: 1,
        SupportTicketTypeID: 1,
        RequestYear: 1,
        RequestSeason: 1,
        TicketSourceID: 1,
        TicketDescription: 1,
        LossDate: 1,
        LossTime: 1,
        OnTimeIntimationFlag: 1,
        VillageName: 1,
        ApplicationCropName: 1,
        CropName: 1,
        AREA: 1,
        DistrictRequestorID: 1,
        PostHarvestDate: 1,
        TicketStatusID: 1,
        StatusUpdateTime: 1,
        StatusUpdateUserID: 1,
        ApplicationNo: 1,
        InsuranceCompanyCode: 1,
        InsuranceCompanyID: 1,
        InsurancePolicyNo: 1,
        InsurancePolicyDate: 1,
        InsuranceExpiryDate: 1,
        BankMasterID: 1,
        AgentUserID: 1,
        SchemeID: 1,
        AttachmentPath: 1,
        HasDocument: 1,
        Relation: 1,
        RelativeName: 1,
        SubDistrictID: 1,
        SubDistrictName: 1,
        PolicyPremium: 1,
        PolicyArea: 1,
        PolicyType: 1,
        LandSurveyNumber: 1,
        LandDivisionNumber: 1,
        PlotVillageName: 1,
        PlotDistrictName: 1,
        PlotStateName: 1,
        ApplicationSource: 1,
        CropShare: 1,
        IFSCCode: 1,
        FarmerShare: 1,
        SowingDate: 1,
        CropSeasonName: 1,
        TicketSourceName: 1,
        TicketCategoryName: 1,
        TicketStatus: 1,
        InsuranceCompany: 1,
        Created: 1,
        TicketTypeName: 1,
        StateMasterName: 1,
        DistrictMasterName: 1,
        TicketHeadName: 1,
        BMCGCode: 1,
        BusinessRelationName: 1,
        CropLossDetailID: 1,
        CallingUniqueID: 1,
        CallingInsertUserID: 1,
        CropStage: 1,
        CategoryHeadID: 1,
        TicketReOpenDate: 1,
        Sos: 1,
        IsSos: 1,
        TicketNCIPDocketNo: 1,
        FilterDistrictRequestorID: 1,
        FilterStateID: 1,
        SchemeName: 1,
        InsertUserID: 1,
        InsertDateTime: 1,
        InsertIPAddress: 1,
        UpdateUserID: 1,
        AgentName: 1,
        CreatedBY: 1,
        CallingUserID: 1,
        UpdateDateTime: 1,
        UpdateIPAddress: 1,
        CreatedAt: 1
      }
    });

    data = await db.collection('SLA_Ticket_listing').aggregate(pipeline, { allowDiskUse: true }).toArray();

    if (data.length === 0) {
      return {
        data: [],
        message: {
          msg: "Record Not Found",
          code: "0"
        },
        totalCount: 0,
        totalPages: 0
      };
    }

    const totalPages = Math.ceil(totalCount / pageSize);

    // -- TicketStatusID to Status name mapping
    const statusMap: Record<number, string> = {
      109301: "Open",
      109302: "In-progress",
      109303: "Resolved",
      109304: "Re-Open"
    };

    // -- Aggregate ticket counts by status with combined "Resolved (Information)"
    const aggPipeline = [
      { $match: match },
      {
        $group: {
          _id: "$TicketStatusID",
          count: { $sum: 1 }
        }
      }
    ];

    const aggResults = await db.collection('SLA_Ticket_listing').aggregate(aggPipeline).toArray();

    let resolvedCount = 0;
    const ticketSummary: { Total: string, TicketStatus: string }[] = [];

    aggResults.forEach(item => {
      const statusID = item._id;
      const count = item.count;

      if (statusID === 109303) {
        resolvedCount += count;  
      } else {
        ticketSummary.push({
          Total: count.toString(),
          TicketStatus: statusMap[statusID] || "Unknown"
        });
      }
    });

    if (ticketHeaderID && ticketHeaderID !== 0) {
      const additionalResolvedCount = await db.collection('SLA_Ticket_listing').countDocuments({
        ...match,
        TicketHeaderID: ticketHeaderID,
        TicketStatusID: { $ne: 109303 }
      });
      resolvedCount += additionalResolvedCount;
    }

    if (resolvedCount > 0) {
      ticketSummary.push({
        Total: resolvedCount.toString(),
        TicketStatus: "Resolved (Information)"
      });
    }

    return {
      obj: {
        ticketSummary,
        // status: statusID,
        supportTicket: data,
        // Add this if you want this summary returned
      },
      message: {
        msg: "Fetched Success",
        code: "1"
      },
      totalCount,
      totalPages
    };

  } catch (err) {
    console.log('❌ Top-level error:', err);
    return { data: [], message: 'Unexpected error' };
  }
}

async fetchTicketListingFirstClassWorking(payload: any) {
  try {
   
    const db = this.db;
     await this.createIndexesForTicketListing(this.db)
    let {
      fromdate,
      toDate,
      viewTYP,
      supportTicketID,
      ticketCategoryID,
      ticketSourceID,
      supportTicketTypeID,
      supportTicketNo,
      applicationNo,
      docketNo,
      statusID,
      RequestorMobileNo,
      schemeID,
      ticketHeaderID,
      stateID,
      districtID,
      insuranceCompanyID,
      pageIndex = 1,
      pageSize = 100,
      objCommon
    } = payload;

    ticketHeaderID = Number(ticketHeaderID);
    ticketCategoryID = Number(ticketCategoryID);
    supportTicketTypeID = Number(supportTicketTypeID);
    statusID = Number(statusID);
    // insuranceCompanyID = Number(insuranceCompanyID)
    schemeID = Number(schemeID);



    let pipeline: any[] = [];
    let message = {};
    let data: any = '';


    if(!objCommon.insertedUserID && objCommon.insertedUserID == ""){
      return {
        data: [],
        message: {
          msg: `User Id is required`,
          code: "0"
        },
       
      };

    }
    const Delta = await this.getSupportTicketUserDetail(objCommon.insertedUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];
    if (!item) return { rcode: 0, rmessage: 'User details not found.' };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID
        ? await this.convertStringToArray(item.InsuranceCompanyID)
        : [],
      StateMasterID: item.StateMasterID
        ? await this.convertStringToArray(item.StateMasterID)
        : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
    };

    console.log(userDetail, "userDetail")

    const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length) {
      locationFilter = { FilterStateID: { $in: StateMasterID } };
    } else if (LocationTypeID === 2 && item.DistrictIDs?.length) {
      locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };
    }

    const match: any = { ...locationFilter };

    if (ticketHeaderID && ticketHeaderID !== 0) {
      match.TicketHeaderID = ticketHeaderID;
    }

    // Insurance company filter
    if (insuranceCompanyID && insuranceCompanyID !== 0) {
      console.log(insuranceCompanyID, "insuranceCompanyID")
      // const requestedInsuranceIDs = insuranceCompanyID
      //   .split(',')
      //   .map(id => Number(id.trim()));
      const requestedInsuranceIDs = String(insuranceCompanyID)
  .split(',')
  .map(id => Number(id.trim()));

      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter(id =>
        allowedInsuranceIDs.includes(id)
      );

      if (validInsuranceIDs.length === 0) {
        return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
      }

      match.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else {
      if (InsuranceCompanyID?.length) {
        match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
      }
    }

   

   if (stateID && stateID !== '') {
  const requestedStateIDs = String(stateID)
    .split(',')
    .map(id => Number(id.trim()));

  const validStateIDs = requestedStateIDs.filter(id =>
    StateMasterID.map(Number).includes(id)
  );

  if (validStateIDs.length === 0) {
    return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
  }

  match.StateMasterID = { $in: validStateIDs };

} else if (StateMasterID?.length && LocationTypeID !== 2) {
  match.StateMasterID = { $in: StateMasterID.map(Number) };
}


    if (viewTYP === 'FILTER') {
      if (fromdate && toDate) {
        match.Created = {
          $gte: new Date(`${fromdate}T00:00:00.000Z`),
          $lte: new Date(`${toDate}T23:59:59.999Z`)
        };
      }
      if (supportTicketID) match.SupportTicketID = supportTicketID;
      if (ticketCategoryID) match.TicketCategoryID = ticketCategoryID;
      if (ticketSourceID) match.TicketSourceID = ticketSourceID;
      if (supportTicketTypeID) match.SupportTicketTypeID = supportTicketTypeID;
      if (statusID) match.TicketStatusID = statusID;
      if (schemeID) match.SchemeID = schemeID;
      if (ticketHeaderID) match.TicketHeaderID = ticketHeaderID;
      // if (stateID) match.StateMasterID = parseInt(stateID);
      if (districtID) match.DistrictMasterID = districtID;
      if (insuranceCompanyID) match.InsuranceCompanyID = insuranceCompanyID;
      if (supportTicketNo) match.SupportTicketNo = supportTicketNo;
      if (applicationNo) match.ApplicationNo = applicationNo;
      if (docketNo) match.TicketNCIPDocketNo = docketNo;
      if (RequestorMobileNo) match.RequestorMobileNo = RequestorMobileNo;
    }

    if(viewTYP === 'MOBILE'){
      if (RequestorMobileNo) match.RequestorMobileNo = RequestorMobileNo;
    }

      if(viewTYP === 'TICKET'){
      if (supportTicketNo) match.SupportTicketNo = supportTicketNo;
  
    }
      if(viewTYP === 'APPNO'){
      if (applicationNo) match.ApplicationNo = applicationNo;
     
    }

    if(viewTYP === 'DOCKT'){
    
      if (docketNo) match.TicketNCIPDocketNo = docketNo;
    }



      console.log(JSON.stringify(match))


    
    const totalCount = await db.collection('SLA_Ticket_listing').countDocuments(match);

    pipeline.push({ $match: match });
      pipeline.push({
      $sort:{
        InsertDateTime:-1
      }
    })

    const skipCount = (pageIndex - 1) * pageSize;
    pipeline.push({ $skip: skipCount });
    pipeline.push({ $limit: pageSize });

   

    pipeline.push({
      $project: {
        _id: 0,
        SupportTicketID: 1,
        CallerContactNumber: 1,
        CallingAudioFile: 1,
        TicketRequestorID: 1,
        StateCodeAlpha: 1,
        StateMasterID: 1,
        DistrictMasterID: 1,
        VillageRequestorID: 1,
        NyayPanchayatID: 1,
        NyayPanchayat: 1,
        GramPanchayatID: 1,
        GramPanchayat: 1,
        CallerID: 1,
        CreationMode: 1,
        SupportTicketNo: 1,
        RequestorUniqueNo: 1,
        RequestorName: 1,
        RequestorMobileNo: 1,
        RequestorAccountNo: 1,
        RequestorAadharNo: 1,
        TicketCategoryID: 1,
        CropCategoryOthers: 1,
        CropStageMaster: 1,
        CropStageMasterID: 1,
        TicketHeaderID: 1,
        SupportTicketTypeID: 1,
        RequestYear: 1,
        RequestSeason: 1,
        TicketSourceID: 1,
        TicketDescription: 1,
        LossDate: 1,
        LossTime: 1,
        OnTimeIntimationFlag: 1,
        VillageName: 1,
        ApplicationCropName: 1,
        CropName: 1,
        AREA: 1,
        DistrictRequestorID: 1,
        PostHarvestDate: 1,
        TicketStatusID: 1,
        StatusUpdateTime: 1,
        StatusUpdateUserID: 1,
        ApplicationNo: 1,
        InsuranceCompanyCode: 1,
        InsuranceCompanyID: 1,
        InsurancePolicyNo: 1,
        InsurancePolicyDate: 1,
        InsuranceExpiryDate: 1,
        BankMasterID: 1,
        AgentUserID: 1,
        SchemeID: 1,
        AttachmentPath: 1,
        HasDocument: 1,
        Relation: 1,
        RelativeName: 1,
        SubDistrictID: 1,
        SubDistrictName: 1,
        PolicyPremium: 1,
        PolicyArea: 1,
        PolicyType: 1,
        LandSurveyNumber: 1,
        LandDivisionNumber: 1,
        PlotVillageName: 1,
        PlotDistrictName: 1,
        PlotStateName: 1,
        ApplicationSource: 1,
        CropShare: 1,
        IFSCCode: 1,
        FarmerShare: 1,
        SowingDate: 1,
        CropSeasonName: 1,
        TicketSourceName: 1,
        TicketCategoryName: 1,
        TicketStatus: 1,
        InsuranceCompany: 1,
        CreatedAt: "$Created",
        TicketTypeName: 1,
        StateMasterName: 1,
        DistrictMasterName: 1,
        TicketHeadName: 1,
        BMCGCode: 1,
        BusinessRelationName: 1,
        CropLossDetailID: 1,
        CallingUniqueID: 1,
        CallingInsertUserID: 1,
        CropStage: 1,
        CategoryHeadID: 1,
        TicketReOpenDate: 1,
        Sos: 1,
        IsSos: 1,
        TicketNCIPDocketNo: 1,
        FilterDistrictRequestorID: 1,
        FilterStateID: 1,
        SchemeName: 1,
        InsertUserID: 1,
        InsertDateTime: 1,
        InsertIPAddress: 1,
        UpdateUserID: 1,
        AgentName: 1,
        CreatedBY: 1,
        CallingUserID: 1,
        UpdateDateTime: 1,
        UpdateIPAddress: 1,
        // CreatedAt: 1
      }
    });

  

    data = await db.collection('SLA_Ticket_listing')
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray();

    if (data.length === 0) {
      return {
        data: [],
        message: {
          msg: "Record Not Found",
          code: "0"
        },
        totalCount: 0,
        totalPages: 0
      };
    }

    const totalPages = Math.ceil(totalCount / pageSize);

   /*  const aggPipelineAllStatuses = [
  {
    $match: {
      ...match,
      TicketStatusID: { $in: [109301, 109302, 109303, 109304] }
    }
  },
  {
    $project: {
      TicketStatusID: 1,
      TicketHeaderID: 1,
      customStatus: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $eq: ["$TicketStatusID", 109303] },
                  { $in: ["$TicketHeaderID", [1, 4]] }
                ]
              },
              then: "Resolved"
            },
            {
              case: {
                $and: [
                  { $eq: ["$TicketStatusID", 109303] },
                  { $eq: ["$TicketHeaderID", 2] }
                ]
              },
              then: "Resolved(Information)"
            },
            {
              case: { $eq: ["$TicketStatusID", 109301] },
              then: "Open"
            },
            {
              case: { $eq: ["$TicketStatusID", 109302] },
              then: "In-Progress"
            },
            {
              case: { $eq: ["$TicketStatusID", 109304] },
              then: "Re-Open"
            }
          ],
          default: "Other"
        }
      }
    }
  },
  {
    $group: {
      _id: "$customStatus",
      count: { $sum: 1 }
    }
  }
];

const ticketStatusResults = await db
  .collection('SLA_Ticket_listing')
  .aggregate(aggPipelineAllStatuses)
  .toArray();

  const ticketSummary = ticketStatusResults.map(item => ({
  Total: item.count.toString(),
  TicketStatus: item._id
})); */


const aggPipelineAllStatuses = [
  {
    $match: match // ✅ Use the same filtered match object
  },
  {
    $project: {
      TicketStatusID: 1,
      TicketHeaderID: 1,
      customStatus: {
        $switch: {
          branches: [
            {
              case: {
                $and: [
                  { $eq: ["$TicketStatusID", 109303] },
                  { $in: ["$TicketHeaderID", [1, 4]] }
                ]
              },
              then: "Resolved"
            },
            {
              case: {
                $and: [
                  { $eq: ["$TicketStatusID", 109303] },
                  { $eq: ["$TicketHeaderID", 2] }
                ]
              },
              then: "Resolved(Information)"
            },
            {
              case: { $eq: ["$TicketStatusID", 109301] },
              then: "Open"
            },
            {
              case: { $eq: ["$TicketStatusID", 109302] },
              then: "In-Progress"
            },
            {
              case: { $eq: ["$TicketStatusID", 109304] },
              then: "Re-Open"
            }
          ],
          default: "Other"
        }
      }
    }
  },
  {
    $group: {
      _id: "$customStatus",
      count: { $sum: 1 }
    }
  }
];

const ticketStatusResults = await db
  .collection('SLA_Ticket_listing')
  .aggregate(aggPipelineAllStatuses)
  .toArray();

const ticketSummary = ticketStatusResults.map(item => ({
  Total: item.count.toString(),
  TicketStatus: item._id
}));


    return {
      obj: {
        status: ticketSummary,
        supportTicket: data,
        
        
      },
      message: {
        msg: "Fetched Success",
        code: "1"
      }
    };
  } catch (err) {
    console.log('❌ Top-level error:', err);
    return { data: [], message: 'Unexpected error' };
  }
}





async fetchTicketListing(payload: any) {
  try {
    const db = this.db;
    await this.createIndexesForTicketListing(db);

    let {
      fromdate,
      toDate,
      viewTYP,
      supportTicketID,
      ticketCategoryID,
      ticketSourceID,
      supportTicketTypeID,
      supportTicketNo,
      applicationNo,
      docketNo,
      statusID,
      RequestorMobileNo,
      schemeID,
      ticketHeaderID,
      stateID,
      districtID,
      insuranceCompanyID,
      pageIndex = 1,
      pageSize = 100,
      objCommon
    } = payload;

    ticketHeaderID = Number(ticketHeaderID);
    ticketCategoryID = Number(ticketCategoryID);
    supportTicketTypeID = Number(supportTicketTypeID);
    statusID = Number(statusID);
    schemeID = Number(schemeID);

    if (!objCommon.insertedUserID && objCommon.insertedUserID == "") {
      return {
        data: [],
        message: { msg: "User Id is required", code: "0" }
      };
    }

    const Delta = await this.getSupportTicketUserDetail(objCommon.insertedUserID);

    // return
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];
    if (!item) return { rcode: 0, rmessage: "User details not found." };


    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID
        ? await this.convertStringToArray(item.InsuranceCompanyID)
        : [],
      StateMasterID: item.StateMasterID
        ? await this.convertStringToArray(item.StateMasterID)
        : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
      FromDay:item?.FromDay,
      EscalationFlag:item?.EscalationFlag
    };

    const { InsuranceCompanyID, StateMasterID, LocationTypeID, FromDay, EscalationFlag} = userDetail;

    let locationFilter: any = {};
    if (LocationTypeID === 1 && StateMasterID?.length) {
      locationFilter = { FilterStateID: { $in: StateMasterID } };
    } else if (LocationTypeID === 2 && item.DistrictIDs?.length) {
      locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };
    }

    const match: any = { ...locationFilter };

    if (ticketHeaderID && ticketHeaderID !== 0) {
      match.TicketHeaderID = ticketHeaderID;
    }

    if (insuranceCompanyID && insuranceCompanyID !== 0) {
      const requestedInsuranceIDs = String(insuranceCompanyID)
        .split(",")
        .map(id => Number(id.trim()));

      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter(id =>
        allowedInsuranceIDs.includes(id)
      );

      if (validInsuranceIDs.length === 0) {
        return { rcode: 0, rmessage: "Unauthorized InsuranceCompanyID(s)." };
      }

      match.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else if (InsuranceCompanyID?.length) {
      match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
    }

    if (stateID && stateID !== "") {
      const requestedStateIDs = String(stateID)
        .split(",")
        .map(id => Number(id.trim()));

      const validStateIDs = requestedStateIDs.filter(id =>
        StateMasterID.map(Number).includes(id)
      );

      if (validStateIDs.length === 0) {
        return { rcode: 0, rmessage: "Unauthorized StateID(s)." };
      }

      match.StateMasterID = { $in: validStateIDs };
    } else if (StateMasterID?.length && LocationTypeID !== 2) {
      match.StateMasterID = { $in: StateMasterID.map(Number) };
    }

    if (viewTYP === "FILTER") {
      if (fromdate && toDate) {
        match.Created = {
          $gte: new Date(`${fromdate}T00:00:00.000Z`),
          $lte: new Date(`${toDate}T23:59:59.999Z`)
        };
      }
      if (supportTicketID) match.SupportTicketID = supportTicketID;
      if (ticketCategoryID) match.TicketCategoryID = ticketCategoryID;
      if (ticketSourceID) match.TicketSourceID = ticketSourceID;
      if (supportTicketTypeID) match.SupportTicketTypeID = supportTicketTypeID;
      if (statusID) match.TicketStatusID = statusID;
      if (schemeID) match.SchemeID = schemeID;
      if (ticketHeaderID) match.TicketHeaderID = ticketHeaderID;
      if (districtID) match.DistrictMasterID = districtID;
      if (insuranceCompanyID) match.InsuranceCompanyID = insuranceCompanyID;
      if (supportTicketNo) match.SupportTicketNo = supportTicketNo;
      if (applicationNo) match.ApplicationNo = applicationNo;
      if (docketNo) match.TicketNCIPDocketNo = docketNo;
      if (RequestorMobileNo) match.RequestorMobileNo = RequestorMobileNo;
    }

    if (viewTYP === "MOBILE" && RequestorMobileNo) {
      match.RequestorMobileNo = RequestorMobileNo;
    }
    if (viewTYP === "TICKET" && supportTicketNo) {
      match.SupportTicketNo = supportTicketNo;
    }
    if (viewTYP === "APPNO" && applicationNo) {
      match.ApplicationNo = applicationNo;
    }
    if (viewTYP === "DOCKT" && docketNo) {
      match.TicketNCIPDocketNo = docketNo;
    }

if (viewTYP === "ESCAL") {
//   if(userDetail.EscalationFlag === "Y"){
//  const fromDay = new Date(userDetail.FromDay + "T00:00:00.000Z");


//   match.TicketStatusID = { $ne: 109303 };
//   match.$expr = {
//     $lte: [
//       {
//         $cond: [
//           { $and: [{ $ne: ["$TicketReOpenDate", null] }, { $ne: ["$TicketReOpenDate", ""] }] },
//           "$TicketReOpenDate", 
//           "$InsertDateTime"    
//         ]
//       },
//       fromDay
//     ]
//   };
//   }else{
//     return {
//         data: [],
//         message: { msg: "Not Authorized For Escalation", code: "0" },
//         totalCount: 0,
//         totalPages: 0
//       };

//   }
 
 match.TicketStatusID = { $eq: 109301 };
}


// if (viewTYP === "ESCAL") {
//   if(userDetail.EscalationFlag === "Y"){
//  const fromDay = new Date(userDetail.FromDay + "T00:00:00.000Z");

//   match.TicketStatusID = { $ne: 109303 };
//   match.$expr = {
//     $lte: [
//       {
//         $cond: [
//           { $and: [{ $ne: ["$TicketReOpenDate", null] }, { $ne: ["$TicketReOpenDate", ""] }] },
//           "$TicketReOpenDate", 
//           "$InsertDateTime"    
//         ]
//       },
//       fromDay
//     ]
//   };
//   }else{
//     return {
//         data: [],
//         message: { msg: "Not Authorized For Escalation", code: "0" },
//         totalCount: 0,
//         totalPages: 0
//       };

//   }
 
// }





if (viewTYP === "DEFESCAL") {

  match.TicketStatusID = { $eq: 109301 };

 
}
    const totalCount = await db.collection("SLA_Ticket_listing").countDocuments(match);
    

    /* const pipeline: any[] = [
      { $match: match },
      { $sort: { InsertDateTime: -1 } },
      { $skip: (pageIndex - 1) * pageSize },
      { $limit: pageSize },
      {
        $project: {
          _id: 0,
          SupportTicketID: 1,
          CallerContactNumber: 1,
          CallingAudioFile: 1,
          TicketRequestorID: 1,
          StateCodeAlpha: 1,
          StateMasterID: 1,
          DistrictMasterID: 1,
          VillageRequestorID: 1,
          NyayPanchayatID: 1,
          NyayPanchayat: 1,
          GramPanchayatID: 1,
          GramPanchayat: 1,
          CallerID: 1,
          CreationMode: 1,
          SupportTicketNo: 1,
          RequestorUniqueNo: 1,
          RequestorName: 1,
          RequestorMobileNo: 1,
          RequestorAccountNo: 1,
          RequestorAadharNo: 1,
          TicketCategoryID: 1,
          CropCategoryOthers: 1,
          CropStageMaster: 1,
          CropStageMasterID: 1,
          TicketHeaderID: 1,
          SupportTicketTypeID: 1,
          RequestYear: 1,
          RequestSeason: 1,
          TicketSourceID: 1,
          TicketDescription: 1,
          LossDate: 1,
          LossTime: 1,
          OnTimeIntimationFlag: 1,
          VillageName: 1,
          ApplicationCropName: 1,
          CropName: 1,
          AREA: 1,
          DistrictRequestorID: 1,
          PostHarvestDate: 1,
          TicketStatusID: 1,
          StatusUpdateTime: 1,
          StatusUpdateUserID: 1,
          ApplicationNo: 1,
          InsuranceCompanyCode: 1,
          InsuranceCompanyID: 1,
          InsurancePolicyNo: 1,
          InsurancePolicyDate: 1,
          InsuranceExpiryDate: 1,
          BankMasterID: 1,
          AgentUserID: 1,
          SchemeID: 1,
          AttachmentPath: 1,
          HasDocument: 1,
          Relation: 1,
          RelativeName: 1,
          SubDistrictID: 1,
          SubDistrictName: 1,
          PolicyPremium: 1,
          PolicyArea: 1,
          PolicyType: 1,
          LandSurveyNumber: 1,
          LandDivisionNumber: 1,
          PlotVillageName: 1,
          PlotDistrictName: 1,
          PlotStateName: 1,
          ApplicationSource: 1,
          CropShare: 1,
          IFSCCode: 1,
          FarmerShare: 1,
          SowingDate: 1,
          CropSeasonName: 1,
          TicketSourceName: 1,
          TicketCategoryName: 1,
          TicketStatus: 1,
          InsuranceCompany: 1,
          CreatedAt: "$Created",
          TicketTypeName: 1,
          StateMasterName: 1,
          DistrictMasterName: 1,
          TicketHeadName: 1,
          BMCGCode: 1,
          BusinessRelationName: 1,
          CropLossDetailID: 1,
          CallingUniqueID: 1,
          CallingInsertUserID: 1,
          CropStage: 1,
          CategoryHeadID: 1,
          TicketReOpenDate: 1,
          Sos: 1,
          IsSos: 1,
          TicketNCIPDocketNo: 1,
          FilterDistrictRequestorID: 1,
          FilterStateID: 1,
          SchemeName: 1,
          InsertUserID: 1,
          InsertDateTime: 1,
          InsertIPAddress: 1,
          UpdateUserID: 1,
          AgentName: 1,
          CreatedBY: 1,
          CallingUserID: 1,
          UpdateDateTime: 1,
          UpdateIPAddress: 1
        }
      }
    ]; */

    const pipeline: any[] = [
  { $match: match },
  { $sort: { InsertDateTime: -1 } },
];

if (pageIndex !== -1) {
  pipeline.push(
    { $skip: (pageIndex - 1) * pageSize },
    { $limit: pageSize }
  );
}

/* pipeline.push({
  $project: {
    _id: 0,
    SupportTicketID: 1,
    CallerContactNumber: 1,
    CallingAudioFile: 1,
    TicketRequestorID: 1,
    StateCodeAlpha: 1,
    StateMasterID: 1,
    DistrictMasterID: 1,
    VillageRequestorID: 1,
    NyayPanchayatID: 1,
    NyayPanchayat: 1,
    GramPanchayatID: 1,
    GramPanchayat: 1,
    CallerID: 1,
    CreationMode: 1,
    SupportTicketNo: 1,
    RequestorUniqueNo: 1,
    RequestorName: 1,
    RequestorMobileNo: 1,
    RequestorAccountNo: 1,
    RequestorAadharNo: 1,
    TicketCategoryID: 1,
    CropCategoryOthers: 1,
    CropStageMaster: 1,
    CropStageMasterID: 1,
    TicketHeaderID: 1,
    SupportTicketTypeID: 1,
    RequestYear: 1,
    RequestSeason: 1,
    TicketSourceID: 1,
    TicketDescription: 1,
    LossDate: 1,
    LossTime: 1,
    OnTimeIntimationFlag: 1,
    VillageName: 1,
    ApplicationCropName: 1,
    CropName: 1,
    AREA: 1,
    DistrictRequestorID: 1,
    PostHarvestDate: 1,
    TicketStatusID: 1,
    StatusUpdateTime: 1,
    StatusUpdateUserID: 1,
    ApplicationNo: 1,
    InsuranceCompanyCode: 1,
    InsuranceCompanyID: 1,
    InsurancePolicyNo: 1,
    InsurancePolicyDate: 1,
    InsuranceExpiryDate: 1,
    BankMasterID: 1,
    AgentUserID: 1,
    SchemeID: 1,
    AttachmentPath: 1,
    HasDocument: 1,
    Relation: 1,
    RelativeName: 1,
    SubDistrictID: 1,
    SubDistrictName: 1,
    PolicyPremium: 1,
    PolicyArea: 1,
    PolicyType: 1,
    LandSurveyNumber: 1,
    LandDivisionNumber: 1,
    PlotVillageName: 1,
    PlotDistrictName: 1,
    PlotStateName: 1,
    ApplicationSource: 1,
    CropShare: 1,
    IFSCCode: 1,
    FarmerShare: 1,
    SowingDate: 1,
    CropSeasonName: 1,
    TicketSourceName: 1,
    TicketCategoryName: 1,
    TicketStatus: 1,
    InsuranceCompany: 1,
    CreatedAt: "$Created",
    TicketTypeName: 1,
    StateMasterName: 1,
    DistrictMasterName: 1,
    TicketHeadName: 1,
    BMCGCode: 1,
    BusinessRelationName: 1,
    CropLossDetailID: 1,
    CallingUniqueID: 1,
    CallingInsertUserID: 1,
    CropStage: 1,
    CategoryHeadID: 1,
    TicketReOpenDate: 1,
    Sos: 1,
    IsSos: 1,
    TicketNCIPDocketNo: 1,
    FilterDistrictRequestorID: 1,
    FilterStateID: 1,
    SchemeName: 1,
    InsertUserID: 1,
    InsertDateTime: 1,
    InsertIPAddress: 1,
    UpdateUserID: 1,
    AgentName: 1,
    CreatedBY: 1,
    CallingUserID: 1,
    UpdateDateTime: 1,
    UpdateIPAddress: 1,
  }
}); */ 
   

pipeline.push({
  $project: {
    _id: 0,
    SupportTicketID: 1,
    CallerContactNumber: 1,
    CallingAudioFile: 1,
    TicketRequestorID: 1,
    StateCodeAlpha: 1,
    StateMasterID: 1,
    DistrictMasterID: 1,
    VillageRequestorID: 1,
    NyayPanchayatID: 1,
    NyayPanchayat: 1,
    GramPanchayatID: 1,
    GramPanchayat: 1,
    CallerID: 1,
    CreationMode: 1,
    SupportTicketNo: 1,
    RequestorUniqueNo: 1,
    RequestorName: 1,
    RequestorMobileNo: 1,
    RequestorAccountNo: 1,
    RequestorAadharNo: 1,
    TicketCategoryID: 1,
    CropCategoryOthers: 1,
    CropStageMaster: 1,
    CropStageMasterID: 1,
    TicketHeaderID: 1,
    SupportTicketTypeID: 1,
    RequestYear: 1,
    RequestSeason: 1,
    TicketSourceID: 1,
    TicketDescription: 1,
    LossDate: 1,
    LossTime: 1,
    OnTimeIntimationFlag: 1,
    VillageName: 1,
    ApplicationCropName: 1,
    CropName: 1,
    AREA: 1,
    DistrictRequestorID: 1,
    PostHarvestDate: 1,
    TicketStatusID: 1,
    StatusUpdateTime: 1,
    StatusUpdateUserID: 1,
    ApplicationNo: 1,
    InsuranceCompanyCode: 1,
    InsuranceCompanyID: 1,
    InsurancePolicyNo: 1,
    InsurancePolicyDate: 1,
    InsuranceExpiryDate: 1,
    BankMasterID: 1,
    AgentUserID: 1,
    SchemeID: 1,
    AttachmentPath: 1,
    HasDocument: 1,
    Relation: 1,
    RelativeName: 1,
    SubDistrictID: 1,
    SubDistrictName: 1,
    PolicyPremium: 1,
    PolicyArea: 1,
    PolicyType: 1,
    LandSurveyNumber: 1,
    LandDivisionNumber: 1,
    PlotVillageName: 1,
    PlotDistrictName: 1,
    PlotStateName: 1,
    ApplicationSource: 1,
    CropShare: 1,
    IFSCCode: 1,
    FarmerShare: 1,

    // ✅ Safe IST conversion
    SowingDate: {
      $cond: {
        if: { $or: [{ $eq: ["$SowingDate", null] }, { $eq: ["$SowingDate", ""] }] },
        then: null,
        else: {
          $dateToString: {
            date: { $toDate: "$SowingDate" },
            format: "%Y-%m-%dT%H:%M:%S",
            timezone: "Asia/Kolkata"
          }
        }
      }
    },

    CropSeasonName: 1,
    TicketSourceName: 1,
    TicketCategoryName: 1,
    TicketStatus: 1,
    InsuranceCompany: 1,

    // ✅ Always converted safely
    CreatedAt: {
      $dateToString: {
        date: { $toDate: "$Created" },
        format: "%Y-%m-%dT%H:%M:%S",
        timezone: "Asia/Kolkata"
      }
    },

    TicketTypeName: 1,
    StateMasterName: 1,
    DistrictMasterName: 1,
    TicketHeadName: 1,
    BMCGCode: 1,
    BusinessRelationName: 1,
    CropLossDetailID: 1,
    CallingUniqueID: 1,
    CallingInsertUserID: 1,
    CropStage: 1,
    CategoryHeadID: 1,

    // ✅ Safe IST conversion
    TicketReOpenDate: {
      $cond: {
        if: { $or: [{ $eq: ["$TicketReOpenDate", null] }, { $eq: ["$TicketReOpenDate", ""] }] },
        then: null,
        else: {
          $dateToString: {
            date: { $toDate: "$TicketReOpenDate" },
            format: "%Y-%m-%dT%H:%M:%S",
            timezone: "Asia/Kolkata"
          }
        }
      }
    },

    Sos: 1,
    IsSos: 1,
    TicketNCIPDocketNo: 1,
    FilterDistrictRequestorID: 1,
    FilterStateID: 1,
    SchemeName: 1,
    InsertUserID: 1,

    // ✅ Safe IST conversion
    InsertDateTime: {
      $cond: {
        if: { $or: [{ $eq: ["$InsertDateTime", null] }, { $eq: ["$InsertDateTime", ""] }] },
        then: null,
        else: {
          $dateToString: {
            date: { $toDate: "$InsertDateTime" },
            format: "%Y-%m-%dT%H:%M:%S",
            timezone: "Asia/Kolkata"
          }
        }
      }
    },

    InsertIPAddress: 1,
    UpdateUserID: 1,
    AgentName: 1,
    CreatedBY: 1,
    CallingUserID: 1,

    // ✅ Safe IST conversion
    UpdateDateTime: {
      $cond: {
        if: { $or: [{ $eq: ["$UpdateDateTime", null] }, { $eq: ["$UpdateDateTime", ""] }] },
        then: null,
        else: {
          $dateToString: {
            date: { $toDate: "$UpdateDateTime" },
            format: "%Y-%m-%dT%H:%M:%S",
            timezone: "Asia/Kolkata"
          }
        }
      }
    },

    UpdateIPAddress: 1,
  }
});






console.log(JSON.stringify(pipeline), "new")
 
    const data = await db.collection("SLA_Ticket_listing").aggregate(pipeline, { allowDiskUse: true }).toArray();

    if (data.length === 0) {
      return {
        data: [],
        message: { msg: "Record Not Found", code: "0" },
        totalCount: 0,
        totalPages: 0
      };
    }

/*     const aggPipelineAllStatuses = [
      { $match: match },
      {
        $project: {
          TicketStatusID: 1,
          TicketHeaderID: 1,
          customStatus: {
            $switch: {
              branches: [
                {
                  case: { $and: [{ $eq: ["$TicketStatusID", 109303] }, { $in: ["$TicketHeaderID", [1, 4]] }] },
                  then: "Resolved"
                },
                {
                  case: { $and: [{ $eq: ["$TicketStatusID", 109303] }, { $eq: ["$TicketHeaderID", 2] }] },
                  then: "Resolved(Information)"
                },
                { case: { $eq: ["$TicketStatusID", 109301] }, then: "Open" },
                { case: { $eq: ["$TicketStatusID", 109302] }, then: "In-Progress" },
                { case: { $eq: ["$TicketStatusID", 109304] }, then: "Re-Open" }
              ],
              default: "Other"
            }
          }
        }
      },
      { $group: { _id: "$customStatus", count: { $sum: 1 } } }
    ]; */

   

//     const aggPipelineAllStatuses: any[] = [
//   { $match: match },
//   {
//     $project: {
//       TicketStatusID: 1,
//       TicketHeaderID: 1,
//       customStatus: {
//         $switch: {
//           branches: [
//             {
//               case: { $and: [{ $eq: ["$TicketStatusID", 109303] }, { $in: ["$TicketHeaderID", [1, 4]] }] },
//               then: "Resolved"
//             },
//             {
//               case: { $and: [{ $eq: ["$TicketStatusID", 109303] }, { $eq: ["$TicketHeaderID", 2] }] },
//               then: "Resolved(Information)"
//             },
//             { case: { $eq: ["$TicketStatusID", 109301] }, then: "Open" },
//             { case: { $eq: ["$TicketStatusID", 109302] }, then: "In-Progress" },
//             { case: { $eq: ["$TicketStatusID", 109304] }, then: "Re-Open" }
//           ],
//           default: "Other"
//         }
//       }
//     }
//   }
// ];

// if(viewTYP === 'DEFESCAL'){
//   aggPipelineAllStatuses.push({
//     $match: { TicketStatusID: 109301 }
//   });
// }

// aggPipelineAllStatuses.push({
//   $group: { _id: "$customStatus", count: { $sum: 1 } }
// });


const aggPipelineAllStatuses: any[] = [
  { $match: match },
  {
    $project: {
      TicketStatusID: 1,
      TicketHeaderID: 1,
      customStatus: {
        $switch: {
          branches: [
            { case: { $and: [{ $eq: ["$TicketStatusID", 109303] }, { $in: ["$TicketHeaderID", [1, 4]] }] }, then: "Resolved" },
            { case: { $and: [{ $eq: ["$TicketStatusID", 109303] }, { $eq: ["$TicketHeaderID", 2] }] }, then: "Resolved(Information)" },
            { case: { $eq: ["$TicketStatusID", 109301] }, then: "Open" },
            { case: { $eq: ["$TicketStatusID", 109302] }, then: "In-Progress" },
            { case: { $eq: ["$TicketStatusID", 109304] }, then: "Re-Open" }
          ],
          default: "Other"
        }
      }
    }
  }
];

if (viewTYP === "DEFESCAL") {
  aggPipelineAllStatuses.push({
    $match: { TicketStatusID: 109301 }
  });
}

if (viewTYP === "ESCAL" && userDetail.EscalationFlag === "Y") {
 /*  aggPipelineAllStatuses.push({
    $match: { TicketStatusID: { $in: [109301, 109302, 109304] }, TicketHeaderID:{$in: [1,4] }}

  }); */
  aggPipelineAllStatuses.push({
    $match: { TicketStatusID: 109301 }
  });
}

aggPipelineAllStatuses.push({
  $group: { _id: "$customStatus", count: { $sum: 1 } }
});



console.log(JSON.stringify(aggPipelineAllStatuses), "aggPipelineAllStatuses")

    const ticketStatusResults = await db.collection("SLA_Ticket_listing").aggregate(aggPipelineAllStatuses).toArray();
    const ticketSummary = ticketStatusResults.map(item => ({
      Total: item.count.toString(),
      TicketStatus: item._id
    }));

    return {
      obj: { status: ticketSummary, supportTicket: data },
      message: { msg: "Fetched Success", code: "1" }
    };
  } catch (err) {
    console.error("❌ Top-level error:", err);
    return { data: [], message: "Unexpected error" };
  }
}




async toIndianTime(dateString: string) {
  const date = new Date(dateString);

  return date.toLocaleString("en-IN", {
    timeZone: "Asia/Kolkata", // IST timezone
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
}







/* 
async fetchTicketListing(payload: any) {
  try {
    const db = this.db;
    const {
      fromdate,
      toDate,
      viewTYP,
      supportTicketID,
      ticketCategoryID,
      ticketSourceID,
      supportTicketTypeID,
      supportTicketNo,
      applicationNo,
      docketNo,
      statusID,
      RequestorMobileNo,
      schemeID,
      ticketHeaderID,
      stateID,
      districtID,
      insuranceCompanyID,
      pageIndex = 1,
      pageSize = 20
    } = payload;

    let pipeline: any[] = [];
    let message = '';

    

    if (viewTYP === 'FILTER') {
      const matchStage: any = {};

      if (fromdate && toDate) {
        matchStage.Created = {
          $gte: new Date(`${fromdate}T00:00:00.000Z`),
          $lte: new Date(`${toDate}T23:59:59.999Z`)
        };
      }

      if (supportTicketID) matchStage.SupportTicketID = supportTicketID;
      if (ticketCategoryID) matchStage.TicketCategoryID = ticketCategoryID;
      if (ticketSourceID) matchStage.TicketSourceID = ticketSourceID;
      if (supportTicketTypeID) matchStage.SupportTicketTypeID = supportTicketTypeID;
      if (statusID) matchStage.TicketStatusID = statusID;
      if (schemeID) matchStage.SchemeID = schemeID;
      if (ticketHeaderID) matchStage.TicketHeaderID = ticketHeaderID;
      if (stateID) matchStage.StateMasterID = parseInt(stateID);
      if (districtID) matchStage.DistrictMasterID = districtID;
      if (insuranceCompanyID) matchStage.InsuranceCompanyID = insuranceCompanyID;

      if (supportTicketNo) matchStage.SupportTicketNo = supportTicketNo;
      if (applicationNo) matchStage.ApplicationNo = applicationNo;
      if (docketNo) matchStage.TicketNCIPDocketNo = docketNo;
      if (RequestorMobileNo) matchStage.RequestorMobileNo = RequestorMobileNo;

      pipeline.push({ $match: matchStage });

      const skipCount = (pageIndex - 1) * pageSize;
      pipeline.push({ $skip: skipCount });
      pipeline.push({ $limit: pageSize });

      pipeline.push({
        $project: {
          _id: 0,
          SupportTicketID: 1,
          CallerContactNumber: 1,
          CallingAudioFile: 1,
          TicketRequestorID: 1,
          StateCodeAlpha: 1,
          StateMasterID: 1,
          DistrictMasterID: 1,
          VillageRequestorID: 1,
          NyayPanchayatID: 1,
          NyayPanchayat: 1,
          GramPanchayatID: 1,
          GramPanchayat: 1,
          CallerID: 1,
          CreationMode: 1,
          SupportTicketNo: 1,
          RequestorUniqueNo: 1,
          RequestorName: 1,
          RequestorMobileNo: 1,
          RequestorAccountNo: 1,
          RequestorAadharNo: 1,
          TicketCategoryID: 1,
          CropCategoryOthers: 1,
          CropStageMaster: 1,
          CropStageMasterID: 1,
          TicketHeaderID: 1,
          SupportTicketTypeID: 1,
          RequestYear: 1,
          RequestSeason: 1,
          TicketSourceID: 1,
          TicketDescription: 1,
          LossDate: 1,
          LossTime: 1,
          OnTimeIntimationFlag: 1,
          VillageName: 1,
          ApplicationCropName: 1,
          CropName: 1,
          AREA: 1,
          DistrictRequestorID: 1,
          PostHarvestDate: 1,
          TicketStatusID: 1,
          StatusUpdateTime: 1,
          StatusUpdateUserID: 1,
          ApplicationNo: 1,
          InsuranceCompanyCode: 1,
          InsuranceCompanyID: 1,
          InsurancePolicyNo: 1,
          InsurancePolicyDate: 1,
          InsuranceExpiryDate: 1,
          BankMasterID: 1,
          AgentUserID: 1,
          SchemeID: 1,
          AttachmentPath: 1,
          HasDocument: 1,
          Relation: 1,
          RelativeName: 1,
          SubDistrictID: 1,
          SubDistrictName: 1,
          PolicyPremium: 1,
          PolicyArea: 1,
          PolicyType: 1,
          LandSurveyNumber: 1,
          LandDivisionNumber: 1,
          PlotVillageName: 1,
          PlotDistrictName: 1,
          PlotStateName: 1,
          ApplicationSource: 1,
          CropShare: 1,
          IFSCCode: 1,
          FarmerShare: 1,
          SowingDate: 1,
          CropSeasonName: 1,
          TicketSourceName: 1,
          TicketCategoryName: 1,
          TicketStatus: 1,
          InsuranceCompany: 1,
          Created: 1,
          TicketTypeName: 1,
          StateMasterName: 1,
          DistrictMasterName: 1,
          TicketHeadName: 1,
          BMCGCode: 1,
          BusinessRelationName: 1,
          CropLossDetailID: 1,
          CallingUniqueID: 1,
          CallingInsertUserID: 1,
          CropStage: 1,
          CategoryHeadID: 1,
          TicketReOpenDate: 1,
          Sos: 1,
          IsSos: 1,
          TicketNCIPDocketNo: 1,
          FilterDistrictRequestorID: 1,
          FilterStateID: 1,
          SchemeName: 1,
          InsertUserID: 1,
          InsertDateTime: 1,
          InsertIPAddress: 1,
          UpdateUserID: 1,
          AgentName: 1,
          CreatedBY: 1,
          CallingUserID: 1,
          UpdateDateTime: 1,
          UpdateIPAddress: 1,
          CreatedAt: 1
        }
      });
    }

    console.log('📦 Aggregation Pipeline:', JSON.stringify(pipeline, null, 2));

    let data: any = [];
    try {
      data = await db
        .collection('SLA_Ticket_listing')
        .aggregate(pipeline, { allowDiskUse: true })
        .toArray();
    } catch (err) {
      console.error('❌ Error while querying DB:', err);
    }

    return { data, message };

  } catch (err) {
    console.log('❌ Top-level error:', err);
    return { data: [], message: 'Unexpected error' };
  }
}
 */


/* async  createIndexesForTicketListing(db: any) {
  try {
    const collection = db.collection('SLA_Ticket_listing');

    await collection.createIndex({
      FilterStateID: 1,
      InsuranceCompanyID: 1,
      TicketHeaderID: 1,
      Created: -1,
    }, { name: 'idx_state_insurance_ticketheader_created' });

    await collection.createIndex({ SupportTicketID: 1 }, { name: 'idx_supportTicketID' });

    await collection.createIndex({ TicketStatusID: 1 }, { name: 'idx_ticketStatusID' });


    console.log('Indexes created successfully');
  } catch (err) {
    console.error('Error creating indexes:', err);
  }
} */

async  createIndexesForTicketListingxd(db: any) {
  try {
    const collection = db.collection('SLA_Ticket_listing');
    const allIndexes = await collection.indexes();

    // === 1. Index used for Date range filters (DO NOT REMOVE) ===
    const createdIndexName = 'idx_created_ticketheader_insurance_filterstate_statemaster';
    const createdIndexKey = {
      Created: 1,
      TicketHeaderID: 1,
      InsuranceCompanyID: 1,
      FilterStateID: 1,
      StateMasterID: 1
    };

    const createdIndexExists = allIndexes.some(idx =>
      idx.name === createdIndexName &&
      JSON.stringify(idx.key) === JSON.stringify(createdIndexKey)
    );

    if (!createdIndexExists) {
      // Do NOT drop this — it’s in use elsewhere
      await collection.createIndex(createdIndexKey, { name: createdIndexName });
      console.log(`Created index: ${createdIndexName}`);
    } else {
      console.log(`Index ${createdIndexName} already exists.`);
    }

    // === 2. New Index for queries using RequestorMobileNo ===
    const mobileIndexName = 'idx_mobile_ticketheader_insurance_filterstate_statemaster';
    const mobileIndexKey = {
      RequestorMobileNo: 1,
      TicketHeaderID: 1,
      InsuranceCompanyID: 1,
      FilterStateID: 1,
      StateMasterID: 1
    };

    const mobileIndexExists = allIndexes.some(idx =>
      idx.name === mobileIndexName &&
      JSON.stringify(idx.key) === JSON.stringify(mobileIndexKey)
    );

    if (!mobileIndexExists) {
      await collection.createIndex(mobileIndexKey, { name: mobileIndexName });
      console.log(`Created index: ${mobileIndexName}`);
    } else {
      console.log(`Index ${mobileIndexName} already exists.`);
    }

    // === 3. Other single-field indexes ===
    if (!allIndexes.some(idx => idx.name === 'idx_supportTicketID')) {
      await collection.createIndex({ SupportTicketID: 1 }, { name: 'idx_supportTicketID' });
      console.log('Created index: idx_supportTicketID');
    }

    if (!allIndexes.some(idx => idx.name === 'idx_ticketStatusID')) {
      await collection.createIndex({ TicketStatusID: 1 }, { name: 'idx_ticketStatusID' });
      console.log('Created index: idx_ticketStatusID');
    }

    console.log('✅ Index setup completed successfully.');
  } catch (err) {
    console.error('❌ Error creating indexes:', err);
  }
}


/* async createIndexesForTicketListing(db: any) {
  try {
    const collection = db.collection('SLA_Ticket_listing');
    const allIndexes = await collection.indexes();

    // === 1. idx_created_ticketheader_insurance_filterstate_statemaster ===
    const createdIndexName = 'idx_created_ticketheader_insurance_filterstate_statemaster';
    const createdIndexKey = {
      Created: 1,
      TicketHeaderID: 1,
      InsuranceCompanyID: 1,
      FilterStateID: 1,
      StateMasterID: 1
    };
    if (!allIndexes.some(idx => idx.name === createdIndexName && JSON.stringify(idx.key) === JSON.stringify(createdIndexKey))) {
      await collection.createIndex(createdIndexKey, { name: createdIndexName });
      console.log(`Created index: ${createdIndexName}`);
    } else console.log(`Index ${createdIndexName} already exists.`);

    // === 2. idx_mobile_ticketheader_insurance_filterstate_statemaster ===
    const mobileIndexName = 'idx_mobile_ticketheader_insurance_filterstate_statemaster';
    const mobileIndexKey = {
      RequestorMobileNo: 1,
      TicketHeaderID: 1,
      InsuranceCompanyID: 1,
      FilterStateID: 1,
      StateMasterID: 1
    };
    if (!allIndexes.some(idx => idx.name === mobileIndexName && JSON.stringify(idx.key) === JSON.stringify(mobileIndexKey))) {
      await collection.createIndex(mobileIndexKey, { name: mobileIndexName });
      console.log(`Created index: ${mobileIndexName}`);
    } else console.log(`Index ${mobileIndexName} already exists.`);

    // === 3. idx_supportTicketID ===
    // if (!allIndexes.some(idx => idx.name === 'idx_supportTicketID')) {
    //   await collection.createIndex({ SupportTicketID: 1 }, { name: 'idx_supportTicketID' });
    //   console.log('Created index: idx_supportTicketID');
    // }

    // === 4. idx_ticketStatusID ===
    if (!allIndexes.some(idx => idx.name === 'idx_ticketStatusID')) {
      await collection.createIndex({ TicketStatusID: 1 }, { name: 'idx_ticketStatusID' });
      console.log('Created index: idx_ticketStatusID');
    }

    // === 5. InsertDateTime_1 ===
    if (!allIndexes.some(idx => idx.name === 'InsertDateTime_1')) {
      await collection.createIndex({ InsertDateTime: 1 }, { name: 'InsertDateTime_1' });
      console.log('Created index: InsertDateTime_1');
    }

    // === 6. SupportTicketNo_1 ===
    if (!allIndexes.some(idx => idx.name === 'SupportTicketNo_1')) {
      await collection.createIndex({ SupportTicketNo: 1 }, { name: 'SupportTicketNo_1' });
      console.log('Created index: SupportTicketNo_1');
    }

    // === 7. ApplicationNo_1 ===
    if (!allIndexes.some(idx => idx.name === 'ApplicationNo_1')) {
      await collection.createIndex({ ApplicationNo: 1 }, { name: 'ApplicationNo_1' });
      console.log('Created index: ApplicationNo_1');
    }

    // === 8. TicketNCIPDocketNo_1 ===
    if (!allIndexes.some(idx => idx.name === 'TicketNCIPDocketNo_1')) {
      await collection.createIndex({ TicketNCIPDocketNo: 1 }, { name: 'TicketNCIPDocketNo_1' });
      console.log('Created index: TicketNCIPDocketNo_1');
    }

    console.log('✅ Index setup completed successfully.');
  } catch (err) {
    console.error('❌ Error creating indexes:', err);
  }
}
 */


async createIndexesForTicketListing(db: any) {
  try {
    const collection = db.collection('SLA_Ticket_listing');
    const allIndexes = await collection.indexes();

    // === 1. idx_created_ticketheader_insurance_filterstate_statemaster ===
    const createdIndexName = 'idx_created_ticketheader_insurance_filterstate_statemaster';
    const createdIndexKey = {
      Created: 1,
      TicketHeaderID: 1,
      InsuranceCompanyID: 1,
      FilterStateID: 1,
      StateMasterID: 1
    };
    if (!allIndexes.some(idx => idx.name === createdIndexName && JSON.stringify(idx.key) === JSON.stringify(createdIndexKey))) {
      await collection.createIndex(createdIndexKey, { name: createdIndexName });
      console.log(`Created index: ${createdIndexName}`);
    } else console.log(`Index ${createdIndexName} already exists.`);

    // === 2. idx_mobile_ticketheader_insurance_filterstate_statemaster ===
    const mobileIndexName = 'idx_mobile_ticketheader_insurance_filterstate_statemaster';
    const mobileIndexKey = {
      RequestorMobileNo: 1,
      TicketHeaderID: 1,
      InsuranceCompanyID: 1,
      FilterStateID: 1,
      StateMasterID: 1
    };
    if (!allIndexes.some(idx => idx.name === mobileIndexName && JSON.stringify(idx.key) === JSON.stringify(mobileIndexKey))) {
      await collection.createIndex(mobileIndexKey, { name: mobileIndexName });
      console.log(`Created index: ${mobileIndexName}`);
    } else console.log(`Index ${mobileIndexName} already exists.`);

    // === 3. idx_supportTicketID ===
    // if (!allIndexes.some(idx => idx.name === 'idx_supportTicketID')) {
    //   await collection.createIndex({ SupportTicketID: 1 }, { name: 'idx_supportTicketID' });
    //   console.log('Created index: idx_supportTicketID');
    // }

    // === 4. idx_ticketStatusID ===
    if (!allIndexes.some(idx => idx.name === 'idx_ticketStatusID')) {
      await collection.createIndex({ TicketStatusID: 1 }, { name: 'idx_ticketStatusID' });
      console.log('Created index: idx_ticketStatusID');
    }

    // === 5. InsertDateTime_1 ===
    if (!allIndexes.some(idx => idx.name === 'InsertDateTime_1')) {
      await collection.createIndex({ InsertDateTime: 1 }, { name: 'InsertDateTime_1' });
      console.log('Created index: InsertDateTime_1');
    }

    // === 6. SupportTicketNo_1 ===
    if (!allIndexes.some(idx => idx.name === 'SupportTicketNo_1')) {
      await collection.createIndex({ SupportTicketNo: 1 }, { name: 'SupportTicketNo_1' });
      console.log('Created index: SupportTicketNo_1');
    }

    // === 7. ApplicationNo_1 ===
    if (!allIndexes.some(idx => idx.name === 'ApplicationNo_1')) {
      await collection.createIndex({ ApplicationNo: 1 }, { name: 'ApplicationNo_1' });
      console.log('Created index: ApplicationNo_1');
    }

    // === 8. TicketNCIPDocketNo_1 ===
    if (!allIndexes.some(idx => idx.name === 'TicketNCIPDocketNo_1')) {
      await collection.createIndex({ TicketNCIPDocketNo: 1 }, { name: 'TicketNCIPDocketNo_1' });
      console.log('Created index: TicketNCIPDocketNo_1');
    }

    // === 9. escal index for aggregation queries ===
    const escalIndexName = 'idx_escal_full';
    const escalIndexKey = {
      FilterStateID: 1,
      InsuranceCompanyID: 1,
      StateMasterID: 1,
      TicketStatusID: 1,
      TicketHeaderID: 1,
      TicketReOpenDate: 1,
      InsertDateTime: -1
    };
    if (!allIndexes.some(idx => idx.name === escalIndexName && JSON.stringify(idx.key) === JSON.stringify(escalIndexKey))) {
      await collection.createIndex(escalIndexKey, { name: escalIndexName });
      console.log(`Created index: ${escalIndexName}`);
    } else console.log(`Index ${escalIndexName} already exists.`);

    console.log('✅ Index setup completed successfully.');
  } catch (err) {
    console.error('❌ Error creating indexes:', err);
  }
}


}



