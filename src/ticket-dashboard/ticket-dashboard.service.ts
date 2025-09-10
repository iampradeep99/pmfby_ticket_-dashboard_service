import { Injectable, Inject } from '@nestjs/common';
import * as streamBuffers from 'stream-buffers';
import { Db, Collection } from 'mongodb';
import * as NodeCache from 'node-cache';
import axios from 'axios'
import {UtilService} from "../commonServices/utilService";
import * as fs from 'fs-extra';
import * as path from 'path';
import * as archiver from 'archiver';
import { RedisWrapper } from '../commonServices/redisWrapper';
const XLSX = require('xlsx');
// const ExcelJS = require('exceljs');
import * as ExcelJS from 'exceljs'
import { MailService } from '../mail/mail.service';
import {generateSupportTicketEmailHTML,getCurrentFormattedDateTime} from '../templates/mailTemplates'
import {GCPServices} from '../commonServices/GCSFileUpload'
import { format } from '@fast-csv/format';


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



  async getSupportTicketUserDetail(userID)
  {
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

  async  convertStringToArray(str) {
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

async getSupportTicketHistotReport(ticketPayload: any): Promise<{ data: any[], message: string, pagination:any }> {
  const result = await this.processTicketHistoryView(ticketPayload);
  return {
    data: result.data,
    message: result.rmessage || 'Success',
    pagination :result?.pagination
  };
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
  this.AddIndex(db);

  if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
  if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };

  const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
  const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;
  if (cachedData) {
    return {
      rcode: 1,
      rmessage: 'Success',
      data: cachedData.data,
      pagination: cachedData.pagination,
    };
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
    .map(id => Number(id.trim())); // convert to integer

  const allowedInsuranceIDs = InsuranceCompanyID.map(Number); // from user profile (ensure integers)

  const validInsuranceIDs = requestedInsuranceIDs.filter(id =>
    allowedInsuranceIDs.includes(id)
  );

  if (validInsuranceIDs.length === 0) {
    return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
  }

  match.InsuranceCompanyID = { $in: validInsuranceIDs };
} else {
  // If #ALL, limit to allowed insurance companies
  if (InsuranceCompanyID?.length) {
    match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) }; // force integers
  }
}


  // STATE FILTER
if (SPStateID && SPStateID !== '#ALL') {
  const requestedStateIDs = SPStateID
    .split(',')
    .map(id => Number(id.trim())); // convert to number

  const validStateIDs = requestedStateIDs.filter(id =>
    StateMasterID.map(Number).includes(id) // compare as numbers
  );

  if (validStateIDs.length === 0) {
    return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
  }

  match.FilterStateID = { $in: validStateIDs };
} else if (StateMasterID?.length && LocationTypeID !== 2) {
  match.FilterStateID = { $in: StateMasterID.map(Number) }; // ensure integers
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

  console.log(match);
  // return; // Uncomment if you want to debug only

  const totalCount = await db.collection('SLA_KRPH_SupportTickets_Records').countDocuments(match);
  const totalPages = Math.ceil(totalCount / limit);

  // const pipeline: any[] = [
  //   { $match: match },

  //   {
  //     $lookup: {
  //       from: 'SLA_KRPH_SupportTicketsHistory_Records',
  //       let: { ticketId: '$SupportTicketID' },
  //       pipeline: [
  //         {
  //           $match: {
  //             $expr: {
  //               $and: [
  //                 { $eq: ['$SupportTicketID', '$$ticketId'] },
  //                 { $eq: ['$TicketStatusID', 109304] }
  //               ]
  //             }
  //           }
  //         },
  //         { $sort: { TicketHistoryID: -1 } },
  //         { $limit: 1 }
  //       ],
  //       as: 'ticketHistory',
  //     },
  //   },

  //   {
  //     $lookup: {
  //       from: 'support_ticket_claim_intimation_report_history',
  //       localField: 'SupportTicketNo',
  //       foreignField: 'SupportTicketNo',
  //       as: 'claimInfo',
  //     },
  //   },

  //   {
  //     $lookup: {
  //       from: 'csc_agent_master',
  //       localField: 'InsertUserID',
  //       foreignField: 'UserLoginID',
  //       as: 'agentInfo',
  //     },
  //   },
     

  //   { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
  //   { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
  //   { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },

  //   {
  //     $project: {
  //       SupportTicketID: 1,
        
  //       ApplicationNo: 1,
  //       InsurancePolicyNo: 1,
  //       TicketStatusID: 1,
  //       TicketStatus: 1,
  //       CallerContactNumber: 1,
  //       RequestorName: 1,
  //       RequestorMobileNo: 1,
  //       StateMasterName: 1,
  //       DistrictMasterName: 1,
  //       SubDistrictName: 1,
  //       TicketHeadName: 1,
  //       TicketCategoryName: 1,
  //       RequestSeason: 1,
  //       RequestYear: 1,
  //       ApplicationCropName: 1,
  //       Relation: 1,
  //       RelativeName: 1,
  //       PolicyPremium: 1,
  //       PolicyArea: 1,
  //       PolicyType: 1,
  //       LandSurveyNumber: 1,
  //       LandDivisionNumber: 1,
  //       IsSos: 1,
  //       PlotStateName: 1,
  //       PlotDistrictName: 1,
  //       PlotVillageName: 1,
  //       ApplicationSource: 1,
  //       CropShare: 1,
  //       IFSCCode: 1,
  //       FarmerShare: 1,
  //       SowingDate: 1,
  //       LossDate: 1,
  //       CreatedBY: 1,
  //       CreatedAt: "$InsertDateTime",
  //       Sos: 1,
  //       NCIPDocketNo: "$TicketNCIPDocketNo",
  //       TicketDescription: 1,
  //       CallingUniqueID: 1,
  //       TicketDate: {
  //         $dateToString: {
  //           format: "%Y-%m-%d %H:%M:%S",
  //           date: "$Created"
  //         }
  //       },
  //       StatusDate: {
  //         $dateToString: {
  //           format: "%Y-%m-%d %H:%M:%S",
  //           date: "$StatusUpdateTime"
  //         }
  //       },
  //       SupportTicketTypeName: "$TicketTypeName",
  //       SupportTicketNo: 1,
  //       InsuranceMasterName: "$InsuranceCompany",
  //       ReOpenDate: "$TicketReOpenDate",
  //       CallingUserID: "$agentInfo.UserID",
  //       SchemeName: 1,
  //     }
  //   },

  //   { $skip: (page - 1) * limit },
  //   { $limit: limit },
  // ];

const pipeline: any[] = [
  { $match: match },

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
                { $eq: ['$TicketStatusID', 109304] },
              ],
            },
          },
        },
        { $sort: { TicketHistoryID: -1 } },
        { $limit: 1 },
      ],
      as: 'ticketHistory',
    },
  },

  {
    $lookup: {
      from: 'support_ticket_claim_intimation_report_history',
      localField: 'SupportTicketNo',
      foreignField: 'SupportTicketNo',
      as: 'claimInfo',
    },
  },

  {
    $lookup: {
      from: 'csc_agent_master',
      localField: 'InsertUserID',
      foreignField: 'UserLoginID',
      as: 'agentInfo',
    },
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
  // { $unwind: { path: '$ticket_comment_journey', preserveNullAndEmptyArrays: true } },

   {
    $addFields: {
      ticket_comment_journey: { $ifNull: ['$ticket_comment_journey', []] }
    }
  },

  {
    $project: {
      SupportTicketID: 1,
      // TicketComments: {
      //   $arrayToObject: {
      //     $map: {
      //       input: '$ticket_comment_journey',
      //       as: 'comment',
      //       in: {
      //         k: {
      //           $concat: [
      //             'Comment (',
      //             {
      //               $dateToString: {
      //                 format: '%Y-%m-%d',
      //                 date: '$$comment.ResolvedDate',
      //               },
      //             },
      //             ')',
      //           ],
      //         },
      //         v: '$$comment.ResolvedComment',
      //       },
      //     },
      //   },
      // },
      ticket_comment_journey:1,
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
      CreatedAt: '$InsertDateTime',
      Sos: 1,
      NCIPDocketNo: '$TicketNCIPDocketNo',
      TicketDescription: 1,
      CallingUniqueID: 1,
      TicketDate: {
        $dateToString: {
          format: '%Y-%m-%d %H:%M:%S',
          date: '$Created',
        },
      },
      StatusDate: {
        $dateToString: {
          format: '%Y-%m-%d %H:%M:%S',
          date: '$StatusUpdateTime',
        },
      },
      SupportTicketTypeName: '$TicketTypeName',
      SupportTicketNo: 1,
      InsuranceMasterName: '$InsuranceCompany',
      ReOpenDate: '$TicketReOpenDate',
      CallingUserID: '$agentInfo.UserID',
      SchemeName: 1,
    },
  },

  {
    $project:{
      "_id":0,
      "Agent ID" : "$CallingUserID",
      "Calling ID":"$CallingUniqueID",
      "NCIP Docket No" : "$NCIPDocketNo",
      "Ticket No":"$SupportTicketNo",
      "Creation Date":"$CreatedAt",
      "Re-Open Date":"$ReOpenDate",
      "Ticket Status":"$TicketStatus",
      "Status Date":"$StatusDate",
      "State":"$StateMasterName",
      "District":"$DistrictMasterName",
      "Type" :"$TicketHeadName",
      "Category":"$SupportTicketTypeName",
      "Sub Category":"$TicketCategoryName",
      "Season":"$RequestSeason",
      "Year":"$RequestYear",
      "Insurance Company":"$InsuranceMasterName",
      "Application No":"$ApplicationNo",
      "Policy No":"$InsurancePolicyNo",
      "Caller Mobile No":"$CallerContactNumber",
      "Farmer Name":"$RequestorName",
      "Mobile No":"$RequestorMobileNo",
      "Created By":"$CreatedBY",
      "Description":"$TicketDescription",
      // "TicketComments":"$TicketComments",
      "ticket_comment_journey":"$ticket_comment_journey"
    }
  },

  { $skip: (page - 1) * limit },
  { $limit: limit },
];


 
  let results = await db.collection('SLA_KRPH_SupportTickets_Records')
    .aggregate(pipeline, { allowDiskUse: true })
    .toArray();
    if(results.length === 0){
        return {
    rcode: 1,
    rmessage: 'Success',
    data: results,
    pagination: null,
  };
    }

  results = Array.isArray(results) ? results : [results];
  console.log(results[0].ticket_comment_journey)

  
results.forEach(doc => {
  if (Array.isArray(doc.ticket_comment_journey)) {
    const journey = doc.ticket_comment_journey;

    // if (journey.length > 0) {
      journey.forEach((commentObj, index) => {
        const commentDate = this.formatToDDMMYYYY(commentObj.ResolvedDate);

        // Clean the comment text by removing HTML tags
        const rawComment = commentObj.ResolvedComment || '';
        const cleanComment = rawComment.replace(/<\/?[^>]+(>|$)/g, '').trim();

        doc[`Comment Date ${index + 1}`] = commentDate;
        doc[`Comment ${index + 1}`] = cleanComment;
      });
    // } 

    delete doc.ticket_comment_journey;
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

  await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);

  return {
    rcode: 1,
    rmessage: 'Success',
    data: results,
    pagination: responsePayload.pagination,
  };
}




async processTicketHistoryAndGenerateZipWithoutChunkworking(ticketPayload: any) {
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

  async function processDateRecursive(currentDate: Date, endDate: Date) {
    if (currentDate > endDate) return;

    const startOfDay = new Date(currentDate);
    startOfDay.setUTCHours(0, 0, 0, 0);
    const endOfDay = new Date(currentDate);
    endOfDay.setUTCHours(23, 59, 59, 999);

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
      { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
      { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
      { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
      {
        $project: {
          agentInfo: 1,
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
      }
    ];

    const docs = await db.collection('SLA_KRPH_SupportTickets_Records').aggregate(pipeline, { allowDiskUse: true }).toArray();

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

    const nextDate = new Date(currentDate);
    nextDate.setDate(nextDate.getDate() + 1);
    await processDateRecursive(nextDate, endDate);
  }

  await processDateRecursive(new Date(SPFROMDATE), new Date(SPTODATE));

  const excelFileName = `support_ticket_data_${Date.now()}.xlsx`;
  const excelFilePath = path.join(folderPath, excelFileName);

  await workbook.xlsx.writeFile(excelFilePath);
  console.log(`Excel file created at: ${excelFilePath}`);

  // --- ZIP Excel File ---
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

  // --- Upload to GCP ---
  const gcpService = new GCPServices();
  const fileBuffer = await fs.promises.readFile(zipFilePath);
  const uploadResult = await gcpService.uploadFileToGCP({
    filePath: 'krph/reports/',
    uploadedBy: 'KRPH',
    file: { buffer: fileBuffer, originalname: zipFileName },
  });
  const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
  if (gcpDownloadUrl) await fs.promises.unlink(zipFilePath).catch(console.error);

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

  const responsePayload = {
    data: [], pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
    downloadUrl: gcpDownloadUrl
  };

  const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);
  try {
    await this.mailService.sendMail({ to: userEmail, subject: 'Support Ticket History Report Download Service', text: 'Support Ticket History Report', html: supportTicketTemplate });
    console.log("Mail sent successfully");
  } catch (err) { console.error(`Failed to send email to ${userEmail}:`, err); }

  await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);

  // return responsePayload;
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

   await this.insertOrUpdateDownloadLog(SPUserID,SPInsuranceCompanyID,SPStateID,SPTicketHeaderID,SPFROMDATE,SPTODATE,"","",this.db)

  
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
  "Creation Date":"$Created",
  "Re-Open Date": "$TicketReOpenDate",
  "Ticket Status": "$TicketStatus",
  "Status Date":  "$StatusUpdateTime" ,
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
  "TicketComments":"$TicketComments"
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
   await this.insertOrUpdateDownloadLog(SPUserID,SPInsuranceCompanyID,SPStateID,SPTicketHeaderID,SPFROMDATE,SPTODATE,zipFileName,gcpDownloadUrl,this.db)

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
//         {
//           $project: {
//   "Agent ID": "$AgentID",                    
//   "Calling ID": "$CallingUniqueID",
//   "NCIP Docket No": "$TicketNCIPDocketNo",
//   "Ticket No": "$SupportTicketNo",
//   "Creation Date":"$Created",
//   "Re-Open Date": "$TicketReOpenDate",
//   "Ticket Status": "$TicketStatus",
//   "Status Date":  "$StatusUpdateTime" ,
//   "State": "$StateMasterName",
//   "District": "$DistrictMasterName",
//   "Sub District": "$SubDistrictName",
//   "Type": "$TicketHeadName",
//   "Category": "$TicketTypeName",
//   "Sub Category": "$TicketCategoryName",
//   "Season": "$CropSeasonName",
//   "Year": "$RequestYear",
//   "Insurance Company": "$InsuranceCompany",
//   "Application No": "$ApplicationNo",
//   "Policy No": "$InsurancePolicyNo",
//   "Caller Mobile No": "$CallerContactNumber",
//   "Farmer Name": "$RequestorName",
//   "Mobile No": "$RequestorMobileNo",
//   "Relation": "$Relation",
//   "Relative Name": "$RelativeName",
//   "Policy Premium": "$PolicyPremium",
//   "Policy Area": "$PolicyArea",
//   "Policy Type": "$PolicyType",
//   "Land Survey Number": "$LandSurveyNumber",
//   "Land Division Number": "$LandDivisionNumber",
//   "Plot State": "$PlotStateName",
//   "Plot District": "$PlotDistrictName",
//   "Plot Village": "$PlotVillageName",
//   "Application Source": "$ApplicationSource",
//   "Crop Share": "$CropShare",
//   "IFSC Code": "$IFSCCode",
//   "Farmer Share": "$FarmerShare",
//   "Sowing Date": "$SowingDate",
//   "Created By": "$CreatedBY",
//   "Description": "$TicketDescription",
//   "TicketComments":"$TicketComments"
// }


//         }
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

  // finalize streaming workbook
  await workbook.commit();
  console.log(`Excel file created at: ${excelFilePath}`);

  // ZIP + upload to GCP (same as original code)
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
    zipFileName:zipFileName
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


async insertOrGetDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, db) {
  const existingLog = await db.collection('support_ticket_download_logs').findOne({
    userId: SPUserID,
    insuranceCompanyId: SPInsuranceCompanyID,
    stateId: SPStateID,
    ticketHeaderId: SPTicketHeaderID,
    fromDate: SPFROMDATE,
    toDate: SPTODATE,
    zipFileName: '',
    downloadUrl: '',
  });

  if (existingLog) {
    return existingLog._id;
  }

  const result = await db.collection('support_ticket_download_logs').insertOne({
    userId: SPUserID,
    insuranceCompanyId: SPInsuranceCompanyID,
    stateId: SPStateID,
    ticketHeaderId: SPTicketHeaderID,
    fromDate: SPFROMDATE,
    toDate: SPTODATE,
    zipFileName: '',
    downloadUrl: '',
    status: 'in-progress',
    createdAt: new Date(),
    updatedAt: new Date(),
  });

  return result.insertedId;
}



async updateDownloadLogFileInfo(logId, zipFileName, downloadUrl, db) {
  await db.collection('support_ticket_download_logs').updateOne(
    { _id: logId },
    {
      $set: {
        zipFileName,
        downloadUrl,
        status: 'completed',
        updatedAt: new Date(),
      }
    }
  );
}







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
          _id:0,
          SupportTicketID: 1,
          ApplicationNo:1,
          InsurancePolicyNo:1,
          TicketStatusID:1,
          TicketStatus:1,
          CallerContactNumber:1,
          RequestorName:1,
          RequestorMobileNo:1,
          StateMasterName:1,
          DistrictMasterName:1,
          SubDistrictName:1,
          TicketHeadName:1,
          TicketCategoryName:1,
          RequestSeason:1,
          RequestYear:1,
          ApplicationCropName:1,
          Relation:1,
          RelativeName:1, 
          PolicyPremium:1,
          PolicyArea:1,
          PolicyType:1,
          LandSurveyNumber:1,
          LandDivisionNumber:1,
          IsSos:1,
          PlotStateName:1,
          PlotDistrictName:1,
          PlotVillageName:1,
          ApplicationSource:1,
          CropShare:1,
          IFSCCode:1,
          FarmerShare:1,
          SowingDate:1,
          LossDate:1,
          CreatedBY:1,
          CreatedAt:"$InsertDateTime",
          Sos:1,
          NCIPDocketNo:"$TicketNCIPDocketNo",
          TicketDescription:1,
          CallingUniqueID:1,
          TicketDate: { $dateToString: { format: "%Y-%m-%d %H:%M:%S", date: "$Created" } },
          StatusDate: { $dateToString: { format: "%Y-%m-%d %H:%M:%S", date: "$StatusUpdateTime" } },
          SupportTicketTypeName: "$TicketTypeName",
          SupportTicketNo:1,
          InsuranceMasterName: "$InsuranceCompany",
          ReOpenDate: "$TicketReOpenDate",
          CallingUserID: "$agentInfo.UserID",
          SchemeName:1,
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









async AddIndex(db){
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



async downloadHistory(payload){
    console.log(payload)
  let collectionName = 'support_ticket_download_logs'
  let pipeline = [
  {
    $match: {
        userId:payload.userID
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
    $project:{
      ReqestorUserID : "$userId",
      RequestedParamsTicketHeaderID : "$ticketHeaderId",
      RequestedParamsInsuranceCompany:"$insuranceCompanyId",
      RequestedParamsStateID : "$stateId",
      RequestedParamsFromDate : "$fromDate",
      RequestedParamsToDate : "$toDate",
      ZippedFileName :"$zipFileName",
      DownloadURL :"$downloadUrl",
      RequestCreationDate :"$createdAt",
      RequestorUserName : "$data.AppAccessUserName",
      RequestorRole :"$data.BRHeadTypeID"
      
    }
  }, {
    $sort:{RequestCreationDate:-1}
  }
]

    let result = await this.db.collection(collectionName).aggregate(pipeline).toArray()


     return {
      data: result,
      message: { msg: '✅ Data fetched successfully', code: 1 }
    };
}









}
