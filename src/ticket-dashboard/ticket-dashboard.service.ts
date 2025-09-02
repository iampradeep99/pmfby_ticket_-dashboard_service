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
import { MailService } from '../mail/mail.service';
import {generateSupportTicketEmailHTML,getCurrentFormattedDateTime} from '../templates/mailTemplates'
import {GCPServices} from '../commonServices/GCSFileUpload'

import * as ExcelJS from 'exceljs';
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

async fetchTickets(ticketInfo: any): Promise<{ data: any; message: { msg: string; code: number } }> {
  const cacheKey = 'ticket-stats';

  try {
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey);
    if (cachedData) {
      return {
        data: cachedData,
        message: { msg: 'Data fetched from cache', code: 1 },
      };
    }

    const pipeline = [
      {
        $facet: {
          Grievance: [
            { $match: { TicketHeaderID: 1 } },
            { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
            { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } },
          ],
          Information: [
            { $match: { TicketHeaderID: 2 } },
            {
              $group: {
                _id: {
                  status: "$TicketStatus",
                  head: "$TicketHeadName",
                  code: "$BMCGCode",
                },
                Total: { $sum: 1 },
              },
            },
            {
              $project: {
                _id: 0,
                TicketStatus: {
                  $cond: [
                    { $eq: ["$_id.code", 109025] },
                    { $concat: ["$_id.status", " (", "$_id.head", ")"] },
                    "$_id.status",
                  ],
                },
                Total: 1,
              },
            },
          ],
          CropLoss: [
            { $match: { TicketHeaderID: 4 } },
            { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
            { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } },
          ],
        },
      },
    ];

    const result = await this.ticketDbCollection.aggregate(pipeline).toArray();
    const response = result[0];

    await this.redisWrapper.setRedisCache(cacheKey, response, 3600);

    return {
      data: response,
      message: { msg: 'Data fetched successfully', code: 1 },
    };
  } catch (error) {
    console.error('❌ Error in fetchTickets:', error);

    return {
      data: null,
      message: { msg: 'Failed to fetch ticket data', code: 0 },
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

/* async getSupportTicketHistot(ticketPayload: any): Promise<any> {
  const {
    SPFROMDATE,
    SPTODATE,
    SPInsuranceCompanyID,
    SPStateID,
    SPTicketHeaderID,
    SPUserID,
  } = ticketPayload;

  const db = this.db; // Assuming db is injected via constructor

  // Validate required inputs
  if (!SPInsuranceCompanyID) {
    return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
  }

  if (!SPStateID) {
    return { rcode: 0, rmessage: 'StateID Missing!' };
  }

  // 🔹 Get user-level filters
  const Delta = await this.getSupportTicketUserDetail(SPUserID);
  let responseInfo = await new UtilService().unGZip(Delta.responseDynamic);

  const users = (responseInfo.data as any)?.user ?? [];
  const item = users[0];

  let userDetail = {
    UserProfileID: item.UserProfileID,
    FromDay: item.FromDay,
    ToDay: item.ToDay,
    AppAccessTypeID: item.AppAccessTypeID,
    AppAccessID: item.AppAccessID,
    BRHeadTypeID: item.BRHeadTypeID,
    LocationTypeID: item.LocationTypeID,
    UserType: item.UserType,
    EscalationFlag: item.EscalationFlag,
    InsuranceCompanyID: item.InsuranceCompanyID
      ? await this.convertStringToArray(item.InsuranceCompanyID)
      : [],
    StateMasterID: item.StateMasterID
      ? await this.convertStringToArray(item.StateMasterID)
      : [],
    TicketCategoryID: item.TicketCategoryID
      ? await this.convertStringToArray(item.TicketCategoryID)
      : [],
  };

  // 🔹 Destructure (now will be used in match)
  const {
    InsuranceCompanyID,
    StateMasterID,
    BRHeadTypeID,
    LocationTypeID,
  } = userDetail;

  // 🔹 Build match query
  const match: any = {
    ...(SPStateID !== '#ALL' && { FilterStateID: { $in: SPStateID.split(',') } }),
    ...(SPInsuranceCompanyID !== '#ALL' && {
      InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',') },
    }),
    ...(SPTicketHeaderID && SPTicketHeaderID !== 0 && {
      TicketHeaderID: SPTicketHeaderID,
    }),
    ...(InsuranceCompanyID?.length && { InsuranceCompanyID: { $in: InsuranceCompanyID } }),
    ...(StateMasterID?.length && { FilterStateID: { $in: StateMasterID } }),
    // ...(BRHeadTypeID && { BRHeadTypeID }),
    // ...(LocationTypeID && { LocationTypeID }),
  };

  if (SPFROMDATE || SPTODATE) {
    match.InsertDateTime = {};
    if (SPFROMDATE) match.InsertDateTime.$gte = new Date(SPFROMDATE);
    if (SPTODATE) match.InsertDateTime.$lte = new Date(SPTODATE);
  }

  console.log("Final MongoDB Match Filter:", JSON.stringify(match, null, 2));

  // return

  // 🔹 Aggregation pipeline
  const pipeline = [
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
    { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
    {
      $project: {
        SupportTicketID: 1,
        TicketHeaderID: 1,
        TicketTypeName: 1,
        InsuranceCompany: 1,
        Created: 1,
        StatusUpdateTime: 1,
        InsertDateTime: 1,
        TicketDate: {
          $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$Created' },
        },
        StatusDate: {
          $dateToString: {
            format: '%Y-%m-%d %H:%M:%S',
            date: '$StatusUpdateTime',
          },
        },
        SupportTicketTypeName: '$TicketTypeName',
        InsuranceMasterName: '$InsuranceCompany',
        ReOpenDate: '$ticketHistory.TicketHistoryDate',
        NCIPDocketNo: {
          $replaceAll: {
            input: '$claimInfo.ClaimReportNo',
            find: '`',
            replacement: '',
          },
        },
        CallingUserID: '$agentInfo.UserID',
      },
    },
    { $sort: { InsertDateTime: 1 } },
  ];

  const results = await db
    .collection('SLA_KRPH_SupportTickets_Records')
    .aggregate(pipeline)
    .toArray();

  return {
    rcode: 1,
    rmessage: 'Success',
    data: results,
  };
} */


/*   async getSupportTicketHistot(ticketPayload: any): Promise<any> {
  const {
    SPFROMDATE,
    SPTODATE,
    SPInsuranceCompanyID,
    SPStateID,
    SPTicketHeaderID,
    SPUserID,
    page = 1,
    limit = 100000, // Optional: pagination
  } = ticketPayload;

  const db = this.db; // Assuming Mongo DB is injected

  // this.AddIndex(db);
  // return
  

  // 🔹 1. Validate Inputs
  if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
  if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };

  // 🔹 2. Get User Details
  const Delta = await this.getSupportTicketUserDetail(SPUserID);
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

  const {
    InsuranceCompanyID,
    StateMasterID,
  } = userDetail;

  // 🔹 3. Build Match Filter
  const match: any = {
    ...(SPStateID !== '#ALL' && { FilterStateID: { $in: SPStateID.split(',') } }),
    ...(SPInsuranceCompanyID !== '#ALL' && {
      InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',') },
    }),
    ...(SPTicketHeaderID && SPTicketHeaderID !== 0 && { TicketHeaderID: SPTicketHeaderID }),
    ...(InsuranceCompanyID?.length && { InsuranceCompanyID: { $in: InsuranceCompanyID } }),
    ...(StateMasterID?.length && { FilterStateID: { $in: StateMasterID } }),
  };

  if (SPFROMDATE || SPTODATE) {
    match.InsertDateTime = {};
    if (SPFROMDATE) match.InsertDateTime.$gte = new Date(SPFROMDATE);
    if (SPTODATE) match.InsertDateTime.$lte = new Date(SPTODATE);
  }

  // 🔹 4. Build Aggregation Pipeline
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
    { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
    {
      $project: {
        SupportTicketID: 1,
        TicketHeaderID: 1,
        TicketTypeName: 1,
        InsuranceCompany: 1,
        Created: 1,
        StatusUpdateTime: 1,
        InsertDateTime: 1,
        TicketDate: {
          $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$Created' },
        },
        StatusDate: {
          $dateToString: {
            format: '%Y-%m-%d %H:%M:%S',
            date: '$StatusUpdateTime',
          },
        },
        SupportTicketTypeName: '$TicketTypeName',
        InsuranceMasterName: '$InsuranceCompany',
        ReOpenDate: '$ticketHistory.TicketHistoryDate',
        NCIPDocketNo: {
          $replaceAll: {
            input: '$claimInfo.ClaimReportNo',
            find: '`',
            replacement: '',
          },
        },
        CallingUserID: '$agentInfo.UserID',
      },
    },
    // { $sort: { InsertDateTime: -1 } },
    { $skip: (page - 1) * limit },
    { $limit: limit },
  ];

  // 🔹 5. Run Aggregation
  const results = await db
    .collection('SLA_KRPH_SupportTickets_Records')
    .aggregate(pipeline, { allowDiskUse: true })
    .toArray();

  return {
    rcode: 1,
    rmessage: 'Success',
    data: results,
  };
} */


//without paging
/*   async getSupportTicketHistot(ticketPayload: any): Promise<any> {
  const {
    SPFROMDATE,
    SPTODATE,
    SPInsuranceCompanyID,
    SPStateID,
    SPTicketHeaderID,
    SPUserID,
    page = 1,
    limit = 1000,
  } = ticketPayload;

  const db = this.db;

  if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
  if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };

  const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;

  const cachedData = this.cache.get(cacheKey);
  if (cachedData) {
    return {
      rcode: 1,
      rmessage: 'Success (from cache)',
      data: cachedData,
    };
  }

  const Delta = await this.getSupportTicketUserDetail(SPUserID);
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

  const {
    InsuranceCompanyID,
    StateMasterID,
  } = userDetail;

  const match: any = {
    ...(SPStateID !== '#ALL' && { FilterStateID: { $in: SPStateID.split(',') } }),
    ...(SPInsuranceCompanyID !== '#ALL' && {
      InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',') },
    }),
    ...(SPTicketHeaderID && SPTicketHeaderID !== 0 && { TicketHeaderID: SPTicketHeaderID }),
    ...(InsuranceCompanyID?.length && { InsuranceCompanyID: { $in: InsuranceCompanyID } }),
    ...(StateMasterID?.length && { FilterStateID: { $in: StateMasterID } }),
  };

  if (SPFROMDATE || SPTODATE) {
    match.InsertDateTime = {};
    if (SPFROMDATE) match.InsertDateTime.$gte = new Date(SPFROMDATE);
    if (SPTODATE) match.InsertDateTime.$lte = new Date(SPTODATE);
  }

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
    { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
    {
      $project: {
        SupportTicketID: 1,
        TicketHeaderID: 1,
        TicketTypeName: 1,
        InsuranceCompany: 1,
        Created: 1,
        StatusUpdateTime: 1,
        InsertDateTime: 1,
        TicketDate: {
          $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$Created' },
        },
        StatusDate: {
          $dateToString: {
            format: '%Y-%m-%d %H:%M:%S',
            date: '$StatusUpdateTime',
          },
        },
        SupportTicketTypeName: '$TicketTypeName',
        InsuranceMasterName: '$InsuranceCompany',
        ReOpenDate: '$ticketHistory.TicketHistoryDate',
        NCIPDocketNo: {
          $replaceAll: {
            input: '$claimInfo.ClaimReportNo',
            find: '`',
            replacement: '',
          },
        },
        CallingUserID: '$agentInfo.UserID',
      },
    },
    { $skip: (page - 1) * limit },
    { $limit: limit },
  ];

  const results = await db
    .collection('SLA_KRPH_SupportTickets_Records')
    .aggregate(pipeline, { allowDiskUse: true })
    .toArray();

  this.cache.set(cacheKey, results);

  return {
    rcode: 1,
    rmessage: 'Success',
    data: results,
  };
} */


//   async getSupportTicketHistot(ticketPayload: any): Promise<any> {
//   const {
//     SPFROMDATE,
//     SPTODATE,
//     SPInsuranceCompanyID,
//     SPStateID,
//     SPTicketHeaderID,
//     SPUserID,
//     page = 1,
//     limit = 1000,
//   } = ticketPayload;

//   const db = this.db;

//   if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
//   if (!SPStateID) return { rcode: 0, rmessage: 'StateID Missing!' };

//   const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
// const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as {
//   data: any[];
//   pagination: {
//     total: number;
//     page: number;
//     limit: number;
//     totalPages: number;
//     hasNextPage: boolean;
//     hasPrevPage: boolean;
//   };
// };

//   if (cachedData) {
//     return {
//       rcode: 1,
//       rmessage: 'Success (from cache)',
//       data: cachedData.data,
//       pagination: cachedData.pagination,
//     };
//   }

//   const Delta = await this.getSupportTicketUserDetail(SPUserID);
//   const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
//   const item = (responseInfo.data as any)?.user?.[0];

//   if (!item) return { rcode: 0, rmessage: 'User details not found.' };

//   const userDetail = {
//     InsuranceCompanyID: item.InsuranceCompanyID
//       ? await this.convertStringToArray(item.InsuranceCompanyID)
//       : [],
//     StateMasterID: item.StateMasterID
//       ? await this.convertStringToArray(item.StateMasterID)
//       : [],
//     BRHeadTypeID: item.BRHeadTypeID,
//     LocationTypeID: item.LocationTypeID,
//   };

//   const { InsuranceCompanyID, StateMasterID } = userDetail;

//   const match: any = {
//     ...(SPStateID !== '#ALL' && { FilterStateID: { $in: SPStateID.split(',') } }),
//     ...(SPInsuranceCompanyID !== '#ALL' && {
//       InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',') },
//     }),
//     ...(SPTicketHeaderID && SPTicketHeaderID !== 0 && { TicketHeaderID: SPTicketHeaderID }),
//     ...(InsuranceCompanyID?.length && { InsuranceCompanyID: { $in: InsuranceCompanyID } }),
//     ...(StateMasterID?.length && { FilterStateID: { $in: StateMasterID } }),
//   };

//   if (SPFROMDATE || SPTODATE) {
//     match.InsertDateTime = {};
//     if (SPFROMDATE) match.InsertDateTime.$gte = new Date(SPFROMDATE);
//     if (SPTODATE) match.InsertDateTime.$lte = new Date(SPTODATE);
//   }

//   // Get total count for pagination
//   const totalCount = await db
//     .collection('SLA_KRPH_SupportTickets_Records')
//     .countDocuments(match);

//   const totalPages = Math.ceil(totalCount / limit);

//   const pipeline: any[] = [
//     { $match: match },
//     {
//       $lookup: {
//         from: 'SLA_KRPH_SupportTicketsHistory_Records',
//         let: { ticketId: '$SupportTicketID' },
//         pipeline: [
//           {
//             $match: {
//               $expr: {
//                 $and: [
//                   { $eq: ['$SupportTicketID', '$$ticketId'] },
//                   { $eq: ['$TicketStatusID', 109304] },
//                 ],
//               },
//             },
//           },
//           { $sort: { TicketHistoryID: -1 } },
//           { $limit: 1 },
//         ],
//         as: 'ticketHistory',
//       },
//     },
//     {
//       $lookup: {
//         from: 'support_ticket_claim_intimation_report_history',
//         localField: 'SupportTicketNo',
//         foreignField: 'SupportTicketNo',
//         as: 'claimInfo',
//       },
//     },
//     {
//       $lookup: {
//         from: 'csc_agent_master',
//         localField: 'InsertUserID',
//         foreignField: 'UserLoginID',
//         as: 'agentInfo',
//       },
//     },
//     { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
//     { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
//     { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
//     {
//       $project: {
//         SupportTicketID: 1,
//         TicketHeaderID: 1,
//         TicketTypeName: 1,
//         InsuranceCompany: 1,
//         Created: 1,
//         StatusUpdateTime: 1,
//         InsertDateTime: 1,
//         TicketDate: {
//           $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$Created' },
//         },
//         StatusDate: {
//           $dateToString: {
//             format: '%Y-%m-%d %H:%M:%S',
//             date: '$StatusUpdateTime',
//           },
//         },
//         SupportTicketTypeName: '$TicketTypeName',
//         InsuranceMasterName: '$InsuranceCompany',
//         ReOpenDate: '$ticketHistory.TicketHistoryDate',
//         NCIPDocketNo: {
//           $replaceAll: {
//             input: '$claimInfo.ClaimReportNo',
//             find: '`',
//             replacement: '',
//           },
//         },
//         CallingUserID: '$agentInfo.UserID',
//       },
//     },
//     // {$sort:{Created:-1}},
//     { $skip: (page - 1) * limit },
//     { $limit: limit },
//   ];

//   const results = await db
//     .collection('SLA_KRPH_SupportTickets_Records')
//     .aggregate(pipeline, { allowDiskUse: true })
//     .toArray();

//   const responsePayload = {
//     data: results,
//     pagination: {
//       total: totalCount,
//       page,
//       limit,
//       totalPages,
//       hasNextPage: page < totalPages,
//       hasPrevPage: page > 1,
//     },
//   };

//   // this.cache.set(cacheKey, responsePayload);
//   await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600); // TTL 1 hour

//   return {
//     rcode: 1,
//     rmessage: 'Success',
//     ...responsePayload,
//   };
// }











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


// old code ticket

// async processTicketHistory(ticketPayload: any) {
//   const {
//     SPFROMDATE,
//     SPTODATE,
//     SPInsuranceCompanyID,
//     SPStateID,
//     SPTicketHeaderID,
//     SPUserID,
//     page,
//     limit,
//   } = ticketPayload;

//   const db = this.db;

//   if (!SPInsuranceCompanyID) {
//     console.log('InsuranceCompanyID Missing!');
//     return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
//   }

//   if (!SPStateID) {
//     console.log('StateID Missing!');
//     return { rcode: 0, rmessage: 'StateID Missing!' };
//   }

//   const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
//   const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;

//   let results: any[] = [];
//   let totalCount = 0;
//   let totalPages = 0;

//   if (cachedData) {
//     console.log("Using cached data");
//     results = cachedData.data;
//     totalCount = cachedData.pagination.total;
//     totalPages = cachedData.pagination.totalPages;
//   } else {
//     const Delta = await this.getSupportTicketUserDetail(SPUserID);
//     const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
//     const item = (responseInfo.data as any)?.user?.[0];

//     if (!item) {
//       console.log('User details not found.');
//       return { rcode: 0, rmessage: 'User details not found.' };
//     }

//     const userDetail = {
//       InsuranceCompanyID: item.InsuranceCompanyID
//         ? await this.convertStringToArray(item.InsuranceCompanyID)
//         : [],
//       StateMasterID: item.StateMasterID
//         ? await this.convertStringToArray(item.StateMasterID)
//         : [],
//       BRHeadTypeID: item.BRHeadTypeID,
//       LocationTypeID: item.LocationTypeID,
//     };

//     const { InsuranceCompanyID, StateMasterID } = userDetail;

//     const match: any = {
//       ...(SPStateID !== '#ALL' && { FilterStateID: { $in: SPStateID.split(',') } }),
//       ...(SPInsuranceCompanyID !== '#ALL' && { InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',') } }),
//       ...(SPTicketHeaderID && SPTicketHeaderID !== 0 && { TicketHeaderID: SPTicketHeaderID }),
//       ...(InsuranceCompanyID?.length && { InsuranceCompanyID: { $in: InsuranceCompanyID } }),
//       ...(StateMasterID?.length && { FilterStateID: { $in: StateMasterID } }),
//     };

//     if (SPFROMDATE || SPTODATE) {
//       match.InsertDateTime = {};
//       if (SPFROMDATE) match.InsertDateTime.$gte = new Date(SPFROMDATE);
//       if (SPTODATE) match.InsertDateTime.$lte = new Date(SPTODATE);
//     }

//     // Count total records for pagination
//     totalCount = await db.collection('SLA_KRPH_SupportTickets_Records').countDocuments(match);
//     totalPages = Math.ceil(totalCount / limit);

//     const pipeline: any[] = [
//       { $match: match },
//       {
//         $lookup: {
//           from: 'SLA_KRPH_SupportTicketsHistory_Records',
//           let: { ticketId: '$SupportTicketID' },
//           pipeline: [
//             {
//               $match: {
//                 $expr: {
//                   $and: [
//                     { $eq: ['$SupportTicketID', '$$ticketId'] },
//                     { $eq: ['$TicketStatusID', 109304] },
//                   ],
//                 },
//               },
//             },
//             { $sort: { TicketHistoryID: -1 } },
//             { $limit: 1 },
//           ],
//           as: 'ticketHistory',
//         },
//       },
//       {
//         $lookup: {
//           from: 'support_ticket_claim_intimation_report_history',
//           localField: 'SupportTicketNo',
//           foreignField: 'SupportTicketNo',
//           as: 'claimInfo',
//         },
//       },
//       {
//         $lookup: {
//           from: 'csc_agent_master',
//           localField: 'InsertUserID',
//           foreignField: 'UserLoginID',
//           as: 'agentInfo',
//         },
//       },
//       { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
//       { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
//       { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
//       {
//         $project: {
//           SupportTicketID: 1,
//           TicketHeaderID: 1,
//           TicketTypeName: 1,
//           InsuranceCompany: 1,
//           Created: 1,
//           StatusUpdateTime: 1,
//           InsertDateTime: 1,
//           TicketDate: { $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$Created' } },
//           StatusDate: { $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$StatusUpdateTime' } },
//           SupportTicketTypeName: '$TicketTypeName',
//           InsuranceMasterName: '$InsuranceCompany',
//           ReOpenDate: '$ticketHistory.TicketHistoryDate',
//           NCIPDocketNo: { $replaceAll: { input: '$claimInfo.ClaimReportNo', find: '`', replacement: '' } },
//           CallingUserID: '$agentInfo.UserID',
//         },
//       },
//       { $skip: (page - 1) * limit },
//       { $limit: limit },
//     ];

//     results = await db.collection('SLA_KRPH_SupportTickets_Records')
//       .aggregate(pipeline, { allowDiskUse: true })
//       .toArray();

//     // Ensure results is always an array
//     results = Array.isArray(results) ? results : [results];

//     const responsePayload = {
//       data: results,
//       pagination: {
//         total: totalCount,
//         page,
//         limit,
//         totalPages,
//         hasNextPage: page < totalPages,
//         hasPrevPage: page > 1,
//       },
//     };

//     await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);
//     console.log('Cached response payload in Redis');
//   }

//   // console.log({
//   //   rcode: 1,
//   //   rmessage: 'Success',
//   //   data: results,
//   //   pagination: {
//   //     total: totalCount,
//   //     page,
//   //     limit,
//   //     totalPages,
//   //     hasNextPage: page < totalPages,
//   //     hasPrevPage: page > 1,
//   //   },
//   // })
//   // Final consistent response
//   return {
//     rcode: 1,
//     rmessage: 'Success',
//     data: results,
//     pagination: {
//       total: totalCount,
//       page,
//       limit,
//       totalPages,
//       hasNextPage: page < totalPages,
//       hasPrevPage: page > 1,
//     },
//   };
// }




async processTicketHistoryView(ticketPayload: any) {
  const {
    SPFROMDATE,
    SPTODATE,
    SPInsuranceCompanyID,
    SPStateID,
    SPTicketHeaderID,
    SPUserID,
    page = 1,
    limit = 20,
  } = ticketPayload;

  const db = this.db;
  this.AddIndex(db)

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
    ...(SPStateID !== '#ALL' && { FilterStateID: { $in: SPStateID.split(',') } }),
    ...(SPInsuranceCompanyID !== '#ALL' && { InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',') } }),
    ...(SPTicketHeaderID && SPTicketHeaderID !== 0 && { TicketHeaderID: SPTicketHeaderID }),
    ...(InsuranceCompanyID?.length && { InsuranceCompanyID: { $in: InsuranceCompanyID } }),
    ...(StateMasterID?.length && LocationTypeID !== 2 && { FilterStateID: { $in: StateMasterID } }),
  };

  // if (SPFROMDATE || SPTODATE) {
  //   match.InsertDateTime = {};
  //   if (SPFROMDATE) match.InsertDateTime.$gte = new Date(SPFROMDATE);
  //   if (SPTODATE) match.InsertDateTime.$lte = new Date(SPTODATE);
  // }

 if (SPFROMDATE || SPTODATE) {
  match.InsertDateTime = {};

  if (SPFROMDATE) {
    match.InsertDateTime.$gte = new Date(`${SPFROMDATE}T00:00:00.000Z`);
  }

  if (SPTODATE) {
    match.InsertDateTime.$lte = new Date(`${SPTODATE}T23:59:59.999Z`);
  }
}

console.log(match.InsertDateTime);


  const totalCount = await db.collection('SLA_KRPH_SupportTickets_Records').countDocuments(match);
  const totalPages = Math.ceil(totalCount / limit);

  const pipeline: any[] = [
    { $match: match },

    {
      $lookup: {
        from: 'SLA_KRPH_SupportTicketsHistory_Records',
        let: { ticketId: '$SupportTicketID' },
        pipeline: [
          { $match: { $expr: { $and: [
            { $eq: ['$SupportTicketID', '$$ticketId'] },
            { $eq: ['$TicketStatusID', 109304] }
          ] } } },
          { $sort: { TicketHistoryID: -1 } },
          { $limit: 1 }
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

    { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
    { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },

 {
    $project: {
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
      TicketDate: {
        $dateToString: {
          format: "%Y-%m-%d %H:%M:%S",
          date: "$Created"
        }
      },
      StatusDate: {
        $dateToString: {
          format: "%Y-%m-%d %H:%M:%S",
          date: "$StatusUpdateTime"
        }
      },
      SupportTicketTypeName: "$TicketTypeName",
      SupportTicketNo:1,
      InsuranceMasterName: "$InsuranceCompany",
      ReOpenDate: "$TicketReOpenDate",
      CallingUserID: "$agentInfo.UserID",
      SchemeName:1,
      
    }
  },
    { $skip: (page - 1) * limit },
    { $limit: limit },
  ];
  let results = await db.collection('SLA_KRPH_SupportTickets_Records')
    .aggregate(pipeline, { allowDiskUse: true })
    .toArray();

  results = Array.isArray(results) ? results : [results];

  const responsePayload = {
    data: results,
    pagination: { total: totalCount, page, limit, totalPages, hasNextPage: page < totalPages, hasPrevPage: page > 1 },
  };
  await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);

  return {
    rcode: 1,
    rmessage: 'Success',
    data: results,
    pagination: responsePayload.pagination,
  };
}





//   async processTicketHistoryAndGenerateZip(ticketPayload: any) {
//   const {
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

//   if (!SPInsuranceCompanyID) {
//     console.log('InsuranceCompanyID Missing!');
//     return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
//   }

//   if (!SPStateID) {
//     console.log('StateID Missing!');
//     return { rcode: 0, rmessage: 'StateID Missing!' };
//   }

//   const RequestDateTime = await getCurrentFormattedDateTime();

//   // Exclude userEmail from cacheKey
//   const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
//   const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;

//   let results: any[] = [];
//   let totalCount = 0;
//   let totalPages = 0;
//   let isFromCache = false;

//   if (cachedData) {
//     console.log("Using cached data");
//     isFromCache = true;
//     results = cachedData.data;
//     totalCount = cachedData.pagination.total;
//     totalPages = cachedData.pagination.totalPages;
//   } else {
//     const Delta = await this.getSupportTicketUserDetail(SPUserID);
//     const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
//     const item = (responseInfo.data as any)?.user?.[0];

//     if (!item) {
//       console.log('User details not found.');
//       return { rcode: 0, rmessage: 'User details not found.' };
//     }

//       const userDetail = {
//     InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
//     StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
//     BRHeadTypeID: item.BRHeadTypeID,
//     LocationTypeID: item.LocationTypeID,
//   };
//     const {InsuranceCompanyID, StateMasterID, LocationTypeID, BRHeadTypeID  } = userDetail;

//      let locationFilter: any = {};
//       if (LocationTypeID === 1 && StateMasterID?.length) {
//     locationFilter = {
//       FilterStateID: { $in: StateMasterID },
//     };
//   } else if (LocationTypeID === 2 && item.DistrictIDs?.length) {
//     locationFilter = {
//       FilterDistrictRequestorID: { $in: item.DistrictIDs },
//     };
//   } else {
//     locationFilter = {};
//   }


//    const match: any = {
//     ...locationFilter,
//     ...(SPStateID !== '#ALL' && { FilterStateID: { $in: SPStateID.split(',') } }),
//     ...(SPInsuranceCompanyID !== '#ALL' && { InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',') } }),
//     ...(SPTicketHeaderID && SPTicketHeaderID !== 0 && { TicketHeaderID: SPTicketHeaderID }),
//     ...(InsuranceCompanyID?.length && { InsuranceCompanyID: { $in: InsuranceCompanyID } }),
//     ...(StateMasterID?.length && LocationTypeID !== 2 && { FilterStateID: { $in: StateMasterID } }),
//   };

//     if (SPFROMDATE || SPTODATE) {
//   match.InsertDateTime = {};

//   if (SPFROMDATE) {
//     match.InsertDateTime.$gte = new Date(`${SPFROMDATE}T00:00:00.000Z`);
//   }

//   if (SPTODATE) {
//     match.InsertDateTime.$lte = new Date(`${SPTODATE}T23:59:59.999Z`);
//   }
// }

//     totalCount = await db
//       .collection('SLA_KRPH_SupportTickets_Records')
//       .countDocuments(match);

//     totalPages = Math.ceil(totalCount / limit);

//     /* const pipeline: any[] = [
//       { $match: match },
//       {
//         $lookup: {
//           from: 'SLA_KRPH_SupportTicketsHistory_Records',
//           let: { ticketId: '$SupportTicketID' },
//           pipeline: [
//             {
//               $match: {
//                 $expr: {
//                   $and: [
//                     { $eq: ['$SupportTicketID', '$$ticketId'] },
//                     { $eq: ['$TicketStatusID', 109304] },
//                   ],
//                 },
//               },
//             },
//             { $sort: { TicketHistoryID: -1 } },
//             { $limit: 1 },
//           ],
//           as: 'ticketHistory',
//         },
//       },
//       {
//         $lookup: {
//           from: 'support_ticket_claim_intimation_report_history',
//           localField: 'SupportTicketNo',
//           foreignField: 'SupportTicketNo',
//           as: 'claimInfo',
//         },
//       },
//       {
//         $lookup: {
//           from: 'csc_agent_master',
//           localField: 'InsertUserID',
//           foreignField: 'UserLoginID',
//           as: 'agentInfo',
//         },
//       },
//       { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
//       { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
//       { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
//       {
//         $project: {
//           SupportTicketID: 1,
//           TicketHeaderID: 1,
//           TicketTypeName: 1,
//           InsuranceCompany: 1,
//           Created: 1,
//           StatusUpdateTime: 1,
//           InsertDateTime: 1,
//           TicketDate: {
//             $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$Created' },
//           },
//           StatusDate: {
//             $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$StatusUpdateTime' },
//           },
//           SupportTicketTypeName: '$TicketTypeName',
//           InsuranceMasterName: '$InsuranceCompany',
//           ReOpenDate: '$ticketHistory.TicketHistoryDate',
//           NCIPDocketNo: {
//             $replaceAll: {
//               input: '$claimInfo.ClaimReportNo',
//               find: '`',
//               replacement: '',
//             },
//           },
//           CallingUserID: '$agentInfo.UserID',
//         },
//       },
//       { $skip: (page - 1) * limit },
//       { $limit: limit },
//     ]; */

//     const pipeline: any[] = [
//     { $match: match },

//     {
//       $lookup: {
//         from: 'SLA_KRPH_SupportTicketsHistory_Records',
//         let: { ticketId: '$SupportTicketID' },
//         pipeline: [
//           { $match: { $expr: { $and: [
//             { $eq: ['$SupportTicketID', '$$ticketId'] },
//             { $eq: ['$TicketStatusID', 109304] }
//           ] } } },
//           { $sort: { TicketHistoryID: -1 } },
//           { $limit: 1 }
//         ],
//         as: 'ticketHistory',
//       },
//     },

//     {
//       $lookup: {
//         from: 'support_ticket_claim_intimation_report_history',
//         localField: 'SupportTicketNo',
//         foreignField: 'SupportTicketNo',
//         as: 'claimInfo',
//       },
//     },

//     {
//       $lookup: {
//         from: 'csc_agent_master',
//         localField: 'InsertUserID',
//         foreignField: 'UserLoginID',
//         as: 'agentInfo',
//       },
//     },

//     { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
//     { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
//     { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },

//  {
//     $project: {
//       SupportTicketID: 1,
//       ApplicationNo:1,
//       InsurancePolicyNo:1,
//       TicketStatusID:1,
//       TicketStatus:1,
//       CallerContactNumber:1,
//       RequestorName:1,
//       RequestorMobileNo:1,
//       StateMasterName:1,
//       DistrictMasterName:1,
//       SubDistrictName:1,
//       TicketHeadName:1,
//       TicketCategoryName:1,
//       RequestSeason:1,
//       RequestYear:1,
//       ApplicationCropName:1,
//       Relation:1,
//       RelativeName:1, 
//       PolicyPremium:1,
//       PolicyArea:1,
//       PolicyType:1,
//       LandSurveyNumber:1,
//       LandDivisionNumber:1,
//       IsSos:1,
//       PlotStateName:1,
//      PlotDistrictName:1,
//       PlotVillageName:1,
//       ApplicationSource:1,
//       CropShare:1,
//       IFSCCode:1,
//       FarmerShare:1,
//       SowingDate:1,
//       LossDate:1,
//       CreatedBY:1,
//       CreatedAt:"$InsertDateTime",
//       Sos:1,
//       NCIPDocketNo:"$TicketNCIPDocketNo",
//       TicketDescription:1,
//       CallingUniqueID:1,
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
//       SupportTicketNo:1,
//       InsuranceMasterName: "$InsuranceCompany",
//       ReOpenDate: "$TicketReOpenDate",
//       CallingUserID: "$agentInfo.UserID",
//       SchemeName:1,
      
//     }
//   },
//     { $skip: (page - 1) * limit },
//     { $limit: limit },
//   ];

//     results = await db
//       .collection('SLA_KRPH_SupportTickets_Records')
//       .aggregate(pipeline, { allowDiskUse: true })
//       .toArray();
//   }

//   const folderPath = path.join(process.cwd(), 'downloads');
//   await fs.ensureDir(folderPath);

//   const timestamp = Date.now();
//   const excelFileName = `support_ticket_data_${timestamp}.xlsx`;
//   const excelFilePath = path.join(folderPath, excelFileName);

//   const ws = XLSX.utils.json_to_sheet(results);
//   const wb = XLSX.utils.book_new();
//   XLSX.utils.book_append_sheet(wb, ws, 'Support Ticket Data');
//   XLSX.writeFile(wb, excelFilePath);
//   console.log(`Excel file created at: ${excelFilePath}`);

//   const zipFileName = excelFileName.replace('.xlsx', '.zip');
//   const zipFilePath = path.join(folderPath, zipFileName);

//   const output = fs.createWriteStream(zipFilePath);
//   const archive = archiver('zip', { zlib: { level: 9 } });

//   archive.pipe(output);
//   archive.file(excelFilePath, { name: excelFileName });
//   await archive.finalize();

//   await new Promise<void>((resolve, reject) => {
//     output.on('close', () => {
//       console.log(`ZIP file created at: ${zipFilePath} (${archive.pointer()} total bytes)`);
//       resolve();
//     });
//     output.on('error', (err) => {
//       console.error('Error during ZIP file creation:', err);
//       reject(err);
//     });
//   });

//   try {
//     await fs.remove(excelFilePath);
//     console.log(`Deleted Excel file: ${excelFilePath}`);
//   } catch (err) {
//     console.error(`Failed to delete Excel file ${excelFilePath}:`, err);
//   }

//   const gcpService = new GCPServices();
//   const fileBuffer = await fs.readFile(zipFilePath);

//   let uploadResult;
//   try {
//     uploadResult = await gcpService.uploadFileToGCP({
//       filePath: 'krph/reports/',
//       uploadedBy: "KRPH",
//       file: {
//         buffer: fileBuffer,
//         originalname: zipFileName,
//       },
//     });
//     console.log('Upload to GCP successful');
//   } catch (err) {
//     console.error('Upload to GCP failed:', err);
//     throw err;
//   }

//   const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
//   console.log('GCP Download URL:', gcpDownloadUrl);

//   // Remove ZIP file after successful upload
//   if (gcpDownloadUrl) {
//     try {
//       await fs.remove(zipFilePath);
//       console.log(`Deleted ZIP file: ${zipFilePath}`);
//     } catch (err) {
//       console.error(`Failed to delete ZIP file ${zipFilePath}:`, err);
//     }
//   }

//   // Log download info in DB
//   await db.collection('support_ticket_download_logs').insertOne({
//     userId: SPUserID,
//     insuranceCompanyId: SPInsuranceCompanyID,
//     stateId: SPStateID,
//     ticketHeaderId: SPTicketHeaderID,
//     fromDate: SPFROMDATE,
//     toDate: SPTODATE,
//     zipFileName,
//     downloadUrl: gcpDownloadUrl,
//     createdAt: new Date(),
//   });
//   console.log('Inserted download log into DB');

//   const responsePayload = {
//     data: results,
//     pagination: {
//       total: totalCount,
//       page,
//       limit,
//       totalPages,
//       hasNextPage: page < totalPages,
//       hasPrevPage: page > 1,
//     },
//     zipPath: zipFilePath,
//     downloadUrl: gcpDownloadUrl,
//   };

//   // Generate email HTML and send mail
//   const supportTicketTemplate = await generateSupportTicketEmailHTML(
//     'Portal User',
//     RequestDateTime,
//     gcpDownloadUrl
//   );

//   try {
//     await this.mailService.sendMail({
//       to: userEmail,
//       subject: 'Support Ticket History Report Download Service',
//       text: 'Support Ticket History Report',
//       html: supportTicketTemplate,
//     });
//     console.log(`Email sent to ${userEmail}`);
//   } catch (err) {
//     console.error(`Failed to send email to ${userEmail}:`, err);
//   }

//   // Cache response payload without userEmail in cacheKey
//   if (!isFromCache) {
//     await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);
//     console.log('Cached response payload in Redis');
//   }

//   // Optionally uncomment to return the response
//   // return {
//   //   rcode: 1,
//   //   rmessage: isFromCache ? 'Success (from cache)' : 'Success',
//   //   ...responsePayload,
//   // };
// }



async processTicketHistoryAndGenerateZip(ticketPayload: any) {
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



}
