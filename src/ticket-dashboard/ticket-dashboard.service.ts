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


  async getSupportTicketHistot(ticketPayload: any): Promise<any> {
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
const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as {
  data: any[];
  pagination: {
    total: number;
    page: number;
    limit: number;
    totalPages: number;
    hasNextPage: boolean;
    hasPrevPage: boolean;
  };
};

  if (cachedData) {
    return {
      rcode: 1,
      rmessage: 'Success (from cache)',
      data: cachedData.data,
      pagination: cachedData.pagination,
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

  const { InsuranceCompanyID, StateMasterID } = userDetail;

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

  // Get total count for pagination
  const totalCount = await db
    .collection('SLA_KRPH_SupportTickets_Records')
    .countDocuments(match);

  const totalPages = Math.ceil(totalCount / limit);

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
    // {$sort:{Created:-1}},
    { $skip: (page - 1) * limit },
    { $limit: limit },
  ];

  const results = await db
    .collection('SLA_KRPH_SupportTickets_Records')
    .aggregate(pipeline, { allowDiskUse: true })
    .toArray();

  const responsePayload = {
    data: results,
    pagination: {
      total: totalCount,
      page,
      limit,
      totalPages,
      hasNextPage: page < totalPages,
      hasPrevPage: page > 1,
    },
  };

  // this.cache.set(cacheKey, responsePayload);
  await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600); // TTL 1 hour

  return {
    rcode: 1,
    rmessage: 'Success',
    ...responsePayload,
  };
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






 


//NodeCache
async processTicketHistoryAndGenerateZipNodeCache(ticketPayload: any) {
  const {
    SPFROMDATE,
    SPTODATE,
    SPInsuranceCompanyID,
    SPStateID,
    SPTicketHeaderID,
    SPUserID,
    page = 1,
    limit = 1000000000,
  } = ticketPayload;

  const db = this.db;

  if (!SPInsuranceCompanyID) {
    const response = { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
    console.log('Returning response:', response);
  }

  if (!SPStateID) {
    const response = { rcode: 0, rmessage: 'StateID Missing!' };
    console.log('Returning response:', response);
  }

  const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
  const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;

  let results: any[] = [];
  let totalCount = 0;
  let totalPages = 0;
  let isFromCache = false;

  if (cachedData) {
    console.log('✅ Data retrieved from cache.');
    isFromCache = true;
    results = cachedData.data;
    totalCount = cachedData.pagination.total;
    totalPages = cachedData.pagination.totalPages;
  } else {
    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];

    if (!item) {
      const response = { rcode: 0, rmessage: 'User details not found.' };
      console.log('Returning response:', response);
      return response;
    }

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

    const { InsuranceCompanyID, StateMasterID } = userDetail;

    const match: any = {
      ...(SPStateID !== '#ALL' && { FilterStateID: { $in: SPStateID.split(',') } }),
      ...(SPInsuranceCompanyID !== '#ALL' && { InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',') } }),
      ...(SPTicketHeaderID && SPTicketHeaderID !== 0 && { TicketHeaderID: SPTicketHeaderID }),
      ...(InsuranceCompanyID?.length && { InsuranceCompanyID: { $in: InsuranceCompanyID } }),
      ...(StateMasterID?.length && { FilterStateID: { $in: StateMasterID } }),
    };

    if (SPFROMDATE || SPTODATE) {
      match.InsertDateTime = {};
      if (SPFROMDATE) match.InsertDateTime.$gte = new Date(SPFROMDATE);
      if (SPTODATE) match.InsertDateTime.$lte = new Date(SPTODATE);
    }

    totalCount = await db
      .collection('SLA_KRPH_SupportTickets_Records')
      .countDocuments(match);

    totalPages = Math.ceil(totalCount / limit);

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
            $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$StatusUpdateTime' },
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

    results = await db
      .collection('SLA_KRPH_SupportTickets_Records')
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray();
  }

  // ✅ Step: Excel + ZIP Generation (applies to both DB and Cache results)
  const folderPath = path.join(process.cwd(), 'downloads');
  await fs.ensureDir(folderPath);

  const timestamp = Date.now();
  const excelFileName = `support_ticket_data_${timestamp}.xlsx`;
  const excelFilePath = path.join(folderPath, excelFileName);

  const ws = XLSX.utils.json_to_sheet(results);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Support Ticket Data');
  XLSX.writeFile(wb, excelFilePath);

  const zipFileName = excelFileName.replace('.xlsx', '.zip');
  const zipFilePath = path.join(folderPath, zipFileName);

  const output = fs.createWriteStream(zipFilePath);
  const archive = archiver('zip', { zlib: { level: 9 } });

  archive.pipe(output);
  archive.file(excelFilePath, { name: excelFileName });
  await archive.finalize();

  await fs.remove(excelFilePath);
  const downloadUrl = `http://10.128.60.46:3010/downloads/${zipFileName}`;

  // ✅ Step: Log Download
  await db.collection('support_ticket_download_logs').insertOne({
    userId: SPUserID,
    insuranceCompanyId: SPInsuranceCompanyID,
    stateId: SPStateID,
    ticketHeaderId: SPTicketHeaderID,
    fromDate: SPFROMDATE,
    toDate: SPTODATE,
    zipFileName,
    zipFilePath,
    createdAt: new Date(),
    downloadUrl:downloadUrl
  });

  // ✅ Step: Build Response

  const responsePayload = {
    data: results,
    pagination: {
      total: totalCount,
      page,
      limit,
      totalPages,
      hasNextPage: page < totalPages,
      hasPrevPage: page > 1,
    },
    zipPath: zipFilePath,
    downloadUrl,
  };

  // Only cache if not from cache already
  if (!isFromCache) {
    await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);
  }

  const finalResponse = {
    rcode: 1,
    rmessage: isFromCache ? 'Success (from cache)' : 'Success',
    ...responsePayload,
  };

  console.log('Returning response:', finalResponse);
  // return finalResponse;
}

/* 
oldCode
async processTicketHistoryAndGenerateZip(ticketPayload: any) {
  const {
    SPFROMDATE,
    SPTODATE,
    SPInsuranceCompanyID,
    SPStateID,
    SPTicketHeaderID,
    SPUserID,
    page = 1,
    limit = 1000000000,
    userEmail
  } = ticketPayload;

  const db = this.db;



  if (!SPInsuranceCompanyID) {
    const response = { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
    console.log('Returning response:', response);
    return response;
  }

  if (!SPStateID) {
    const response = { rcode: 0, rmessage: 'StateID Missing!' };
    console.log('Returning response:', response);
    return response;
  }
  let RequestDateTime = await getCurrentFormattedDateTime()

  const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;

  let results: any[] = [];
  let totalCount = 0;
  let totalPages = 0;
  let isFromCache = false;

  if (cachedData) {
   console.log('✅ Data retrieved from Redis cache.');
      isFromCache = true;
      results = cachedData.data;
      totalCount = cachedData.pagination.total;
      totalPages = cachedData.pagination.totalPages;
  } else {
    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];

    if (!item) {
      const response = { rcode: 0, rmessage: 'User details not found.' };
      console.log('Returning response:', response);
      return response;
    }

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

    const { InsuranceCompanyID, StateMasterID } = userDetail;

    const match: any = {
      ...(SPStateID !== '#ALL' && { FilterStateID: { $in: SPStateID.split(',') } }),
      ...(SPInsuranceCompanyID !== '#ALL' && { InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',') } }),
      ...(SPTicketHeaderID && SPTicketHeaderID !== 0 && { TicketHeaderID: SPTicketHeaderID }),
      ...(InsuranceCompanyID?.length && { InsuranceCompanyID: { $in: InsuranceCompanyID } }),
      ...(StateMasterID?.length && { FilterStateID: { $in: StateMasterID } }),
    };

    if (SPFROMDATE || SPTODATE) {
      match.InsertDateTime = {};
      if (SPFROMDATE) match.InsertDateTime.$gte = new Date(SPFROMDATE);
      if (SPTODATE) match.InsertDateTime.$lte = new Date(SPTODATE);
    }

    totalCount = await db
      .collection('SLA_KRPH_SupportTickets_Records')
      .countDocuments(match);

    totalPages = Math.ceil(totalCount / limit);

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
            $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$StatusUpdateTime' },
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

    results = await db
      .collection('SLA_KRPH_SupportTickets_Records')
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray();
  }

  // ✅ Step: Excel + ZIP Generation (applies to both DB and Cache results)
  const folderPath = path.join(process.cwd(), 'downloads');
  await fs.ensureDir(folderPath);

  const timestamp = Date.now();
  const excelFileName = `support_ticket_data_${timestamp}.xlsx`;
  const excelFilePath = path.join(folderPath, excelFileName);

  const ws = XLSX.utils.json_to_sheet(results);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Support Ticket Data');
  XLSX.writeFile(wb, excelFilePath);

  const zipFileName = excelFileName.replace('.xlsx', '.zip');
  const zipFilePath = path.join(folderPath, zipFileName);

  const output = fs.createWriteStream(zipFilePath);
  const archive = archiver('zip', { zlib: { level: 9 } });

  archive.pipe(output);
  archive.file(excelFilePath, { name: excelFileName });
  await archive.finalize();

  await fs.remove(excelFilePath);

  // ✅ Step: Log Download
  await db.collection('support_ticket_download_logs').insertOne({
    userId: SPUserID,
    insuranceCompanyId: SPInsuranceCompanyID,
    stateId: SPStateID,
    ticketHeaderId: SPTicketHeaderID,
    fromDate: SPFROMDATE,
    toDate: SPTODATE,
    zipFileName,
    zipFilePath,
    createdAt: new Date(),
  });

  // ✅ Step: Build Response
  const downloadUrl = `http://10.128.60.46:3010/downloads/${zipFileName}`;

  const responsePayload = {
    data: results,
    pagination: {
      total: totalCount,
      page,
      limit,
      totalPages,
      hasNextPage: page < totalPages,
      hasPrevPage: page > 1,
    },
    zipPath: zipFilePath,
    downloadUrl,
  };
  let supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, downloadUrl)
    let sendMailPayload = {
      to:userEmail,
      subject:"Support Ticket History Report Download Service",
       text: 'Support Ticket History Report',
       html:supportTicketTemplate
    }
  await this.mailService.sendMail(sendMailPayload)

  if (!isFromCache) {
   await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600); // TTL 1 hour
  }

  const finalResponse = {
    rcode: 1,
    rmessage: isFromCache ? 'Success (from cache)' : 'Success',
    ...responsePayload,
  };

  // console.log('Returning response:', finalResponse);
  // return finalResponse;
} */



async processTicketHistoryAndGenerateZip(ticketPayload: any) {
  const {
    SPFROMDATE,
    SPTODATE,
    SPInsuranceCompanyID,
    SPStateID,
    SPTicketHeaderID,
    SPUserID,
    page = 1,
    limit = 1000000000,
    userEmail
  } = ticketPayload;

  const db = this.db;

  if (!SPInsuranceCompanyID) {
    return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
  }

  if (!SPStateID) {
    return { rcode: 0, rmessage: 'StateID Missing!' };
  }

  const RequestDateTime = await getCurrentFormattedDateTime();
  const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
  const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;

  let results: any[] = [];
  let totalCount = 0;
  let totalPages = 0;
  let isFromCache = false;

  if (cachedData) {
    isFromCache = true;
    results = cachedData.data;
    totalCount = cachedData.pagination.total;
    totalPages = cachedData.pagination.totalPages;
  } else {
    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];

    if (!item) {
      return { rcode: 0, rmessage: 'User details not found.' };
    }

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

    const { InsuranceCompanyID, StateMasterID } = userDetail;

    const match: any = {
      ...(SPStateID !== '#ALL' && { FilterStateID: { $in: SPStateID.split(',') } }),
      ...(SPInsuranceCompanyID !== '#ALL' && { InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',') } }),
      ...(SPTicketHeaderID && SPTicketHeaderID !== 0 && { TicketHeaderID: SPTicketHeaderID }),
      ...(InsuranceCompanyID?.length && { InsuranceCompanyID: { $in: InsuranceCompanyID } }),
      ...(StateMasterID?.length && { FilterStateID: { $in: StateMasterID } }),
    };

    if (SPFROMDATE || SPTODATE) {
      match.InsertDateTime = {};
      if (SPFROMDATE) match.InsertDateTime.$gte = new Date(SPFROMDATE);
      if (SPTODATE) match.InsertDateTime.$lte = new Date(SPTODATE);
    }

    totalCount = await db
      .collection('SLA_KRPH_SupportTickets_Records')
      .countDocuments(match);

    totalPages = Math.ceil(totalCount / limit);

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
            $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$StatusUpdateTime' },
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

    results = await db
      .collection('SLA_KRPH_SupportTickets_Records')
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray();
  }

  // === ✅ Step: Generate Excel + Zip Locally ===
  const folderPath = path.join(process.cwd(), 'downloads');
  await fs.ensureDir(folderPath);

  const timestamp = Date.now();
  const excelFileName = `support_ticket_data_${timestamp}.xlsx`;
  const excelFilePath = path.join(folderPath, excelFileName);

  const ws = XLSX.utils.json_to_sheet(results);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Support Ticket Data');
  XLSX.writeFile(wb, excelFilePath);

  const zipFileName = excelFileName.replace('.xlsx', '.zip');
  const zipFilePath = path.join(folderPath, zipFileName);

  const output = fs.createWriteStream(zipFilePath);
  const archive = archiver('zip', { zlib: { level: 9 } });

  archive.pipe(output);
  archive.file(excelFilePath, { name: excelFileName });
  await archive.finalize();

  await fs.remove(excelFilePath); // remove the Excel after zipping

  // === ✅ Step: Upload to GCP ===
  const gcpService = new GCPServices();

  const fileBuffer = await fs.readFile(zipFilePath);

  const uploadResult = await gcpService.uploadFileToGCP({
    filePath: 'krph/reports/',
    uploadedBy:  "KRPH",
    file: {
      buffer: fileBuffer,
      originalname: zipFileName,
    },
  });

  console.log(uploadResult, "uploadResult")

 const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';


  // === ✅ Delete ZIP after uploading ===
  if (gcpDownloadUrl) {
    await fs.remove(zipFilePath);
  }

  // === ✅ Step: Log Download ===
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

  // === ✅ Step: Build Response ===
  const responsePayload = {
    data: results,
    pagination: {
      total: totalCount,
      page,
      limit,
      totalPages,
      hasNextPage: page < totalPages,
      hasPrevPage: page > 1,
    },
    zipPath: zipFilePath,
    downloadUrl: gcpDownloadUrl,
  };

  const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);

  await this.mailService.sendMail({
    to: userEmail,
    subject: 'Support Ticket History Report Download Service',
    text: 'Support Ticket History Report',
    html: supportTicketTemplate,
  });

  if (!isFromCache) {
    await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);
  }

  // return {
  //   rcode: 1,
  //   rmessage: isFromCache ? 'Success (from cache)' : 'Success',
  //   ...responsePayload,
  // };
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
