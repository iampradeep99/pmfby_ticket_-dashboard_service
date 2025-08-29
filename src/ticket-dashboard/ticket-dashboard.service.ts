import { Injectable, Inject } from '@nestjs/common';
import { Db, Collection } from 'mongodb';
import * as NodeCache from 'node-cache';
import axios from 'axios'
import {UtilService} from "../commonServices/utilService";
import * as fs from 'fs-extra';
import * as path from 'path';
import * as archiver from 'archiver';
const XLSX = require('xlsx');


@Injectable()
export class TicketDashboardService {
  private ticketCollection: Collection;
  private ticketDbCollection: Collection;
  private cache: NodeCache;

  constructor(@Inject('MONGO_DB') private readonly db: Db) {
    this.ticketCollection = this.db.collection('tickets');
    this.ticketDbCollection = this.db.collection('SLA_KRPH_SupportTickets_Records');

    this.cache = new NodeCache({ stdTTL: 7200 }); // 2 hours = 7200 seconds

  }

   async createTicket(ticketData: any): Promise<any> {
        const result = await this.ticketCollection.insertOne(ticketData);
        return {
            message: 'Ticket created successfully',
            ticketId: result.insertedId
        };
    }

  async fetchTickets(ticketInfo: any): Promise<any> {
    const cacheKey = 'ticket-stats';

    const cachedData = this.cache.get(cacheKey);
    if (cachedData) {
      return cachedData;
    }

    const pipeline = [
      {
        $facet: {
          Grievance: [
            { $match: { TicketHeaderID: 1 } },
            { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
            { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } }
          ],
          Information: [
            { $match: { TicketHeaderID: 2 } },
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
          ],
          CropLoss: [
            { $match: { TicketHeaderID: 4 } },
            { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
            { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } }
          ]
        }
      }
    ];

    const result = await this.ticketDbCollection.aggregate(pipeline).toArray();
    const response = result[0];

    this.cache.set(cacheKey, response);

    return response;
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
 const cachedData = this.cache.get(cacheKey) as {
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

  this.cache.set(cacheKey, responsePayload);

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



async processTicketHistoryAndGenerateZipX(ticketPayload: any) {
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
  
  const cachedData = this.cache.get(cacheKey) as {
    data: any[];
    pagination: {
      total: number;
      page: number;
      limit: number;
      totalPages: number;
      hasNextPage: boolean;
      hasPrevPage: boolean;
    };
    zipPath?: string;
  };

  if (cachedData) {
    return {
      rcode: 1,
      rmessage: 'Success (from cache)',
      data: cachedData.data,
      pagination: cachedData.pagination,
      zipPath: cachedData.zipPath || '',
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
    { $skip: (page - 1) * limit },
    { $limit: limit },
  ];

  const results = await db
    .collection('SLA_KRPH_SupportTickets_Records')
    .aggregate(pipeline, { allowDiskUse: true })
    .toArray();
  // 🔽 Step 1: Write to Excel file
  const folderPath = path.join(__dirname, '..', 'downloads');
  await fs.ensureDir(folderPath);

  const timestamp = Date.now();
  const excelFileName = `support_ticket_data_${timestamp}.xlsx`;
  const excelFilePath = path.join(folderPath, excelFileName);

  // Create a worksheet
  const ws = XLSX.utils.json_to_sheet(results);

  // Create a workbook
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Support Ticket Data');

  // Write to Excel file
  XLSX.writeFile(wb, excelFilePath);

  // 🔽 Step 2: Zip the file
  const zipFileName = excelFileName.replace('.xlsx', '.zip');
  const zipFilePath = path.join(folderPath, zipFileName);

  const output = fs.createWriteStream(zipFilePath);
  const archive = archiver('zip', { zlib: { level: 9 } });

  archive.pipe(output);
  archive.file(excelFilePath, { name: excelFileName });
  await archive.finalize();

  // Optional: Remove original Excel after zipping
  await fs.remove(excelFilePath);

  // 🔽 Step 3: Save path to MongoDB
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

  // 🔽 Step 4: Prepare and cache the response
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
  };

  this.cache.set(cacheKey, responsePayload);
  console.log(`Support ticket history processed and zipped: ${zipFilePath}`);
}


 async processTicketHistoryAndGenerateZip(ticketPayload: any) {
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
    
    const cachedData = this.cache.get(cacheKey) as {
      data: any[];
      pagination: {
        total: number;
        page: number;
        limit: number;
        totalPages: number;
        hasNextPage: boolean;
        hasPrevPage: boolean;
      };
      zipPath?: string;
    };

    if (cachedData) {
      return {
        rcode: 1,
        rmessage: 'Success (from cache)',
        data: cachedData.data,
        pagination: cachedData.pagination,
        zipPath: cachedData.zipPath || '',
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
            { $match: { $expr: { $and: [{ $eq: ['$SupportTicketID', '$$ticketId'] }, { $eq: ['$TicketStatusID', 109304] }] } } },
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
          TicketDate: { $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$Created' } },
          StatusDate: { $dateToString: { format: '%Y-%m-%d %H:%M:%S', date: '$StatusUpdateTime' } },
          SupportTicketTypeName: '$TicketTypeName',
          InsuranceMasterName: '$InsuranceCompany',
          ReOpenDate: '$ticketHistory.TicketHistoryDate',
          NCIPDocketNo: { $replaceAll: { input: '$claimInfo.ClaimReportNo', find: '`', replacement: '' } },
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

    // 🔽 Step 1: Write to Excel file
    const folderPath = path.join(__dirname, '../../downloads');  // Modify to write inside src/downloads

    await fs.ensureDir(folderPath);  // Ensure the folder exists

    const timestamp = Date.now();
    const excelFileName = `support_ticket_data_${timestamp}.xlsx`;
    const excelFilePath = path.join(folderPath, excelFileName);

    // Create worksheet
    const ws = XLSX.utils.json_to_sheet(results);

    // Create workbook
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Support Ticket Data');

    // Write to Excel file
    XLSX.writeFile(wb, excelFilePath);

    // 🔽 Step 2: Zip the file
    const zipFileName = excelFileName.replace('.xlsx', '.zip');
    const zipFilePath = path.join(folderPath, zipFileName);

    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver('zip', { zlib: { level: 9 } });

    archive.pipe(output);
    archive.file(excelFilePath, { name: excelFileName });
    await archive.finalize();

    // Optional: Remove original Excel after zipping
    await fs.remove(excelFilePath);

    // 🔽 Step 3: Save path to MongoDB
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

    // 🔽 Step 4: Prepare and cache the response
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
    };

    this.cache.set(cacheKey, responsePayload);
    console.log(`Support ticket history processed and zipped: ${zipFilePath}`);
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
