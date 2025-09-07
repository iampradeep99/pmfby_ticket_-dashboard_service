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
import { format } from '@fast-csv/format';


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

/* async fetchTickets(ticketInfo: any): Promise<{ data: any; message: { msg: string; code: number } }> {


  console.log("entering in this process");
  
  const cacheKey = 'ticket-stats';
  this.AddIndex(this.db)
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
} */


/*   async fetchTickets(ticketInfo: any): Promise<{ data: any; message: { msg: string; code: number } }> {
  console.log("🚀 Entering fetchTickets process");

  const cacheKey = 'ticket-stats';
  this.AddIndex(this.db); // Ensure indexes are created

  try {
    // Check Redis cache
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey);
    if (cachedData) {
      return {
        data: cachedData,
        message: { msg: '✅ Data fetched from cache', code: 1 },
      };
    }

    // Optimized aggregation pipeline
    const pipeline = [
      {
        $match: {
          TicketHeaderID: { $in: [1, 2, 4] }, // Pre-filter for better performance
        },
      },
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

    // Use allowDiskUse to prevent memory errors with large data
    const result = await this.ticketDbCollection
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray();

    const response = result[0];

    // Cache the result
    await this.redisWrapper.setRedisCache(cacheKey, response, 3600); // cache for 1 hour

    return {
      data: response,
      message: { msg: '✅ Data fetched successfully', code: 1 },
    };
  } catch (error) {
    console.error('❌ Error in fetchTickets:', error);

    return {
      data: null,
      message: { msg: '❌ Failed to fetch ticket data', code: 0 },
    };
  }
} */



async createOptimizedIndex(db: any): Promise<void> {
  try {
    const collection = db.collection('SLA_KRPH_SupportTickets_Records');

    // Get existing indexes
    const indexes = await collection.indexes();

    // Drop old index if it exists
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

  
//   async fetchTickets(ticketInfo: any): Promise<{ data: any; message: { msg: string; code: number } }> {
//   console.log("🚀 Entering fetchTickets process");

//   const cacheKey = 'ticket-stats';
//   // this.AddIndex(this.db); // Ensure indexes are created

//   this.createOptimizedIndex(this.db)

//   try {
//     // 🧠 Step 1: Check Redis cache
//     const cachedData = await this.redisWrapper.getRedisCache(cacheKey);
//     if (cachedData) {
//       return {
//         data: cachedData,
//         message: { msg: '✅ Data fetched from cache', code: 1 },
//       };
//     }

//     // 📅 Step 2: Calculate date range (yesterday to 3 months ago)
//     const now = new Date();
//     const endDate = new Date(now);
//     endDate.setDate(endDate.getDate() - 1); // yesterday

//     const startDate = new Date(now);
//     startDate.setMonth(startDate.getMonth() - 3);
//     startDate.setDate(startDate.getDate() - 1); // also exclude today

//     // 🔍 Step 3: Aggregation pipeline
//     const pipeline = [
//       {
//         $match: {
//           TicketHeaderID: { $in: [1, 2, 4] },
//           InsertDateTime: {
//             $gte: startDate,
//             $lte: endDate
//           }
//         },
//       },
//       {
//         $facet: {
//           Grievance: [
//             { $match: { TicketHeaderID: 1 } },
//             { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
//             { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } },
//           ],
//           Information: [
//             { $match: { TicketHeaderID: 2 } },
//             {
//               $group: {
//                 _id: {
//                   status: "$TicketStatus",
//                   head: "$TicketHeadName",
//                   code: "$BMCGCode",
//                 },
//                 Total: { $sum: 1 },
//               },
//             },
//             {
//               $project: {
//                 _id: 0,
//                 TicketStatus: {
//                   $cond: [
//                     { $eq: ["$_id.code", 109025] },
//                     { $concat: ["$_id.status", " (", "$_id.head", ")"] },
//                     "$_id.status",
//                   ],
//                 },
//                 Total: 1,
//               },
//             },
//           ],
//           CropLoss: [
//             { $match: { TicketHeaderID: 4 } },
//             { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
//             { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } },
//           ],
//         },
//       },
//     ];

//     // 🚀 Step 4: Run aggregation with allowDiskUse for large datasets
//     const result = await this.ticketDbCollection
//       .aggregate(pipeline, { allowDiskUse: true })
//       .toArray();

//     const response = result[0];

//     // 🧊 Step 5: Cache the result in Redis
//     await this.redisWrapper.setRedisCache(cacheKey, response, 3600); // 1 hour

//     return {
//       data: response,
//       message: { msg: '✅ Data fetched successfully', code: 1 },
//     };
//   } catch (error) {
//     console.error('❌ Error in fetchTickets:', error);

//     return {
//       data: null,
//       message: { msg: '❌ Failed to fetch ticket data', code: 0 },
//     };
//   }
// }
// async createOptimizedIndex(db: any) {
//   await db.collection('SLA_KRPH_SupportTickets_Records').createIndex(
//     { InsertDateTime: 1, TicketStatus: 1, TicketHeadName: 1, BMCGCode: 1 },
//     { partialFilterExpression: { TicketHeaderID: 1 } }
//   );
//   await db.collection('SLA_KRPH_SupportTickets_Records').createIndex(
//     { InsertDateTime: 1, TicketStatus: 1, TicketHeadName: 1, BMCGCode: 1 },
//     { partialFilterExpression: { TicketHeaderID: 2 } }
//   );
//   await db.collection('SLA_KRPH_SupportTickets_Records').createIndex(
//     { InsertDateTime: 1, TicketStatus: 1, TicketHeadName: 1, BMCGCode: 1 },
//     { partialFilterExpression: { TicketHeaderID: 4 } }
//   );
// }

/* async fetchTickets(ticketInfo: any): Promise<{ data: any; message: { msg: string; code: number } }> {
  console.log("🚀 Entering fetchTickets process");

   const Delta = await this.getSupportTicketUserDetail(ticketInfo?.userID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);

  const item = (responseInfo.data as any)?.user?.[0];
 const userDetail = {
    InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
    StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
    BRHeadTypeID: item.BRHeadTypeID,
    LocationTypeID: item.LocationTypeID,
  };
  return;

  try {
    const fromDate = new Date(`${ticketInfo.fromDate}T00:00:00.000Z`);
    const toDate = new Date(`${ticketInfo.toDate}T23:59:59.999Z`);

    if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
      return {
        data: null,
        message: { msg: "❌ Invalid date format", code: 0 },
      };
    }

    const cacheKey = `ticket-stats-${ticketInfo.fromDate}-to-${ticketInfo.toDate}`;

    const cachedData = await this.redisWrapper.getRedisCache(cacheKey);
    if (cachedData) {
      console.log('✅ Redis cache hit:', cacheKey);
      return {
        data: cachedData,
        message: { msg: '✅ Data fetched from cache', code: 1 },
      };
    }

    const pipelines = {
      Grievance: [
        { $match: { TicketHeaderID: 1, InsertDateTime: { $gte: fromDate, $lte: toDate } } },
        { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
        { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } },
      ],
      Information: [
        { $match: { TicketHeaderID: 2, InsertDateTime: { $gte: fromDate, $lte: toDate } } },
        {
          $group: {
            _id: { status: "$TicketStatus", head: "$TicketHeadName", code: "$BMCGCode" },
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
        { $match: { TicketHeaderID: 4, InsertDateTime: { $gte: fromDate, $lte: toDate } } },
        { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
        { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } },
      ]
    };

    const [grievance, information, cropLoss] = await Promise.all([
      this.ticketDbCollection.aggregate(pipelines.Grievance, { allowDiskUse: true }).toArray(),
      this.ticketDbCollection.aggregate(pipelines.Information, { allowDiskUse: true }).toArray(),
      this.ticketDbCollection.aggregate(pipelines.CropLoss, { allowDiskUse: true }).toArray(),
    ]);

    const response = { Grievance: grievance, Information: information, CropLoss: cropLoss };

    await this.redisWrapper.setRedisCache(cacheKey, response, 3600);

    return {
      data: response,
      message: { msg: '✅ Data fetched successfully', code: 1 },
    };
  } catch (error) {
    console.error('❌ Error in fetchTickets:', error);
    return {
      data: null,
      message: { msg: '❌ Failed to fetch ticket data', code: 0 },
    };
  }
} */



  async fetchTicketsdddd(ticketInfo: any): Promise<{ data: any; message: { msg: string; code: number } }> {
  console.log("🚀 Entering fetchTickets process");


  // this.createOptimizedIndex(this.db)

  try {
    const Delta = await this.getSupportTicketUserDetail(ticketInfo?.userID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
      DistrictIDs: item.DistrictIDs || []
    };
    const { InsuranceCompanyID, StateMasterID, LocationTypeID, DistrictIDs } = userDetail;

    const fromDate = new Date(`${ticketInfo.fromDate}T00:00:00.000Z`);
    const toDate = new Date(`${ticketInfo.toDate}T23:59:59.999Z`);

    if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
      return {
        data: null,
        message: { msg: "❌ Invalid date format", code: 0 },
      };
    }

    const cacheKey = `ticket-stats-${ticketInfo.fromDate}-to-${ticketInfo.toDate}-user-${ticketInfo.userID}`;
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey);
    if (cachedData) {
      console.log('✅ Redis cache hit:', cacheKey);
      return {
        data: cachedData,
        message: { msg: '✅ Data fetched from cache', code: 1 },
      };
    }

    const match: any = {};

    if (InsuranceCompanyID?.length) {
      match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
    }

    if (LocationTypeID === 1 && StateMasterID?.length) {
      match.FilterStateID = { $in: StateMasterID.map(Number) };
    } else if (LocationTypeID === 2 && DistrictIDs?.length) {
      match.FilterDistrictRequestorID = { $in: DistrictIDs.map(Number) };
    }

    match.InsertDateTime = { $gte: fromDate, $lte: toDate };
    
    const pipelines = {
      Grievance: [
        { $match: { ...match, TicketHeaderID: 1 } },
        { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
        { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } },
      ],
      Information: [
        { $match: { ...match, TicketHeaderID: 2 } },
        {
          $group: {
            _id: { status: "$TicketStatus", head: "$TicketHeadName", code: "$BMCGCode" },
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
        { $match: { ...match, TicketHeaderID: 4 } },
        { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
        { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } },
      ]
    };

  

    const [grievance, information, cropLoss] = await Promise.all([
      this.ticketDbCollection.aggregate(pipelines.Grievance, { allowDiskUse: true }).toArray(),
      this.ticketDbCollection.aggregate(pipelines.Information, { allowDiskUse: true }).toArray(),
      this.ticketDbCollection.aggregate(pipelines.CropLoss, { allowDiskUse: true }).toArray(),
    ]);

    const response = { Grievance: grievance, Information: information, CropLoss: cropLoss };
    await this.redisWrapper.setRedisCache(cacheKey, response, 3600);

    return {
      data: response,
      message: { msg: '✅ Data fetched successfully', code: 1 },
    };

  } catch (error) {
    console.error('❌ Error in fetchTickets:', error);
    return {
      data: null,
      message: { msg: '❌ Failed to fetch ticket data', code: 0 },
    };
  }
}



async fetchTicketsLastUpdated(ticketInfo: any): Promise<{ data: any; message: { msg: string; code: number } }> {
  console.log("🚀 Entering fetchTickets process");

  try {
    // Step 1: Fetch user detail
    const Delta = await this.getSupportTicketUserDetail(ticketInfo?.userID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];

    // Step 2: Extract filter data
    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
      DistrictIDs: item.DistrictIDs || []
    };

    const { InsuranceCompanyID, StateMasterID, LocationTypeID, DistrictIDs } = userDetail;

    // Step 3: Validate dates
    const fromDate = new Date(`${ticketInfo.fromDate}T00:00:00.000Z`);
    const toDate = new Date(`${ticketInfo.toDate}T23:59:59.999Z`);
    if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
      return {
        data: null,
        message: { msg: "❌ Invalid date format", code: 0 }
      };
    }

    // Step 4: Check cache
    const cacheKey = `ticket-stats-${ticketInfo.fromDate}-to-${ticketInfo.toDate}-user-${ticketInfo.userID}`;
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey);
    if (cachedData) {
      console.log('✅ Redis cache hit:', cacheKey);
      return {
        data: cachedData,
        message: { msg: '✅ Data fetched from cache', code: 1 }
      };
    }

    // Step 5: Build match filter
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

    // Step 6: Define pipelines separately

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

    // Step 7: Run all aggregations concurrently
    const [grievance, information, cropLoss] = await Promise.all([
      this.ticketDbCollection.aggregate(grievancePipeline, { allowDiskUse: true }).toArray(),
      this.ticketDbCollection.aggregate(informationPipeline, { allowDiskUse: true }).toArray(),
      this.ticketDbCollection.aggregate(cropLossPipeline, { allowDiskUse: true }).toArray()
    ]);

    // Step 8: Save to cache and return response
    const response = { Grievance: grievance, Information: information, CropLoss: cropLoss };
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
    if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
      return {
        data: null,
        message: { msg: "❌ Invalid date format", code: 0 }
      };
    }

    // Step 4: Check cache
    const cacheKey = `ticket-stats-${ticketInfo.fromDate}-to-${ticketInfo.toDate}-user-${ticketInfo.userID}`;
    const cachedData = await this.redisWrapper.getRedisCache(cacheKey);
    if (cachedData) {
      console.log('✅ Redis cache hit:', cacheKey);
      return {
        data: cachedData,
        message: { msg: '✅ Data fetched from cache', code: 1 }
      };
    }

    // Step 5: Build match filter
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

    // Step 6: Define pipelines separately
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

    // Step 7: Run all aggregations concurrently
    const [grievance, information, cropLoss] = await Promise.all([
      this.ticketDbCollection.aggregate(grievancePipeline, { allowDiskUse: true }).toArray(),
      this.ticketDbCollection.aggregate(informationPipeline, { allowDiskUse: true }).toArray(),
      this.ticketDbCollection.aggregate(cropLossPipeline, { allowDiskUse: true }).toArray()
    ]);

    // Step 8: Add total rows inside each array
    const addTotalRow = (arr: any[]) => {
      const total = arr.reduce((sum, item) => sum + item.Total, 0);
      return [...arr, { TicketStatus: "Total", Total: total }];
    };

    const response = {
      Grievance: addTotalRow(grievance),
      Information: addTotalRow(information),
      CropLoss: addTotalRow(cropLoss)
    };

    // Step 9: Save to cache and return response
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




async processTicketHistoryViewOlder(ticketPayload: any) {
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
  // console.log(JSON.stringify(responseInfo))

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

console.log(JSON.stringify(match));
return

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
  // console.log(JSON.stringify(responseInfo))

  const item = (responseInfo.data as any)?.user?.[0];
  console.log(item, "test");
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

  // ✅ SECURE FILTERING
  const match: any = {
    ...locationFilter,
  };

  // TicketHeaderID filter
  if (SPTicketHeaderID && SPTicketHeaderID !== 0) {
    match.TicketHeaderID = SPTicketHeaderID;
  }

  // INSURANCE COMPANY FILTER
  // if (SPInsuranceCompanyID && SPInsuranceCompanyID !== '#ALL') {
  //   const requestedInsuranceIDs = SPInsuranceCompanyID.split(',').map(id => id.trim());
  //   const allowedInsuranceIDs = InsuranceCompanyID.map(String); // from user profile
  //   const validInsuranceIDs = requestedInsuranceIDs.filter(id => allowedInsuranceIDs.includes(id));

  //   if (validInsuranceIDs.length === 0) {
  //     return { rcode: 0, rmessage: 'Unauthorized InsuranceCompanyID(s).' };
  //   }

  //   match.InsuranceCompanyID = { $in: validInsuranceIDs };
  // } else {
  //   // If #ALL, limit to allowed insurance companies
  //   if (InsuranceCompanyID?.length) {
  //     match.InsuranceCompanyID = { $in: InsuranceCompanyID };
  //   }
  // }

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

  console.log(JSON.stringify(match));
  // return; // Uncomment if you want to debug only

  const totalCount = await db.collection('SLA_KRPH_SupportTickets_Records').countDocuments(match);
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
                  { $eq: ['$TicketStatusID', 109304] }
                ]
              }
            }
          },
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
        SupportTicketNo: 1,
        InsuranceMasterName: "$InsuranceCompany",
        ReOpenDate: "$TicketReOpenDate",
        CallingUserID: "$agentInfo.UserID",
        SchemeName: 1,
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






  async processTicketHistoryAndGenerateZipx(ticketPayload: any) {
  const {
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

  if (!SPInsuranceCompanyID) {
    console.log('InsuranceCompanyID Missing!');
    return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
  }

  if (!SPStateID) {
    console.log('StateID Missing!');
    return { rcode: 0, rmessage: 'StateID Missing!' };
  }

  const RequestDateTime = await getCurrentFormattedDateTime();

  // Exclude userEmail from cacheKey
  const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
  const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;

  let results: any[] = [];
  let totalCount = 0;
  let totalPages = 0;
  let isFromCache = false;

  if (cachedData) {
    console.log("Using cached data");
    isFromCache = true;
    results = cachedData.data;
    totalCount = cachedData.pagination.total;
    totalPages = cachedData.pagination.totalPages;
  } else {
    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];

    if (!item) {
      console.log('User details not found.');
      return { rcode: 0, rmessage: 'User details not found.' };
    }

      const userDetail = {
    InsuranceCompanyID: item.InsuranceCompanyID ? await this.convertStringToArray(item.InsuranceCompanyID) : [],
    StateMasterID: item.StateMasterID ? await this.convertStringToArray(item.StateMasterID) : [],
    BRHeadTypeID: item.BRHeadTypeID,
    LocationTypeID: item.LocationTypeID,
  };
    const {InsuranceCompanyID, StateMasterID, LocationTypeID, BRHeadTypeID  } = userDetail;

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

    if (SPFROMDATE || SPTODATE) {
  match.InsertDateTime = {};

  if (SPFROMDATE) {
    match.InsertDateTime.$gte = new Date(`${SPFROMDATE}T00:00:00.000Z`);
  }

  if (SPTODATE) {
    match.InsertDateTime.$lte = new Date(`${SPTODATE}T23:59:59.999Z`);
  }
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

    results = await db
      .collection('SLA_KRPH_SupportTickets_Records')
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray();
  }

  const folderPath = path.join(process.cwd(), 'downloads');
  await fs.ensureDir(folderPath);

  const timestamp = Date.now();
  const excelFileName = `support_ticket_data_${timestamp}.xlsx`;
  const excelFilePath = path.join(folderPath, excelFileName);

  const ws = XLSX.utils.json_to_sheet(results);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Support Ticket Data');
  XLSX.writeFile(wb, excelFilePath);
  console.log(`Excel file created at: ${excelFilePath}`);

  const zipFileName = excelFileName.replace('.xlsx', '.zip');
  const zipFilePath = path.join(folderPath, zipFileName);

  const output = fs.createWriteStream(zipFilePath);
  const archive = archiver('zip', { zlib: { level: 9 } });

  archive.pipe(output);
  archive.file(excelFilePath, { name: excelFileName });
  await archive.finalize();

  await new Promise<void>((resolve, reject) => {
    output.on('close', () => {
      console.log(`ZIP file created at: ${zipFilePath} (${archive.pointer()} total bytes)`);
      resolve();
    });
    output.on('error', (err) => {
      console.error('Error during ZIP file creation:', err);
      reject(err);
    });
  });

  try {
    await fs.remove(excelFilePath);
    console.log(`Deleted Excel file: ${excelFilePath}`);
  } catch (err) {
    console.error(`Failed to delete Excel file ${excelFilePath}:`, err);
  }

  const gcpService = new GCPServices();
  const fileBuffer = await fs.readFile(zipFilePath);

  let uploadResult;
  try {
    uploadResult = await gcpService.uploadFileToGCP({
      filePath: 'krph/reports/',
      uploadedBy: "KRPH",
      file: {
        buffer: fileBuffer,
        originalname: zipFileName,
      },
    });
    console.log('Upload to GCP successful');
  } catch (err) {
    console.error('Upload to GCP failed:', err);
    throw err;
  }

  const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
  console.log('GCP Download URL:', gcpDownloadUrl);

  // Remove ZIP file after successful upload
  if (gcpDownloadUrl) {
    try {
      await fs.remove(zipFilePath);
      console.log(`Deleted ZIP file: ${zipFilePath}`);
    } catch (err) {
      console.error(`Failed to delete ZIP file ${zipFilePath}:`, err);
    }
  }

  // Log download info in DB
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
  console.log('Inserted download log into DB');

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

  // Generate email HTML and send mail
  const supportTicketTemplate = await generateSupportTicketEmailHTML(
    'Portal User',
    RequestDateTime,
    gcpDownloadUrl
  );

  try {
    await this.mailService.sendMail({
      to: userEmail,
      subject: 'Support Ticket History Report Download Service',
      text: 'Support Ticket History Report',
      html: supportTicketTemplate,
    });
    console.log(`Email sent to ${userEmail}`);
  } catch (err) {
    console.error(`Failed to send email to ${userEmail}:`, err);
  }

  // Cache response payload without userEmail in cacheKey
  if (!isFromCache) {
    await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);
    console.log('Cached response payload in Redis');
  }

  // Optionally uncomment to return the response
  // return {
  //   rcode: 1,
  //   rmessage: isFromCache ? 'Success (from cache)' : 'Success',
  //   ...responsePayload,
  // };
}




// import { format } from '@fast-csv/format';
// import * as fs from 'fs';
// import * as path from 'path';
// import * as archiver from 'archiver';

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
    userEmail,
  } = ticketPayload;
  const db = this.db;

  if (!SPInsuranceCompanyID) {
    console.log('InsuranceCompanyID Missing!');
    return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
  }
  if (!SPStateID) {
    console.log('StateID Missing!');
    return { rcode: 0, rmessage: 'StateID Missing!' };
  }

  const RequestDateTime = await getCurrentFormattedDateTime();
  const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;
  const cachedData = await this.redisWrapper.getRedisCache(cacheKey) as any;

  let totalCount = 0;
  let totalPages = 0;
  let isFromCache = false;

  if (cachedData) {
    console.log('Using cached data');
    isFromCache = true;
    totalCount = cachedData.pagination.total;
    totalPages = cachedData.pagination.totalPages;
  }

  if (!isFromCache) {
    const Delta = await this.getSupportTicketUserDetail(SPUserID);
    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];
    if (!item) {
      console.log('User details not found.');
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
    const { InsuranceCompanyID, StateMasterID, LocationTypeID,BRHeadTypeID } = userDetail;

    // let locationFilter: any = {};
    // if (LocationTypeID === 1 && StateMasterID?.length) {
    //   locationFilter = { FilterStateID: { $in: StateMasterID } };
    // } else if (LocationTypeID === 2 && item.DistrictIDs?.length) {
    //   locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };
    // }

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

    // const match: any = {
    //   ...locationFilter,
    //   ...(SPStateID !== '#ALL' && { FilterStateID: { $in: SPStateID.split(',') } }),
    //   ...(SPInsuranceCompanyID !== '#ALL' && { InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',') } }),
    //   ...(SPTicketHeaderID && SPTicketHeaderID !== 0 && { TicketHeaderID: SPTicketHeaderID }),
    //   ...(InsuranceCompanyID?.length && { InsuranceCompanyID: { $in: InsuranceCompanyID } }),
    //   ...(StateMasterID?.length && LocationTypeID !== 2 && { FilterStateID: { $in: StateMasterID } }),
    // };

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
    match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) }; 
  }
}

  if (SPStateID && SPStateID !== '#ALL') {
    const requestedStateIDs = SPStateID.split(',').map(id => id.trim());
    const validStateIDs = requestedStateIDs.filter(id => StateMasterID.includes(id));

    if (validStateIDs.length === 0) {
      return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
    }

    match.FilterStateID = { $in: validStateIDs };
  } else if (StateMasterID?.length && LocationTypeID !== 2) {
    match.FilterStateID = { $in: StateMasterID };
  }

    if (SPFROMDATE || SPTODATE) {
      match.InsertDateTime = {};
      if (SPFROMDATE) {
        match.InsertDateTime.$gte = new Date(`${SPFROMDATE}T00:00:00.000Z`);
      }
      if (SPTODATE) {
        match.InsertDateTime.$lte = new Date(`${SPTODATE}T23:59:59.999Z`);
      }
    }

    totalCount = await db.collection('SLA_KRPH_SupportTickets_Records').countDocuments(match);
    totalPages = Math.ceil(totalCount / limit);

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

    // Prepare CSV streaming
    const folderPath = path.join(process.cwd(), 'downloads');
    await fs.promises.mkdir(folderPath, { recursive: true });

    const timestamp = Date.now();
    const csvFileName = `support_ticket_data_${timestamp}.csv`;
    const csvFilePath = path.join(folderPath, csvFileName);

    const writeStream = fs.createWriteStream(csvFilePath);
    const csvStream = format({ headers: true });
    csvStream.pipe(writeStream);

    const cursor = db
      .collection('SLA_KRPH_SupportTickets_Records')
      .aggregate(pipeline, { allowDiskUse: true })
      .stream();

    for await (const doc of cursor) {
      csvStream.write(doc);
    }
    csvStream.end();

    await new Promise<void>((resolve, reject) => {
      writeStream.on('finish', resolve);
      writeStream.on('error', reject);
    });
    console.log(`CSV file created at: ${csvFilePath}`);

    // Zip it
    const zipFileName = csvFileName.replace('.csv', '.zip');
    const zipFilePath = path.join(folderPath, zipFileName);
    const output = fs.createWriteStream(zipFilePath);
    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(output);
    archive.file(csvFilePath, { name: csvFileName });
    await archive.finalize();

    await new Promise<void>((resolve, reject) => {
      output.on('close', resolve);
      output.on('error', reject);
    });

    console.log(`ZIP file created at: ${zipFilePath} (${archive.pointer()} bytes)`);

    // Cleanup CSV
    await fs.promises.unlink(csvFilePath).catch(console.error);

    const gcpService = new GCPServices();
    const fileBuffer = await fs.promises.readFile(zipFilePath);
    let uploadResult;
    try {
      uploadResult = await gcpService.uploadFileToGCP({
        filePath: 'krph/reports/',
        uploadedBy: 'KRPH',
        file: { buffer: fileBuffer, originalname: zipFileName },
      });
      console.log('Upload to GCP successful');
    } catch (err) {
      console.error('Upload to GCP failed:', err);
      throw err;
    }

    const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
    console.log('GCP Download URL:', gcpDownloadUrl);

    if (gcpDownloadUrl) {
      await fs.promises.unlink(zipFilePath).catch(console.error);
    }

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
    console.log('Inserted download log into DB');

    const responsePayload = {
      data: [], // streaming CSV, so omit data or optionally provide page summary
      pagination: { total: totalCount, page, limit, totalPages, hasNextPage: page < totalPages, hasPrevPage: page > 1 },
      downloadUrl: gcpDownloadUrl,
    };

    const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);
    try {
      await this.mailService.sendMail({
        to: userEmail,
        subject: 'Support Ticket History Report Download Service',
        text: 'Support Ticket History Report',
        html: supportTicketTemplate,
      });
      console.log(`Email sent to ${userEmail}`);
    } catch (err) {
      console.error(`Failed to send email to ${userEmail}:`, err);
    }

    await this.redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);
    console.log('Cached response payload in Redis');

    // Optionally return
    // return { rcode: 1, rmessage: 'Success', ...responsePayload };
  }

}





/* async processTicketHistoryAndGenerateZipOldWithBatch(ticketPayload: any) {
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


async processTicketHistoryAndGenerateZip(ticketPayload: any) {
  const {
    SPFROMDATE,
    SPTODATE,
    SPInsuranceCompanyID,
    SPStateID,
    SPTicketHeaderID,
    SPUserID,
    limit = 50000,
    userEmail,
  } = ticketPayload;

  const db = this.db;

  if (!SPInsuranceCompanyID || !SPStateID) {
    return {
      rcode: 0,
      rmessage: `${!SPInsuranceCompanyID ? 'InsuranceCompanyID' : 'StateID'} Missing!`,
    };
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
  console.log(`Total documents to export: ${totalCount}`);

  const folderPath = path.join(process.cwd(), 'downloads');
  await fs.ensureDir(folderPath);
  const timestamp = Date.now();
  const excelFileName = `support_ticket_data_${timestamp}.xlsx`;
  const excelFilePath = path.join(folderPath, excelFileName);

  // ExcelJS streaming
  console.log('Starting Excel file creation...');
  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
  const worksheet = workbook.addWorksheet('Support Ticket Data');
  let headersWritten = false;

  for (let currentPage = 0; currentPage < totalPages; currentPage++) {
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
            { $project: { TicketHistoryDate: 1 } },
          ],
          as: 'ticketHistory',
        },
      },
      {
        $lookup: {
          from: 'support_ticket_claim_intimation_report_history',
          localField: 'SupportTicketNo',
          foreignField: 'SupportTicketNo',
          pipeline: [{ $project: { ClaimReportNo: 1 } }],
          as: 'claimInfo',
        },
      },
      {
        $lookup: {
          from: 'csc_agent_master',
          localField: 'InsertUserID',
          foreignField: 'UserLoginID',
          pipeline: [{ $project: { UserID: 1 } }],
          as: 'agentInfo',
        },
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
        },
      },
    ];

    const batchResults = await db.collection('SLA_KRPH_SupportTickets_Records')
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray();

    for (const row of batchResults) {
      if (!headersWritten) {
        const dynamicHeaders = Object.keys(row);
        worksheet.addRow(dynamicHeaders).commit();
        headersWritten = true;
        console.log('Excel headers written:', dynamicHeaders);
      }
      worksheet.addRow(Object.values(row)).commit();
    }

    console.log(`Processed batch ${currentPage + 1} / ${totalPages}`);
  }

  await workbook.commit();
  console.log(`Excel file created at: ${excelFilePath}`);

  // ZIP the Excel
  console.log('Starting to ZIP the Excel file...');
  const zipFileName = excelFileName.replace('.xlsx', '.zip');
  const zipFilePath = path.join(folderPath, zipFileName);
  const output = fs.createWriteStream(zipFilePath);
  const archive = archiver('zip', { zlib: { level: 9 } });
  archive.pipe(output);
  archive.file(excelFilePath, { name: excelFileName });
  await archive.finalize();
  await fs.remove(excelFilePath);
  console.log(`Zipping completed. ZIP file path: ${zipFilePath}`);

  // Upload to GCP
  console.log('Uploading ZIP to GCP...');
  const gcpService = new GCPServices();
  const fileBuffer = await fs.readFile(zipFilePath);
  const uploadResult = await gcpService.uploadFileToGCP({
    filePath: 'krph/reports/',
    uploadedBy: "KRPH",
    file: { buffer: fileBuffer, originalname: zipFileName },
  });
  const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
  await fs.remove(zipFilePath);
  console.log('File uploaded to GCP. URL:', gcpDownloadUrl);

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
  console.log('Sending email to user...');
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
  console.log(`Email sent to ${userEmail}`);

  return {
    rcode: 1,
    rmessage: 'Success',
    downloadUrl: gcpDownloadUrl,
    totalCount,
  };
} */








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
