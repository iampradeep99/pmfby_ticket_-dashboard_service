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
import config from '../environment/config'; // import dynamic config


// import * as ExcelJS from 'exceljs';

@Injectable()
export class TicketEscalationService {
  private ticketCollection: Collection;
  private ticketDbCollection: Collection;
    private readonly PMFBY_ROLE_URL = config.pmfbyRoleURL

  public gcp = new GCPServices();
  logDir = path.join(__dirname, '..', 'logs');

  constructor(
    @Inject('MONGO_DB') private readonly db: Db,
    @Inject('SEQUELIZE') private readonly sequelize: Sequelize,
    private readonly redisWrapper: RedisWrapper,
    private readonly mailService: MailService,
    private readonly utilServices : UtilService,
  ) {
    this.ticketCollection = this.db.collection('tickets');
    this.ticketDbCollection = this.db.collection('SLA_KRPH_SupportTickets_Records');
  }

  


  
async fetchRoles(payload: any): Promise<{ data: any; message: { msg: string; code: number } }> {
  try {
    const cacheKey = await this.utilServices.generateCacheKey('roles', payload);
    console.log("Generated cache key:", cacheKey);

    const cachedData = await this.redisWrapper.getRedisCache<any>(cacheKey);

    if (cachedData) {
      console.log("Data fetched from Redis cache");
      return {
        data: cachedData,
        message: { msg: "Data fetched from cache", code: 1 },
      };
    }

    console.log("No cache found, fetching data from API");

    const response: AxiosResponse<any> = await axios.get(this.PMFBY_ROLE_URL, {
      params: payload || {},
      timeout: 10000,
      headers: { "Content-Type": "application/json" },
    });

    if (!response || !response.data) {
      console.log("No data received from API");
      return {
        data: null,
        message: { msg: "No data received from API", code: 0 },
      };
    }

    const responseData = response.data.data;
    console.log("Data fetched from API, caching it now");
    await this.redisWrapper.setRedisCache(cacheKey, responseData, 86400);

    return {
      data: responseData,
      message: { msg: "Data fetched successfully", code: 1 },
    };
  } catch (error: any) {
    console.log("Error fetching roles:", error);

    let errorMsg = "Failed to fetch roles";
    if (error.response) {
      errorMsg = `API responded with status ${error.response.status}`;
    } else if (error.request) {
      errorMsg = "No response received from API";
    } else if (error.message) {
      errorMsg = `Error: ${error.message}`;
    }

    return {
      data: null,
      message: { msg: errorMsg, code: 0 },
    };
  }
}









  async insuracneTicketListingServicePrevious(payload: any) {
  try {
    const db = this.db;

    let {
      fromdate,
      toDate,
      ticketCategoryID,
      supportTicketTypeID,
      supportTicketNo,
      ticketHeaderID,
      stateID,
      insuranceCompanyID,
      pageIndex = 1,
      pageSize = 100,
      objCommon
    } = payload;

    ticketHeaderID = Number(ticketHeaderID);
    ticketCategoryID = Number(ticketCategoryID);
    supportTicketTypeID = Number(supportTicketTypeID);
    insuranceCompanyID = Number(insuranceCompanyID);

    if (!objCommon || !objCommon.insertedUserID || objCommon.insertedUserID === "") {
      return {
        data: [],
        message: { msg: "User Id is required", code: "0" }
      };
    }

    const [Delta] = await Promise.all([
      new UtilService().getSupportTicketUserDetail(objCommon.insertedUserID),
    ]);

    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];

    console.log(responseInfo);
    if (!item) return { rcode: 0, rmessage: "User details not found." };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await new UtilService().convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await new UtilService().convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
      FromDay: item?.FromDay,
      EscalationFlag: item?.EscalationFlag,
      AppAccessID: item?.AppAccessID
    };

    const { InsuranceCompanyID, StateMasterID, LocationTypeID, AppAccessID } = userDetail;
    let locationFilter: any = {};

    if (LocationTypeID === 1 && StateMasterID?.length) {
      locationFilter = { FilterStateID: { $in: StateMasterID.map(Number) } };
    } else if (LocationTypeID === 2) {
      const districtInfo = await new UtilService().GetDetailsForDistrictUsers(Number(AppAccessID));
      const collectedDistrictInfo = await new UtilService().unGZip(districtInfo.responseDynamic);

      const districtId: number[] = [];
      if (collectedDistrictInfo?.masterdatabinding && Array.isArray(collectedDistrictInfo.masterdatabinding)) {
        for (const itemData of collectedDistrictInfo.masterdatabinding) {
          districtId.push(itemData.DistrictCodeAlpha);
        }
        locationFilter = { DistrictMasterID: { $in: districtId } };
      } else {
        console.warn("Invalid district info format:", collectedDistrictInfo);
        locationFilter = {};
      }
    }

    const match: any = { ...locationFilter };

    if (ticketHeaderID && ticketHeaderID !== 0) match.TicketHeaderID = ticketHeaderID;

    if (insuranceCompanyID && insuranceCompanyID !== 0) {
      const requestedInsuranceIDs = String(insuranceCompanyID).split(",").map(id => Number(id.trim()));
      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter(id => allowedInsuranceIDs.includes(id));
      if (validInsuranceIDs.length === 0) {
        return { rcode: 0, rmessage: "Unauthorized InsuranceCompanyID(s)." };
      }
      match.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else if (InsuranceCompanyID?.length) {
      match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
    }

    if (stateID && Number(stateID) !== 0 && LocationTypeID !== 2) {
      const requestedStateIDs = String(stateID).split(",").map(id => Number(id.trim()));
      const validStateIDs = requestedStateIDs.filter(id => StateMasterID.map(Number).includes(id));
      if (validStateIDs.length === 0) {
        return { rcode: 0, rmessage: "Unauthorized StateID(s)." };
      }
      match.StateMasterID = { $in: validStateIDs };
    } else if (StateMasterID.length && LocationTypeID !== 2) {
      match.FilterStateID = { $in: StateMasterID.map(Number) };
    }

    if (fromdate && toDate) {
      match.Created = {
        $gte: new Date(`${fromdate}T00:00:00.000Z`),
        $lte: new Date(`${toDate}T23:59:59.999Z`)
      };
    }

    if (ticketCategoryID && ticketCategoryID !== 0) match.TicketCategoryID = ticketCategoryID;
    if (supportTicketTypeID && supportTicketTypeID !== 0) match.SupportTicketTypeID = supportTicketTypeID;
    if (supportTicketNo) match.SupportTicketNo = supportTicketNo;

    const pipeline: any[] = [
      { 
        $match: { ...match, TicketStatusID: { $ne: 109303 } } 
      },
      {
        $lookup: {
          from: "Ticket_Assignment_History",
          localField: "SupportTicketID",
          foreignField: "SupportTicketID",
          as: "assignmentHistory"
        }
      },
      {
        $match: { assignmentHistory: { $size: 0 } }
      },
      {
        $facet: {
          data: [
            { $sort: { InsertDateTime: -1 } },
            ...(pageIndex !== -1
              ? [
                  { $skip: (pageIndex - 1) * pageSize },
                  { $limit: pageSize }
                ]
              : []),
            {
              $project: {
                _id: 0,
                SupportTicketNo: 1,
                SupportTicketID: 1,
                RequestorName: 1,
                RequestorMobileNo: 1,
                RequestYear: 1,
                RequestSeason: 1,
                ApplicationNo: 1,
                InsurancePolicyNo: 1,
                TicketCategoryName: 1,
                TicketTypeName: 1,
                TicketHeadName: 1,
                StateMasterName:1,
                TicketDescription:1,
                Created: {
                  $dateToString: {
                    date: { $toDate: "$Created" },
                    format: "%Y-%m-%dT%H:%M:%S",
                    timezone: "Asia/Kolkata"
                  }
                }
              }
            }
          ],
          totalCount: [
            { $count: "count" }
          ],
          ticketStatusSummary: [
            {
              $project: {
                TicketStatusID: 1,
                TicketHeaderID: 1,
                customStatus: {
                  $switch: {
                    branches: [
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
          ]
        }
      }
    ];

    console.log(JSON.stringify(pipeline));
    const aggResult = await db.collection("SLA_Ticket_listing").aggregate(pipeline, { allowDiskUse: true }).toArray();
    const result = aggResult[0] || { data: [], totalCount: [], ticketStatusSummary: [] };

    if (result.data.length === 0) {
      return {
        data: [],
        message: { msg: "Record Not Found", code: "0" },
        totalCount: 0,
        totalPages: 0
      };
    }

    const ticketSummary = result.ticketStatusSummary.map(item => ({
      Total: item.count.toString(),
      TicketStatus: item._id
    }));

    return {
      obj: { status: ticketSummary, supportTicket: result.data },
      message: { msg: "Fetched Success", code: "1" },
      totalCount: result.totalCount[0]?.count || 0,
      totalPages: pageSize > 0 ? Math.ceil((result.totalCount[0]?.count || 0) / pageSize) : 1
    };

  } catch (err) {
    console.error("Top-level error:", err);
    return { data: [], message: "Unexpected error" };
  }
}

async insuracneTicketListingService(payload: any) {
  try {
    const db = this.db;

    let {
      fromdate,
      toDate,
      ticketCategoryID,
      supportTicketTypeID,
      supportTicketNo,
      ticketHeaderID,
      stateID,
      insuranceCompanyID,
      pageIndex = 1,
      pageSize = 100,
      objCommon
    } = payload;

    ticketHeaderID = Number(ticketHeaderID);
    ticketCategoryID = Number(ticketCategoryID);
    supportTicketTypeID = Number(supportTicketTypeID);
    insuranceCompanyID = Number(insuranceCompanyID);

    if (!objCommon || !objCommon.insertedUserID || objCommon.insertedUserID === "") {
      return {
        data: [],
        message: { msg: "User Id is required", code: "0" }
      };
    }

    const [Delta] = await Promise.all([
      new UtilService().getSupportTicketUserDetail(objCommon.insertedUserID),
    ]);

    const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
    const item = (responseInfo.data as any)?.user?.[0];

    if (!item) return { rcode: 0, rmessage: "User details not found." };

    const userDetail = {
      InsuranceCompanyID: item.InsuranceCompanyID ? await new UtilService().convertStringToArray(item.InsuranceCompanyID) : [],
      StateMasterID: item.StateMasterID ? await new UtilService().convertStringToArray(item.StateMasterID) : [],
      BRHeadTypeID: item.BRHeadTypeID,
      LocationTypeID: item.LocationTypeID,
      FromDay: item?.FromDay,
      EscalationFlag: item?.EscalationFlag,
      AppAccessID: item?.AppAccessID
    };

    const { InsuranceCompanyID, StateMasterID, LocationTypeID, AppAccessID } = userDetail;
    let locationFilter: any = {};

    if (LocationTypeID === 1 && StateMasterID?.length) {
      locationFilter = { FilterStateID: { $in: StateMasterID.map(Number) } };
    } else if (LocationTypeID === 2) {
      const districtInfo = await new UtilService().GetDetailsForDistrictUsers(Number(AppAccessID));
      const collectedDistrictInfo = await new UtilService().unGZip(districtInfo.responseDynamic);
      const districtId: number[] = [];
      if (collectedDistrictInfo?.masterdatabinding && Array.isArray(collectedDistrictInfo.masterdatabinding)) {
        for (const itemData of collectedDistrictInfo.masterdatabinding) {
          districtId.push(itemData.DistrictCodeAlpha);
        }
        locationFilter = { DistrictMasterID: { $in: districtId } };
      } else {
        locationFilter = {};
      }
    }

    const match: any = { ...locationFilter };

    if (ticketHeaderID && ticketHeaderID !== 0) match.TicketHeaderID = ticketHeaderID;

    if (insuranceCompanyID && insuranceCompanyID !== 0) {
      const requestedInsuranceIDs = String(insuranceCompanyID).split(",").map(id => Number(id.trim()));
      const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
      const validInsuranceIDs = requestedInsuranceIDs.filter(id => allowedInsuranceIDs.includes(id));
      if (validInsuranceIDs.length === 0) {
        return { rcode: 0, rmessage: "Unauthorized InsuranceCompanyID(s)." };
      }
      match.InsuranceCompanyID = { $in: validInsuranceIDs };
    } else if (InsuranceCompanyID?.length) {
      match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
    }

    if (stateID && Number(stateID) !== 0 && LocationTypeID !== 2) {
      const requestedStateIDs = String(stateID).split(",").map(id => Number(id.trim()));
      const validStateIDs = requestedStateIDs.filter(id => StateMasterID.map(Number).includes(id));
      if (validStateIDs.length === 0) {
        return { rcode: 0, rmessage: "Unauthorized StateID(s)." };
      }
      match.StateMasterID = { $in: validStateIDs };
    } else if (StateMasterID.length && LocationTypeID !== 2) {
      match.FilterStateID = { $in: StateMasterID.map(Number) };
    }

    if (fromdate && toDate) {
      match.Created = {
        $gte: new Date(`${fromdate}T00:00:00.000Z`),
        $lte: new Date(`${toDate}T23:59:59.999Z`)
      };
    }

    if (ticketCategoryID && ticketCategoryID !== 0) match.TicketCategoryID = ticketCategoryID;
    if (supportTicketTypeID && supportTicketTypeID !== 0) match.SupportTicketTypeID = supportTicketTypeID;
    if (supportTicketNo) match.SupportTicketNo = supportTicketNo;

    const pipeline: any[] = [
      { $match: { ...match, TicketStatusID: { $ne: 109303 } } },
      { $sort: { InsertDateTime: -1 } },
      { $facet: {
          data: [
            { $skip: (pageIndex - 1) * pageSize },
            { $limit: pageSize },
            { $lookup: {
                from: "Ticket_Assignment_History",
                localField: "SupportTicketID",
                foreignField: "SupportTicketID",
                as: "assignmentHistory"
              }
            },
            { $match: { assignmentHistory: { $size: 0 } } },
            { $project: {
                _id: 0,
                SupportTicketNo: 1,
                SupportTicketID: 1,
                RequestorName: 1,
                RequestorMobileNo: 1,
                RequestYear: 1,
                RequestSeason: 1,
                ApplicationNo: 1,
                InsurancePolicyNo: 1,
                TicketCategoryName: 1,
                TicketTypeName: 1,
                TicketHeadName: 1,
                StateMasterName:1,
                TicketDescription:1,
                Created: {
                  $dateToString: {
                    date: { $toDate: "$Created" },
                    format: "%Y-%m-%dT%H:%M:%S",
                    timezone: "Asia/Kolkata"
                  }
                }
              }
            }
          ],
          totalCount: [
            { $count: "count" }
          ],
          ticketStatusSummary: [
            { $project: {
                TicketStatusID: 1,
                TicketHeaderID: 1,
                customStatus: {
                  $switch: {
                    branches: [
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
          ]
        }
      }
    ];

    const aggResult = await db.collection("SLA_Ticket_listing").aggregate(pipeline, { allowDiskUse: true }).toArray();
    const result = aggResult[0] || { data: [], totalCount: [], ticketStatusSummary: [] };

    if (result.data.length === 0) {
      return {
        data: [],
        message: { msg: "Record Not Found", code: "0" },
        totalCount: 0,
        totalPages: 0
      };
    }

    const ticketSummary = result.ticketStatusSummary.map(item => ({
      Total: item.count.toString(),
      TicketStatus: item._id
    }));

    return {
      obj: { status: ticketSummary, supportTicket: result.data },
      message: { msg: "Fetched Success", code: "1" },
      totalCount: result.totalCount[0]?.count || 0,
      totalPages: pageSize > 0 ? Math.ceil((result.totalCount[0]?.count || 0) / pageSize) : 1
    };

  } catch (err) {
    console.error("Top-level error:", err);
    return { data: [], message: "Unexpected error" };
  }
}



async AssignTicketServie(payload: any) {
  const db = this.db;
  const ticketCollection = db.collection('SLA_Ticket_listing');
  const assignHistoryCollection = db.collection('Ticket_Assignment_History');

  // Validate payload
  if (!payload || typeof payload !== 'object') {
    return { success: false, summary: null, details: [], message: 'Invalid payload: payload must be an object.' };
  }

  const { ticketIds, assignedBy, assignedTo, roleRightsMasterID, stakeholderUserID } = payload;

  if (!ticketIds || typeof ticketIds !== 'string' || ticketIds.trim() === '') {
    return { success: false, summary: null, details: [], message: 'Invalid payload: ticketIds is required and must be a comma-separated string.' };
  }

  if (!assignedBy || !assignedTo || !roleRightsMasterID || !stakeholderUserID) {
    return { success: false, summary: null, details: [], message: 'Missing required fields: assignedBy, assignedTo, roleRightsMasterID, and stakeholderUserID are all required.' };
  }

  const ticketIdArray = ticketIds
    .split(',')
    .map(id => id.trim())
    .filter(id => id !== '');

  if (ticketIdArray.length === 0) {
    return { success: false, summary: null, details: [], message: 'No valid ticket IDs provided in ticketIds.' };
  }

  const results: { ticketId: string; ticketNo?: string; status: string; reason?: string }[] = [];
  const now = new Date();

  for (const ticketId of ticketIdArray) {
    try {
      const ticketIdNum = Number(ticketId);
      if (isNaN(ticketIdNum)) {
        results.push({ ticketId, status: 'Failed', reason: 'Invalid ticket ID format' });
        continue;
      }

      const existingTicket = await ticketCollection.findOne({ SupportTicketID: ticketIdNum });
      if (!existingTicket) {
        results.push({ ticketId, status: 'Failed', reason: 'Ticket not found in the system' });
        continue;
      }

      const alreadyAssigned = await assignHistoryCollection.findOne({ SupportTicketID: ticketIdNum });
      if (alreadyAssigned) {
        results.push({ ticketId, ticketNo: existingTicket.SupportTicketNo, status: 'Failed', reason: 'Ticket is already assigned' });
        continue;
      }

      const assignmentRecord = {
        SupportTicketID: ticketIdNum,
        SupportTicketNo: existingTicket.SupportTicketNo,
        TicketStatusID: existingTicket.TicketStatusID || null,
        TicketStatus: existingTicket.TicketStatus || null,
        assignedBy,
        assignedTo,
        RoleRightMasterID: roleRightsMasterID,
        StakeHolderUserID: stakeholderUserID,
        AssignedDate: now,
      };

      const insertResult = await assignHistoryCollection.insertOne(assignmentRecord);

      if (!insertResult.insertedId) {
        results.push({ ticketId, ticketNo: existingTicket.SupportTicketNo, status: 'Failed', reason: 'Failed to save assignment history' });
        continue;
      }

      results.push({ ticketId, ticketNo: existingTicket.SupportTicketNo, status: 'Success', reason: `Ticket ${existingTicket.SupportTicketNo} assigned successfully` });
    } catch (err: any) {
      results.push({ ticketId, status: 'Error', reason: err.message || 'Unexpected error occurred' });
    }
  }

  const successCount = results.filter(r => r.status === 'Success').length;
  const failedCount = results.filter(r => r.status === 'Failed' || r.status === 'Error').length;

  const summaryMessage =
    successCount === ticketIdArray.length
      ? 'All tickets assigned successfully.'
      : successCount === 0
      ? 'No tickets were assigned. All failed.'
      : `${successCount} ticket(s) assigned successfully, ${failedCount} ticket(s) failed.`;

  const summary = {
    totalTickets: ticketIdArray.length,
    successCount,
    failedCount,
    message: summaryMessage,
  };

  return { success: true, summary, details: results };
}


async UserWiseState(payload: any) {
  try {
    const db = this.db;
    const utilService = new UtilService();

    if (!payload || typeof payload !== "object") {
      console.log("Invalid payload");
      return { data: [], msg: "Invalid payload", code: "0" };
    }

    const { userID } = payload;

    if (!userID || (typeof userID !== "string" && typeof userID !== "number")) {
      console.log("User ID is required and must be valid");
      return { data: [], msg: "User ID is required and must be valid", code: "0" };
    }

    const cacheKey = await this.utilServices.generateCacheKey("UserWiseState", payload);
    console.log("Generated cache key:", cacheKey);

    const cachedData = await this.redisWrapper.getRedisCache<any>(cacheKey);
    if (cachedData) {
      console.log("Data fetched from Redis cache");
      return { data: cachedData, msg: "Data fetched from cache", code: "1" };
    }

    console.log("No cache found, fetching data from DB");

    let Delta;
    try {
      [Delta] = await Promise.all([utilService.getSupportTicketUserDetail(userID)]);
    } catch (dbFetchError) {
      console.log("Error fetching user details:", dbFetchError);
      return { data: [], msg: "Failed to fetch user details", code: "0" };
    }

    if (!Delta || !Delta.responseDynamic) {
      console.log("User does not exist");
      return { data: [], msg: "User does not exist", code: "0" };
    }

    let responseInfo;
    try {
      responseInfo = await utilService.unGZip(Delta.responseDynamic);
    } catch (gzipError) {
      console.log("Error unGZip user data:", gzipError);
      return { data: [], msg: "Failed to process user data", code: "0" };
    }

    const item = (responseInfo?.data?.user?.[0]) || null;

    if (!item) {
      console.log("User does not exist after unGZip");
      return { data: [], msg: "User does not exist", code: "0" };
    }

    let StateMasterID: number[] = [];
    try {
      if (item.StateMasterID) {
        const arr = await utilService.convertStringToArray(item.StateMasterID);
        StateMasterID = Array.isArray(arr) ? arr.map(Number).filter(n => !isNaN(n)) : [];
      }
    } catch (convertError) {
      console.log("Error converting StateMasterID:", convertError);
      return { data: [], msg: "Invalid StateMasterID format", code: "0" };
    }

    if (!StateMasterID.length) {
      console.log("No states assigned to user");
      return { data: [], msg: "No states assigned to user", code: "0" };
    }

    const pipeline = [
      { $match: { StateMasterID: { $in: StateMasterID } } },
      {
        $group: {
          _id: "$StateMasterID",
          StateMasterName: { $first: "$StateMasterName" },
        },
      },
      { $addFields: { order: { $indexOfArray: [StateMasterID, "$_id"] } } },
      { $sort: { order: 1 } },
      {
        $project: {
          _id: 0,
          StateMasterID: "$_id",
          StateName: "$StateMasterName",
        },
      },
    ];

    let extractedData = [];
    try {
      extractedData = await db.collection("STATEMASTERSQL").aggregate(pipeline).toArray();
      console.log("Data fetched from DB");
    } catch (dbError) {
      console.log("Error fetching state data:", dbError);
      return { data: [], msg: "Failed to fetch state data", code: "0" };
    }

    await this.redisWrapper.setRedisCache(cacheKey, extractedData, 86400);
    console.log("Cached DB result in Redis");

    return { data: extractedData, msg: "Fetched successfully", code: "1" };

  } catch (err) {
    console.log("Top-level error:", err);
    return { data: [], msg: "Unexpected error occurred", code: "0" };
  }
}




async RoleWiseAssignedTickets(payload: any) {
  try {
    if (!payload || !payload.loggedInUserId) {
      return { data: [], message: { msg: "Invalid payload", code: "0" } };
    }

    const db = this.db;
    let {
      loggedInUserId,
      SupportTicketNo,
      ApplicationNo,
      TicketHeaderID,
      fromDate,
      toDate,
      page = 1,
      limit = 20
    } = payload;

    TicketHeaderID = TicketHeaderID ? Number(TicketHeaderID) : null;

    const collection = db.collection("Ticket_Assignment_History");
    if (!collection) {
      return { data: [], message: { msg: "Collection not found", code: "0" } };
    }

    const fromDateISO = fromDate ? moment(fromDate, "YYYY-MM-DD").startOf("day").toDate() : null;
    const toDateISO = toDate ? moment(toDate, "YYYY-MM-DD").endOf("day").toDate() : null;

    const pipeline: any[] = [
      { $match: { RoleRightMasterID: loggedInUserId } },
      {
        $lookup: {
          from: "SLA_Ticket_listing",
          localField: "SupportTicketID",
          foreignField: "SupportTicketID",
          as: "TicketRecords",
        },
      },
      {
        $addFields: {
          TicketRecords: {
            $filter: {
              input: "$TicketRecords",
              as: "ticket",
              cond: {
                $and: [
                  SupportTicketNo ? { $eq: ["$$ticket.SupportTicketNo", SupportTicketNo] } : {},
                  ApplicationNo ? { $eq: ["$$ticket.ApplicationNo", ApplicationNo] } : {},
                  TicketHeaderID ? { $eq: ["$$ticket.TicketHeaderID", TicketHeaderID] } : {},
                  fromDateISO ? { $gte: ["$$ticket.Created", fromDateISO] } : {},
                  toDateISO ? { $lte: ["$$ticket.Created", toDateISO] } : {},
                ],
              },
            },
          },
        },
      },
      {
        $group: {
          _id: "$RoleRightMasterID",
          TicketRecords: { $push: "$TicketRecords" },
        },
      },
      {
        $project: {
          _id: 0,
          RoleRightMasterID: "$_id",
          TicketRecords: {
            $reduce: {
              input: "$TicketRecords",
              initialValue: [],
              in: { $concatArrays: ["$$value", { $ifNull: ["$$this", []] }] },
            },
          },
        },
      },
      {
        $addFields: {
          TicketCount: { $size: { $ifNull: ["$TicketRecords", []] } },
        },
      },
      {
        $addFields: {
          TicketRecords: {
            $slice: ["$TicketRecords", (Number(page) - 1) * Number(limit), Number(limit)],
          },
        },
      },
    ];

    const data = await collection.aggregate(pipeline).toArray();

    return {
      data: Array.isArray(data) && data.length ? data : [],
      message: Array.isArray(data) && data.length
        ? { msg: "Success", code: "1" }
        : { msg: "No Record Found", code: "0" },
    };
  } catch (error) {
    return { data: null, message: { msg: error?.message || "Failed", code: "0" } };
  }
}






}
