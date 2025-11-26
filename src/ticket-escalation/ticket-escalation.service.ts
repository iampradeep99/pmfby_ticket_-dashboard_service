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
    const response: AxiosResponse<any> = await axios.get(this.PMFBY_ROLE_URL, {
      params: payload || {},
      timeout: 10000,
      headers: { "Content-Type": "application/json" },
    });

    if (!response?.data?.data) {
      return {
        data: null,
        message: { msg: "No data received from API", code: 0 },
      };
    }

    const allRoles = response.data.data;

    if (!payload?.type) {
      const uniqueUserTypes = Array.from(new Set(allRoles.map((r: any) => r.userType)))
        .filter(Boolean)
        .sort()
        .map((type) => ({ userType: type }));

      return {
        data: uniqueUserTypes,
        message: { msg: "Fetched all unique user types", code: 1 },
      };
    }

    const filteredRoles = allRoles.filter(
      (r: any) => r.userType?.toLowerCase() === payload.type.toLowerCase()
    );

    return {
      data: filteredRoles,
      message: { msg: `Fetched roles for userType: ${payload.type}`, code: 1 },
    };
  } catch (error: any) {
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


async getRole(payload: any) {
  try {
    const { StateID } = payload;
    if (!StateID)
      return { data: {}, message: { msg: "StateID required", code: 0 } };

    const db = this.db;
    const collectionName = "escalation_matrix_master";

    const results = await db
      .collection(collectionName)
      .aggregate([
        {
          $match: { state_id: StateID }
        },
        {
          $project: {
            _id: 0,
            Bank_id: "$bank_id",
            Bank_name: "$bank_name",
            Bank_type: "$bank_type",
            Pfms_bank_code: "$pfms_bank_code",
            Pfms_bank_name: "$pfms_bank_name",
            Bank_code: "$bank_code",
            Administering_agency: "$administering_agency",
            Ownership_type: "$ownership_type",
            Apex_bank_id: "$apex_bank_id",
            State_id: "$state_id",
            State_code: "$state_code"
          }
        }
      ])
      .toArray();

    if (!results.length) {
      return { data: {}, message: { msg: "No data found", code: 0 } };
    }
let obj = {
BankInfo: results
}
    return {
      data: obj,
      message: { msg: "Fetched", code: 1 }
    };
  } catch (err) {
    return { data: {}, message: { msg: "Error", code: 0 } };
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
      { $limit: pageSize * 2 },
      { $sort: { InsertDateTime: -1 } },
      { $lookup: {
          from: "Ticket_Assignment_History",
          localField: "SupportTicketID",
          foreignField: "SupportTicketID",
          as: "assignmentHistory"
        }
      },
      { $match: { assignmentHistory: { $size: 0 } } },
      { $facet: {
          data: [
            { $skip: (pageIndex - 1) * pageSize },
            { $limit: pageSize },
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


/* async UserWiseState(payload: any) {
  try {
    const db = this.db;
    const utilService = new UtilService();

    if (!payload || typeof payload !== "object") {
      return { data: [], msg: "Invalid payload", code: "0" };
    }

    const userID = payload.userID;
    if (!userID) {
      return { data: [], msg: "User ID required", code: "0" };
    }

    const cacheKey = await this.utilServices.generateCacheKey("UserWiseState", payload);
    const cachedData = await this.redisWrapper.getRedisCache<any>(cacheKey);
    if (cachedData) {
      return { data: cachedData, msg: "Data fetched from cache", code: "1" };
    }

    let userDetail;
    try {
      userDetail = await utilService.getSupportTicketUserDetail(userID);
    } catch {
      return { data: [], msg: "Failed to fetch user details", code: "0" };
    }

    if (!userDetail?.responseDynamic) {
      return { data: [], msg: "User not found", code: "0" };
    }

    let responseInfo;
    try {
      responseInfo = await utilService.unGZip(userDetail.responseDynamic);
    } catch {
      return { data: [], msg: "Failed to process user data", code: "0" };
    }

    const item = responseInfo?.data?.user?.[0];
    if (!item) {
      return { data: [], msg: "User not found", code: "0" };
    }

    let StateMasterID: number[] = [];
    try {
      const arr = await utilService.convertStringToArray(item.StateMasterID);
      StateMasterID = Array.isArray(arr) ? arr.map(Number).filter(n => !isNaN(n)) : [];
    } catch {
      return { data: [], msg: "Invalid StateMasterID", code: "0" };
    }

    if (!StateMasterID.length) {
      return { data: [], msg: "No states assigned", code: "0" };
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

    let extractedData;
    try {
      extractedData = await db.collection("STATEMASTERSQL").aggregate(pipeline).toArray();
    } catch {
      return { data: [], msg: "Failed to fetch state data", code: "0" };
    }

    await this.redisWrapper.setRedisCache(cacheKey, extractedData, 86400);

    return { data: extractedData, msg: "Fetched successfully", code: "1" };
  } catch {
    return { data: [], msg: "Unexpected error", code: "0" };
  }
} */


  async UserWiseState(payload: any) {
  try {
    if (!payload?.userID) return { data: [], msg: "User ID required", code: "0" };

    const cacheKey = await this.utilServices.generateCacheKey("UserWiseState", payload);
    const cached = await this.redisWrapper.getRedisCache<any>(cacheKey);
    if (cached) return { data: cached, msg: "Data fetched from cache", code: "1" };

    const util = new UtilService();
    const userDetail = await util.getSupportTicketUserDetail(payload.userID).catch(() => null);
    if (!userDetail?.responseDynamic) return { data: [], msg: "User not found", code: "0" };

    const info = await util.unGZip(userDetail.responseDynamic).catch(() => null);
    const item = info?.data?.user?.[0];
    if (!item) return { data: [], msg: "User not found", code: "0" };

    const arr = await util.convertStringToArray(item.StateMasterID).catch(() => []);
    const StateMasterID = arr.map(Number).filter(n => !isNaN(n));
    if (!StateMasterID.length) return { data: [], msg: "No states assigned", code: "0" };

    const pipeline = [
      { $match: { StateMasterID: { $in: StateMasterID } } },
      { $group: { _id: "$StateMasterID", StateMasterName: { $first: "$StateMasterName" } } },
      { $addFields: { order: { $indexOfArray: [StateMasterID, "$_id"] } } },
      { $sort: { order: 1 } },
      { $project: { _id: 0, StateMasterID: "$_id", StateName: "$StateMasterName" } },
    ];

    const data = await this.db.collection("STATEMASTERSQL").aggregate(pipeline).toArray();
    await this.redisWrapper.setRedisCache(cacheKey, data, 86400);

    return { data, msg: "Fetched successfully", code: "1" };
  } catch {
    return { data: [], msg: "Unexpected error", code: "0" };
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
    //   {
    //     $project: {
    //       _id: 0,
    //       RoleRightMasterID: "$_id",
    //       TicketRecords: {
    //         $reduce: {
    //           input: "$TicketRecords",
    //           initialValue: [],
    //           in: { $concatArrays: ["$$value", { $ifNull: ["$$this", []] }] },
    //         },
    //       },
    //     },
    //   },
    {
  $project: {
    _id: 0,
    RoleRightMasterID: "$_id",
    TicketRecords: {
      $map: {
        input: {
          $reduce: {
            input: "$TicketRecords",
            initialValue: [],
            in: { $concatArrays: ["$$value", { $ifNull: ["$$this", []] }] },
          },
        },
        as: "record",
        in: {
          SupportTicketID: "$$record.SupportTicketID",
          CallerContactNumber: "$$record.CallerContactNumber",
          CallingAudioFile: "$$record.CallingAudioFile",
          TicketRequestorID: "$$record.TicketRequestorID",
          StateCodeAlpha: "$$record.StateCodeAlpha",
          StateMasterID: "$$record.StateMasterID",
          DistrictMasterID: "$$record.DistrictMasterID",
          VillageRequestorID: "$$record.VillageRequestorID",
          NyayPanchayatID: "$$record.NyayPanchayatID",
          NyayPanchayat: "$$record.NyayPanchayat",
          GramPanchayatID: "$$record.GramPanchayatID",
          GramPanchayat: "$$record.GramPanchayat",
          CallerID: "$$record.CallerID",
          CreationMode: "$$record.CreationMode",
          SupportTicketNo: "$$record.SupportTicketNo",
          RequestorUniqueNo: "$$record.RequestorUniqueNo",
          RequestorName: "$$record.RequestorName",
          RequestorMobileNo: "$$record.RequestorMobileNo",
          RequestorAccountNo: "$$record.RequestorAccountNo",
          RequestorAadharNo: "$$record.RequestorAadharNo",
          TicketCategoryID: "$$record.TicketCategoryID",
          CropCategoryOthers: "$$record.CropCategoryOthers",
          CropStageMaster: "$$record.CropStageMaster",
          CropStageMasterID: "$$record.CropStageMasterID",
          TicketHeaderID: "$$record.TicketHeaderID",
          SupportTicketTypeID: "$$record.SupportTicketTypeID",
          RequestYear: "$$record.RequestYear",
          RequestSeason: "$$record.RequestSeason",
          TicketSourceID: "$$record.TicketSourceID",
          TicketDescription: "$$record.TicketDescription",
          LossDate: "$$record.LossDate",
          LossTime: "$$record.LossTime",
          OnTimeIntimationFlag: "$$record.OnTimeIntimationFlag",
          VillageName: "$$record.VillageName",
          ApplicationCropName: "$$record.ApplicationCropName",
          CropName: "$$record.CropName",
          AREA: "$$record.AREA",
          DistrictRequestorID: "$$record.DistrictRequestorID",
          PostHarvestDate: "$$record.PostHarvestDate",
          TicketStatusID: "$$record.TicketStatusID",
          StatusUpdateTime: "$$record.StatusUpdateTime",
          StatusUpdateUserID: "$$record.StatusUpdateUserID",
          ApplicationNo: "$$record.ApplicationNo",
          InsuranceCompanyCode: "$$record.InsuranceCompanyCode",
          InsuranceCompanyID: "$$record.InsuranceCompanyID",
          InsurancePolicyNo: "$$record.InsurancePolicyNo",
          InsurancePolicyDate: "$$record.InsurancePolicyDate",
          InsuranceExpiryDate: "$$record.InsuranceExpiryDate",
          BankMasterID: "$$record.BankMasterID",
          AgentUserID: "$$record.AgentUserID",
          SchemeID: "$$record.SchemeID",
          AttachmentPath: "$$record.AttachmentPath",
          HasDocument: "$$record.HasDocument",
          Relation: "$$record.Relation",
          RelativeName: "$$record.RelativeName",
          SubDistrictID: "$$record.SubDistrictID",
          SubDistrictName: "$$record.SubDistrictName",
          PolicyPremium: "$$record.PolicyPremium",
          PolicyArea: "$$record.PolicyArea",
          PolicyType: "$$record.PolicyType",
          LandSurveyNumber: "$$record.LandSurveyNumber",
          LandDivisionNumber: "$$record.LandDivisionNumber",
          PlotVillageName: "$$record.PlotVillageName",
          PlotDistrictName: "$$record.PlotDistrictName",
          PlotStateName: "$$record.PlotStateName",
          ApplicationSource: "$$record.ApplicationSource",
          CropShare: "$$record.CropShare",
          IFSCCode: "$$record.IFSCCode",
          FarmerShare: "$$record.FarmerShare",
          CropSeasonName: "$$record.CropSeasonName",
          TicketSourceName: "$$record.TicketSourceName",
          TicketCategoryName: "$$record.TicketCategoryName",
          TicketStatus: "$$record.TicketStatus",
          InsuranceCompany: "$$record.InsuranceCompany",
          TicketTypeName: "$$record.TicketTypeName",
          StateMasterName: "$$record.StateMasterName",
          DistrictMasterName: "$$record.DistrictMasterName",
          TicketHeadName: "$$record.TicketHeadName",
          BMCGCode: "$$record.BMCGCode",
          BusinessRelationName: "$$record.BusinessRelationName",
          CropLossDetailID: "$$record.CropLossDetailID",
          CallingUniqueID: "$$record.CallingUniqueID",
          CallingInsertUserID: "$$record.CallingInsertUserID",
          CropStage: "$$record.CropStage",
          CategoryHeadID: "$$record.CategoryHeadID",
          Sos: "$$record.Sos",
          IsSos: "$$record.IsSos",
          TicketNCIPDocketNo: "$$record.TicketNCIPDocketNo",
          FilterDistrictRequestorID: "$$record.FilterDistrictRequestorID",
          FilterStateID: "$$record.FilterStateID",
          SchemeName: "$$record.SchemeName",
          InsertUserID: "$$record.InsertUserID",
          InsertIPAddress: "$$record.InsertIPAddress",
          UpdateUserID: "$$record.UpdateUserID",
          AgentName: "$$record.AgentName",
          CreatedBY: "$$record.CreatedBY",
          CallingUserID: "$$record.CallingUserID",
          TicketReOpenDate: {
            $cond: {
              if: { $or: [{ $eq: ["$$record.TicketReOpenDate", null] }, { $eq: ["$$record.TicketReOpenDate", ""] }] },
              then: null,
              else: {
                $dateToString: {
                  date: { $toDate: "$$record.TicketReOpenDate" },
                  format: "%Y-%m-%dT%H:%M:%S",
                  timezone: "Asia/Kolkata",
                },
              },
            },
          },
          InsertDateTime: {
            $cond: {
              if: { $or: [{ $eq: ["$$record.InsertDateTime", null] }, { $eq: ["$$record.InsertDateTime", ""] }] },
              then: null,
              else: {
                $dateToString: {
                  date: { $toDate: "$$record.InsertDateTime" },
                  format: "%Y-%m-%dT%H:%M:%S",
                  timezone: "Asia/Kolkata",
                },
              },
            },
          },
          UpdateDateTime: {
            $cond: {
              if: { $or: [{ $eq: ["$$record.UpdateDateTime", null] }, { $eq: ["$$record.UpdateDateTime", ""] }] },
              then: null,
              else: {
                $dateToString: {
                  date: { $toDate: "$$record.UpdateDateTime" },
                  format: "%Y-%m-%dT%H:%M:%S",
                  timezone: "Asia/Kolkata",
                },
              },
            },
          },
          SowingDate: {
            $cond: {
              if: { $or: [{ $eq: ["$$record.SowingDate", null] }, { $eq: ["$$record.SowingDate", ""] }] },
              then: null,
              else: {
                $dateToString: {
                  date: { $toDate: "$$record.SowingDate" },
                  format: "%Y-%m-%dT%H:%M:%S",
                  timezone: "Asia/Kolkata",
                },
              },
            },
          },
          CreatedAt: {
            $dateToString: {
              date: { $toDate: "$$record.Created" },
              format: "%Y-%m-%dT%H:%M:%S",
              timezone: "Asia/Kolkata",
            },
          },
        },
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


 
// async uploadTicketPDFService(payload: {
//   fileBuffer: Buffer;
//   fileName: string;
//   mimeType: string;
//   uploadedBy: string;
// }): Promise<{ data: any; message: { msg: string; code: number } }> {
//   try {
//     const { fileBuffer, fileName, mimeType, uploadedBy } = payload;

//     const uniqueHash = randomBytes(16).toString('hex');
//     const timestamp = new Date().getTime();
//     const sanitizedName = fileName.replace(/[^a-zA-Z0-9.\-_]/g, '_');
//     const safeFileName = `${timestamp}_${uniqueHash}_${sanitizedName}`;
//     const filePath = path.join('tickets', safeFileName);

//     const gcpService = new GCPServices();
//     const uploadResponse = await gcpService.uploadFileToGCP({
//       filePath,
//       uploadedBy,
//       file: {
//         buffer: fileBuffer,
//         originalname: safeFileName,
//       },
//     });

//     if (!uploadResponse.success) {
//       throw new Error(uploadResponse.message || 'GCP upload failed');
//     }

//     return {
//       data: {
//         fileName: safeFileName,
//         mimeType,
//         filePath: uploadResponse.url || filePath,
//         size: fileBuffer.length,
//         integrity: uniqueHash,
//       },
//       message: {
//         msg: 'PDF uploaded successfully to GCP',
//         code: 200,
//       },
//     };
//   } catch (err: any) {
//     const msg = err instanceof Error ? err.message : 'Unknown error occurred';
//     throw {
//       message: {
//         msg,
//         code: 500,
//       },
//     };
//   }
// }






/* async uploadTicketPDFService(payload: {
  fileBuffer: Buffer;
  fileName: string;
  mimeType: string;
}): Promise<{ data: any; message: { msg: string; code: number } }> {
  try {
    const { fileBuffer, fileName, mimeType } = payload;

    const uniqueHash = randomBytes(16).toString('hex');
    const timestamp = new Date().getTime();
    const sanitizedName = fileName.replace(/[^a-zA-Z0-9.\-_]/g, '_');
    const safeFileName = `${timestamp}_${uniqueHash}_${sanitizedName}`;

    const form = new FormData();
    form.append('filePath', 'krph_reports/October2025/'); 
    form.append('uploadedBy', 'KRPH'); 
    form.append('documents', fileBuffer, safeFileName);

    // Call external API
    const response = await axios.post(
      'https://pmfby.gov.in/krphapi/FGMS/GCPFileUpload',
      form,
      {
        headers: form.getHeaders(),
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
      }
    );

    const responseData = response.data;

    if (responseData.responseCode !== '1') {
      throw new Error(responseData.responseMessage || 'File upload failed');
    }

    const compressedBuffer = Buffer.from(responseData.responseDynamic, 'base64');
    const decompressedBuffer = gunzipSync(compressedBuffer);
    const uploadedFiles = JSON.parse(decompressedBuffer.toString());
    const gcpDownloadUrl = uploadedFiles?.[0]?.gcsUrl || '';
    return {
      data: {
        fileName: safeFileName,
        mimeType,
        filePath: gcpDownloadUrl,
        size: fileBuffer.length,
        integrity: uniqueHash,
      },
      message: {
        msg: 'PDF uploaded successfully to GCP',
        code: 1,
      },
    };
  } catch (err: any) {
    const msg = err instanceof Error ? err.message : 'Unknown error occurred';
    throw {
      message: {
        msg,
        code: 500,
      },
    };
  }
}
 */




async uploadTicketPDFService(payload: any) {
  try {
    const {
      fileBuffer,
      fileName,
      SupportTicketID,
      SupportTicketNo,
      TicketHistoryID,
      TicketStatusID,
      LastTicketStatusID,
      RequestorMobileNo,
      UpdatedByID,
      UpdatedBy,
      UpdateDateTime,
    } = payload;

    if (!fileBuffer || !(fileBuffer instanceof Buffer) || fileBuffer.length === 0) {
      throw new Error('Invalid or missing fileBuffer');
    }

    if (!fileName || typeof fileName !== 'string' || !fileName.trim()) {
      throw new Error('Invalid or missing fileName');
    }

    if (!SupportTicketID || !TicketHistoryID) {
      throw new Error('Missing essential ticket identifiers');
    }

    const database = this.db;

    const uniqueHash = require('crypto').randomBytes(16).toString('hex');
    const timestamp = Date.now();
    const sanitizedName = fileName.replace(/[^a-zA-Z0-9.\-_]/g, '_');
    const safeFileName = `${timestamp}_${uniqueHash}_${sanitizedName}`;

    const FormData = require('form-data');
    const form = new FormData();
    form.append('filePath', 'krph_reports/October2025/');
    form.append('uploadedBy', "KRPH");
    form.append('documents', fileBuffer, {
      filename: safeFileName,
      contentType: 'application/pdf',
    });

    const axios = require('axios');
    const response = await axios.post(
      'https://pmfby.gov.in/krphapi/FGMS/GCPFileUpload',
      form,
      {
        headers: { ...form.getHeaders() },
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 30000,
        validateStatus: status => status >= 200 && status < 500,
      }
    );

    const responseData = response.data;
    if (!responseData || responseData.responseCode !== '1') {
      throw new Error(responseData?.responseMessage || 'External file upload failed');
    }

    let gcpDownloadUrl = '';
    try {
      const compressedBuffer = Buffer.from(responseData.responseDynamic || '', 'base64');
      if (!compressedBuffer || compressedBuffer.length === 0) throw new Error('Empty compressed response');

      const { gunzipSync } = require('zlib');
      const decompressedBuffer = gunzipSync(compressedBuffer);
      if (!decompressedBuffer || decompressedBuffer.length === 0) throw new Error('Failed to decompress response');

      const uploadedFiles = JSON.parse(decompressedBuffer.toString());
      if (!Array.isArray(uploadedFiles) || !uploadedFiles[0]?.gcsUrl) {
        throw new Error('Invalid response structure from GCP upload');
      }

      gcpDownloadUrl = uploadedFiles[0].gcsUrl;
      if (!gcpDownloadUrl.startsWith('https://')) {
        throw new Error('Invalid GCP download URL');
      }
    } catch (err) {
      throw new Error('Failed to parse or decompress upload response: ' + err);
    }

    const craftedPayloadForDB = {
      SupportTicketID,
      SupportTicketNo: SupportTicketNo || '',
      TicketHistoryID,
      TicketStatusID: TicketStatusID || null,
      LastTicketStatusID: LastTicketStatusID || null,
      RequestorMobileNo:RequestorMobileNo || null,
      UpdatedByID: UpdatedByID || null,
      UpdatedBy: UpdatedBy || 'Unknown',
      UpdateDateTime: UpdateDateTime || new Date().toISOString(),
      gcpDownloadUrl,
    };

    const insertedRecords = await this.InsertToDBService(craftedPayloadForDB, database);
    if (!insertedRecords || !insertedRecords.ticketUrl) {
      throw new Error('Database insertion failed');
    }

    return {
      data: insertedRecords.ticketUrl,
      message: {
        msg: 'PDF uploaded successfully to GCP and ticket info processed',
        code: 1,
      },
    };
  } catch (err: any) {
    console.error('[uploadTicketPDFService] Error:', err);
    return {
      data: null,
      message: {
        msg: err instanceof Error ? err.message : 'Unknown error occurred',
        code: 500,
      },
    };
  }
}



/* async InsertToDBService(payload: any, db: any) {
  try {
    if (!payload || typeof payload !== 'object') {
      throw new Error('Invalid payload');
    }

    if (!db) {
      throw new Error('Database instance is required');
    }

    const collectionName = 'KRPH_Ticket_PDF_History';

    try {
      const collections = await db.listCollections({ name: collectionName }).toArray();
      if (!Array.isArray(collections)) {
        throw new Error('Failed to list collections from DB');
      }
      if (collections.length === 0) {
        await db.createCollection(collectionName);
      }
    } catch (err) {
      throw new Error('Failed to verify or create collection: ' + err);
    }

    const toNumber = (value: any) => {
      const num = Number(value);
      return isNaN(num) ? null : num;
    };

    const document = {
      SupportTicketID: toNumber(payload.SupportTicketID),
      SupportTicketNo: payload.SupportTicketNo || '',
      TicketHistoryID: toNumber(payload.TicketHistoryID),
      TicketStatusID: toNumber(payload.TicketStatusID),
      TicketStatus: payload.TicketStatusID
        ? await this.utilServices.getStatusName(toNumber(payload.TicketStatusID))
        : 'Unknown',
      LastTicketStatusID: toNumber(payload.LastTicketStatusID),
      LastTicketStatus: payload.LastTicketStatusID
        ? await this.utilServices.getStatusName(toNumber(payload.LastTicketStatusID))
        : 'Unknown',
        RequestorMobileNo:payload?.RequestorMobileNo,
      TicketFileURl: payload.gcpDownloadUrl || '',
      UpdatedByID: toNumber(payload.UpdatedByID),
      UpdatedBy: payload.UpdatedBy || 'Unknown',
      UpdateDateTime: payload.UpdateDateTime || new Date().toISOString(),
      InsertedDateTime: new Date(),
    };

    const result = await db.collection(collectionName).insertOne(document);
    if (!result?.insertedId) {
      throw new Error('Failed to insert document into database');
    }

    return {
      success: true,
      insertedId: result?.insertedId,
      message: 'Ticket PDF info inserted successfully',
    };
  } catch (err: any) {
    const msg = err instanceof Error ? err.message : 'Unknown error occurred';
    return {
      success: false,
      message: msg,
    };
  }
}
 */

async InsertToDBService(payload: any, db: any) {
  try {
    if (!payload || typeof payload !== 'object') {
      throw new Error('Invalid payload');
    }

    if (!db) {
      throw new Error('Database instance is required');
    }

    const collectionName = 'KRPH_Ticket_PDF_History';

    try {
      const collections = await db.listCollections({ name: collectionName }).toArray();
      if (!Array.isArray(collections)) {
        throw new Error('Failed to list collections from DB');
      }
      if (collections.length === 0) {
        await db.createCollection(collectionName);
      }
    } catch (err) {
      throw new Error('Failed to verify or create collection: ' + err);
    }

    const toNumber = (value: any) => {
      const num = Number(value);
      return isNaN(num) ? null : num;
    };

    const document = {
      SupportTicketID: toNumber(payload.SupportTicketID),
      SupportTicketNo: payload.SupportTicketNo || '',
      TicketHistoryID: toNumber(payload.TicketHistoryID),
      TicketStatusID: toNumber(payload.TicketStatusID),
      TicketStatus: payload.TicketStatusID
        ? await this.utilServices.getStatusName(toNumber(payload.TicketStatusID))
        : 'Unknown',
      LastTicketStatusID: toNumber(payload.LastTicketStatusID),
      LastTicketStatus: payload.LastTicketStatusID
        ? await this.utilServices.getStatusName(toNumber(payload.LastTicketStatusID))
        : 'Unknown',
      RequestorMobileNo: payload?.RequestorMobileNo,
      TicketFileURl: payload.gcpDownloadUrl || '',
      UpdatedByID: toNumber(payload.UpdatedByID),
      UpdatedBy: payload.UpdatedBy || 'Unknown',
      UpdateDateTime: payload.UpdateDateTime || new Date().toISOString(),
      InsertedDateTime: new Date(),
    };

    const result = await db.collection(collectionName).insertOne(document);
    if (!result?.insertedId) {
      throw new Error('Failed to insert document into database');
    }

    return {
      success: true,
      ticketUrl: document.TicketFileURl,
      message: 'Ticket PDF info inserted successfully',
    };
  } catch (err: any) {
    const msg = err instanceof Error ? err.message : 'Unknown error occurred';
    return {
      success: false,
      message: msg,
    };
  }
}



}







