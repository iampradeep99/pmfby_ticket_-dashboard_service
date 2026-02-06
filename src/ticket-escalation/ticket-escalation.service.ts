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
import * as https from 'https';
const Logger = require("../commonServices/logger");



// import * as ExcelJS from 'exceljs';

@Injectable()
export class TicketEscalationService {
  private logger: InstanceType<typeof Logger>;

  private ticketCollection: Collection;
  private ticketDbCollection: Collection;
  private tokenCache = new NodeCache({ stdTTL: 30 * 60 });

  private readonly PMFBY_ROLE_URL = config.pmfbyRoleURL
  private readonly RoleURL = "https://pmfby.gov.in/krishirakshak/v1/user/user/roleWiseUserList"
  //  private readonly RoleURL = "http://10.128.60.9:3011/krishirakshak/v1/user/user/roleWiseUserList"

  public gcp = new GCPServices();
  logDir = path.join(__dirname, '..', 'logs');

  constructor(
    @Inject('MONGO_DB') private readonly db: Db,
    @Inject('SEQUELIZE') private readonly sequelize: Sequelize,
    private readonly redisWrapper: RedisWrapper,
    private readonly mailService: MailService,
    private readonly utilServices: UtilService,
  ) {
    this.ticketCollection = this.db.collection('tickets');
    this.ticketDbCollection = this.db.collection('SLA_KRPH_SupportTickets_Records');
    this.logger = new Logger("Call_Quality_Assurance_Service.log");

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


  async getTokenOld() {
    try {
      const payload = {
        deviceType: config.pmfbyConfig.deviceType,
        otp: Number(config.pmfbyConfig.otp),
        password: config.pmfbyConfig.password,
        mobile: config.pmfbyConfig.mobile,
      };

      const response = await axios.post(
        config.pmfbyConfig.login_api_url,
        payload,
        {
          httpsAgent: new https.Agent({ rejectUnauthorized: false }),
        }
      );

      const token = response?.data?.data?.token;
      console.log(token);

      if (!token) {
        throw new Error("Token not received from login API");
      }

      return token;
    } catch (error) {
      console.error("Error in getToken():", error);
      throw error;
    }
  }

  async getToken() {
    try {
      const payload = {
        deviceType: "android",
        otp: 123456,
        password: "af0ea0b9a3da1d35cae98df93385c49c0dc5185573b005041e973ee3683d20d91ec9a089f4647ab47287753278580adf3501828c5ef9047c1f168a707fa40f8c",
        mobile: "9899499022"
      };




      const response = await axios.post(
        // "https://pmfbydemo.amnex.co.in/api/v2/external/service/login",
        "https://pmfby.gov.in/krishirakshak/v1/user/user/login",
        payload,
        {
          httpsAgent: new https.Agent({ rejectUnauthorized: false }),
        }
      );

      const token = response?.data?.data?.token;
      console.log(token);

      if (!token) {
        throw new Error("Token not received from login API");
      }

      return token;
    } catch (error) {
      console.error("Error in getToken():", error);
      throw error;
    }
  }





  /* async getRolesForGovt(payload: any) {
    try {
      let token: string;
      try {
        token = await this.getToken();
      } catch (tokenErr: any) {
        return {
          data: null,
          message: {
            msg: "Failed to generate token",
            code: 0,
            errorType: "TOKEN_ERROR",
            detail: tokenErr?.message
          },
        };
      }
  
      const EnumRole: Record<string, number> = {
        STATE_GOVT_ADMIN: 1,
        STATE_GOVT_USER: 2,
        DEPUTY_DIRECTOR: 3
      };
  
      const roleMap: Record<number, string> = {
        1: "STATE_GOVT_ADMIN",
        2: "STATE_GOVT_USER",
        3: "DEPUTY_DIRECTOR"
      };
  
      const axiosPayload = {
        roleName: roleMap[payload?.roleName],
        stateID: payload?.stateID,
      };
  
      if (!axiosPayload.roleName) {
        return {
          data: null,
          message: {
            msg: "Invalid roleName provided in payload",
            code: 0,
            errorType: "INPUT_VALIDATION_ERROR",
            detail: { received: payload?.roleName }
          },
        };
      }
  
      console.log("Calling External API With Payload:", axiosPayload);
  
      let apiResponse: any;
      try {
        apiResponse = await axios.get(this.RoleURL, {
          params: axiosPayload,
          timeout: 10000,
          headers: { token },
        });
      } catch (axiosErr: any) {
        const status = axiosErr?.response?.status;
        const apiMsg = axiosErr?.response?.data?.message || axiosErr?.message;
  
        return {
          data: null,
          message: {
            msg: "External API call failed",
            code: 0,
            errorType: "EXTERNAL_API_ERROR",
            httpStatus: status,
            detail: apiMsg,
            url: this.RoleURL,
          },
        };
      }
  
      const data = apiResponse?.data;
  
      if (!data?.data) {
        return {
          data: null,
          message: {
            msg: "No data received from external API",
            code: 0,
            errorType: "API_NO_DATA",
          },
        };
      }
  
      let updatedData: any[] = [];
      try {
        updatedData = data.data
          .map((item: any) => ({
            ...item,
            roleName: EnumRole[item.roleName] || item.roleName,
          }))
          .filter(
            (item: any, index: number, self: any[]) =>
              index === self.findIndex((t) => t.userID === item.userID)
          );
      } catch (processingErr: any) {
        return {
          data: null,
          message: {
            msg: "Error occurred while processing API data",
            code: 0,
            errorType: "DATA_PROCESSING_ERROR",
            detail: processingErr.message,
          },
        };
      }
  
      return {
        data: updatedData,
        message: { msg: "Success", code: 1 },
      };
    } catch (unexpectedErr: any) {
      return {
        data: null,
        message: {
          msg: "Unexpected internal server error",
          code: 0,
          errorType: "UNHANDLED_ERROR",
          detail: unexpectedErr?.message,
        },
      };
    }
  } */



  async getRolesForGovt(payload: any) {
    try {
      let token: string;
      try {
        token = await this.getToken();
      } catch (tokenErr: any) {
        return {
          data: null,
          message: {
            msg: "Failed to generate token",
            code: 0,
            errorType: "TOKEN_ERROR",
            detail: tokenErr?.message
          },
        };
      }

      const EnumRole: Record<string, number> = {
        STATE_GOVT_ADMIN: 1,
        STATE_GOVT_USER: 2,
        DEPUTY_DIRECTOR: 3
      };

      const roleMap: Record<number, string> = {
        1: "STATE_GOVT_ADMIN",
        2: "STATE_GOVT_USER",
        3: "DEPUTY_DIRECTOR"
      };

      const axiosPayload = {
        roleName: roleMap[payload?.roleName],
        stateID: payload?.stateID,
      };

      if (!axiosPayload.roleName) {
        return {
          data: null,
          message: {
            msg: "Invalid roleName provided in payload",
            code: 0,
            errorType: "INPUT_VALIDATION_ERROR",
            detail: { received: payload?.roleName }
          },
        };
      }

      let apiResponse: any;
      try {
        apiResponse = await axios.get(this.RoleURL, {
          params: axiosPayload,
          timeout: 10000,
          headers: { token },
        });
      } catch (axiosErr: any) {
        console.log(axiosErr)
        return {
          data: null,
          message: {
            msg: "External API call failed",
            code: 0,
            errorType: "EXTERNAL_API_ERROR",
            detail: axiosErr?.message,
          },
        };
      }

      const data = apiResponse?.data;

      if (!data?.data) {
        return {
          data: null,
          message: {
            msg: "No data received from external API",
            code: 0,
            errorType: "API_NO_DATA",
          },
        };
      }

      let updatedData: any[] = [];
      try {
        // 🔹 Existing logic (UNCHANGED)
        updatedData = data.data
          .map((item: any) => ({
            ...item,
            roleName: EnumRole[item.roleName] || item.roleName,
          }))
          .filter(
            (item: any, index: number, self: any[]) =>
              index === self.findIndex((t) => t.userID === item.userID)
          );

        // 🔹 NEW ADDITION (StateWiseDistrict)
        if (payload?.viewMode === "StateWiseDistrict") {
          const districtMap = new Map<string, any>();

          updatedData.forEach((item: any) => {
            if (!districtMap.has(item.districtID)) {
              districtMap.set(item.districtID, {
                districtID: item.districtID,
                districtName: item.districtName,
                stateID: item.stateID,
                stateName: item.stateName,
              });
            }
          });

          return {
            data: Array.from(districtMap.values()),
            message: { msg: "Success", code: 1 },
          };
        }

        // 🔹 NEW: DistrictWiseUser (filter users by district)
        if (payload?.viewMode === "DistrictWiseUser") {
          if (!payload?.districtID) {
            return {
              data: null,
              message: {
                msg: "districtID is required for DistrictWiseUser view",
                code: 0,
                errorType: "INPUT_VALIDATION_ERROR",
              },
            };
          }

          const districtUsers = updatedData.filter(
            (item: any) => item.districtID === payload.districtID
          );

          return {
            data: districtUsers,
            message: { msg: "Success", code: 1 },
          };
        }


      } catch (processingErr: any) {
        console.log(processingErr)
        return {
          data: null,
          message: {
            msg: "Error occurred while processing API data",
            code: 0,
            errorType: "DATA_PROCESSING_ERROR",
            detail: processingErr.message,
          },
        };
      }

      // 🔹 Default response (UNCHANGED)
      return {
        data: updatedData,
        message: { msg: "Success", code: 1 },
      };

    } catch (unexpectedErr: any) {
      return {
        data: null,
        message: {
          msg: "Unexpected internal server error",
          code: 0,
          errorType: "UNHANDLED_ERROR",
          detail: unexpectedErr?.message,
        },
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
                  StateMasterName: 1,
                  TicketDescription: 1,
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

      console.log(item, "item")

      if (!item) return { data: {}, message: { msg: "Not Found", code: "0" } };

      const userDetail = {
        InsuranceCompanyID: item.InsuranceCompanyID ? await new UtilService().convertStringToArray(item.InsuranceCompanyID) : [],
        StateMasterID: item.StateMasterID ? await new UtilService().convertStringToArray(item.StateMasterID) : [],
        BRHeadTypeID: item.BRHeadTypeID,
        LocationTypeID: item.LocationTypeID,
        FromDay: item?.FromDay,
        EscalationFlag: item?.EscalationFlag,
        AppAccessID: item?.AppAccessID
      };

      console.log(userDetail)
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
        // if (validInsuranceIDs.length === 0) {
        //   // return { rcode: 0, rmessage: "Unauthorized InsuranceCompanyID(s)." };
        //   return { data:[] ,message:{msg:"Unauthorized InsuranceCompanyID(s).", code:"0"}};

        // }
        match.InsuranceCompanyID = { $in: validInsuranceIDs };
      } else if (InsuranceCompanyID?.length) {
        match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
      }

      if (stateID && Number(stateID) !== 0 && LocationTypeID !== 2) {
        console.log(stateID)
        const requestedStateIDs = String(stateID).split(",").map(id => Number(id.trim()));
        const validStateIDs = requestedStateIDs.filter(id => StateMasterID.map(Number).includes(id));
        console.log(validStateIDs)

        // if (validStateIDs.length === 0) {
        //   return { data:[] ,message:{msg:"Un-Authorised State", code:"0"}};
        // }
        match.StateMasterID = { $in: validStateIDs };
      } else if (StateMasterID.length && LocationTypeID !== 2) {
        match.FilterStateID = { $in: StateMasterID.map(Number) };
      }


      console.log(JSON.stringify(match))


      if (fromdate && toDate) {
        match.Created = {
          $gte: new Date(`${fromdate}T00:00:00.000Z`),
          $lte: new Date(`${toDate}T23:59:59.999Z`)
        };
      }

      if (ticketCategoryID && ticketCategoryID !== 0) match.TicketCategoryID = ticketCategoryID;
      if (supportTicketTypeID && supportTicketTypeID !== 0) match.SupportTicketTypeID = supportTicketTypeID;
      if (supportTicketNo) match.SupportTicketNo = supportTicketNo;

      const pipelineInfo: any[] = [
        { $match: { ...match, TicketStatusID: { $eq: 109302 } } },
        { $limit: pageSize * 2 },
        { $sort: { InsertDateTime: -1 } },
        {
          $lookup: {
            from: "Ticket_Assignment_History",
            localField: "SupportTicketID",
            foreignField: "SupportTicketID",
            as: "assignmentHistory"
          }
        },
        { $match: { assignmentHistory: { $size: 0 } } },
        {
          $facet: {
            data: [
              { $skip: (pageIndex - 1) * pageSize },
              { $limit: pageSize },
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
                  StateMasterName: 1,
                  TicketDescription: 1,
                  TicketStatusID: 1,
                  TicketStatus: 1,
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

      console.log(JSON.stringify(pipelineInfo))
      const aggResult = await db.collection("SLA_Ticket_listing").aggregate(pipelineInfo, { allowDiskUse: true }).toArray();
      const result = aggResult[0] || { data: [], totalCount: [], ticketStatusSummary: [] };

      if (result.data.length === 0) {
        return {
          obj: [],
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
      return { data: [], message: { msg: "Error", code: "0" } };
    }
  }



  /*   async AssignTicketService(payload: any) {
      const {
        ticketIds,
        assignedBy,
        assignedByName,
        assignedTo,
        assignToName,
        roleName,
        stateID,
        mobileNo,
        districtID,
        ticketDescription
      } = payload || {};
  
      if (!ticketIds) {
        return { data: {}, message: { msg: "ticketIds is required.", code: "0" } };
      }
  
      const roleId = roleName;
  
      const ticketIdArray = ticketIds
        .split(",")
        .map(id => id.trim())
        .filter(Boolean);
  
      if (!ticketIdArray.length) {
        return { data: {}, message: { msg: "No valid ticket IDs provided.", code: "0" } };
      }
  
      const ticketCollection = this.db.collection("SLA_Ticket_listing");
      const assignHistoryCollection = this.db.collection("Ticket_Assignment_History");
      const currentAssignCollection = this.db.collection("Ticket_Assignment");
  
      const now = new Date();
      const results: any[] = [];
  
      let assignedRoleName = "";
      if (roleId == 1) assignedRoleName = "STATE_GOVT_ADMIN";
      if (roleId == 2) assignedRoleName = "STATE_GOVT_USER";
      if (roleId == 3) assignedRoleName = "DEPUTY_DIRECTOR";
  
      for (const ticketIdStr of ticketIdArray) {
        const ticketId = Number(ticketIdStr);
  
        if (isNaN(ticketId)) {
          results.push({ ticketId: ticketIdStr, status: "Failed", reason: "Invalid ticket ID" });
          continue;
        }
  
        try {
          const ticket = await ticketCollection.findOne({ SupportTicketID: ticketId });
          if (!ticket) {
            results.push({ ticketId, status: "Failed", reason: "Ticket not found" });
            continue;
          }
  
          const currentAssignment = await currentAssignCollection.findOne({
            SupportTicketID: ticketId
          });
  
          if (currentAssignment && currentAssignment.assignedTo === assignedTo) {
            results.push({
              ticketId,
              ticketNo: ticket.SupportTicketNo,
              status: "Failed",
              reason: "Ticket already assigned to this user"
            });
            continue;
          }
  
          const assignmentData = {
            SupportTicketID: ticketId,
            SupportTicketNo: ticket.SupportTicketNo,
            TicketStatusID: ticket.TicketStatusID || null,
            TicketStatus: ticket.TicketStatus || null,
            assignedBy,
            assignedByName,
            assignedTo,
            assignToName,
            AssignedDate: now,
            AssigneeStateID: stateID,
            AssignedDistrictID: districtID,
            AssigneeMobileNo: mobileNo,
            AssigneRoleName: assignedRoleName,
            AssigneeRoleID: roleId,
            InsuranceCompanyId: ticket?.InsuranceCompanyID,
            InsuranceCompanyName: ticket?.InsuranceCompany,
            TicketComment:ticketDescription
  
          };
  
         let insertedRecords =  await assignHistoryCollection.insertOne({
            ...assignmentData,
            CreatedDate: now
          });
        
  
          await currentAssignCollection.updateOne(
            { SupportTicketID: ticketId },
            {
              $set: {
                ...assignmentData,
                UpdatedDate: now
              }
            },
            { upsert: true }
          );
  
          results.push({
            ticketId,
            ticketNo: ticket.SupportTicketNo,
            status: "Success",
            reason: `Ticket ${ticket.SupportTicketNo} assigned successfully`
          });
  
        } catch (err: any) {
          results.push({
            ticketId,
            status: "Error",
            reason: err.message || "Unexpected error"
          });
        }
      }
  
      const successCount = results.filter(r => r.status === "Success").length;
      const failedCount = results.length - successCount;
  
      for (const item of results) {
        if (item.status === "Success") {
          await this.sendSMSToUser({
            ticket: item.ticketNo,
            mobileNO: "916386236314",
            Name: assignToName
          });
        }
      }
  
      const summary = {
        totalTickets: results.length,
        successCount,
        failedCount,
        message:
          successCount === results.length
            ? "All tickets assigned successfully."
            : successCount === 0
              ? "All tickets failed."
              : `${successCount} assigned, ${failedCount} failed.`
      };
  
      return successCount === 0
        ? { data: summary, message: { msg: "All Failed", code: "0" } }
        : { data: summary, message: { msg: "Success", code: "1" } };
    } */


  async AssignTicketService(payload: any) {
    try {
      const {
        ticketIds,
        assignedBy,
        assignedByName,
        assignedTo,
        assignToName,
        roleName,
        previousRoleName,
        stateID,
        mobileNo,
        districtID,
        ticketDescription
      } = payload || {};

      if (!ticketIds) {
        return { data: {}, message: { msg: "ticketIds is required.", code: "0" } };
      }

      const ticketIdArray = ticketIds
        .split(",")
        .map(id => Number(id.trim()))
        .filter(id => !isNaN(id));

      if (!ticketIdArray.length) {
        return { data: {}, message: { msg: "No valid ticket IDs provided.", code: "0" } };
      }

      const ticketCollection = this.db.collection("SLA_Ticket_listing");
      const historyCol = this.db.collection("Ticket_Assignment_History");
      const currentCol = this.db.collection("Ticket_Assignment");

      const now = new Date();
      const results: any[] = [];

      const roleMap: any = {
        0: "INSURANCE_COMPANY",
        1: "STATE_GOVT_ADMIN",
        2: "STATE_GOVT_USER",
        3: "DEPUTY_DIRECTOR",
      };

      for (const ticketId of ticketIdArray) {
        try {
          const ticket = await ticketCollection.findOne({ SupportTicketID: ticketId });
          if (!ticket) {
            results.push({ ticketId, status: "Failed", reason: "Ticket not found" });
            continue;
          }

          const currentAssignment = await currentCol.findOne({ SupportTicketID: ticketId });

          if (currentAssignment?.assignedTo === assignedTo) {
            results.push({
              ticketId,
              ticketNo: ticket.SupportTicketNo,
              status: "Failed",
              reason: "Ticket already assigned to this user"
            });
            continue;
          }

          const assignmentData = {
            SupportTicketID: ticketId,
            SupportTicketNo: ticket.SupportTicketNo,
            TicketStatusID: ticket.TicketStatusID || null,
            TicketStatus: ticket.TicketStatus || null,
            assignedBy,
            assignedByName,
            assignedTo,
            assignToName,
            AssignedDate: now,
            AssigneeStateID: stateID || "",
            AssignedDistrictID: districtID || "",
            AssigneeMobileNo: mobileNo || "",
            AssigneRoleName: roleMap[roleName] || "",
            AssigneeRoleID: roleName,
            InsuranceCompanyId: ticket?.InsuranceCompanyID,
            InsuranceCompanyName: ticket?.InsuranceCompany,
            TicketComment: ticketDescription || "",
            PreviousRoleId: previousRoleName,
            PreviousRoleName: roleMap[previousRoleName]
          };

          await historyCol.insertOne({
            ...assignmentData,
            CreatedDate: now
          });

          let SqlAssignpayload = {
            SupportTicketID: ticketId,
            SupportTicketNo: ticket.SupportTicketNo,
            TicketStatusID: ticket.TicketStatusID || null,
            TicketStatus: ticket.TicketStatus || null,
            assignedBy,
            assignedByName,
            assignedTo,
            assignToName,
            AssignedDate: now,
            UpdatedDate: now,
            AssigneeStateID: stateID || "",
            AssignedDistrictID: districtID || "",
            AssigneeMobileNo: mobileNo || "",
            AssigneRoleName: roleMap[roleName] || "",
            AssigneeRoleID: roleName,
            InsuranceCompanyId: ticket?.InsuranceCompanyID,
            InsuranceCompanyName: ticket?.InsuranceCompany,
            TicketComment: ticketDescription || "",
            PreviousRoleId: previousRoleName,
            PreviousRoleName: roleMap[previousRoleName]
          }





          await currentCol.updateOne(
            { SupportTicketID: ticketId },
            {
              $set: {
                ...assignmentData,
                UpdatedDate: now
              }
            },
            { upsert: true }
          );


          const transaction = await this.sequelize.transaction();

          try {
            await this.InsertionIntoSql(SqlAssignpayload, transaction);
            await this.InsertionIntoSqlHistory(SqlAssignpayload, transaction);

            await transaction.commit();
          } catch (err) {
            await transaction.rollback();
            throw err;
          }



          results.push({
            ticketId,
            ticketNo: ticket.SupportTicketNo,
            status: "Success",
            reason: "Assigned successfully"
          });

        } catch (err: any) {
          results.push({
            ticketId,
            status: "Error",
            reason: err.message || "Unexpected error"
          });
        }
      }

      const successCount = results.filter(r => r.status === "Success").length;

      for (const item of results) {
        if (item.status === "Success") {
          await this.sendSMSToUser({
            ticket: item.ticketNo,
            mobileNO: item.AssigneeMobileNo,
            Name: assignToName
          });
        }
      }

      return {
        data: {
          totalTickets: results.length,
          successCount,
          failedCount: results.length - successCount
        },
        message: {
          msg: successCount ? "Success" : "All Failed",
          code: successCount ? "1" : "0"
        }
      };
    } catch (err) {
      console.log(err)
    }
  }



  async InsertionIntoSql(payload: any, transaction?: any) {
    console.log(payload, "ss");

    try {
      const formattedPayload = {
        SupportTicketID: payload.SupportTicketID,
        SupportTicketNo: payload.SupportTicketNo,
        TicketStatusID: payload.TicketStatusID,
        TicketStatus: payload.TicketStatus,

        AssignedBy: payload.assignedBy,
        AssignedByName: payload.assignedByName,
        AssignedTo: payload.assignedTo,
        AssignToName: payload.assignToName,

        AssignedDate: payload.AssignedDate,
        UpdatedDate: payload.UpdatedDate,

        AssigneeStateID: payload.AssigneeStateID,
        AssignedDistrictID: payload.AssignedDistrictID || null,
        AssigneeMobileNo: payload.AssigneeMobileNo,
        AssigneRoleName: payload.AssigneRoleName,
        AssigneeRoleID: payload.AssigneeRoleID,

        InsuranceCompanyId: payload.InsuranceCompanyId,
        InsuranceCompanyName: payload.InsuranceCompanyName,

        TicketComment: payload.TicketComment,
        PreviousRoleId: payload.PreviousRoleId,
        PreviousRoleName: payload.PreviousRoleName
      };

      const query = `
      INSERT INTO krishi_rakshak_pro.krph_ticket_assignment (
        SupportTicketID,
        AssigneRoleName,
        AssignedDate,
        AssignedDistrictID,
        AssigneeMobileNo,
        AssigneeRoleID,
        AssigneeStateID,
        InsuranceCompanyId,
        InsuranceCompanyName,
        PreviousRoleId,
        PreviousRoleName,
        SupportTicketNo,
        TicketComment,
        TicketStatus,
        TicketStatusID,
        UpdatedDate,
        AssignToName,
        AssignedBy,
        AssignedByName,
        AssignedTo
      ) VALUES (
        :SupportTicketID,
        :AssigneRoleName,
        :AssignedDate,
        :AssignedDistrictID,
        :AssigneeMobileNo,
        :AssigneeRoleID,
        :AssigneeStateID,
        :InsuranceCompanyId,
        :InsuranceCompanyName,
        :PreviousRoleId,
        :PreviousRoleName,
        :SupportTicketNo,
        :TicketComment,
        :TicketStatus,
        :TicketStatusID,
        :UpdatedDate,
        :AssignToName,
        :AssignedBy,
        :AssignedByName,
        :AssignedTo
      );
    `;

      await this.sequelize.query(query, {
        replacements: formattedPayload,
        type: QueryTypes.INSERT,
        transaction
      });

    } catch (error) {
      console.error('InsertionIntoSql Error:', error);
      throw error;
    }
  }



  async InsertionIntoSqlHistory(payload: any, transaction?: any) {
    try {
      const formattedPayload = {
        SupportTicketID: payload.SupportTicketID,
        AssigneRoleName: payload.AssigneRoleName,
        AssignedDate: payload.AssignedDate,
        AssignedDistrictID: payload.AssignedDistrictID || null,
        AssigneeMobileNo: payload.AssigneeMobileNo,
        AssigneeRoleID: payload.AssigneeRoleID,
        AssigneeStateID: payload.AssigneeStateID,
        InsuranceCompanyId: payload.InsuranceCompanyId,
        InsuranceCompanyName: payload.InsuranceCompanyName,
        PreviousRoleId: payload.PreviousRoleId,
        PreviousRoleName: payload.PreviousRoleName,
        SupportTicketNo: payload.SupportTicketNo,
        TicketComment: payload.TicketComment,
        TicketStatus: payload.TicketStatus,
        TicketStatusID: payload.TicketStatusID,
        UpdatedDate: payload.UpdatedDate,
        AssignToName: payload.assignToName,
        AssignedBy: payload.assignedBy,
        AssignedByName: payload.assignedByName,
        AssignedTo: payload.assignedTo
      };

      const query = `
      INSERT INTO krishi_rakshak_pro.krph_ticket_assignment_history (
        SupportTicketID,
        AssigneRoleName,
        AssignedDate,
        AssignedDistrictID,
        AssigneeMobileNo,
        AssigneeRoleID,
        AssigneeStateID,
        InsuranceCompanyId,
        InsuranceCompanyName,
        PreviousRoleId,
        PreviousRoleName,
        SupportTicketNo,
        TicketComment,
        TicketStatus,
        TicketStatusID,
        UpdatedDate,
        AssignToName,
        AssignedBy,
        AssignedByName,
        AssignedTo
      ) VALUES (
        :SupportTicketID,
        :AssigneRoleName,
        :AssignedDate,
        :AssignedDistrictID,
        :AssigneeMobileNo,
        :AssigneeRoleID,
        :AssigneeStateID,
        :InsuranceCompanyId,
        :InsuranceCompanyName,
        :PreviousRoleId,
        :PreviousRoleName,
        :SupportTicketNo,
        :TicketComment,
        :TicketStatus,
        :TicketStatusID,
        :UpdatedDate,
        :AssignToName,
        :AssignedBy,
        :AssignedByName,
        :AssignedTo
      );
    `;

      await this.sequelize.query(query, {
        replacements: formattedPayload,
        type: QueryTypes.INSERT,
        transaction
      });

    } catch (error) {
      console.error('InsertionIntoSqlHistory Error:', error);
      throw error;
    }
  }







  async sendSMSToUser(payload) {
    try {

      let templateID = "1707176646596240405";

      let customTemplate = `Dear ${payload?.Name}, Grievance Ticket Number ${payload?.ticket} has been assigned to you for review and action. Please log in to the system and proceed as per the prescribed timelines. Portal: https://pmfby.gov.in/krph Regards CSC SPV/Ministry of Agriculture & Farmers Welfare Government of India`;
      let definedTemplate = await this.GetSingleUnicodeHex(customTemplate)
      let mobileNumber = this.normalizeMobileNumber(payload.mobileNO)
      
      const response = await axios.post(`https://bulksmsapi.vispl.in/?username=cscetrnapi3&password=csce_123&messageType=unicode&mobile=${mobileNumber}&senderId=CSCSPV&ContentID=${templateID}&EntityID=1301157363501533886&message=${definedTemplate}`);

      if (response.status === 200) {
        const val = response.data.split('#');
        if (val.length === 0) {
          throw new Error('Could not send Message');
        }
        console.log("SMS Sent")

        let collection = "SMS_Send_History_Records";
        let payloadForSms = {
          SupportTicketNo: payload?.ticket,
          SMSReferenceNo: val[2] || '',
          WhatsAppReferenceNo: '',
          TemplateID: templateID,
          MobileNo: mobileNumber,
          createdAt: new Date()   
        };
        await this.db.collection(collection).insertOne(payloadForSms)

      }


    } catch (err) {
      console.log(err);
    }
  }


  async normalizeMobileNumber(mobile) {
  if (!mobile) return '';

  let mobileNo = mobile.toString().replace(/\D/g, '');

  if (mobileNo.length === 10) {
    mobileNo = `91${mobileNo}`;
  }

  return mobileNo;
}

  async GetSingleUnicodeHex(x) {
    let result = "", notation = "";
    for (let i = 0; i < x.length; i++)
      result += notation + ("000" + x[i].charCodeAt(0).toString(16)).substr(-4);
    return result;
  }


  async UserWiseState(payload: any) {
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
            StateCodeAlpha: { $first: "$StateCodeAlpha" }
          }
        },
        { $addFields: { order: { $indexOfArray: [StateMasterID, "$_id"] } } },
        { $sort: { order: 1 } },
        {
          $project: {
            _id: 0,
            StateMasterID: "$_id",
            StateName: "$StateMasterName",
            StateCodeAlpha: 1
          }
        }
      ];

      let extractedData;
      try {
        extractedData = await db.collection("STATEMASTERSQL").aggregate(pipeline).toArray();
      } catch {
        return { data: [], msg: "Failed to fetch state data", code: "0" };
      }

      return { data: extractedData, msg: "Fetched successfully", code: "1" };
    } catch {
      return { data: [], msg: "Unexpected error", code: "0" };
    }
  }




  /*   async RoleWiseAssignedTickets(payload: any) {
      try {
        if (!payload || !payload.loggedInUserId || payload?.loggedInUserId == "") {
          return { data: [], message: { msg: "loggedInUserId is required", code: "0" } };
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
          
          { $match: { assignedTo: loggedInUserId} },
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
              _id: "$assignedTo",
              TicketRecords: { $push: "$TicketRecords" },
            },
          },
  
          {
            $project: {
              _id: 0,
              userID: "$_id",
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
  console.log(JSON.stringify(pipeline))
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
    } */

  async RoleWiseAssignedTickets(payload: any) {
    try {
      if (!payload || !payload.loggedInUserId || payload?.loggedInUserId == "") {
        return { data: [], message: { msg: "loggedInUserId is required", code: "0" } };
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

        // 🔑 1. Always resolve LATEST assignment first
        {
          $sort: { AssignedDate: -1, _id: -1 }
        },
        {
          $group: {
            _id: "$SupportTicketID",
            latestAssignment: { $first: "$$ROOT" }
          }
        },
        {
          $replaceRoot: { newRoot: "$latestAssignment" }
        },

        // 🔑 2. Now filter by logged-in user
        {
          $match: { assignedTo: loggedInUserId }
        },

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
            _id: "$assignedTo",
            TicketRecords: { $push: "$TicketRecords" },
          },
        },

        {
          $project: {
            _id: 0,
            userID: "$_id",
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
    } catch (error: any) {
      return { data: null, message: { msg: error?.message || "Failed", code: "0" } };
    }
  }


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
        RequestorMobileNo: RequestorMobileNo || null,
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




  async syncAudioFiles(payload: any) {
    try {
      console.log("🔍 Starting Audio Sync Process...");

      const db = this.db;
      const sourceCollection = db.collection("KRPH_Calling_CDR_files_paths");
      const targetCollection = db.collection("SLA_Ticket_listing");
      const logCollection = db.collection("KRPH_Sync_Log");

      let syncedCount = 0;
      let failedCount = 0;

      console.log("📡 Fetching records to sync...");

      const sourceData = await sourceCollection.find({
        $or: [{ isSynced: { $exists: false } }, { isSynced: false }]
      }).project({ uniqueId: 1, path: 1 }).toArray();

      console.log(`📁 Total records found: ${sourceData.length}`);

      let processed = 0;

      for (let item of sourceData) {
        processed++;

        console.log(`🔧 Processing ${processed}/${sourceData.length} | UniqueID: ${item.uniqueId}`);

        const result = await targetCollection.findOneAndUpdate(
          { CallingUniqueID: item.uniqueId },
          { $set: { CallingAudioFile: item.path } },
          { returnDocument: "after" }
        );

        if (result?.value) {
          syncedCount++;
          console.log(`   ✅ Synced: ${item.uniqueId}`);

          await sourceCollection.updateOne(
            { _id: item._id },
            {
              $set: {
                isSynced: true,
                syncedAt: new Date(),
                syncStatus: "success"
              }
            }
          );
        } else {
          failedCount++;
          console.log(`   ❌ Failed: ${item.uniqueId} (No match found)`);

          await sourceCollection.updateOne(
            { _id: item._id },
            {
              $set: {
                isSynced: false,
                syncedAt: new Date(),
                syncStatus: "failed",
                error: "Matching entry not found in SLA_Ticket_listing"
              }
            }
          );
        }
      }

      console.log("📦 Saving Sync Summary Log...");

      await logCollection.insertOne({
        executedAt: new Date(),
        totalProcessed: sourceData.length,
        synced: syncedCount,
        failed: failedCount,
        status: syncedCount > 0 && failedCount > 0 ? "PARTIAL" :
          failedCount === 0 ? "SUCCESS" : "FAILED"
      });

      console.log(`
================ SUMMARY ================
Total Records    : ${sourceData.length}
Synced Successfully : ${syncedCount}
Failed to Sync      : ${failedCount}
Status              : ${syncedCount > 0 && failedCount > 0 ? "PARTIAL" : failedCount === 0 ? "SUCCESS" : "FAILED"}
=========================================
    `);

      return {
        data: { total: sourceData.length, synced: syncedCount, failed: failedCount },
        message: { msg: "Mongo Sync Completed & Logged", code: 1 }
      };

    } catch (err) {
      console.error("❌ Error Occurred During Sync:", err);

      return {
        data: {},
        message: { msg: "Sync failed", code: 0 }
      };
    }
  }






  async getPhotoServie(payload?: any) {
    try {
      const sourceFolder = "/home/pradeep/Desktop/DCIM/100D5600";
      const destinationFolder = "/home/pradeep/Desktop/filteredPhoto";

      const validNumbers = [
        "0007", "0011", "0013", "0018", "0023", "0041", "0053", "0073", "0078", "0105", "0118", "0129",
        "0139", "0148", "0151", "0205", "0229", "0236", "0244", "0260", "0264", "0269", "0273", "0276",
        "0292", "0302", "0306", "0313", "0322", "0323", "0326", "0332", "0345", "0354", "0362", "0365",
        "0367", "0380", "0403", "0407", "0411", "0416", "0427", "0443", "0449", "0463", "0465", "0469",
        "0475", "0478", "0479", "0482", "0493", "0496", "0502", "0506", "0510", "0513", "0528", "0537",
        "0540", "0542", "0550", "0552", "0554", "0560", "0567", "0570", "0572", "0575", "0579", "0583",
        "0592", "0597", "0598", "0601", "0602", "0608", "0613", "0615", "0617", "0618", "0625", "0627",
        "0631", "0637", "0639", "0642", "0643", "0650", "0657", "0665", "0667", "0680"
      ];

      if (!fs.existsSync(destinationFolder)) {
        fs.mkdirSync(destinationFolder, { recursive: true });
      }

      const files = fs.readdirSync(sourceFolder);

      let copiedFiles: string[] = [];
      let failedFiles: Array<{ file: string; error: string }> = [];
      let foundNumbers = new Set<string>();

      for (const file of files) {
        const match = file.match(/(\d{4})/);
        if (!match) continue;

        const fileNumber = match[1];

        if (validNumbers.includes(fileNumber)) {
          foundNumbers.add(fileNumber);

          const src = path.join(sourceFolder, file);
          const dest = path.join(destinationFolder, file);

          try {
            if (!fs.existsSync(dest)) {
              fs.copyFileSync(src, dest);
            }

            copiedFiles.push(file);

          } catch (error: any) {
            failedFiles.push({ file, error: error.message });
          }
        }
      }

      // Numbers not found in ANY file
      const missingNumbers = validNumbers.filter(num => !foundNumbers.has(num));

      return {
        data: {
          totalCopied: copiedFiles.length,
          copiedFiles,
          missingNumbersCount: missingNumbers.length,
          missingNumbers,
          failedFilesCount: failedFiles.length,
          failedFiles,
        },
        message: { msg: "Photo Sync Completed Successfully", code: 1 }
      };

    } catch (err) {
      console.log(err);

      return {
        data: null,
        message: { msg: "Error while syncing photos", code: 0 }
      };
    }
  }



  async insuranceWiseTicketList(payload: any) {
    let { month, year } = payload;
    let message = { msg: "", code: "" };

    if (!month) {
      message.msg = "Month is required";
      message.code = "1";
      return { data: {}, message };
    }

    if (!year) {
      message.msg = "Year is required";
      message.code = "1";
      return { data: {}, message };
    }

    const startDate = new Date(year, month - 1, 1, 0, 0, 0, 0);
    const endDate = new Date(year, month, 1, 0, 0, 0, 0);

    const pipeline = [
      {
        $match: {
          AssignedDate: {
            $gte: startDate,
            $lt: endDate
          }
        }
      },

      {
        $group: {
          _id: "$InsuranceCompanyId",
          InsuranceCompany: { $first: "$InsuranceCompanyName" },
          TicketCount: { $sum: 1 }
        }
      },
      {
        $project: {
          _id: 0,
          InsuranceCompanyId: "$_id",
          InsuranceCompany: 1,
          TicketCount: 1
        }
      },
      {
        $sort: { TicketCount: -1 }
      }
    ];

    console.log(JSON.stringify(pipeline))
    const fetchedData = await this.db
      .collection("Ticket_Assignment")
      .aggregate(pipeline)
      .toArray();

    const getInsuranceCompany = await this.db
      .collection("KRPH_InsuranceMaster")
      .find({})
      .toArray();

    const normalize = this.normalize.bind(this);

    const insuranceCountMap = fetchedData.reduce((acc, fd) => {
      acc[normalize(fd.InsuranceCompany)] = fd.TicketCount;
      return acc;
    }, {});

    const conbinedData = getInsuranceCompany.map(item => {
      const key = this.normalize(item.InsuranceMasterName);
      return {
        InsuranceCompanyId: item?.InsuranceMasterID,
        InsuranceCompany: item.InsuranceMasterName,
        AssignedTicketCount: insuranceCountMap[key] || 0
      };
    });

    message.msg = "Success";
    message.code = "1";
    let obj = {
      data: conbinedData
    }

    return {
      data: obj,
      message
    };
  }

  normalize(str = "") {
    return str
      .toUpperCase()
      .replace(/,/g, "")     // remove commas
      .replace(/\s+/g, " ")  // normalize spaces
      .trim();
  }


  async getBucketTicketCount(payload: any) {
    try {
      const { loginId } = payload;
      let message = {
        msg: "",
        code: "",
      };
      let count;

      const ticketCount = await this.db.collection('Ticket_Assignment_History')
        .find({ assignedBy: loginId.toString() })
        .count();

      if (ticketCount === 0) {
        message["msg"] = "Success";
        message["code"] = "0";
        count = ticketCount
      } else {
        message["msg"] = "Success";
        message["code"] = "1";
        count = ticketCount
      }


      return { data: count, message: message };

    } catch (err) {
      console.log("Error while fetching bucket ticket count:", err);
      return { data: "", message: { msg: "Error", code: "0" } };
    }
  }


  async getBucketTicket(payload: any) {
    try {

      let { loginId } = payload;
      let ticketData;
      let message = {
        msg: "",
        code: ""
      }
      let pipeline = [
        {
          $match: {
            assignedBy: loginId.toString()
          }
        },
        {
          $project: {
            _id: 0,
            SupportTicketID: 1,
            SupportTicketNo: 1,
            assignedBy: 1,
            assignedTo: 1,
            AssignedDate: 1,
            AssigneeStateID: 1,
            AssigneeMobileNo: 1,
            AssigneeRoleName: 1,
            AssigneeRoleID: 1,
            AssigneRoleName: 1,
            assignToName: 1,
            assignedByName: 1

          }
        },
        {
          $lookup: {
            from: "SLA_Ticket_listing",
            localField: "SupportTicketID",
            foreignField: "SupportTicketID",
            as: "Ticket"
          }
        },
        {
          $unwind: {
            path: "$Ticket",
            preserveNullAndEmptyArrays: true
          }
        },
        {
          $lookup: {
            from: "STATEMASTERDATA",
            localField: "AssigneeStateID",
            foreignField: "StateCodeAlpha",
            as: "State"
          }
        },
        {
          $unwind: {
            path: "$State",
            preserveNullAndEmptyArrays: true
          }
        },


        {
          $project: {
            _id: 0,
            SupportTicketID: 1,
            SupportTicketNo: 1,
            TicketStatusID: "$Ticket.TicketStatusID",
            TicketStatus: "$Ticket.TicketStatus",
            AssignedFrom: "$assignedBy",
            AssignedTo: "$assignedTo",
            AssignedDate: "$AssignedDate",
            AssignedStateID: "$AssigneeStateID",
            AssignedStateName: "$State.StateMasterName",
            AssignedStateCode: "$State.StateCodeAlpha",
            AssignedMobile: "$AssigneeMobileNo",
            AssignedRoleName: "$AssigneeRoleName",
            AssignedRoleID: "$AssigneeRoleID",
            AssigneRoleName: "$AssigneRoleName",
            AssignedUserName: "$assignToName",
            AssignedByName: "$assignedByName",

            TicketInformation: {
              SupportTicketID: "$Ticket.SupportTicketID",
              CallerContactNumber: "$Ticket.CallerContactNumber",
              CallingAudioFile: "$Ticket.CallingAudioFile",
              TicketRequestorID: "$Ticket.TicketRequestorID",
              StateCodeAlpha: "$Ticket.StateCodeAlpha",
              StateMasterID: "$Ticket.StateMasterID",
              DistrictMasterID: "$Ticket.DistrictMasterID",
              VillageRequestorID: "$Ticket.VillageRequestorID",
              NyayPanchayatID: "$Ticket.NyayPanchayatID",
              NyayPanchayat: "$Ticket.NyayPanchayat",
              GramPanchayatID: "$Ticket.GramPanchayatID",
              GramPanchayat: "$Ticket.GramPanchayat",
              CallerID: "$Ticket.CallerID",
              CreationMode: "$Ticket.CreationMode",
              SupportTicketNo: "$Ticket.SupportTicketNo",
              RequestorUniqueNo: "$Ticket.RequestorUniqueNo",
              RequestorName: "$Ticket.RequestorName",
              RequestorMobileNo: "$Ticket.RequestorMobileNo",
              RequestorAccountNo: "$Ticket.RequestorAccountNo",
              RequestorAadharNo: "$Ticket.RequestorAadharNo",
              TicketCategoryID: "$Ticket.TicketCategoryID",
              CropCategoryOthers: "$Ticket.CropCategoryOthers",
              CropStageMaster: "$Ticket.CropStageMaster",
              CropStageMasterID: "$Ticket.CropStageMasterID",
              TicketHeaderID: "$Ticket.TicketHeaderID",
              SupportTicketTypeID: "$Ticket.SupportTicketTypeID",
              RequestYear: "$Ticket.RequestYear",
              RequestSeason: "$Ticket.RequestSeason",
              TicketSourceID: "$Ticket.TicketSourceID",
              TicketDescription: "$Ticket.TicketDescription",
              LossDate: "$Ticket.LossDate",
              LossTime: "$Ticket.LossTime",
              OnTimeIntimationFlag: "$Ticket.OnTimeIntimationFlag",
              VillageName: "$Ticket.VillageName",
              ApplicationCropName: "$Ticket.ApplicationCropName",
              CropName: "$Ticket.CropName",
              AREA: "$Ticket.AREA",
              DistrictRequestorID: "$Ticket.DistrictRequestorID",
              PostHarvestDate: "$Ticket.PostHarvestDate",
              TicketStatusID: "$Ticket.TicketStatusID",
              StatusUpdateTime: "$Ticket.StatusUpdateTime",
              StatusUpdateUserID: "$Ticket.StatusUpdateUserID",
              ApplicationNo: "$Ticket.ApplicationNo",
              InsuranceCompanyCode: "$Ticket.InsuranceCompanyCode",
              InsuranceCompanyID: "$Ticket.InsuranceCompanyID",
              InsurancePolicyNo: "$Ticket.InsurancePolicyNo",
              InsurancePolicyDate: "$Ticket.InsurancePolicyDate",
              InsuranceExpiryDate: "$Ticket.InsuranceExpiryDate",
              BankMasterID: "$Ticket.BankMasterID",
              AgentUserID: "$Ticket.AgentUserID",
              SchemeID: "$Ticket.SchemeID",
              AttachmentPath: "$Ticket.AttachmentPath",
              HasDocument: "$Ticket.HasDocument",
              Relation: "$Ticket.Relation",
              RelativeName: "$Ticket.RelativeName",
              SubDistrictID: "$Ticket.SubDistrictID",
              SubDistrictName: "$Ticket.SubDistrictName",
              PolicyPremium: "$Ticket.PolicyPremium",
              PolicyArea: "$Ticket.PolicyArea",
              PolicyType: "$Ticket.PolicyType",
              LandSurveyNumber: "$Ticket.LandSurveyNumber",
              LandDivisionNumber: "$Ticket.LandDivisionNumber",
              PlotVillageName: "$Ticket.PlotVillageName",
              PlotDistrictName: "$Ticket.PlotDistrictName",
              PlotStateName: "$Ticket.PlotStateName",
              ApplicationSource: "$Ticket.ApplicationSource",
              CropShare: "$Ticket.CropShare",
              IFSCCode: "$Ticket.IFSCCode",
              FarmerShare: "$Ticket.FarmerShare",
              CropSeasonName: "$Ticket.CropSeasonName",
              TicketSourceName: "$Ticket.TicketSourceName",
              TicketCategoryName: "$Ticket.TicketCategoryName",
              TicketStatus: "$Ticket.TicketStatus",
              InsuranceCompany: "$Ticket.InsuranceCompany",
              TicketTypeName: "$Ticket.TicketTypeName",
              StateMasterName: "$Ticket.StateMasterName",
              DistrictMasterName: "$Ticket.DistrictMasterName",
              TicketHeadName: "$Ticket.TicketHeadName",
              BMCGCode: "$Ticket.BMCGCode",
              BusinessRelationName: "$Ticket.BusinessRelationName",
              CropLossDetailID: "$Ticket.CropLossDetailID",
              CallingUniqueID: "$Ticket.CallingUniqueID",
              CallingInsertUserID: "$Ticket.CallingInsertUserID",
              CropStage: "$Ticket.CropStage",
              CategoryHeadID: "$Ticket.CategoryHeadID",
              Sos: "$Ticket.Sos",
              IsSos: "$Ticket.IsSos",
              TicketNCIPDocketNo: "$Ticket.TicketNCIPDocketNo",
              FilterDistrictRequestorID: "$Ticket.FilterDistrictRequestorID",
              FilterStateID: "$Ticket.FilterStateID",
              SchemeName: "$Ticket.SchemeName",
              InsertUserID: "$Ticket.InsertUserID",
              InsertIPAddress: "$Ticket.InsertIPAddress",
              UpdateUserID: "$Ticket.UpdateUserID",
              AgentName: "$Ticket.AgentName",
              CreatedBY: "$Ticket.CreatedBY",
              CallingUserID: "$Ticket.CallingUserID",

              TicketReOpenDate: {
                $cond: {
                  if: {
                    $or: [
                      { $eq: ["$Ticket.TicketReOpenDate", null] },
                      { $eq: ["$Ticket.TicketReOpenDate", ""] }
                    ]
                  },
                  then: null,
                  else: {
                    $dateToString: {
                      date: { $toDate: "$Ticket.TicketReOpenDate" },
                      format: "%Y-%m-%dT%H:%M:%S",
                      timezone: "Asia/Kolkata"
                    }
                  }
                }
              },

              InsertDateTime: {
                $cond: {
                  if: {
                    $or: [
                      { $eq: ["$Ticket.InsertDateTime", null] },
                      { $eq: ["$Ticket.InsertDateTime", ""] }
                    ]
                  },
                  then: null,
                  else: {
                    $dateToString: {
                      date: { $toDate: "$Ticket.InsertDateTime" },
                      format: "%Y-%m-%dT%H:%M:%S",
                      timezone: "Asia/Kolkata"
                    }
                  }
                }
              },

              UpdateDateTime: {
                $cond: {
                  if: {
                    $or: [
                      { $eq: ["$Ticket.UpdateDateTime", null] },
                      { $eq: ["$Ticket.UpdateDateTime", ""] }
                    ]
                  },
                  then: null,
                  else: {
                    $dateToString: {
                      date: { $toDate: "$Ticket.UpdateDateTime" },
                      format: "%Y-%m-%dT%H:%M:%S",
                      timezone: "Asia/Kolkata"
                    }
                  }
                }
              },

              SowingDate: {
                $cond: {
                  if: {
                    $or: [
                      { $eq: ["$Ticket.SowingDate", null] },
                      { $eq: ["$Ticket.SowingDate", ""] }
                    ]
                  },
                  then: null,
                  else: {
                    $dateToString: {
                      date: { $toDate: "$Ticket.SowingDate" },
                      format: "%Y-%m-%dT%H:%M:%S",
                      timezone: "Asia/Kolkata"
                    }
                  }
                }
              },

              CreatedAt: {
                $dateToString: {
                  date: { $toDate: "$Ticket.Created" },
                  format: "%Y-%m-%dT%H:%M:%S",
                  timezone: "Asia/Kolkata"
                }
              }
            }






          }
        }
      ]
      let getBucketData = await this.db.collection('Ticket_Assignment_History').aggregate(pipeline).toArray()
      if (getBucketData.length > 0) {
        ticketData = getBucketData
        message["msg"] = "Success";
        message["code"] = "1"
      } else {
        ticketData = [];
        message["msg"] = "Success";
        message["code"] = "0"
      }

      return { data: ticketData, message: message }

    } catch (err) {
      console.log(err)

    }
  }


  async AssignedTicketByInsuranceService(payload: any) {
    try {
      const { insuranceCompanyId, year, month } = payload;

      if (!insuranceCompanyId) {
        return {
          data: {},
          message: { msg: "Failed - Missing required field(s): insuranceCompanyId", code: "0" }
        };
      }

      const insuranceId = Number(insuranceCompanyId);
      const startDate = new Date(year, month - 1, 1); // 2025-12-01
      const endDate = new Date(year, month, 1);

      const statePipeline = this.buildAssignedTicketPipeline({
        InsuranceCompanyId: insuranceId,
        AssigneeStateID: { $exists: true, $ne: "" },
        AssignedDate: {
          $gte: startDate,
          $lt: endDate
        }
      });



      const [stateAssigned] = await Promise.all([
        this.db.collection("Ticket_Assignment").aggregate(statePipeline).toArray(),
      ]);

      if (stateAssigned.length === 0) {
        return {
          data: {},
          message: { msg: "Failed - No Record Found", code: "0" }
        };
      }
      let obj = {
        stateAssigned,
      }
      return {
        data: obj,
        message: { msg: "Success", code: "1" }
      };

    } catch (err) {
      console.error(err);
      return {
        data: {},
        message: { msg: "Internal Server Error", code: "0" }
      };
    }
  }


  private buildAssignedTicketPipeline(matchCondition: any) {
    return [
      { $match: matchCondition },

      {
        $lookup: {
          from: "SLA_Ticket_listing",
          localField: "SupportTicketID",
          foreignField: "SupportTicketID",
          as: "Ticket"
        }
      },
      {
        $unwind: {
          path: "$Ticket",
          preserveNullAndEmptyArrays: true
        }
      },
      {
        $project: {
          assignedBy: 1,
          assignedTo: 1,
          assignToName: 1,
          AssignedDate: 1,
          AssigneeStateID: 1,
          AssigneRoleName: 1,

          SupportTicketID: "$Ticket.SupportTicketID",
          CallerContactNumber: "$Ticket.CallerContactNumber",
          CallingAudioFile: "$Ticket.CallingAudioFile",
          TicketRequestorID: "$Ticket.TicketRequestorID",
          StateCodeAlpha: "$Ticket.StateCodeAlpha",
          StateMasterID: "$Ticket.StateMasterID",
          DistrictMasterID: "$Ticket.DistrictMasterID",
          VillageRequestorID: "$Ticket.VillageRequestorID",
          NyayPanchayatID: "$Ticket.NyayPanchayatID",
          NyayPanchayat: "$Ticket.NyayPanchayat",
          GramPanchayatID: "$Ticket.GramPanchayatID",
          GramPanchayat: "$Ticket.GramPanchayat",
          CallerID: "$Ticket.CallerID",
          CreationMode: "$Ticket.CreationMode",
          SupportTicketNo: "$Ticket.SupportTicketNo",
          RequestorUniqueNo: "$Ticket.RequestorUniqueNo",
          RequestorName: "$Ticket.RequestorName",
          RequestorMobileNo: "$Ticket.RequestorMobileNo",
          RequestorAccountNo: "$Ticket.RequestorAccountNo",
          RequestorAadharNo: "$Ticket.RequestorAadharNo",
          TicketCategoryID: "$Ticket.TicketCategoryID",
          CropCategoryOthers: "$Ticket.CropCategoryOthers",
          CropStageMaster: "$Ticket.CropStageMaster",
          CropStageMasterID: "$Ticket.CropStageMasterID",
          TicketHeaderID: "$Ticket.TicketHeaderID",
          SupportTicketTypeID: "$Ticket.SupportTicketTypeID",
          RequestYear: "$Ticket.RequestYear",
          RequestSeason: "$Ticket.RequestSeason",
          TicketSourceID: "$Ticket.TicketSourceID",
          TicketDescription: "$Ticket.TicketDescription",
          LossDate: "$Ticket.LossDate",
          LossTime: "$Ticket.LossTime",
          OnTimeIntimationFlag: "$Ticket.OnTimeIntimationFlag",
          VillageName: "$Ticket.VillageName",
          ApplicationCropName: "$Ticket.ApplicationCropName",
          CropName: "$Ticket.CropName",
          AREA: "$Ticket.AREA",
          DistrictRequestorID: "$Ticket.DistrictRequestorID",
          PostHarvestDate: "$Ticket.PostHarvestDate",
          TicketStatusID: "$Ticket.TicketStatusID",
          StatusUpdateTime: "$Ticket.StatusUpdateTime",
          StatusUpdateUserID: "$Ticket.StatusUpdateUserID",
          ApplicationNo: "$Ticket.ApplicationNo",
          InsuranceCompanyCode: "$Ticket.InsuranceCompanyCode",
          InsuranceCompanyID: "$Ticket.InsuranceCompanyID",
          InsurancePolicyNo: "$Ticket.InsurancePolicyNo",
          InsurancePolicyDate: "$Ticket.InsurancePolicyDate",
          InsuranceExpiryDate: "$Ticket.InsuranceExpiryDate",
          BankMasterID: "$Ticket.BankMasterID",
          AgentUserID: "$Ticket.AgentUserID",
          SchemeID: "$Ticket.SchemeID",
          AttachmentPath: "$Ticket.AttachmentPath",
          HasDocument: "$Ticket.HasDocument",
          Relation: "$Ticket.Relation",
          RelativeName: "$Ticket.RelativeName",
          SubDistrictID: "$Ticket.SubDistrictID",
          SubDistrictName: "$Ticket.SubDistrictName",
          PolicyPremium: "$Ticket.PolicyPremium",
          PolicyArea: "$Ticket.PolicyArea",
          PolicyType: "$Ticket.PolicyType",
          LandSurveyNumber: "$Ticket.LandSurveyNumber",
          LandDivisionNumber: "$Ticket.LandDivisionNumber",
          PlotVillageName: "$Ticket.PlotVillageName",
          PlotDistrictName: "$Ticket.PlotDistrictName",
          PlotStateName: "$Ticket.PlotStateName",
          ApplicationSource: "$Ticket.ApplicationSource",
          CropShare: "$Ticket.CropShare",
          IFSCCode: "$Ticket.IFSCCode",
          FarmerShare: "$Ticket.FarmerShare",
          CropSeasonName: "$Ticket.CropSeasonName",
          TicketSourceName: "$Ticket.TicketSourceName",
          TicketCategoryName: "$Ticket.TicketCategoryName",
          TicketStatus: "$Ticket.TicketStatus",
          InsuranceCompany: "$Ticket.InsuranceCompany",
          TicketTypeName: "$Ticket.TicketTypeName",
          StateMasterName: "$Ticket.StateMasterName",
          DistrictMasterName: "$Ticket.DistrictMasterName",
          TicketHeadName: "$Ticket.TicketHeadName",
          BMCGCode: "$Ticket.BMCGCode",
          BusinessRelationName: "$Ticket.BusinessRelationName",
          CropLossDetailID: "$Ticket.CropLossDetailID",
          CallingUniqueID: "$Ticket.CallingUniqueID",
          CallingInsertUserID: "$Ticket.CallingInsertUserID",
          CropStage: "$Ticket.CropStage",
          CategoryHeadID: "$Ticket.CategoryHeadID",
          Sos: "$Ticket.Sos",
          IsSos: "$Ticket.IsSos",
          TicketNCIPDocketNo: "$Ticket.TicketNCIPDocketNo",
          FilterDistrictRequestorID: "$Ticket.FilterDistrictRequestorID",
          FilterStateID: "$Ticket.FilterStateID",
          SchemeName: "$Ticket.SchemeName",
          InsertUserID: "$Ticket.InsertUserID",
          InsertIPAddress: "$Ticket.InsertIPAddress",
          UpdateUserID: "$Ticket.UpdateUserID",
          AgentName: "$Ticket.AgentName",
          CreatedBY: "$Ticket.CreatedBY",
          CallingUserID: "$Ticket.CallingUserID",

          TicketReOpenDate: {
            $cond: {
              if: {
                $or: [
                  { $eq: ["$Ticket.TicketReOpenDate", null] },
                  { $eq: ["$Ticket.TicketReOpenDate", ""] }
                ]
              },
              then: null,
              else: {
                $dateToString: {
                  date: { $toDate: "$Ticket.TicketReOpenDate" },
                  format: "%Y-%m-%dT%H:%M:%S",
                  timezone: "Asia/Kolkata"
                }
              }
            }
          },

          InsertDateTime: {
            $cond: {
              if: {
                $or: [
                  { $eq: ["$Ticket.InsertDateTime", null] },
                  { $eq: ["$Ticket.InsertDateTime", ""] }
                ]
              },
              then: null,
              else: {
                $dateToString: {
                  date: { $toDate: "$Ticket.InsertDateTime" },
                  format: "%Y-%m-%dT%H:%M:%S",
                  timezone: "Asia/Kolkata"
                }
              }
            }
          },

          UpdateDateTime: {
            $cond: {
              if: {
                $or: [
                  { $eq: ["$Ticket.UpdateDateTime", null] },
                  { $eq: ["$Ticket.UpdateDateTime", ""] }
                ]
              },
              then: null,
              else: {
                $dateToString: {
                  date: { $toDate: "$Ticket.UpdateDateTime" },
                  format: "%Y-%m-%dT%H:%M:%S",
                  timezone: "Asia/Kolkata"
                }
              }
            }
          },

          SowingDate: {
            $cond: {
              if: {
                $or: [
                  { $eq: ["$Ticket.SowingDate", null] },
                  { $eq: ["$Ticket.SowingDate", ""] }
                ]
              },
              then: null,
              else: {
                $dateToString: {
                  date: { $toDate: "$Ticket.SowingDate" },
                  format: "%Y-%m-%dT%H:%M:%S",
                  timezone: "Asia/Kolkata"
                }
              }
            }
          },

          CreatedAt: {
            $dateToString: {
              date: { $toDate: "$Ticket.Created" },
              format: "%Y-%m-%dT%H:%M:%S",
              timezone: "Asia/Kolkata"
            }
          }



        }
      }
    ];
  }


  async EscalationHistoryTrailService(payload: any) {
    try {
      const SupportTicketID = Number(payload?.SupportTicketID);

      if (!SupportTicketID || Number.isNaN(SupportTicketID)) {
        return {
          data: {},
          message: {
            msg: "Failed - Missing required field(s): SupportTicketID",
            code: "0"
          }
        };
      }

      const pipeline = [
        {
          $match: {
            SupportTicketID
          }
        },
        {
          $project: {
            _id: 1,
            SupportTicketID: 1,
            SupportTicketNo: 1,
            TicketStatusID: 1,
            TicketStatus: 1,
            assignedBy: 1,
            assignedTo: 1,
            AssignedDate: 1,
            AssigneeStateID: 1,
            AssigneeMobileNo: 1,
            AssigneRoleName: 1,
            AssigneeRoleID: 1,
            DistrictID: 1,
            IsActive: 1,
            UpdatedDate: 1,
            CreatedDate: 1,
            InsuranceCompanyName: 1,
            assignedByName: 1,
            assignToName: 1,
            Comment: { $ifNull: ["$TicketComment", ""] },
            PreviousRoleName: 1,
            PreviousRoleId: 1

          }
        }
      ];

      const historyInfo = await this.db
        .collection("Ticket_Assignment_History")
        .aggregate(pipeline, { allowDiskUse: true })
        .toArray();

      if (!Array.isArray(historyInfo) || historyInfo.length === 0) {
        return {
          data: {},
          message: {
            msg: "Failed - No Record Found For History",
            code: "0"
          }
        };
      }
      let response = { data: historyInfo }
      return {
        data: response,
        message: {
          msg: "Success",
          code: "1"
        }
      };
    } catch (err) {
      console.error("EscalationHistoryTrailService Error:", err);
      return {
        data: {},
        message: {
          msg: "Internal Server Error",
          code: "0"
        }
      };
    }
  }





}







