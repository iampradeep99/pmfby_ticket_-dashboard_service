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

// import * as ExcelJS from 'exceljs';

@Injectable()
export class TicketEscalationService {
  private ticketCollection: Collection;
  private ticketDbCollection: Collection;
  public gcp = new GCPServices();
  logDir = path.join(__dirname, '..', 'logs');

  constructor(
    @Inject('MONGO_DB') private readonly db: Db,
    @Inject('SEQUELIZE') private readonly sequelize: Sequelize,
    private readonly redisWrapper: RedisWrapper,
    private readonly mailService: MailService,
  ) {
    this.ticketCollection = this.db.collection('tickets');
    this.ticketDbCollection = this.db.collection('SLA_KRPH_SupportTickets_Records');
  }

  

   
  async fetchRoles(payload: any): Promise<{ data: any; message: { msg: string; code: number } }> {
    console.log("🚀 Entering fetchRoles process");

    try {
      const response: AxiosResponse<any> = await axios.get(process.env.pmfby_ROLE_URL, {
        params: payload || {},
        timeout: 10000, 
        headers: {
          'Content-Type': 'application/json'
        }
      });

      if (!response || !response.data) {
        return {
          data: null,
          message: { msg: '❌ No data received from API', code: 0 }
        };
      }

      return {
        data: response.data.data,
        message: { msg: '✅ Data fetched successfully', code: 1 }
      };

    } catch (error: any) {
      let errorMsg = '❌ Failed to fetch roles';

      if (error.response) {
        errorMsg = `❌ API responded with status ${error.response.status}`;
      } else if (error.request) {
        errorMsg = '❌ No response received from API';
      } else if (error.message) {
        errorMsg = `❌ Error: ${error.message}`;
      }

      console.error('❌ Error in fetchRoles:', error);

      return {
        data: null,
        message: { msg: errorMsg, code: 0 }
      };
    }
  }


//     async fetchTicketListing(payload: any) {
//     try {
//       const db = this.db;
  
//       let {
//         fromdate,
//         toDate,
//         viewTYP,
//         ticketCategoryID,
//         supportTicketTypeID,
//         supportTicketNo,
//         statusID,
//         schemeID,
//         ticketHeaderID,
//         stateID,
//         insuranceCompanyID,
//         pageIndex = 1,
//         pageSize = 100,
//         objCommon
//       } = payload;
  
//       ticketHeaderID = Number(ticketHeaderID);
//       ticketCategoryID = Number(ticketCategoryID);
//       supportTicketTypeID = Number(supportTicketTypeID);
//       statusID = Number(statusID);
//       schemeID = Number(schemeID);
  
//       if (!objCommon && objCommon.insertedUserID && objCommon.insertedUserID === "") {
//         return {
//           data: [],
//           message: { msg: "User Id is required", code: "0" }
//         };
//       }
  
//       const [Delta, districtInfoRaw] = await Promise.all([
//         await new UtilService().getSupportTicketUserDetail(objCommon.insertedUserID),
//         Promise.resolve(null)
//       ]);
  
//       const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
//       const item = (responseInfo.data as any)?.user?.[0];
//       if (!item) return { rcode: 0, rmessage: "User details not found." };
  
//       const userDetail = {
           
//         InsuranceCompanyID: item.InsuranceCompanyID ?  await new UtilService().convertStringToArray(item.InsuranceCompanyID) : [],
//         StateMasterID: item.StateMasterID ? await new UtilService().convertStringToArray(item.StateMasterID) : [],
//         BRHeadTypeID: item.BRHeadTypeID,
//         LocationTypeID: item.LocationTypeID,
//         FromDay: item?.FromDay,
//         EscalationFlag: item?.EscalationFlag,
//         AppAccessID: item?.AppAccessID
//       };
  
//       const { InsuranceCompanyID, StateMasterID, LocationTypeID, EscalationFlag, AppAccessID } = userDetail;
//       let locationFilter: any = {};
//       if (LocationTypeID === 1 && StateMasterID?.length) {
//         locationFilter = { FilterStateID: { $in: StateMasterID.map(Number) } };
//       } else if (LocationTypeID === 2) {
//         const districtInfo = await new UtilService().GetDetailsForDistrictUsers(Number(AppAccessID));
//         const collectedDistrictInfo = await new UtilService().unGZip(districtInfo.responseDynamic);
  
//         const districtId: number[] = [];
//         if (collectedDistrictInfo?.masterdatabinding && Array.isArray(collectedDistrictInfo.masterdatabinding)) {
//           for (const itemData of collectedDistrictInfo.masterdatabinding) {
//             districtId.push(itemData.DistrictCodeAlpha);
//           }
//           locationFilter = { FilterDistrictRequestorID: { $in: districtId } };
//         } else {
//           console.warn("Invalid district info format:", collectedDistrictInfo);
//           locationFilter = {};
//         }
//       }
  
//       const match: any = { ...locationFilter };
  
//       if (ticketHeaderID && ticketHeaderID !== 0) match.TicketHeaderID = ticketHeaderID;
  
//       if (insuranceCompanyID && insuranceCompanyID !== 0) {
//         const requestedInsuranceIDs = String(insuranceCompanyID).split(",").map(id => Number(id.trim()));
//         const allowedInsuranceIDs = InsuranceCompanyID.map(Number);
//         const validInsuranceIDs = requestedInsuranceIDs.filter(id => allowedInsuranceIDs.includes(id));
//         if (validInsuranceIDs.length === 0) {
//           return { rcode: 0, rmessage: "Unauthorized InsuranceCompanyID(s)." };
//         }
//         match.InsuranceCompanyID = { $in: validInsuranceIDs };
//       } else if (InsuranceCompanyID?.length) {
//         match.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
//       }
  
//       if(StateMasterID){
//     if (stateID && stateID !== "" && LocationTypeID !== 2) {
//         const requestedStateIDs = String(stateID).split(",").map(id => Number(id.trim()));
//         const validStateIDs = requestedStateIDs.filter(id => StateMasterID.map(Number).includes(id));
//         if (validStateIDs.length === 0) {
//           return { rcode: 0, rmessage: "Unauthorized StateID(s)." };
//         }
//         match.StateMasterID = { $in: validStateIDs };
//       } else if (StateMasterID?.length && LocationTypeID !== 2) {
//         match.FilterStateID = { $in: StateMasterID.map(Number) };
//       }
//       }
  
    
//       if (viewTYP === "FILTER") {
//         if (fromdate && toDate) {
//           match.Created = {
//             $gte: new Date(`${fromdate}T00:00:00.000Z`),
//             $lte: new Date(`${toDate}T23:59:59.999Z`)
//           };
//         }
//         if (ticketCategoryID) match.TicketCategoryID = ticketCategoryID;
//         if (supportTicketTypeID) match.SupportTicketTypeID = supportTicketTypeID;
//         if (supportTicketNo) match.SupportTicketNo = supportTicketNo;
    
//       }
//       const pipeline: any[] = [
//         { $match: match },
//         {
//           $facet: {
//             data: [
//               { $sort: { InsertDateTime: -1 } },
//               ...(pageIndex !== -1
//                 ? [
//                     { $skip: (pageIndex - 1) * pageSize },
//                     { $limit: pageSize }
//                   ]
//                 : []),
//              {
//                 $project: {
//                   _id: 0,
                
//                   SupportTicketNo: 1,
//                   RequestorName: 1,
//                   RequestorMobileNo: 1,
        
//                   RequestYear: 1,
//                   RequestSeason: 1,
                 
//                   ApplicationNo: 1,
                  
//                   InsurancePolicyNo: 1,

//                   TicketCategoryName: 1,
                
//                   TicketTypeName: 1,
//                   TicketHeadName: 1,
                  
                
//                   CreatedAt: {
//         $dateToString: {
//           date: { $toDate: "$Created" },
//           format: "%Y-%m-%dT%H:%M:%S",
//           timezone: "Asia/Kolkata"
//         }
//       },
//                 }
//               }
//             ],
//             totalCount: [
//               { $count: "count" }
//             ],
//             ticketStatusSummary: [
//               {
//                 $project: {
//                   TicketStatusID: 1,
//                   TicketHeaderID: 1,
//                   customStatus: {
//                     $switch: {
//                       branches: [
//                         { case: { $and: [{ $eq: ["$TicketStatusID", 109303] }, { $in: ["$TicketHeaderID", [1, 4]] }] }, then: "Resolved" },
//                         { case: { $and: [{ $eq: ["$TicketStatusID", 109303] }, { $eq: ["$TicketHeaderID", 2] }] }, then: "Resolved(Information)" },
//                         { case: { $eq: ["$TicketStatusID", 109301] }, then: "Open" },
//                         { case: { $eq: ["$TicketStatusID", 109302] }, then: "In-Progress" },
//                         { case: { $eq: ["$TicketStatusID", 109304] }, then: "Re-Open" }
//                       ],
//                       default: "Other"
//                     }
//                   }
//                 }
//               },
//               ...(viewTYP === "DEFESCAL" || (viewTYP === "ESCAL" && EscalationFlag === "Y")
//                 ? [{ $match: { TicketStatusID: 109301 } }]
//                 : []),
//               { $group: { _id: "$customStatus", count: { $sum: 1 } } }
//             ]
//           }
//         }
//       ];
  
  
//       console.log(JSON.stringify(pipeline), "testdd");
//       const aggResult = await db.collection("SLA_Ticket_listing").aggregate(pipeline, { allowDiskUse: true }).toArray();
//       const result = aggResult[0] || { data: [], totalCount: [], ticketStatusSummary: [] };
  
//       if (result.data.length === 0) {
//         return {
//           data: [],
//           message: { msg: "Record Not Found", code: "0" },
//           totalCount: 0,
//           totalPages: 0
//         };
//       }
  
//       const ticketSummary = result.ticketStatusSummary.map(item => ({
//         Total: item.count.toString(),
//         TicketStatus: item._id
//       }));
  
//       return {
//         obj: { status: ticketSummary, supportTicket: result.data },
//         message: { msg: "Fetched Success", code: "1" },
//         totalCount: result.totalCount[0]?.count || 0,
//         totalPages: pageSize > 0 ? Math.ceil((result.totalCount[0]?.count || 0) / pageSize) : 1
//       };
//     } catch (err) {
//       console.error("❌ Top-level error:", err);
//       return { data: [], message: "Unexpected error" };
//     }
//   }
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
        locationFilter = { FilterDistrictRequestorID: { $in: districtId } };
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

    if (StateMasterID) {
      if (stateID && stateID !== "" && LocationTypeID !== 2) {
        const requestedStateIDs = String(stateID).split(",").map(id => Number(id.trim()));
        const validStateIDs = requestedStateIDs.filter(id => StateMasterID.map(Number).includes(id));
        if (validStateIDs.length === 0) {
          return { rcode: 0, rmessage: "Unauthorized StateID(s)." };
        }
        match.StateMasterID = { $in: validStateIDs };
      } else if (StateMasterID?.length && LocationTypeID !== 2) {
        match.FilterStateID = { $in: StateMasterID.map(Number) };
      }
    }
   

    if (fromdate && toDate) {
      match.Created = {
        $gte: new Date(`${fromdate}T00:00:00.000Z`),
        $lte: new Date(`${toDate}T23:59:59.999Z`)
      };
    }
    if (ticketCategoryID) match.TicketCategoryID = ticketCategoryID;
    if (supportTicketTypeID) match.SupportTicketTypeID = supportTicketTypeID;
    if (supportTicketNo) match.SupportTicketNo = supportTicketNo;

   /*  const pipeline: any[] = [
      { $match: match,
        TicketStatusID: { $ne: 109303 },
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
                RequestorName: 1,
                RequestorMobileNo: 1,
                RequestYear: 1,
                RequestSeason: 1,
                ApplicationNo: 1,
                InsurancePolicyNo: 1,
                TicketCategoryName: 1,
                TicketTypeName: 1,
                TicketHeadName: 1,
                CreatedAt: {
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
            },
            { $group: { _id: "$customStatus", count: { $sum: 1 } } }
          ]
        }
      }
    ]; */

/*     const pipeline: any[] = [
  { 
    $match: {
      ...match, 
      TicketStatusID: { $ne: 109303 } 
    }
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
            RequestorName: 1,
            RequestorMobileNo: 1,
            RequestYear: 1,
            RequestSeason: 1,
            ApplicationNo: 1,
            InsurancePolicyNo: 1,
            TicketCategoryName: 1,
            TicketTypeName: 1,
            TicketHeadName: 1,
            CreatedAt: {
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
]; */

    const pipeline: any[] = [
  { 
    $match: {
      ...match,
      TicketStatusID: { $ne: 109303 }
    }
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
    $match: {
      assignmentHistory: { $size: 0 }
    }
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
            SupportTicketID:1,
            RequestorName: 1,
            RequestorMobileNo: 1,
            RequestYear: 1,
            RequestSeason: 1,
            ApplicationNo: 1,
            InsurancePolicyNo: 1,
            TicketCategoryName: 1,
            TicketTypeName: 1,
            TicketHeadName: 1,
            CreatedAt: {
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

    console.log(JSON.stringify(pipeline), "Aggregation Pipeline");
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
    console.error("❌ Top-level error:", err);
    return { data: [], message: "Unexpected error" };
  }
}


async AssignTicketServie(payload: any) {
  const db = this.db;
  const ticketCollection = db.collection('SLA_Ticket_listing');
  const assignHistoryCollection = db.collection('Ticket_Assignment_History');

  // Validate payload
  if (!payload || typeof payload !== 'object') {
    return { success: false, message: 'Invalid payload format' };
  }

  const { ticketIds, assignedBy, assignedTo, Role } = payload;

  if (!ticketIds || typeof ticketIds !== 'string' || ticketIds.trim() === '') {
    return { success: false, message: 'ticketIds is required and must be a comma-separated string' };
  }

  if (!assignedBy || !assignedTo || !Role) {
    return { success: false, message: 'assignedBy, assignedTo, and Role are required fields' };
  }

  const ticketIdArray = ticketIds
    .split(',')
    .map(id => id.trim())
    .filter(id => id !== '');

  if (ticketIdArray.length === 0) {
    return { success: false, message: 'No valid ticket IDs found' };
  }

  const results = [];
  const now = new Date();

  for (const ticketId of ticketIdArray) {
    try {
      const ticketIdNum = Number(ticketId);
      const existingTicket = await ticketCollection.findOne({ SupportTicketID: ticketIdNum });

      if (!existingTicket) {
        results.push({ ticketId, status: 'Failed', reason: 'Ticket not found' });
        continue;
      }

      const currentStatusId = existingTicket.TicketStatusID || null;
      const currentStatusName = existingTicket.TicketStatus || null;

      const assignmentRecord = {
        SupportTicketID: ticketIdNum,
        TicketStatusID: currentStatusId,
        TicketStatus: currentStatusName,
        assignedBy,
        assignedTo,
        Role,
        AssignedDate: now,
      };

      const insertResult = await assignHistoryCollection.insertOne(assignmentRecord);

      if (!insertResult.insertedId) {
        results.push({ ticketId, status: 'Failed', reason: 'History insert failed' });
        continue;
      }

      results.push({ ticketId, status: 'Success' });
    } catch (err: any) {
      results.push({ ticketId, status: 'Error', reason: err.message || 'Unexpected error' });
    }
  }

  const summary = {
    successCount: results.filter(r => r.status === 'Success').length,
    failedCount: results.filter(r => r.status === 'Failed' || r.status === 'Error').length,
    totalTickets: ticketIdArray.length,
  };

  return { success: true, summary, details: results };
}




}
