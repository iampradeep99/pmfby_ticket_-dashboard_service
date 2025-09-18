// // import { parentPort, workerData } from 'worker_threads';
// const { parentPort, workerData } = require('worker_threads');
// // import fs from 'fs';
// // import path from 'path';
// const fs = require('fs');
// const path = require('path');
// const ExcelJS = require('exceljs');
// const archiver = require('archiver');
// const axios = require('axios');
// import { GCPServices } from '../GCSFileUpload';
// import { generateSupportTicketEmailHTML, getCurrentFormattedDateTime } from '../../templates/mailTemplates'
// import { UtilService } from "../../commonServices/utilService";
// import { RedisWrapper } from '../../commonServices/redisWrapper';
// import { MailService } from '../../mail/mail.service';
// // import axios from 'axios'
// import { MongoClient, Db } from 'mongodb';
// const redisWrapper = new RedisWrapper()
// const mailService = new MailService()
// let cachedDb: Db | null = null;

// async function connectToDatabase(uri: string, dbName: string): Promise<Db> {
//   if (cachedDb) return cachedDb;

//   if (!uri) throw new Error('MongoDB URI is required');
//   if (!dbName) throw new Error('Database name is required');

//   const client = new MongoClient(uri, {
//     maxPoolSize: 50,
//     connectTimeoutMS: 10000,
//   });

//   await client.connect();
//   cachedDb = client.db(dbName);

//   console.log(`MongoDB connected to database: ${dbName}`);
//   return cachedDb;
// }


// async function processTicketHistory(ticketPayload: any) {
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

//   const db = await connectToDatabase('mongodb://10.128.60.45:27017', 'krph_db')
//     SPTicketHeaderID = Number(SPTicketHeaderID);

//      if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
//   if (!SPStateID){
//     console.log({ rcode: 0, rmessage: 'StateID Missing!' })
//   }

//   const RequestDateTime = await getCurrentFormattedDateTime();
//   const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;

//   const cachedData = await redisWrapper.getRedisCache(cacheKey) as any;
//   if (cachedData) {
//     console.log('Using cached data');
//     await db.collection('support_ticket_download_logs').updateOne(
//       { SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE },
//       {
//         $set: {
//           downloadUrl: cachedData.downloadUrl || '',
//           zipFileName: cachedData.zipFileName || '',
//           updatedAt: new Date()
//         },
//         $setOnInsert: { createdAt: new Date() }
//       },
//       { upsert: true }
//     );
//     return cachedData;
//   }

//   // ===== User detail auth =====
// //   const Delta = await (ticketPayload as any).getSupportTicketUserDetail(SPUserID);
//   const Delta = await getSupportTicketUserDetail(SPUserID);
//   const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
//   const item = (responseInfo.data as any)?.user?.[0];
//   if (!item) return { rcode: 0, rmessage: 'User details not found.' };

//   const userDetail = {
//     InsuranceCompanyID: item.InsuranceCompanyID ? await convertStringToArray(item.InsuranceCompanyID) : [],
//     StateMasterID: item.StateMasterID ? await convertStringToArray(item.StateMasterID) : [],
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
//   } else if (InsuranceCompanyID?.length) {
//     baseMatch.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
//   }

//   if (SPStateID && SPStateID !== '#ALL') {
//     const requestedStateIDs = SPStateID.split(',').map((id) => id.trim());
//     const validStateIDs = requestedStateIDs.filter((id) => StateMasterID.includes(id));
//     if (!validStateIDs.length)
//       return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
//     baseMatch.FilterStateID = { $in: validStateIDs };
//   } else if (StateMasterID?.length && LocationTypeID !== 2) {
//     baseMatch.FilterStateID = { $in: StateMasterID };
//   }

//   const folderPath = path.join(process.cwd(), 'downloads');
//   await fs.promises.mkdir(folderPath, { recursive: true });

//   const headerTypeMap: Record<number, string> = {
//     1: 'Grievance',
//     2: 'Information',
//     4: 'Crop_Loss',
//   };
//   const ticketTypeName = headerTypeMap[SPTicketHeaderID] || 'General';
//   const currentDateStr = new Date().toLocaleDateString('en-GB').split('/').join('_');
//   const excelFileName = `Support_ticket_data_${ticketTypeName}_${currentDateStr}.xlsx`;
//   const excelFilePath = path.join(folderPath, excelFileName);

//   const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
//   const worksheet = workbook.addWorksheet('Support Tickets');

// //   await (ticketPayload as any).insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, "", "", db);

//    await insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, "", "", db)

//   const CHUNK_SIZE = 10000;

//   const staticColumns = [
//     { header: 'Agent ID', key: 'AgentID', width: 20 },
//     { header: 'Calling ID', key: 'CallingUniqueID', width: 25 },
//     { header: 'Ticket NCIP Docket No', key: 'TicketNCIPDocketNo', width: 25 },
//     { header: 'Ticket No', key: 'SupportTicketNo', width: 30 },
//     { header: 'Creation Date', key: 'Created', width: 25 },
//     { header: 'Ticket ReOpen Date', key: 'TicketReOpenDate', width: 25 },
//     { header: 'Ticket Status', key: 'TicketStatus', width: 20 },
//     { header: 'Status Update Time', key: 'StatusUpdateTime', width: 25 },
//     { header: 'State', key: 'StateMasterName', width: 20 },
//     { header: 'District', key: 'DistrictMasterName', width: 20 },
//     { header: 'Sub District', key: 'SubDistrictName', width: 20 },
//     { header: 'Ticket Head', key: 'TicketHeadName', width: 20 },
//     { header: 'Ticket Type', key: 'TicketTypeName', width: 20 },
//     { header: 'Ticket Category', key: 'TicketCategoryName', width: 20 },
//     { header: 'Crop Season', key: 'CropSeasonName', width: 20 },
//     { header: 'Request Year', key: 'RequestYear', width: 20 },
//     { header: 'Insurance Company', key: 'InsuranceCompany', width: 30 },
//     { header: 'Application No', key: 'ApplicationNo', width: 30 },
//     { header: 'Policy No', key: 'InsurancePolicyNo', width: 30 },
//     { header: 'Caller Contact No', key: 'CallerContactNumber', width: 20 },
//     { header: 'Requestor Name', key: 'RequestorName', width: 20 },
//     { header: 'Requestor Mobile No', key: 'RequestorMobileNo', width: 20 },
//     { header: 'Relation', key: 'Relation', width: 20 },
//     { header: 'Relative Name', key: 'RelativeName', width: 20 },
//     { header: 'Policy Premium', key: 'PolicyPremium', width: 20 },
//     { header: 'Policy Area', key: 'PolicyArea', width: 20 },
//     { header: 'Policy Type', key: 'PolicyType', width: 20 },
//     { header: 'Land Survey No', key: 'LandSurveyNumber', width: 20 },
//     { header: 'Land Division No', key: 'LandDivisionNumber', width: 20 },
//     { header: 'Plot State', key: 'PlotStateName', width: 20 },
//     { header: 'Plot District', key: 'PlotDistrictName', width: 20 },
//     { header: 'Plot Village', key: 'PlotVillageName', width: 20 },
//     { header: 'Application Source', key: 'ApplicationSource', width: 20 },
//     { header: 'Crop Share', key: 'CropShare', width: 20 },
//     { header: 'IFSC Code', key: 'IFSCCode', width: 20 },
//     { header: 'Farmer Share', key: 'FarmerShare', width: 20 },
//     { header: 'Sowing Date', key: 'SowingDate', width: 20 },
//     { header: 'Created By', key: 'CreatedBY', width: 20 },
//     { header: 'Ticket Description', key: 'TicketDescription', width: 50 },
//   ];
//   worksheet.columns = staticColumns;

//   function formatToDDMMYYYY(dateString) {
//     if (!dateString) return '';
//     const date = new Date(dateString);
//     if (isNaN(date.getTime())) return '';
//     const day = String(date.getDate()).padStart(2, '0');
//     const month = String(date.getMonth() + 1).padStart(2, '0');
//     const year = date.getFullYear();
//     const hours = String(date.getHours()).padStart(2, '0');
//     const minutes = String(date.getMinutes()).padStart(2, '0');
//     return `${day}-${month}-${year} ${hours}:${minutes}`;
//   }

//   async function processDateWithChunking(currentDate: Date, endDate: Date) {
//     if (currentDate > endDate) return;

//     const startOfDay = new Date(currentDate);
//     startOfDay.setUTCHours(0, 0, 0, 0);
//     const endOfDay = new Date(currentDate);
//     endOfDay.setUTCHours(23, 59, 59, 999);

//     let skip = 0, hasMore = true;

//     while (hasMore) {
//       const dailyMatch = { ...baseMatch, InsertDateTime: { $gte: startOfDay, $lte: endOfDay } };
//       const pipeline: any[] = [
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
//           },
//         },
//         { $unwind: { path: '$ticketHistory', preserveNullAndEmptyArrays: true } },
//         { $unwind: { path: '$claimInfo', preserveNullAndEmptyArrays: true } },
//         { $unwind: { path: '$agentInfo', preserveNullAndEmptyArrays: true } },
//         { $skip: skip },
//         { $limit: CHUNK_SIZE },
//         { $addFields: { ticket_comment_journey: { $ifNull: ['$ticket_comment_journey', []] } } }
//       ];

//       const cursor = db.collection('SLA_KRPH_SupportTickets_Records').aggregate(pipeline, { allowDiskUse: true });

//     //   console.log(cursor[0])
//       const docs = await cursor.toArray();

//       for (const doc of docs) {
//         const dynamicColumnsBatch: any = {};
//         if (Array.isArray(doc.ticket_comment_journey)) {
//           const seen = new Set();
//           let idx = 1;
//           for (const c of doc.ticket_comment_journey) {
//             const raw = (c.ResolvedComment || '').replace(/<\/?[^>]+>/g, '').trim();
//             const date = formatToDDMMYYYY(c.ResolvedDate);
//             const key = `${date}__${raw}`;
//             if (!seen.has(key)) {
//               dynamicColumnsBatch[`Date ${idx}`] = date;
//               dynamicColumnsBatch[`Comment ${idx}`] = raw;
//               seen.add(key);
//               idx++;
//             }
//           }
//         }

//         worksheet.addRow({
//           AgentID: doc.agentInfo?.UserID?.toString() || '',
//           CallingUniqueID: doc.CallingUniqueID || '',
//           TicketNCIPDocketNo: doc.TicketNCIPDocketNo || '',
//           SupportTicketNo: doc.SupportTicketNo?.toString() || '',
//           Created: doc.Created ? new Date(doc.Created).toISOString() : '',
//           TicketReOpenDate: doc.TicketReOpenDate || '',
//           TicketStatus: doc.TicketStatus || '',
//           StatusUpdateTime: doc.StatusUpdateTime ? new Date(doc.StatusUpdateTime).toISOString() : '',
//           StateMasterName: doc.StateMasterName || '',
//           DistrictMasterName: doc.DistrictMasterName || '',
//           SubDistrictName: doc.SubDistrictName || '',
//           TicketHeadName: doc.TicketHeadName || '',
//           TicketTypeName: doc.TicketTypeName || '',
//           TicketCategoryName: doc.TicketCategoryName || '',
//           CropSeasonName: doc.CropSeasonName || '',
//           RequestYear: doc.RequestYear || '',
//           InsuranceCompany: doc.InsuranceCompany || '',
//           ApplicationNo: doc.ApplicationNo || '',
//           InsurancePolicyNo: doc.InsurancePolicyNo || '',
//           CallerContactNumber: doc.CallerContactNumber || '',
//           RequestorName: doc.RequestorName || '',
//           RequestorMobileNo: doc.RequestorMobileNo || '',
//           Relation: doc.Relation || '',
//           RelativeName: doc.RelativeName || '',
//           PolicyPremium: doc.PolicyPremium || '',
//           PolicyArea: doc.PolicyArea || '',
//           PolicyType: doc.PolicyType || '',
//           LandSurveyNumber: doc.LandSurveyNumber || '',
//           LandDivisionNumber: doc.LandDivisionNumber || '',
//           PlotStateName: doc.PlotStateName || '',
//           PlotDistrictName: doc.PlotDistrictName || '',
//           PlotVillageName: doc.PlotVillageName || '',
//           ApplicationSource: doc.ApplicationSource || '',
//           CropShare: doc.CropShare || '',
//           IFSCCode: doc.IFSCCode || '',
//           FarmerShare: doc.FarmerShare || '',
//           SowingDate: doc.SowingDate || '',
//           CreatedBY: doc.CreatedBY || '',
//           TicketDescription: doc.TicketDescription || '',
//           ...dynamicColumnsBatch
//         }).commit();
//       }

//       hasMore = docs.length === CHUNK_SIZE;
//       skip += CHUNK_SIZE;
//     }

//     const nextDate = new Date(currentDate);
//     nextDate.setDate(nextDate.getDate() + 1);
//     await processDateWithChunking(nextDate, endDate);
//   }

//   await processDateWithChunking(new Date(SPFROMDATE), new Date(SPTODATE));
//   await workbook.commit();
//   console.log(`Excel file created at: ${excelFilePath}`);

//   // ===== Create ZIP =====
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

//   // ===== Upload to GCP =====
//   const gcpService = new GCPServices();
//   const fileBuffer = await fs.promises.readFile(zipFilePath);
//   const uploadResult = await gcpService.uploadFileToGCP({
//     filePath: 'krph/reports/',
//     uploadedBy: 'KRPH',
//     file: { buffer: fileBuffer, originalname: zipFileName },
//   });
//   const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
//   if (gcpDownloadUrl) await fs.promises.unlink(zipFilePath).catch(console.error);

// //   await (ticketPayload as any).insertOrUpdateDownloadLog(
// //     SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID,
// //     SPFROMDATE, SPTODATE, zipFileName, gcpDownloadUrl, db
// //   );

//  await insertOrUpdateDownloadLog(
//     SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID,
//     SPFROMDATE, SPTODATE, zipFileName, gcpDownloadUrl, db
//   );



//   const responsePayload = {
//     data: [],
//     pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
//     downloadUrl: gcpDownloadUrl,
//     zipFileName: zipFileName
//   };

//   const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);
//   try {
//     await mailService.sendMail({
//       to: userEmail,
//       subject: 'Support Ticket History Report Download Service',
//       text: 'Support Ticket History Report',
//       html: supportTicketTemplate
//     });
//     console.log("Mail sent successfully");
//   } catch (err) {
//     console.error(`Failed to send email to ${userEmail}:`, err);
//   }

//   await redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);

//   return responsePayload;
// }


// processTicketHistory(workerData)
//   .then(result => parentPort?.postMessage({ success: true, result }))
//   .catch(err => parentPort?.postMessage({ success: false, error: err.message }));


//     async function getSupportTicketUserDetail(userID) {
//     const data = { userID };
//     const TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHBpcmVzSW4iOiIyMDI0LTEwLTA5VDE4OjA4OjA4LjAyOFoiLCJpYXQiOjE3Mjg0NjEyODguMDI4LCJpZCI6NzA5LCJ1c2VybmFtZSI6InJhamVzaF9iYWcifQ.niMU8WnJCK5SOCpNOCXMBeDrsr2ZqC96LUzQ5Z9MoBk'

//     const url = 'https://pmfby.gov.in/krphapi/FGMS/GetSupportTicketUserDetail'
//     return axios.post(url, data, {
//       headers: {
//         'Content-Type': 'application/json',
//         'Authorization': TOKEN
//       }
//     })
//       .then(response => {
//         return response.data;
//       })
//       .catch(error => {
//         console.error('Error:', error);
//         throw error;
//       });
//   };

//   async function convertStringToArray(str) {
//     return str.split(",").map(Number);
//   }

//    async function insertOrUpdateDownloadLog(
//     userId,
//     insuranceCompanyId,
//     stateId,
//     ticketHeaderId,
//     fromDate,
//     toDate,
//     zipFileName,
//     downloadUrl,
//     db
//   ) {
//     await db.collection('support_ticket_download_logs').updateOne(
//       {
//         userId,
//         insuranceCompanyId,
//         stateId,
//         ticketHeaderId,
//         fromDate,
//         toDate
//       },
//       {
//         $set: {
//           zipFileName,
//           downloadUrl,
//           createdAt: new Date()
//         }
//       },
//       { upsert: true } // Insert if not found, update if exists
//     );
//   }













// worker-ticket-history.js
// Place this file in the same worker location and run as before with workerData

const { parentPort, workerData } = require('worker_threads');
const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');
const archiver = require('archiver');
const axios = require('axios');
const { MongoClient } = require('mongodb');

const { GCPServices } = require('../GCSFileUpload');
const { generateSupportTicketEmailHTML, getCurrentFormattedDateTime } = require('../../templates/mailTemplates');
const { UtilService } = require("../../commonServices/utilService");
const { RedisWrapper } = require('../../commonServices/redisWrapper');
const { MailService } = require('../../mail/mail.service');

const redisWrapper = new RedisWrapper();
const mailService = new MailService();
let cachedDb = null;

async function connectToDatabase(uri, dbName) {
  if (cachedDb) return cachedDb;
  if (!uri) throw new Error('MongoDB URI is required');
  if (!dbName) throw new Error('Database name is required');

  const client = new MongoClient(uri, {
    maxPoolSize: 50,
    connectTimeoutMS: 10000,
  });

  await client.connect();
  cachedDb = client.db(dbName);
  console.log(`MongoDB connected to database: ${dbName}`);
  return cachedDb;
}

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

async function getSupportTicketUserDetail(userID) {
  const data = { userID };
  const TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHBpcmVzSW4iOiIyMDI0LTEwLTA5VDE4OjA4OjA4LjAyOFoiLCJpYXQiOjE3Mjg0NjEyODguMDI4LCJpZCI6NzA5LCJ1c2VybmFtZSI6InJhamVzaF9iYWcifQ.niMU8WnJCK5SOCpNOCXMBeDrsr2ZqC96LUzQ5Z9MoBk'

  const url = 'https://pmfby.gov.in/krphapi/FGMS/GetSupportTicketUserDetail';
  return axios.post(url, data, {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': TOKEN
    }
  })
    .then(response => response.data)
    .catch(error => {
      console.error('Error fetching support ticket user detail:', error && error.message ? error.message : error);
      throw error;
    });
}

async function convertStringToArray(str) {
  if (!str) return [];
  return str.split(",").map(s => Number(s.trim()));
}

async function insertOrUpdateDownloadLog(
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
    { upsert: true }
  );
}

async function processTicketHistory(ticketPayload) {
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

  const db = await connectToDatabase('mongodb://10.128.60.45:27017', 'krph_db');
  SPTicketHeaderID = Number(SPTicketHeaderID);

  if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
  if (!SPStateID) {
    console.log({ rcode: 0, rmessage: 'StateID Missing!' })
  }

  const RequestDateTime = await getCurrentFormattedDateTime();
  const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;

  const cachedData = await redisWrapper.getRedisCache(cacheKey);
  if (cachedData) {
    console.log('Using cached data');
    await db.collection('support_ticket_download_logs').updateOne(
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
  const Delta = await getSupportTicketUserDetail(SPUserID);
  const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
  const item = (responseInfo.data || {}).user && (responseInfo.data.user[0]) ? responseInfo.data.user[0] : null;
  if (!item) return { rcode: 0, rmessage: 'User details not found.' };

  const userDetail = {
    InsuranceCompanyID: item.InsuranceCompanyID ? await convertStringToArray(item.InsuranceCompanyID) : [],
    StateMasterID: item.StateMasterID ? await convertStringToArray(item.StateMasterID) : [],
    BRHeadTypeID: item.BRHeadTypeID,
    LocationTypeID: item.LocationTypeID,
  };
  const { InsuranceCompanyID, StateMasterID, LocationTypeID } = userDetail;

  let locationFilter = {};
  if (LocationTypeID === 1 && StateMasterID?.length)
    locationFilter = { FilterStateID: { $in: StateMasterID } };
  else if (LocationTypeID === 2 && item.DistrictIDs?.length)
    locationFilter = { FilterDistrictRequestorID: { $in: item.DistrictIDs } };

  const baseMatch:any = { ...locationFilter };
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
    const validStateIDs = requestedStateIDs.filter((id) => StateMasterID.includes(Number(id)));
    if (!validStateIDs.length)
      return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
    baseMatch.FilterStateID = { $in: validStateIDs.map(Number) };
  } else if (StateMasterID?.length && LocationTypeID !== 2) {
    baseMatch.FilterStateID = { $in: StateMasterID };
  }

  // Dates
  const fromDate = new Date(SPFROMDATE);
  const toDate = new Date(SPTODATE);
  if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
    return { rcode: 0, rmessage: 'Invalid date(s) provided.' };
  }

  baseMatch.InsertDateTime = { $gte: fromDate, $lte: new Date(toDate.getTime() + 86399999) }; // include end of day

  const folderPath = path.join(process.cwd(), 'downloads');
  await fs.promises.mkdir(folderPath, { recursive: true });

  const headerTypeMap = {
    1: 'Grievance',
    2: 'Information',
    4: 'Crop_Loss',
  };
  const ticketTypeName = headerTypeMap[SPTicketHeaderID] || 'General';
  const currentDateStr = new Date().toLocaleDateString('en-GB').split('/').join('_');
  const excelFileName = `Support_ticket_data_${ticketTypeName}_${currentDateStr}.xlsx`;
  const excelFilePath = path.join(folderPath, excelFileName);

  // Create workbook using streaming writer (memory friendly)
  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
  const worksheet = workbook.addWorksheet('Support Tickets');

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
  worksheet.columns = staticColumns.slice(); // start with static columns

  // Insert initial download log (empty)
  await insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, "", "", db);

  const CHUNK_SIZE = 10000;
  let skip = 0;
  let hasMore = true;
  let globalMaxCommentPairs = 0;

  // We'll incrementally add dynamic columns for comments if we see more comment pairs
  function ensureDynamicColumns(maxPairs) {
    // If we already have enough dynamic comment columns, do nothing
    const existing = worksheet.columns.length - staticColumns.length;
    const existingPairs = Math.floor(existing / 2);
    if (existingPairs >= maxPairs) return;

    const newColumns = [];
    for (let i = existingPairs + 1; i <= maxPairs; i++) {
      newColumns.push({ header: `Date ${i}`, key: `Date ${i}`, width: 20 });
      newColumns.push({ header: `Comment ${i}`, key: `Comment ${i}`, width: 80 });
    }
    // append columns to worksheet.columns (ExcelJS supports reassigning entire columns array)
    worksheet.columns = worksheet.columns.concat(newColumns);
  }

  // Prepare common pipeline parts for lookups with projections to limit data transferred
  // We'll project the required fields inside each lookup pipeline to reduce payload.
  while (hasMore) {
    const pipeline = [
      { $match: baseMatch },
      {
        $lookup: {
          from: 'SLA_KRPH_SupportTicketsHistory_Records',
          let: { ticketId: '$SupportTicketID' },
          pipeline: [
            { $match: { $expr: { $and: [{ $eq: ['$SupportTicketID', '$$ticketId'] }, { $eq: ['$TicketStatusID', 109304] }] } } },
            { $sort: { TicketHistoryID: -1 } },
            { $limit: 1 },
            { $project: { TicketHistoryID: 1, TicketStatusID: 1, _id: 0 } }
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
          pipeline: [
            { $project: { SupportTicketNo: 1, ClaimRefNo: 1, _id: 0 } }
          ]
        }
      },
      {
        $lookup: {
          from: 'csc_agent_master',
          localField: 'InsertUserID',
          foreignField: 'UserLoginID',
          as: 'agentInfo',
          pipeline: [
            { $project: { UserID: 1, UserLoginID: 1, _id: 0 } }
          ]
        }
      },
      {
        $lookup: {
          from: 'ticket_comment_journey',
          localField: 'SupportTicketNo',
          foreignField: 'SupportTicketNo',
          as: 'ticket_comment_journey',
          pipeline: [
            // only fetch fields required for exports
            { $project: { ResolvedComment: 1, ResolvedDate: 1, _id: 0 } },
            { $sort: { ResolvedDate: 1 } }
          ]
        }
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

    // If no docs fetched, break
    if (!docs || docs.length === 0) {
      hasMore = false;
      break;
    }

    // Determine how many dynamic comment pairs we need for this chunk (max across docs)
    let chunkMaxPairs = 0;
    const rowsToAdd = [];

    for (const doc of docs) {
      const dynamicColumnsBatch = {};
      if (Array.isArray(doc.ticket_comment_journey) && doc.ticket_comment_journey.length) {
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
        chunkMaxPairs = Math.max(chunkMaxPairs, Math.max(0, Math.floor(Object.keys(dynamicColumnsBatch).length / 2)));
      }

      const rowObj = {
        AgentID: (doc.agentInfo && doc.agentInfo.UserID) ? doc.agentInfo.UserID.toString() : '',
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
        TicketDescription: doc.TicketDescription || '',
        ...dynamicColumnsBatch
      };

      rowsToAdd.push(rowObj);
    } // end docs loop

    // If this chunk needs more dynamic columns than we've created, create them now
    if (chunkMaxPairs > globalMaxCommentPairs) {
      ensureDynamicColumns(chunkMaxPairs);
      globalMaxCommentPairs = chunkMaxPairs;
    }

    // Add rows to worksheet. With streaming writer we must add row then commit each row.
    for (const r of rowsToAdd) {
      const row = worksheet.addRow(r);
      row.commit();
    }

    // Prepare for next chunk
    hasMore = docs.length === CHUNK_SIZE && (skip + CHUNK_SIZE) < limit;
    skip += CHUNK_SIZE;
  } // end while

  // Finalize workbook
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
  await new Promise((resolve, reject) => {
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

  // Update download log with file info
  await insertOrUpdateDownloadLog(
    SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID,
    SPFROMDATE, SPTODATE, zipFileName, gcpDownloadUrl, db
  );

  const responsePayload = {
    data: [],
    pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
    downloadUrl: gcpDownloadUrl,
    zipFileName: zipFileName
  };

  const supportTicketTemplate = await generateSupportTicketEmailHTML('Portal User', RequestDateTime, gcpDownloadUrl);
  try {
    await mailService.sendMail({
      to: userEmail,
      subject: 'Support Ticket History Report Download Service',
      text: 'Support Ticket History Report',
      html: supportTicketTemplate
    });
    console.log("Mail sent successfully");
  } catch (err) {
    console.error(`Failed to send email to ${userEmail}:`, err);
  }

  await redisWrapper.setRedisCache(cacheKey, responsePayload, 3600);

  return responsePayload;
}

processTicketHistory(workerData)
  .then(result => parentPort?.postMessage({ success: true, result }))
  .catch(err => parentPort?.postMessage({ success: false, error: err.message }));
