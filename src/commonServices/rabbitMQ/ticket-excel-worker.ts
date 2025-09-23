// import { parentPort, workerData } from 'worker_threads';
const { parentPort, workerData } = require('worker_threads');
// import fs from 'fs';
// import path from 'path';
const fs = require('fs');
const path = require('path');
const ExcelJS = require('exceljs');
const archiver = require('archiver');
const axios = require('axios');
import { GCPServices } from '../GCSFileUpload';
import { generateSupportTicketEmailHTML, getCurrentFormattedDateTime } from '../../templates/mailTemplates'
import { UtilService } from "../../commonServices/utilService";
import { RedisWrapper } from '../../commonServices/redisWrapper';
import { MailService } from '../../mail/mail.service';
// import axios from 'axios'
import { MongoClient, Db } from 'mongodb';
const redisWrapper = new RedisWrapper()
const mailService = new MailService()
let cachedDb: Db | null = null;

async function connectToDatabase(uri: string, dbName: string): Promise<Db> {
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


async function processTicketHistory(ticketPayload: any) {
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

  const db = await connectToDatabase('mongodb://10.128.60.45:27017', 'krph_db')
    SPTicketHeaderID = Number(SPTicketHeaderID);

     if (!SPInsuranceCompanyID) return { rcode: 0, rmessage: 'InsuranceCompanyID Missing!' };
  if (!SPStateID){
    console.log({ rcode: 0, rmessage: 'StateID Missing!' })
  }

  const RequestDateTime = await getCurrentFormattedDateTime();
  const cacheKey = `ticketHist:${SPUserID}:${SPInsuranceCompanyID}:${SPStateID}:${SPTicketHeaderID}:${SPFROMDATE}:${SPTODATE}:${page}:${limit}`;

  const cachedData = await redisWrapper.getRedisCache(cacheKey) as any;
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
//   const Delta = await (ticketPayload as any).getSupportTicketUserDetail(SPUserID);
  const Delta = await getSupportTicketUserDetail(SPUserID);
  const responseInfo = await new UtilService().unGZip(Delta.responseDynamic);
  const item = (responseInfo.data as any)?.user?.[0];
  if (!item) return { rcode: 0, rmessage: 'User details not found.' };

  const userDetail = {
    InsuranceCompanyID: item.InsuranceCompanyID ? await convertStringToArray(item.InsuranceCompanyID) : [],
    StateMasterID: item.StateMasterID ? await convertStringToArray(item.StateMasterID) : [],
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
  } else if (InsuranceCompanyID?.length) {
    baseMatch.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) };
  }

  if (SPStateID && SPStateID !== '#ALL') {
    const requestedStateIDs = SPStateID.split(',').map((id) => id.trim());
    const validStateIDs = requestedStateIDs.filter((id) => StateMasterID.includes(id));
    if (!validStateIDs.length)
      return { rcode: 0, rmessage: 'Unauthorized StateID(s).' };
    baseMatch.FilterStateID = { $in: validStateIDs };
  } else if (StateMasterID?.length && LocationTypeID !== 2) {
    baseMatch.FilterStateID = { $in: StateMasterID };
  }

  const folderPath = path.join(process.cwd(), 'downloads');
  await fs.promises.mkdir(folderPath, { recursive: true });

  const headerTypeMap: Record<number, string> = {
    1: 'Grievance',
    2: 'Information',
    4: 'Crop_Loss',
  };
  const ticketTypeName = headerTypeMap[SPTicketHeaderID] || 'General';
  const currentDateStr = new Date().toLocaleDateString('en-GB').split('/').join('_');
  const fromDateStr = new Date(SPFROMDATE).toLocaleDateString('en-GB').split('/').join('_');
const toDateStr = new Date(SPTODATE).toLocaleDateString('en-GB').split('/').join('_');
  const excelFileName = `${ticketTypeName}_fromDate_${fromDateStr}_toDate_${toDateStr}.xlsx`;
const excelFilePath = path.join(folderPath, excelFileName);

  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
  const worksheet = workbook.addWorksheet('Support Tickets');

//   await (ticketPayload as any).insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, "", "", db);

   await insertOrUpdateDownloadLog(SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID, SPFROMDATE, SPTODATE, "", "", db)

  const CHUNK_SIZE = 10000;

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
  worksheet.columns = staticColumns;

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

  async function processDateWithChunking(currentDate: Date, endDate: Date) {
    if (currentDate > endDate) return;

    const startOfDay = new Date(currentDate);
    startOfDay.setUTCHours(0, 0, 0, 0);
    const endOfDay = new Date(currentDate);
    endOfDay.setUTCHours(23, 59, 59, 999);

    let skip = 0, hasMore = true;

    while (hasMore) {
      const dailyMatch = { ...baseMatch, InsertDateTime: { $gte: startOfDay, $lte: endOfDay } };
  //     const pipeline: any[] = [
  //       { $match: dailyMatch },
  //       {
  //         $lookup: {
  //           from: 'SLA_KRPH_SupportTicketsHistory_Records',
  //           let: { ticketId: '$SupportTicketID' },
  //           pipeline: [
  //             { $match: { $expr: { $and: [{ $eq: ['$SupportTicketID', '$$ticketId'] }, { $eq: ['$TicketStatusID', 109304] }] } } },
  //             { $sort: { TicketHistoryID: -1 } },
  //             { $limit: 1 }
  //           ],
  //           as: 'ticketHistory',
  //         }
  //       },
  //       {
  //         $lookup: {
  //           from: 'support_ticket_claim_intimation_report_history',
  //           localField: 'SupportTicketNo',
  //           foreignField: 'SupportTicketNo',
  //           as: 'claimInfo',
  //         }
  //       },
  //       {
  //         $lookup: {
  //           from: 'csc_agent_master',
  //           localField: 'InsertUserID',
  //           foreignField: 'UserLoginID',
  //           as: 'agentInfo',
  //         }
  //       },
  //       {
  //         $lookup: {
  //           from: 'ticket_comment_journey',
  //           localField: 'SupportTicketNo',
  //           foreignField: 'SupportTicketNo',
  //           as: 'ticket_comment_journey',
  //         },
  //       },

  //        {
  //   $addFields: {
  //     ticketHistory: { $arrayElemAt: ['$ticketHistory', 0] },
  //     claimInfo: { $arrayElemAt: ['$claimInfo', 0] },
  //     agentInfo: { $arrayElemAt: ['$agentInfo', 0] },
  //     ticket_comment_journey: { $ifNull: ['$ticket_comment_journey', []] }
  //   }
  // },
        
  //       { $skip: skip },
  //       { $limit: CHUNK_SIZE },
  //     ];

console.log("test")
/*   const pipeline: any[] = [
  { $match: dailyMatch },

  // Lookup ticket history (latest record with TicketStatusID 109304)
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

  // Lookup claim info
  {
    $lookup: {
      from: 'support_ticket_claim_intimation_report_history',
      localField: 'SupportTicketNo',
      foreignField: 'SupportTicketNo',
      as: 'claimInfo',
    }
  },

  // Lookup agent info
  {
    $lookup: {
      from: 'csc_agent_master',
      localField: 'InsertUserID',
      foreignField: 'UserLoginID',
      as: 'agentInfo',
    }
  },

  // Lookup ticket comments journey
  {
    $lookup: {
      from: 'ticket_comment_journey',
      localField: 'SupportTicketNo',
      foreignField: 'SupportTicketNo',
      as: 'ticket_comment_journey',
    },
  },

  // Flatten arrays and handle nulls
  {
    $addFields: {
      ticketHistory: { $arrayElemAt: ['$ticketHistory', 0] },
      claimInfo: { $arrayElemAt: ['$claimInfo', 0] },
      agentInfo: { $arrayElemAt: ['$agentInfo', 0] },
      ticket_comment_journey: { $ifNull: ['$ticket_comment_journey', []] }
    }
  },

  {
    $group: {
      _id: '$SupportTicketNo',
      doc: { $first: '$$ROOT' } 
    }
  },

  {
    $replaceRoot: { newRoot: '$doc' }
  },

  { $skip: skip },
  { $limit: CHUNK_SIZE },
]; */


const pipeline: any[] = [
  { $match: dailyMatch },

  // Lookup latest ticket history
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

  // Lookup claim info
  {
    $lookup: {
      from: 'support_ticket_claim_intimation_report_history',
      localField: 'SupportTicketNo',
      foreignField: 'SupportTicketNo',
      as: 'claimInfo',
    }
  },

  // Lookup agent info
  {
    $lookup: {
      from: 'csc_agent_master',
      localField: 'InsertUserID',
      foreignField: 'UserLoginID',
      as: 'agentInfo',
    }
  },

  // Lookup ticket comments journey
  {
    $lookup: {
      from: 'ticket_comment_journey',
      localField: 'SupportTicketNo',
      foreignField: 'SupportTicketNo',
      as: 'ticket_comment_journey',
    }
  },

  // Flatten single-item arrays, keep comments as array
  {
    $addFields: {
      ticketHistory: { $arrayElemAt: ['$ticketHistory', 0] },
      claimInfo: { $arrayElemAt: ['$claimInfo', 0] },
      agentInfo: { $arrayElemAt: ['$agentInfo', 0] },
      ticket_comment_journey: { $ifNull: ['$ticket_comment_journey', []] } // keep all comments
    }
  },

  // Ensure one document per ticket
  {
    $group: {
      _id: '$SupportTicketNo',
      doc: { $first: '$$ROOT' }
    }
  },

  {
    $replaceRoot: { newRoot: '$doc' }
  },

  { $skip: skip },
  { $limit: CHUNK_SIZE },
];

      const cursor = db.collection('SLA_KRPH_SupportTickets_Records').aggregate(pipeline, { allowDiskUse: true });

    //   console.log(cursor[0])
      const docs = await cursor.toArray();

      for (const doc of docs) {
        const dynamicColumnsBatch: any = {};
        if (Array.isArray(doc.ticket_comment_journey)) {
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
        }

        worksheet.addRow({
          AgentID: doc.agentInfo?.UserID?.toString() || '',
          CallingUniqueID: doc.CallingUniqueID || '',
          TicketNCIPDocketNo: doc.TicketNCIPDocketNo || '',
          SupportTicketNo: doc.SupportTicketNo?.toString() || '',
          Created: doc.Created ?  formatDate(doc.Created) : '',
          TicketReOpenDate: doc.TicketReOpenDate || '',
          TicketStatus: doc.TicketStatus || '',
          StatusUpdateTime: doc.StatusUpdateTime ?  formatDate(doc.StatusUpdateTime): '',
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
        }).commit();
      }

      hasMore = docs.length === CHUNK_SIZE;
      skip += CHUNK_SIZE;
    }

    const nextDate = new Date(currentDate);
    nextDate.setDate(nextDate.getDate() + 1);
    await processDateWithChunking(nextDate, endDate);
  }

  await processDateWithChunking(new Date(SPFROMDATE), new Date(SPTODATE));
  await workbook.commit();
  console.log(`Excel file created at: ${excelFilePath}`);

  // ===== Create ZIP =====
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

  // ===== Upload to GCP =====
  const gcpService = new GCPServices();
  const fileBuffer = await fs.promises.readFile(zipFilePath);
  const uploadResult = await gcpService.uploadFileToGCP({
    filePath: 'krph/reports/',
    uploadedBy: 'KRPH',
    file: { buffer: fileBuffer, originalname: zipFileName },
  });
  const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
  if (gcpDownloadUrl) await fs.promises.unlink(zipFilePath).catch(console.error);

//   await (ticketPayload as any).insertOrUpdateDownloadLog(
//     SPUserID, SPInsuranceCompanyID, SPStateID, SPTicketHeaderID,
//     SPFROMDATE, SPTODATE, zipFileName, gcpDownloadUrl, db
//   );

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


    async function getSupportTicketUserDetail(userID) {
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

  async function convertStringToArray(str) {
    return str.split(",").map(Number);
  }


  function formatDate(inputDate: string | Date): string {
  const date = new Date(inputDate);

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');

  return `${year}-${month}-${day}:${hours}:${minutes}`;
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
      { upsert: true } // Insert if not found, update if exists
    );
  }