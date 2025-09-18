// ticket-excel-worker.js
import { parentPort, workerData } from 'worker_threads';
import path from 'path';
import fs from 'fs';
import ExcelJS from 'exceljs';
import archiver from 'archiver';
import { MongoClient } from 'mongodb';
import { GCPServices } from '../GCSFileUpload'; // adjust path
import { MailService } from '../../mail/mail.service'; // adjust path
import { RedisWrapper } from '../redisWrapper'; // your Redis wrapper

// Destructure payload
const payload = workerData;
console.log("inside the worker")
async function main() {
  const {
    SPFROMDATE,
    SPTODATE,
    SPInsuranceCompanyID,
    SPStateID,
    SPTicketHeaderID,
    SPUserID,
    userEmail
  } = payload;

  // === DB & Redis setup ===
  const mongoClient = new MongoClient(process.env.MONGO_URI || 'mongodb://10.128.60.45:27017');
  await mongoClient.connect();
  const db = mongoClient.db('krph_db'); // replace with your DB name
  const redisWrapper = new RedisWrapper();

  const folderPath = path.join(process.cwd(), 'downloads');
  await fs.promises.mkdir(folderPath, { recursive: true });

  const excelFileName = `Support_ticket_${SPUserID}_${Date.now()}.xlsx`;
  const excelFilePath = path.join(folderPath, excelFileName);

  // === Excel streaming workbook ===
  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: excelFilePath });
  const worksheet = workbook.addWorksheet('Support Tickets');

  // === Static columns ===
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

  const CHUNK_SIZE = 1000;
  let skip = 0;
  let hasMore = true;
  let dynamicColumns = new Set<string>();
  let headersSet = false;

  function formatToDDMMYYYY(dateString: string) {
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

  while (hasMore) {
    const docs = await db.collection('SLA_KRPH_SupportTickets_Records')
      .find({
        InsertDateTime: { $gte: new Date(SPFROMDATE), $lte: new Date(SPTODATE) },
        InsuranceCompanyID: { $in: SPInsuranceCompanyID.split(',').map(Number) },
        FilterStateID: { $in: SPStateID.split(',').map(Number) },
        TicketHeaderID: SPTicketHeaderID,
      })
      .skip(skip)
      .limit(CHUNK_SIZE)
      .toArray();

    for (const doc of docs) {
      // Handle dynamic comments
      if (Array.isArray(doc.ticket_comment_journey)) {
        const seen = new Set();
        let idx = 1;
        for (const c of doc.ticket_comment_journey) {
          const raw = (c.ResolvedComment || '').replace(/<\/?[^>]+>/g, '').trim();
          const date = formatToDDMMYYYY(c.ResolvedDate);
          const key = `${date}__${raw}`;
          if (!seen.has(key)) {
            doc[`Date ${idx}`] = date;
            doc[`Comment ${idx}`] = raw;
            dynamicColumns.add(`Date ${idx}`);
            dynamicColumns.add(`Comment ${idx}`);
            seen.add(key);
            idx++;
          }
        }
        delete doc.ticket_comment_journey;
      }

      // Add dynamic columns to worksheet
      if (!headersSet) {
        const dynamicDefs = Array.from(dynamicColumns).map(col => ({ header: col, key: col, width: 40 }));
        worksheet.columns = [...staticColumns, ...dynamicDefs];
        headersSet = true;
      }

      worksheet.addRow({
        AgentID: doc.agentInfo?.UserID?.toString() || '',
        CallingUniqueID: doc.CallingUniqueID || '',
        TicketNCIPDocketNo: doc.TicketNCIPDocketNo || '',
        SupportTicketNo: doc.SupportTicketNo?.toString() || '',
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
        ...Object.fromEntries(Object.entries(doc).filter(([k]) => k.startsWith('Date') || k.startsWith('Comment')))
      }).commit();
    }

    hasMore = docs.length === CHUNK_SIZE;
    skip += CHUNK_SIZE;
  }

  await workbook.commit();

  // === Create ZIP ===
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
  await fs.promises.unlink(excelFilePath).catch(() => {});

  // === Upload to GCP ===
  const gcpService = new GCPServices();
  const fileBuffer = await fs.promises.readFile(zipFilePath);
  const uploadResult = await gcpService.uploadFileToGCP({
    filePath: 'krph/reports/',
    uploadedBy: 'KRPH',
    file: { buffer: fileBuffer, originalname: zipFileName },
  });
  const gcpDownloadUrl = uploadResult?.file?.[0]?.gcsUrl || '';
  if (gcpDownloadUrl) await fs.promises.unlink(zipFilePath).catch(() => {});

  // === Send Email ===
  const mailService = new MailService();
  await mailService.sendMail({
    to: userEmail,
    subject: 'Support Ticket History Report Download',
    text: 'Your report is ready',
    html: `<p>Hi, <br>Your report is ready. <a href="${gcpDownloadUrl}">Download here</a></p>`,
  });

  // === Redis cache ===
  await redisWrapper.setRedisCache(`ticketHist:${SPUserID}`, {
    downloadUrl: gcpDownloadUrl,
    zipFileName,
  }, 3600);

  await mongoClient.close();
  parentPort?.postMessage({ success: true, downloadUrl: gcpDownloadUrl });
}

main().catch(err => parentPort?.postMessage({ success: false, error: err.message }));
