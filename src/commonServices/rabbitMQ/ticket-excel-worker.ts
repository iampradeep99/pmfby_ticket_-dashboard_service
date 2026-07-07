const { parentPort, workerData } = require("worker_threads")
const fs = require("fs")
const path = require("path")
const ExcelJS = require("exceljs")
const archiver = require("archiver")
const axios = require("axios")
import * as FormData from "form-data"
import { generateSupportTicketEmailHTML, getCurrentFormattedDateTime } from "../../templates/mailTemplates"
import { UtilService } from "../../commonServices/utilService"
import { RedisWrapper } from "../../commonServices/redisWrapper"
import { MailService } from "../../mail/mail.service"
import config from "../../environment/config"
import * as moment from "moment"
import { MongoClient, type Db } from "mongodb"

const redisWrapper = new RedisWrapper()
const mailService = new MailService()
let cachedDb: Db | null = null

const CHUNK_SIZE = 10000
const MAX_JOURNEY_INDICES = 3
const DB_URI = config.mongodb
const DB_NAME = "krph_db"
const API_TOKEN =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHBpcmVzSW4iOiIyMDI0LTEwLTA5VDE4OjA4OjA4LjAyOFoiLCJpYXQiOjE3Mjg0NjEyODguMDI4LCJpZCI6NzA5LCJ1c2VybmFtZSI6InJhamVzaF9iYWcifQ.niMU8WnJCK5SOCpNOCXMBeDrsr2ZqC96LUzQ5Z9MoBk"
const API_URL = "https://pmfby.gov.in/krphapi/FGMS/GetSupportTicketUserDetail"
const TICKET_COLLECTION = "SLA_Ticket_listing"
const TICKET_HISTORY_COLLECTION = "SLA_KRPH_SupportTicketsHistory_Records"
const DOWNLOAD_LOG_COLLECTION = "support_ticket_download_logs"

const TICKET_TYPE_MAP: Record<number, string> = {
  1: "Grievance",
  2: "Information",
  4: "Crop_Loss",
}

const STATIC_COLUMNS = [
  { header: "Agent ID", key: "AgentID", width: 20 },
  { header: "Calling ID", key: "CallingUniqueID", width: 25 },
  { header: "Ticket NCIP Docket No", key: "TicketNCIPDocketNo", width: 25 },
  { header: "Ticket No", key: "SupportTicketNo", width: 30 },
  { header: "Creation Date", key: "Created", width: 25 },
  { header: "Ticket ReOpen Date", key: "TicketReOpenDate", width: 25 },
  { header: "Ticket Status", key: "TicketStatus", width: 20 },
  { header: "Status Update Time", key: "StatusUpdateTime", width: 25 },
  { header: "State", key: "StateMasterName", width: 20 },
  { header: "District", key: "DistrictMasterName", width: 20 },
  { header: "Sub District", key: "SubDistrictName", width: 20 },
  { header: "Ticket Head", key: "TicketHeadName", width: 20 },
  { header: "Ticket Type", key: "TicketTypeName", width: 20 },
  { header: "Ticket Category", key: "TicketCategoryName", width: 20 },
  { header: "Crop Season", key: "CropSeasonName", width: 20 },
  { header: "Request Year", key: "RequestYear", width: 20 },
  { header: "Insurance Company", key: "InsuranceCompany", width: 30 },
  { header: "Application No", key: "ApplicationNo", width: 30 },
  { header: "Policy No", key: "InsurancePolicyNo", width: 30 },
  { header: "Caller Contact No", key: "CallerContactNumber", width: 20 },
  { header: "Requestor Name", key: "RequestorName", width: 20 },
  { header: "Requestor Mobile No", key: "RequestorMobileNo", width: 20 },
  { header: "Relation", key: "Relation", width: 20 },
  { header: "Relative Name", key: "RelativeName", width: 20 },
  { header: "Policy Premium", key: "PolicyPremium", width: 20 },
  { header: "Policy Area", key: "PolicyArea", width: 20 },
  { header: "Policy Type", key: "PolicyType", width: 20 },
  { header: "Land Survey No", key: "LandSurveyNumber", width: 20 },
  { header: "Land Division No", key: "LandDivisionNumber", width: 20 },
  { header: "Crop Name", key: "CropName", width: 50 },
  { header: "Application Crop Name", key: "ApplicationCropName", width: 50 },
  { header: "Plot State", key: "PlotStateName", width: 20 },
  { header: "Plot District", key: "PlotDistrictName", width: 20 },
  { header: "Plot Village", key: "PlotVillageName", width: 20 },
  { header: "Application Source", key: "ApplicationSource", width: 20 },
  { header: "Other Sub Category", key: "CropCategoryOthers", width: 20 },
  { header: "Crop Stage Type", key: "CropStage", width: 20 },
  { header: "Loss At", key: "LossDate", width: 20 },
  { header: "Intimation", key: "OnTimeIntimationFlag", width: 20 },
  { header: "Harvest Date", key: "PostHarvestDate", width: 20 },
  { header: "Crop Share", key: "CropShare", width: 20 },
  { header: "IFSC Code", key: "IFSCCode", width: 20 },
  { header: "Farmer Share", key: "FarmerShare", width: 20 },
  { header: "Sowing Date", key: "SowingDate", width: 20 },
  { header: "Created By", key: "CreatedBY", width: 20 },
  { header: "Ticket Description", key: "TicketDescription", width: 50 },
]

async function connectToDatabase(uri: string, dbName: string): Promise<Db> {
  if (cachedDb) return cachedDb
  if (!uri) throw new Error("MongoDB URI is required")
  if (!dbName) throw new Error("Database name is required")

  const client = new MongoClient(uri, {
    maxPoolSize: 50,
    connectTimeoutMS: 10000,
  })

  await client.connect()
  cachedDb = client.db(dbName)
  console.log(`MongoDB connected to database: ${dbName}`)
  return cachedDb
}

function getMongoHostForLog(uri: string): string {
  try {
    const parsed = new URL(uri)
    return parsed.host
  } catch {
    return "unknown"
  }
}

function findUrlInObject(value: any): string {
  if (!value) return ""

  if (typeof value === "string") {
    return /^https?:\/\//i.test(value) ? value : ""
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const found = findUrlInObject(item)
      if (found) return found
    }
    return ""
  }

  if (typeof value === "object") {
    const preferredKeys = ["gcsUrl", "GCSUrl", "url", "URL", "downloadUrl", "DownloadURL", "fileUrl", "fileURL"]
    for (const key of preferredKeys) {
      const found = findUrlInObject(value[key])
      if (found) return found
    }

    for (const key of Object.keys(value)) {
      const found = findUrlInObject(value[key])
      if (found) return found
    }
  }

  return ""
}

async function getSupportTicketUserDetail(userID: string): Promise<any> {
  const data = { userID }

  return axios
    .post(API_URL, data, {
      headers: {
        "Content-Type": "application/json",
        Authorization: API_TOKEN,
      },
    })
    .then((response) => response.data)
    .catch((error) => {
      console.error("Error fetching user detail:", error)
      throw error
    })
}

async function convertStringToArray(str: string): Promise<number[]> {
  return str
    .split(",")
    .map((id) => Number(String(id).trim()))
    .filter((id) => Number.isFinite(id))
}

function hasAllAccess(value: any): boolean {
  if (Array.isArray(value)) {
    return value.some((item) => String(item).trim().toUpperCase() === "#ALL")
  }

  return String(value || "")
    .split(",")
    .some((item) => item.trim().toUpperCase() === "#ALL")
}

function formatToDDMMYYYY(dateString: string | null | undefined): string {
  if (!dateString) return ""
  const date = new Date(dateString)
  if (isNaN(date.getTime())) return ""
  const day = String(date.getDate()).padStart(2, "0")
  const month = String(date.getMonth() + 1).padStart(2, "0")
  const year = date.getFullYear()
  const hours = String(date.getHours()).padStart(2, "0")
  const minutes = String(date.getMinutes()).padStart(2, "0")
  return `${day}-${month}-${year} ${hours}:${minutes}`
}

function formatDate(inputDate: string | Date): string {
  const date = new Date(inputDate)
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, "0")
  const day = String(date.getDate()).padStart(2, "0")
  const hours = String(date.getHours()).padStart(2, "0")
  const minutes = String(date.getMinutes()).padStart(2, "0")
  return `${year}-${month}-${day}:${hours}:${minutes}`
}

function parsePayloadDate(dateValue: string, endOfDay = false): Date | null {
  if (!dateValue) return null

  const value = String(dateValue).trim()
  const isoMatch = value.match(/^(\d{4})-(\d{2})-(\d{2})$/)
  const slashMatch = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/)

  let year: number
  let month: number
  let day: number

  if (isoMatch) {
    year = Number(isoMatch[1])
    month = Number(isoMatch[2])
    day = Number(isoMatch[3])
  } else if (slashMatch) {
    year = Number(slashMatch[3])
    month = Number(slashMatch[1])
    day = Number(slashMatch[2])
  } else {
    const parsed = new Date(value)
    if (isNaN(parsed.getTime())) return null
    year = parsed.getUTCFullYear()
    month = parsed.getUTCMonth() + 1
    day = parsed.getUTCDate()
  }

  const time = endOfDay ? "23:59:59.999Z" : "00:00:00.000Z"
  const monthText = String(month).padStart(2, "0")
  const dayText = String(day).padStart(2, "0")
  const parsedDate = new Date(`${year}-${monthText}-${dayText}T${time}`)

  return isNaN(parsedDate.getTime()) ? null : parsedDate
}

function stripHtmlTags(text: string | null | undefined): string {
  if (!text) return "NA"
  return text.replace(/<\/?[^>]+(>|$)/g, "").trim()
}

function buildDynamicColumns(maxIndices: number = MAX_JOURNEY_INDICES): any[] {
  const dynamicColumns = []
  for (let i = 0; i < maxIndices; i++) {
    const suffix = i === 0 ? "" : `${i}`
    dynamicColumns.push(
      { header: `In-Progress Date${suffix}`, key: `In-Progress Date${suffix}`, width: 25 },
      { header: `In-Progress Comment${suffix}`, key: `In-Progress Comment${suffix}`, width: 50 },
      { header: `Resolved Date${suffix}`, key: `Resolved Date${suffix}`, width: 25 },
      { header: `Resolved Comment${suffix}`, key: `Resolved Comment${suffix}`, width: 50 },
      { header: `Re-Open Date${suffix}`, key: `Re-Open Date${suffix}`, width: 25 },
      { header: `Re-Open Comment${suffix}`, key: `Re-Open Comment${suffix}`, width: 50 },
    )
  }
  return dynamicColumns
}

function buildAggregationPipeline(baseMatch: any, skip: number, fetchLimit: number = CHUNK_SIZE): any[] {
  const pipeline = [
    { $match: baseMatch },
    { $sort: { InsertDateTime: -1 } },
    {
      $group: {
        _id: "$SupportTicketNo",
        doc: { $first: "$$ROOT" },
      },
    },
    { $replaceRoot: { newRoot: "$doc" } },
    { $skip: skip },
    { $limit: fetchLimit },
    {
      $lookup: {
        from: "support_ticket_claim_intimation_report_history",
        let: { ticketNo: "$SupportTicketNo" },
        pipeline: [
          { $match: { $expr: { $eq: ["$SupportTicketNo", "$$ticketNo"] } } },
          { $sort: { InsertDateTime: -1 } },
          { $limit: 1 },
        ],
        as: "claimInfo",
      },
    },
    { $unwind: { path: "$claimInfo", preserveNullAndEmptyArrays: true } },
    {
      $lookup: {
        from: "csc_agent_master",
        let: { userLoginId: "$InsertUserID" },
        pipeline: [{ $match: { $expr: { $eq: ["$UserLoginID", "$$userLoginId"] } } }, { $limit: 1 }],
        as: "agentInfo",
      },
    },
    { $unwind: { path: "$agentInfo", preserveNullAndEmptyArrays: true } },
    {
      $lookup: {
        from: "ticket_comment_journey",
        localField: "SupportTicketNo",
        foreignField: "SupportTicketNo",
        as: "ticket_comment_journey",
        pipeline: [
          { $sort: { CreatedDate: -1 } },
          {
            $group: {
              _id: "$ResolvedComment",
              unique_comments: { $first: "$$ROOT" },
            },
          },
          { $replaceRoot: { newRoot: "$unique_comments" } },
        ],
      },
    },
    {
      $project: {
        SupportTicketID: 1,
        SupportTicketNo: 1,
        InsertUserID: 1,
        Created: 1,
        StatusUpdateTime: 1,
        TicketStatusID: 1,
        TicketStatus: 1,
        ApplicationNo: 1,
        InsurancePolicyNo: 1,
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
        CropSeasonName: 1,
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
        InsertDateTime: 1,
        Sos: 1,
        TicketNCIPDocketNo: 1,
        TicketDescription: 1,
        CallingUniqueID: 1,
        TicketTypeName: 1,
        TicketReOpenDate: 1,
        InsuranceCompany: 1,
        SchemeName: 1,
        claimInfo: 1,
        agentInfo: 1,
        ticket_comment_journey: 1,
      },
    },
  ]

  console.log(JSON.stringify(pipeline), "workerExportPipeline")
  return pipeline
}

function buildTicketCommentJourney(source: any, maxIndices: number = MAX_JOURNEY_INDICES): any[] {
  const journey: any[] = []

  for (let i = 0; i < maxIndices; i++) {
    const suffix = i === 0 ? "" : `${i}`

    const inprogressDate = source[`Inprogress${suffix}Date`]
      ? source[`Inprogress${suffix}Date`].$date || source[`Inprogress${suffix}Date`]
      : null
    const inprogressComment = source[`Inprogress${suffix}Comment`] || null
    const resolvedDate = source[`Resolved${suffix}Date`]
      ? source[`Resolved${suffix}Date`].$date || source[`Resolved${suffix}Date`]
      : null
    const resolvedComment = source[`Resolved${suffix}Comment`] || null
    const reopenDate = source[`ReOpen${suffix}Date`]
      ? source[`ReOpen${suffix}Date`].$date || source[`ReOpen${suffix}Date`]
      : null
    const reopenComment = source[`ReOpen${suffix}Comment`] || null

    if (inprogressDate || inprogressComment || resolvedDate || resolvedComment || reopenDate || reopenComment) {
      journey.push({
        InprogressDate: inprogressDate,
        InprogressComment: inprogressComment,
        ResolvedDate: resolvedDate,
        ResolvedComment: resolvedComment,
        ReOpenDate: reopenDate,
        ReOpenComment: reopenComment,
      })
    }
  }

  return journey
}

function extractJourneyFromDoc(doc: any): any[] {
  if (Array.isArray(doc.ticket_comment_journey) && doc.ticket_comment_journey.length > 0) {
    return buildTicketCommentJourney(doc.ticket_comment_journey[0])
  } else {
    return buildTicketCommentJourney(doc)
  }
}

function buildDynamicColumnsBatch(journey: any[], maxIndices: number = MAX_JOURNEY_INDICES): Record<string, any> {
  const dynamicColumnsBatch: Record<string, any> = {}

  for (let idx = 0; idx < maxIndices; idx++) {
    const commentObj = journey[idx] || {}
    const suffix = idx === 0 ? "" : `${idx}`

    const inProgressDate = commentObj.InprogressDate ? formatToDDMMYYYY(commentObj.InprogressDate) : "NA"
    const inProgressComment = stripHtmlTags(commentObj.InprogressComment)
    dynamicColumnsBatch[`In-Progress Date${suffix}`] = inProgressDate
    dynamicColumnsBatch[`In-Progress Comment${suffix}`] = inProgressComment

    const resolvedDate = commentObj.ResolvedDate ? formatToDDMMYYYY(commentObj.ResolvedDate) : "NA"
    const resolvedComment = stripHtmlTags(commentObj.ResolvedComment)
    dynamicColumnsBatch[`Resolved Date${suffix}`] = resolvedDate
    dynamicColumnsBatch[`Resolved Comment${suffix}`] = resolvedComment

    const reOpenDate = commentObj.ReOpenDate ? formatToDDMMYYYY(commentObj.ReOpenDate) : "NA"
    const reOpenComment = stripHtmlTags(commentObj.ReOpenComment)
    dynamicColumnsBatch[`Re-Open Date${suffix}`] = reOpenDate
    dynamicColumnsBatch[`Re-Open Comment${suffix}`] = reOpenComment
  }

  if (journey.length < maxIndices) {
    for (let i = journey.length; i < maxIndices; i++) {
      const suffix = i === 0 ? "" : `${i}`
      dynamicColumnsBatch[`In-Progress Date${suffix}`] = "NA"
      dynamicColumnsBatch[`In-Progress Comment${suffix}`] = "NA"
      dynamicColumnsBatch[`Resolved Date${suffix}`] = "NA"
      dynamicColumnsBatch[`Resolved Comment${suffix}`] = "NA"
      dynamicColumnsBatch[`Re-Open Date${suffix}`] = "NA"
      dynamicColumnsBatch[`Re-Open Comment${suffix}`] = "NA"
    }
  }

  return dynamicColumnsBatch
}

function mapDocumentToRow(doc: any, dynamicColumnsBatch: Record<string, any>): Record<string, any> {
  return {
    AgentID: doc.agentInfo?.UserID?.toString() || "",
    CallingUniqueID: doc.CallingUniqueID || "",
    TicketNCIPDocketNo: doc.TicketNCIPDocketNo || "",
    SupportTicketNo: doc.SupportTicketNo?.toString() || "",
    Created: doc.Created ? formatDate(doc.Created) : "",
    TicketReOpenDate: doc.TicketReOpenDate || "",
    TicketStatus: doc.TicketStatus || "",
    StatusUpdateTime: doc.StatusUpdateTime ? formatDate(doc.StatusUpdateTime) : "",
    StateMasterName: doc.StateMasterName || "",
    DistrictMasterName: doc.DistrictMasterName || "",
    SubDistrictName: doc.SubDistrictName || "",
    TicketHeadName: doc.TicketHeadName || "",
    TicketTypeName: doc.TicketTypeName || "",
    TicketCategoryName: doc.TicketCategoryName || "",
    CropSeasonName: doc.CropSeasonName || "",
    RequestYear: doc.RequestYear || "",
    InsuranceCompany: doc.InsuranceCompany || "",
    ApplicationNo: doc.ApplicationNo || "",
    InsurancePolicyNo: doc.InsurancePolicyNo || "",
    CallerContactNumber: doc.CallerContactNumber || "",
    RequestorName: doc.RequestorName || "",
    RequestorMobileNo: doc.RequestorMobileNo || "",
    Relation: doc.Relation || "",
    RelativeName: doc.RelativeName || "",
    PolicyPremium: doc.PolicyPremium || "",
    PolicyArea: doc.PolicyArea || "",
    PolicyType: doc.PolicyType || "",
    LandSurveyNumber: doc.LandSurveyNumber || "",
    LandDivisionNumber: doc.LandDivisionNumber || "",
    CropName: doc.CropName || "",
    ApplicationCropName: doc.ApplicationCropName || "",
    PlotStateName: doc.PlotStateName || "",
    PlotDistrictName: doc.PlotDistrictName || "",
    PlotVillageName: doc.PlotVillageName || "",
    ApplicationSource: doc.ApplicationSource || "",
    CropCategoryOthers: doc.CropCategoryOthers || "",
    CropStage: doc.CropStage || "",
    LossDate: doc.LossDate ? formatDate(doc.LossDate) : "",
    OnTimeIntimationFlag: doc.OnTimeIntimationFlag || "",
    PostHarvestDate: doc.PostHarvestDate ? formatDate(doc.PostHarvestDate) : "",
    CropShare: doc.CropShare || "",
    IFSCCode: doc.IFSCCode || "",
    FarmerShare: doc.FarmerShare || "",
    SowingDate: doc.SowingDate || "",
    CreatedBY: doc.CreatedBY || "",
    TicketDescription: doc.TicketDescription || "",
    ...dynamicColumnsBatch,
  }
}

async function processChunk(
  db: Db,
  collectionName: string,
  baseMatch: any,
  skip: number,
  worksheet: any,
): Promise<{ hasMore: boolean; processedCount: number }> {
  const FETCH_LIMIT = CHUNK_SIZE + 1
  const pipeline = buildAggregationPipeline(baseMatch, skip, FETCH_LIMIT)
  const cursor = db.collection(collectionName).aggregate(pipeline, { allowDiskUse: true })
  const docs = await cursor.toArray()

  const docsToProcess = docs.slice(0, CHUNK_SIZE)
  const hasMoreRecords = docs.length > CHUNK_SIZE

  for (const doc of docsToProcess) {
    const journey = extractJourneyFromDoc(doc)
    const dynamicColumnsBatch = buildDynamicColumnsBatch(journey)
    const row = mapDocumentToRow(doc, dynamicColumnsBatch)
    worksheet.addRow(row).commit()
  }

  return {
    hasMore: hasMoreRecords,
    processedCount: docsToProcess.length,
  }
}

async function processDateRange(db: Db, baseMatch: any, startDate: Date, endDate: Date, worksheet: any): Promise<void> {
  let currentDate = moment(startDate)

  while (currentDate.isSameOrBefore(endDate, "day")) {
    const dayStart = currentDate.clone().utc().startOf("day").toDate()
    const dayEnd = currentDate.clone().utc().endOf("day").toDate()

    const dailyMatch = { ...baseMatch, InsertDateTime: { $gte: dayStart, $lte: dayEnd } }

    let skip = 0
    let hasMore = true

    while (hasMore) {
      const { hasMore: hasMoreResults, processedCount } = await processChunk(
        db,
        TICKET_COLLECTION,
        dailyMatch,
        skip,
        worksheet,
      )
      hasMore = hasMoreResults
      skip += CHUNK_SIZE

      console.log(`Processed ${processedCount} documents for ${currentDate.format("YYYY-MM-DD")}`)
    }

    currentDate = currentDate.add(1, "day")
  }
}

async function processMatchedRange(db: Db, collectionName: string, baseMatch: any, worksheet: any): Promise<number> {
  let skip = 0
  let hasMore = true
  let totalProcessed = 0

  while (hasMore) {
    const { hasMore: hasMoreResults, processedCount } = await processChunk(
      db,
      collectionName,
      baseMatch,
      skip,
      worksheet,
    )
    hasMore = hasMoreResults
    skip += CHUNK_SIZE
    totalProcessed += processedCount

    console.log(`Processed ${processedCount} documents for export`)
  }

  return totalProcessed
}

async function createExcelFile(
  folderPath: string,
  fileName: string,
  columns: any[],
): Promise<{ workbook: any; filePath: string; worksheet: any }> {
  const filePath = path.join(folderPath, fileName)
  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: filePath })
  const worksheet = workbook.addWorksheet("Support Tickets")
  worksheet.columns = columns
  return { workbook, filePath, worksheet }
}

async function createZipFile(excelFilePath: string, zipFileName: string, folderPath: string): Promise<string> {
  const zipFilePath = path.join(folderPath, zipFileName)
  const output = fs.createWriteStream(zipFilePath)
  const archive = archiver("zip", { zlib: { level: 9 } })

  archive.pipe(output)
  archive.file(excelFilePath, { name: path.basename(excelFilePath) })
  await archive.finalize()

  return new Promise((resolve, reject) => {
    output.on("close", () => resolve(zipFilePath))
    output.on("error", reject)
  })
}

async function uploadToGCP(zipFilePath: string, zipFileName: string): Promise<string> {
  const formData = new FormData()
  formData.append("filePath", "krph/farmer/tickets-pdf/")
  formData.append("uploadedBy", "KRPH")
  formData.append("documents", fs.createReadStream(zipFilePath), zipFileName)

  const response = await axios.post("https://pmfby.gov.in/krphapi/FGMS/GCPFileUploadForCDR", formData, {
    headers: { ...formData.getHeaders() },
    maxBodyLength: Infinity,
  })

  const uploadResult = response.data
  console.log("[TicketExcelWorker] CDN upload response:", JSON.stringify(uploadResult))

  const downloadUrl = findUrlInObject(uploadResult)
  if (!downloadUrl) {
    throw new Error("CDN upload completed but no download URL was returned.")
  }

  return downloadUrl
}

async function insertOrUpdateDownloadLog(
  userId: string,
  insuranceCompanyId: string,
  stateId: string,
  ticketHeaderId: number,
  fromDate: string,
  toDate: string,
  zipFileName: string,
  downloadUrl: string,
  db: Db,
  status = "Completed",
  statusMessage = "Report generated successfully",
): Promise<void> {
  await db.collection(DOWNLOAD_LOG_COLLECTION).updateOne(
    { userId, insuranceCompanyId, stateId, ticketHeaderId, fromDate, toDate },
    {
      $set: {
        zipFileName,
        downloadUrl,
        status,
        statusMessage,
        updatedAt: new Date(),
        ...(status === "Completed" ? { completedAt: new Date() } : {}),
        ...(status === "Failed" ? { failedAt: new Date() } : {}),
        ...(status === "Processing" ? { startedAt: new Date() } : {}),
      },
      $setOnInsert: { createdAt: new Date() },
    },
    { upsert: true },
  )
}

async function updateDownloadStatusFromPayload(
  ticketPayload: any,
  status: string,
  statusMessage: string,
  zipFileName = "",
  downloadUrl = "",
): Promise<void> {
  const db = await connectToDatabase(DB_URI, DB_NAME)

  await insertOrUpdateDownloadLog(
    ticketPayload?.SPUserID,
    ticketPayload?.SPInsuranceCompanyID,
    ticketPayload?.SPStateID,
    Number(ticketPayload?.SPTicketHeaderID),
    ticketPayload?.SPFROMDATE,
    ticketPayload?.SPTODATE,
    zipFileName,
    downloadUrl,
    db,
    status,
    statusMessage,
  )
}

async function sendDownloadEmail(userEmail: string, gcpDownloadUrl: string): Promise<void> {
  const requestDateTime = await getCurrentFormattedDateTime()
  const supportTicketTemplate = await generateSupportTicketEmailHTML("Portal User", requestDateTime, gcpDownloadUrl)

  try {
    await mailService.sendMail({
      to: userEmail,
      subject: "Support Ticket History Report Download Service",
      text: "Support Ticket History Report",
      html: supportTicketTemplate,
    })
    console.log("Mail sent successfully")
  } catch (err) {
    console.error(`Failed to send email to ${userEmail}:`, err)
  }
}

async function validateUserPermissions(
  userDetail: any,
  spInsuranceCompanyId: string,
  spStateId: string,
): Promise<{ valid: boolean; baseMatch?: any; error?: string }> {
  const { InsuranceCompanyID, StateMasterID, LocationTypeID, DistrictIDs, rawInsuranceCompanyID, rawStateMasterID } =
    userDetail
  const hasAllInsuranceAccess = hasAllAccess(rawInsuranceCompanyID)
  const hasAllStateAccess = hasAllAccess(rawStateMasterID)

  let locationFilter: any = {}
  if (LocationTypeID === 1 && StateMasterID?.length) {
    locationFilter = { FilterStateID: { $in: StateMasterID.map(Number) } }
  } else if (LocationTypeID === 2 && DistrictIDs?.length) {
    locationFilter = { FilterDistrictRequestorID: { $in: DistrictIDs } }
  }

  const baseMatch: any = { ...locationFilter }

  if (spInsuranceCompanyId && spInsuranceCompanyId !== "#ALL") {
    const requestedInsuranceIDs = spInsuranceCompanyId.split(",").map((id) => Number(id.trim()))
    const allowedInsuranceIDs = InsuranceCompanyID.map(Number)
    const validInsuranceIDs = hasAllInsuranceAccess
      ? requestedInsuranceIDs
      : requestedInsuranceIDs.filter((id) => allowedInsuranceIDs.includes(id))
    if (!validInsuranceIDs.length) {
      return { valid: false, error: "Unauthorized InsuranceCompanyID(s)." }
    }
    baseMatch.InsuranceCompanyID = { $in: validInsuranceIDs }
  } else if (!hasAllInsuranceAccess && InsuranceCompanyID?.length) {
    baseMatch.InsuranceCompanyID = { $in: InsuranceCompanyID.map(Number) }
  }

  if (spStateId && spStateId !== "#ALL") {
    const requestedStateIDs = spStateId.split(",").map((id) => Number(id.trim()))
    const validStateIDs = hasAllStateAccess
      ? requestedStateIDs
      : requestedStateIDs.filter((id) => StateMasterID.map(Number).includes(id))
    if (!validStateIDs.length) {
      return { valid: false, error: "Unauthorized StateID(s)." }
    }
    baseMatch.FilterStateID = { $in: validStateIDs }
  } else if (!hasAllStateAccess && StateMasterID?.length && LocationTypeID !== 2) {
    baseMatch.FilterStateID = { $in: StateMasterID.map(Number) }
  }

  return { valid: true, baseMatch }
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
  } = ticketPayload

  const db = await connectToDatabase(DB_URI, DB_NAME)
  SPTicketHeaderID = Number(SPTicketHeaderID)

  if (!SPInsuranceCompanyID) {
    await updateDownloadStatusFromPayload(ticketPayload, "Failed", "InsuranceCompanyID Missing!")
    return { rcode: 0, rmessage: "InsuranceCompanyID Missing!" }
  }
  if (!SPStateID) {
    await updateDownloadStatusFromPayload(ticketPayload, "Failed", "StateID Missing!")
    return { rcode: 0, rmessage: "StateID Missing!" }
  }

  await updateDownloadStatusFromPayload(ticketPayload, "Processing", "Report generation started")

  const folderPath = path.join(process.cwd(), "downloads")
  await fs.promises.mkdir(folderPath, { recursive: true })

  const delta = await getSupportTicketUserDetail(SPUserID)
  const responseInfo = await new UtilService().unGZip(delta.responseDynamic)
  const item = (responseInfo.data as any)?.user?.[0]

  if (!item) {
    await updateDownloadStatusFromPayload(ticketPayload, "Failed", "User details not found.")
    return { rcode: 0, rmessage: "User details not found." }
  }

  const userDetail = {
    InsuranceCompanyID: item.InsuranceCompanyID ? await convertStringToArray(item.InsuranceCompanyID) : [],
    StateMasterID: item.StateMasterID ? await convertStringToArray(item.StateMasterID) : [],
    BRHeadTypeID: item.BRHeadTypeID,
    LocationTypeID: Number(item.LocationTypeID),
    DistrictIDs: item.DistrictIDs || [],
    rawInsuranceCompanyID: item.InsuranceCompanyID,
    rawStateMasterID: item.StateMasterID,
  }

  const permissionCheck = await validateUserPermissions(userDetail, SPInsuranceCompanyID, SPStateID)
  if (!permissionCheck.valid) {
    await updateDownloadStatusFromPayload(ticketPayload, "Failed", permissionCheck.error || "Unauthorized request.")
    return { rcode: 0, rmessage: permissionCheck.error }
  }

  const baseMatch = permissionCheck.baseMatch
  if (SPTicketHeaderID && SPTicketHeaderID !== 0) {
    baseMatch.TicketHeaderID = SPTicketHeaderID
  }

  if (SPFROMDATE || SPTODATE) {
    baseMatch.InsertDateTime = {}
    const fromDate = parsePayloadDate(SPFROMDATE)
    const toDate = parsePayloadDate(SPTODATE, true)
    if (SPFROMDATE && !fromDate) {
      await updateDownloadStatusFromPayload(ticketPayload, "Failed", "Invalid from date format.")
      return { rcode: 0, rmessage: "Invalid from date format." }
    }
    if (SPTODATE && !toDate) {
      await updateDownloadStatusFromPayload(ticketPayload, "Failed", "Invalid to date format.")
      return { rcode: 0, rmessage: "Invalid to date format." }
    }
    if (fromDate) baseMatch.InsertDateTime.$gte = fromDate
    if (toDate) baseMatch.InsertDateTime.$lte = toDate
  }

  console.log(
    "[TicketExcelWorker] Runtime:",
    JSON.stringify({
      nodeEnv: process.env.NODE_ENV || "uat",
      mongoHost: getMongoHostForLog(DB_URI),
      dbName: DB_NAME,
      collection: TICKET_COLLECTION,
      payload: {
        SPFROMDATE,
        SPTODATE,
        SPInsuranceCompanyID,
        SPStateID,
        SPTicketHeaderID,
        SPUserID,
      },
      baseMatch,
    }),
  )

  const matchedCount = await db.collection(TICKET_COLLECTION).countDocuments(baseMatch)
  console.log(`[TicketExcelWorker] Matched documents in ${TICKET_COLLECTION}: ${matchedCount}`)

  if (matchedCount === 0) {
    await updateDownloadStatusFromPayload(ticketPayload, "Failed", "No records found for download filters.")
    return { rcode: 0, rmessage: "No records found for download filters." }
  }

  const ticketTypeName = TICKET_TYPE_MAP[SPTicketHeaderID] || "General"
  const currentDateStr = new Date().toLocaleDateString("en-GB").split("/").join("_")
  const fromDateForName = parsePayloadDate(SPFROMDATE) || new Date(SPFROMDATE)
  const toDateForName = parsePayloadDate(SPTODATE) || new Date(SPTODATE)
  const fromDateStr = fromDateForName.toLocaleDateString("en-GB").split("/").join("_")
  const toDateStr = toDateForName.toLocaleDateString("en-GB").split("/").join("_")
  // const excelFileName = `${ticketTypeName}_fromDate_${fromDateStr}_toDate_${toDateStr}.xlsx`
const excelFileName =
  `${ticketTypeName}_fromDate_${fromDateStr}_toDate_${toDateStr}_${Date.now()}.xlsx`;

  const allColumns = [...STATIC_COLUMNS, ...buildDynamicColumns()]
  const { workbook, filePath: excelFilePath, worksheet } = await createExcelFile(folderPath, excelFileName, allColumns)

  await insertOrUpdateDownloadLog(
    SPUserID,
    SPInsuranceCompanyID,
    SPStateID,
    SPTicketHeaderID,
    SPFROMDATE,
    SPTODATE,
    "",
    "",
    db,
    "Processing",
    "Report generation started",
  )

  const processedCount = await processMatchedRange(db, TICKET_COLLECTION, baseMatch, worksheet)
  console.log(`Total export documents processed from ${TICKET_COLLECTION}: ${processedCount}`)

  await workbook.commit()
  console.log(`Excel file created at: ${excelFilePath}`)

  const zipFileName = excelFileName.replace(".xlsx", ".zip")
  const zipFilePath = await createZipFile(excelFilePath, zipFileName, folderPath)
  await fs.promises.unlink(excelFilePath).catch(console.error)

  const gcpDownloadUrl = await uploadToGCP(zipFilePath, zipFileName)
  if (gcpDownloadUrl) {
    await fs.promises.unlink(zipFilePath).catch(console.error)
  }

  if (!gcpDownloadUrl) {
    await updateDownloadStatusFromPayload(ticketPayload, "Failed", "CDN upload did not return a download URL.", zipFileName)
    return { rcode: 0, rmessage: "CDN upload did not return a download URL.", zipFileName }
  }

  await insertOrUpdateDownloadLog(
    SPUserID,
    SPInsuranceCompanyID,
    SPStateID,
    SPTicketHeaderID,
    SPFROMDATE,
    SPTODATE,
    zipFileName,
    gcpDownloadUrl,
    db,
    "Completed",
    "Report generated successfully",
  )

  const responsePayload = {
    data: [],
    pagination: { total: 0, page, limit, totalPages: 0, hasNextPage: false, hasPrevPage: false },
    downloadUrl: gcpDownloadUrl,
    zipFileName: zipFileName,
  }

  await sendDownloadEmail(userEmail, gcpDownloadUrl)

  return responsePayload
}

processTicketHistory(workerData)
  .then((result) => parentPort?.postMessage({ success: true, result }))
  .catch(async (err) => {
    await updateDownloadStatusFromPayload(workerData, "Failed", err.message || "Report generation failed").catch(console.error)
    parentPort?.postMessage({ success: false, error: err.message })
  })
