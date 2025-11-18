import { Injectable, Inject } from '@nestjs/common';
import { Db } from 'mongodb';
import { parse } from 'csv-parse/sync'; // ✅ Use sync version
const Logger = require("../commonServices/logger");
import * as moment from "moment";
import { Document } from 'mongodb';



@Injectable()
export class BillingDashboardService {
      private logger: InstanceType<typeof Logger>;
    
  constructor(@Inject('MONGO_DB') private readonly db: Db) {
     this.logger = new Logger('sla.log'); 
        this.logger.info('SlaReportService initialized');
  }

 

  async ImportInboundRecordService(file: Express.Multer.File, yearMonth: string) {
  setImmediate(async () => {
    try {
      const result = await this.uploadInboundCallsFileService(file, yearMonth);
      console.log('Background processing completed:', result);
    } catch (err) {
      console.error('Background job error:', err);
    }
  });

  return { message: 'File upload started. Processing in the background.', code: 1 };
}
async uploadInboundCallsFileService(file: Express.Multer.File, yearMonth: string) {
  try {
    const db = this.db;
    if (!db) return { data: {}, message: { msg: "DB not available", code: -100 } };

    if (!file || !file.buffer || !file.buffer.length)
      return { data: {}, message: { msg: "Empty or missing file", code: 0 } };

    if (typeof file.size !== "number" || file.size <= 0)
      return { data: {}, message: { msg: "Invalid file", code: 0 } };

    if (file.size > 50 * 1024 * 1024)
      return { data: {}, message: { msg: "File too large (>50MB)", code: -5 } };

    if (typeof yearMonth !== "string" || !/^\d{4}-(0[1-9]|1[0-2])$/.test(yearMonth))
      return { data: {}, message: { msg: "Invalid year-month format", code: -4 } };

    const [yr, mn] = yearMonth.split("-").map(Number);
    const startDate = moment({ year: yr, month: mn - 1, day: 1 }).startOf("month");
    const endDate = startDate.clone().endOf("month");

    if (!startDate.isValid() || !endDate.isValid())
      return { data: {}, message: { msg: "Invalid date range", code: -4 } };

    let records;
    try {
      const raw = file.buffer.toString("utf-8");
      if (!raw.trim()) return { data: {}, message: { msg: "CSV has no content", code: 0 } };
      records = parse(raw, { columns: true, skip_empty_lines: true, trim: true });
    } catch {
      return { data: {}, message: { msg: "Invalid CSV format", code: -2 } };
    }

    if (!Array.isArray(records) || records.length === 0)
      return { data: {}, message: { msg: "CSV has no data", code: 0 } };

    const filtered = records.filter(r => {
      if (!r || typeof r !== "object") return false;
      const dt = r.Call_Start_Time ? moment(r.Call_Start_Time) : null;
      return dt?.isValid() && dt.isBetween(startDate, endDate, undefined, "[]");
    });

    if (!filtered.length)
      return { data: {}, message: { msg: "No records for selected month", code: 0 } };

    const validRecords = filtered.filter(r => typeof r.Unique_ID === "string" && r.Unique_ID.trim() !== "");

    if (!validRecords.length)
      return { data: {}, message: { msg: "No valid records (Unique_ID missing)", code: 0 } };

    const collectionName = "inbound_records";

    const exists = await db.listCollections({ name: collectionName }).toArray().catch(() => []);
    if (!Array.isArray(exists)) return { data: {}, message: { msg: "DB error", code: -100 } };
    if (!exists.length) {
      try { await db.createCollection(collectionName); }
      catch (e) {
        const alreadyExists = (e as any)?.codeName === "NamespaceExists";
        if (!alreadyExists) return { data: {}, message: { msg: "Failed creating collection", code: -100 } };
      }
    }

    const col = db.collection(collectionName);
    const uniqueIds = validRecords.map(r => r.Unique_ID.trim());
    const existing = await col
      .find({ Unique_ID: { $in: uniqueIds } })
      .project({ Unique_ID: 1 })
      .toArray()
      .catch(() => null);

    if (!Array.isArray(existing))
      return { data: {}, message: { msg: "DB query error", code: -100 } };

    const existingSet = new Set(existing.map(e => e.Unique_ID));

    const now = moment().toISOString();
    const newRecords = validRecords
      .filter(r => !existingSet.has(r.Unique_ID.trim()))
      .map(r => ({ ...r, Unique_ID: r.Unique_ID.trim(), InsertedDateTime: now }));

    let insertedCount = 0;
    if (newRecords.length) {
      try {
        const result = await col.insertMany(newRecords, { ordered: false });
        insertedCount = typeof result.insertedCount === "number" ? result.insertedCount : newRecords.length;
      } catch (e) {
        if (e?.writeErrors?.length) {
          insertedCount = newRecords.length - e.writeErrors.length;
        } else {
          return { data: {}, message: { msg: "Insertion error", code: -99 } };
        }
      }
    }

    const duplicateCount = validRecords.length - newRecords.length;

    return {
      data: {
        insertedCount,
        duplicateCount,
        totalFiltered: filtered.length,
        month: yearMonth,
        collection: collectionName
      },
      message: {
        msg: `${insertedCount} inserted, ${duplicateCount} duplicates skipped.`,
        code: insertedCount > 0 ? 1 : 0
      }
    };
  } catch (err) {
    this.logger?.error?.("Unexpected Error", err);
    return { data: {}, message: { msg: "Unexpected error", code: -99 } };
  }
}








// async FetchInboundRecordService(payload: any) {
//   try {
//     const { month_year, page = 1, limit = 20 } = payload; 
//     const db = this.db;

//     if (!month_year || !/^\d{4}-\d{2}$/.test(month_year)) {
//       throw new Error('Invalid month_year. Expected format: YYYY-MM');
//     }

//     const pageNumber = Number(page);
//     const pageSize = Number(limit);

//     if (isNaN(pageNumber) || pageNumber < 1) throw new Error('Invalid page number');
//     if (isNaN(pageSize) || pageSize < 1 || pageSize > 100) throw new Error('Invalid limit (max 100)');

//     const startDate = moment(month_year, 'YYYY-MM').startOf('month').format('YYYY-MM-DD HH:mm:ss');
//     const endDate = moment(month_year, 'YYYY-MM').endOf('month').format('YYYY-MM-DD HH:mm:ss');

//     const collection = db.collection('SLA_Inbound_Calls');

//     const records: Document[] = await collection.aggregate([
//       {
//         $match: {
//           Call_Start_Time: { $gte: startDate, $lte: endDate }
//         }
//       },
//       { 
//         $project: {
//           _id: 1,
//           Farmer_Number: 1,
//           Campaign_Name: 1,
//           Status: 1,
//           Agent_ID: 1,
//           Agent_Name: 1,
//           Call_Start_Time: 1,
//           Call_End_Time: 1,
//           Customer_Call_Sec: 1,
//           Agent_TalkTime: 1,
//           Recording_Path: 1
//         }
//       },
//       { $skip: (pageNumber - 1) * pageSize },
//       { $limit: pageSize }
//     ]).toArray();

//     const totalRecords = await collection.countDocuments({
//       Call_Start_Time: { $gte: startDate, $lte: endDate }
//     });

//     return {
//       data: {
//         // records,
//         // pagination: {
//         //   page: pageNumber,
//         //   limit: pageSize,
//         //   totalRecords,
//         //   totalPages: Math.ceil(totalRecords / pageSize)
//         // }
//       },
//       message: {
//         msg: 'Records fetched successfully',
//         code: 1
//       }
//     };

//   } catch (err) {
//     return {
//       data: {},
//       message: {
//         msg: err.message || 'Something went wrong',
//         code: 0
//       }
//     };
//   }
// }



/* async FetchInboundRecordService(payload: any) {
  try {
    const { year_month, page = 1, limit = 50 } = payload;
    const db = this.db;

    if (!year_month || !/^\d{4}-\d{2}$/.test(year_month)) throw new Error('Invalid year_month. Expected format: YYYY-MM');

    const pageNumber = Number(page);
    const pageSize = Math.min(Number(limit), 50);

    if (isNaN(pageNumber) || pageNumber < 1) throw new Error('Invalid page number');
    if (isNaN(pageSize) || pageSize < 1) throw new Error('Invalid limit (max 50)');

    const [year, month] = year_month.split("-").map(Number);
    if (month < 1 || month > 12) throw new Error('Invalid month in year_month');

    const fromDateStr = `${year}-${month.toString().padStart(2, '0')}-01 00:00:00`;
    const toDateObj = new Date(year, month, 0);
    const toDateStr = `${year}-${month.toString().padStart(2, '0')}-${toDateObj.getDate().toString().padStart(2, '0')} 23:59:59`;

    const collection = db.collection('SLA_Inbound_Calls');
    if (!collection) throw new Error('Database collection not found');
    let pipeline = [
      {
        $match: {
          Call_Start_Time: { $gte: fromDateStr, $lte: toDateStr }
        }
      },
      {
        $addFields: {
          Call_Start_Time: { $dateFromString: { dateString: "$Call_Start_Time", format: "%Y-%m-%d %H:%M:%S" } }
        }
      },
      {
        $facet: {
          paginatedResults: [
            { $skip: (pageNumber - 1) * pageSize },
            { $limit: pageSize },
            {
              $project: {
                _id: 1,
                Farmer_Number: 1,
                Campaign_Name: 1,
                Status: 1,
                Agent_ID: 1,
                Agent_Name: 1,
                Call_Start_Time: 1,
                Call_End_Time: 1,
                Customer_Call_Sec: 1,
                Agent_TalkTime: 1,
                Recording_Path: 1
              }
            }
          ],
          totalCount: [{ $count: "count" }]
        }
      }
    ];

    console.log(JSON.stringify(pipeline));
    const result = await collection.aggregate(pipeline).toArray();

    const records = result[0]?.paginatedResults || [];
    const totalRecords = result[0]?.totalCount[0]?.count || 0;
    const datad = { records, pagination: { page: pageNumber, limit: pageSize, totalRecords, totalPages: Math.ceil(totalRecords / pageSize) } };
    return {
      data: datad,
      message: {
        msg: 'Records fetched successfully',
        code: 1
      }
    };
  } catch (err: any) {
    return {
      data: {},
      message: {
        msg: err.message || 'Something went wrong',
        code: 0
      }
    };
  }
} */

async FetchInboundRecordService(payload: any) {
  try {
    const { year_month, page = 1, limit = 50 } = payload;
    const db = this.db;

    if (!year_month || !/^\d{4}-\d{2}$/.test(year_month)) {
      throw new Error('Invalid year_month. Expected format: YYYY-MM');
    }

    const pageNumber = Number(page);
    const pageSize = Math.min(Number(limit), 50);

    if (isNaN(pageNumber) || pageNumber < 1) throw new Error('Invalid page number');
    if (isNaN(pageSize) || pageSize < 1) throw new Error('Invalid limit (max 50)');

    const [year, month] = year_month.split("-").map(Number);
    if (month < 1 || month > 12) throw new Error('Invalid month in year_month');

    // Build date range strings
    const fromDateStr = `${year}-${month.toString().padStart(2, '0')}-01 00:00:00`;
    const toDateObj = new Date(year, month, 0);
    const toDateStr = `${year}-${month.toString().padStart(2, '0')}-${toDateObj.getDate().toString().padStart(2, '0')} 23:59:59`;

    const collection = db.collection('SLA_Inbound_Calls');
    if (!collection) throw new Error('Database collection not found');

    // ---------------------------------------
    // 1) FAST DATA PIPELINE (Pagination Only)
    // ---------------------------------------
    const dataPipeline = [
      {
        $match: {
          Call_Start_Time: { $gte: fromDateStr, $lte: toDateStr }
        }
      },
      { $sort: { Call_Start_Time: 1 } },
      { $skip: (pageNumber - 1) * pageSize },
      { $limit: pageSize },
    //   {
    //     $project: {
    //       _id: 1,
    //       Farmer_Number: 1,
    //       Campaign_Name: 1,
    //       Status: 1,
    //       Agent_ID: 1,
    //       Agent_Name: 1,
    //       Call_Start_Time: 1,
    //       Call_End_Time: 1,
    //       Customer_Call_Sec: 1,
    //       Agent_TalkTime: 1,
    //       Recording_Path: 1
    //     }
    //   }
    {
  $project: {
    _id: 0
  }
}

    ];

    // ---------------------------------------
    // 2) FAST COUNT PIPELINE (No facet)
    // ---------------------------------------
    const countPipeline = [
      {
        $match: {
          Call_Start_Time: { $gte: fromDateStr, $lte: toDateStr }
        }
      },
      { $count: "count" }
    ];

    // Run both queries
    const [records, countResult] = await Promise.all([
      collection.aggregate(dataPipeline).toArray(),
      collection.aggregate(countPipeline).toArray()
    ]);

    const totalRecords = countResult[0]?.count || 0;

    const responsePayload = {
      records,
      pagination: {
        page: pageNumber,
        limit: pageSize,
        totalRecords,
        totalPages: Math.ceil(totalRecords / pageSize)
      }
    };

    return {
      data: responsePayload,
      message: {
        msg: 'Records fetched successfully',
        code: 1
      }
    };
  } catch (err: any) {
    return {
      data: {},
      message: {
        msg: err.message || 'Something went wrong',
        code: 0
      }
    };
  }
}



}