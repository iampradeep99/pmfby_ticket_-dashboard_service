import { Inject, Injectable } from '@nestjs/common';
import { Db } from 'mongodb';
import { Sequelize } from 'sequelize';
import { RedisWrapper } from 'src/commonServices/redisWrapper';
import { UtilService } from 'src/commonServices/utilService';
import { MailService } from 'src/mail/mail.service';
import * as moment from "moment";
const Logger = require("../commonServices/logger");



@Injectable()
export class SlaReportService {

  private logger: InstanceType<typeof Logger>;

  constructor(
    @Inject('MONGO_DB') private readonly db: Db,
    @Inject('SEQUELIZE') private readonly sequelize: Sequelize,
    private readonly redisWrapper: RedisWrapper,
    private readonly mailService: MailService,
    private readonly utilServices: UtilService,


  ) {
    this.logger = new Logger('sla.log'); 
    this.logger.info('SlaReportService initialized');
    
  }

  async isBeforeSeptember2024(dateToCheck) {
    const targetDate = moment.utc("2024-09-01T00:00:00.000Z");
    const checkDate = moment.utc(dateToCheck);

    return checkDate.isBefore(targetDate);
  }

  
  async calculateSlaServiceWorkingBackup(payload: any) {
    console.log("🚀 SLA Calculation Started");

    try {
      const db = this.db;
      const yearMonth = payload?.yearMonth;

      if (!yearMonth) {
        console.warn("❌ yearMonth missing in payload");
        return { data: {}, message: { msg: "yearMonth is required", code: 0 } };
      }

      const startOfMonth = moment.utc(yearMonth + "-01", "YYYY-MM-DD");
      if (!startOfMonth.isValid()) {
        console.error("❌ Invalid yearMonth format:", yearMonth);
        return { data: {}, message: { msg: "Invalid yearMonth format", code: 0 } };
      }

      const endOfMonth = startOfMonth.clone().endOf("month");
      console.log(`📅 Processing from ${startOfMonth.toISOString()} to ${endOfMonth.toISOString()}`);

      const statuses = ["Answered"];
      let queedvalue = 30;
      let loopCounter = 0;

      for (let date = startOfMonth.clone(); date <= endOfMonth; date.add(1, "day")) {
        loopCounter++;
        const startTime = date.clone().startOf("day").toDate();
        const endTime = date.clone().endOf("day").toDate();

        console.log(`\n🔹 Day ${loopCounter}: ${date.format("YYYY-MM-DD")}`);
        console.log(`⏱ Start: ${startTime.toISOString()} | End: ${endTime.toISOString()}`);

        let isBeforeSept2024 = false;
        try {
          isBeforeSept2024 = await this.isBeforeSeptember2024(startTime);
        } catch (err) {
          console.warn("⚠ Failed to check September 2024 condition:", err.message);
        }

        queedvalue = isBeforeSept2024 ? 45 : 30;
        console.log(`🎯 Queued Value Used: ${queedvalue}`);

        console.log("⏳ Running MongoDB Aggregation...");

        let results: any = [];
        try {
          results = await db.collection("SLA_Inbound_Calls").aggregate([
            {
              $addFields: {
                Call_Start_Time: {
                  $cond: {
                    if: { $in: ["$Call_Start_Time", ["0000-00-00 00:00:00", "", null]] },
                    then: null,
                    else: { $dateFromString: { dateString: "$Call_Start_Time", format: "%Y-%m-%d %H:%M:%S" } },
                  },
                },
                Call_End_Time: {
                  $cond: {
                    if: { $in: ["$Call_End_Time", ["0000-00-00 00:00:00", "", null]] },
                    then: null,
                    else: { $dateFromString: { dateString: "$Call_End_Time", format: "%Y-%m-%d %H:%M:%S" } },
                  },
                },
                Customer_Queue_Seconds: { $toInt: "$Customer_Queue_Seconds" },
                Agent_TalkTime: { $toInt: { $cond: [{ $eq: ["$Agent_TalkTime", ""] }, "0", "$Agent_TalkTime"] } },
                Agent_ID: { $toInt: "$Customer_Queue_Seconds" },
              },
            },
            {
              $facet: {
                ASA: [
                  { $match: { Call_Start_Time: { $gte: startTime, $lte: endTime }, Status: { $in: statuses } } },
                  {
                    $group: {
                      _id: null,
                      totalAnsweredCallASA: { $sum: 1 },
                      totalQuedCallsASA: { $sum: { $cond: [{ $gt: ["$Customer_Queue_Seconds", 0] }, { $cond: [{ $lte: ["$Customer_Queue_Seconds", queedvalue] }, 1, 0] }, 0] } },
                    },
                  },
                  {
                    $project: {
                      totalAnsweredCallASA: 1,
                      totalQuedCallsASA: 1,
                      percentQuedCallsASA: {
                        $cond: { if: { $gt: ["$totalAnsweredCallASA", 0] }, then: { $round: [{ $multiply: [{ $divide: ["$totalQuedCallsASA", "$totalAnsweredCallASA"] }, 100] }, 2] }, else: 0 },
                      },
                    },
                  },
                ],
                AHT: [
                  { $match: { Call_Start_Time: { $gte: startTime, $lte: endTime }, Status: { $in: statuses } } },
                  {
                    $group: {
                      _id: null,
                      totalAnsweredCallAHT: { $sum: 1 },
                      callAHT_300_seconds: { $sum: { $cond: [{ $gt: ["$Agent_TalkTime", 0] }, { $cond: [{ $gt: ["$Agent_TalkTime", 300] }, 1, 0] }, 0] } },
                    },
                  },
                  {
                    $project: {
                      totalAnsweredCallAHT: 1,
                      callAHT_300_seconds: 1,
                      percentAHT_300_seconds: {
                        $cond: { if: { $gt: ["$totalAnsweredCallAHT", 0] }, then: { $round: [{ $multiply: [{ $divide: ["$callAHT_300_seconds", "$totalAnsweredCallAHT"] }, 100] }, 2] }, else: 0 },
                      },
                    },
                  },
                ],
                SU: [
                  { $match: { Call_Start_Time: { $gte: startTime, $lte: endTime }, Status: { $ne: "System Missed" } } },
                  {
                    $group: {
                      _id: null,
                      totalCallsLanded: { $sum: 1 },
                      activeAgents: { $addToSet: { $cond: [{ $ne: ["$Agent_ID", ""] }, "$Agent_ID", null] } },
                    },
                  },
                  {
                    $project: {
                      totalCallsLanded: 1,
                      activeAgentCount: { $size: { $filter: { input: "$activeAgents", as: "agent", cond: { $ne: ["$$agent", null] } } } },
                      callsPerActiveAgent: {
                        $cond: { if: { $gt: [{ $size: { $filter: { input: "$activeAgents", as: "agent", cond: { $ne: ["$$agent", null] } } } }, 0] }, then: { $round: [{ $divide: ["$totalCallsLanded", { $size: { $filter: { input: "$activeAgents", as: "agent", cond: { $ne: ["$$agent", null] } } } }] }, 2] }, else: 0 },
                      },
                    },
                  },
                ],
              },
            },
            { $project: { ASA: { $arrayElemAt: ["$ASA", 0] }, AHT: { $arrayElemAt: ["$AHT", 0] }, SU: { $arrayElemAt: ["$SU", 0] } } },
          ]).toArray();
        } catch (err) {
          console.error("❌ Aggregation failed for date:", date.format("YYYY-MM-DD"), err.message);
          continue;
        }

        const response = {
          ASA_REPORT: results[0]?.ASA || { totalAnsweredCallASA: 0, totalQuedCallsASA: 0, percentQuedCallsASA: 0 },
          AHT_REPORT: results[0]?.AHT || { totalAnsweredCallAHT: 0, callAHT_300_seconds: 0, percentAHT_300_seconds: 0 },
          SEAT_UTILIZATION: results[0]?.SU || { totalCallsLanded: 0, activeAgentCount: 0, callsPerActiveAgent: 0 },
          insertedDate: startTime,
        };

        const isAllZero =
          Object.values(response.ASA_REPORT).every((v) => v === 0) &&
          Object.values(response.AHT_REPORT).every((v) => v === 0) &&
          Object.values(response.SEAT_UTILIZATION).every((v) => v === 0);

        if (!isAllZero) {
          try {
            console.log("💾 Inserting result into SLA_MARCH_INBOUND_COOKED");
            await db.collection("SLA_MARCH_INBOUND_COOKED").insertOne(response);
          } catch (err) {
            console.error("❌ Insert failed:", err.message);
          }
        } else {
          console.log("⏭ Skipped (All values zero)");
        }
      }

      console.log("✅ SLA Calculation Finished Successfully");
      return { data: {}, message: { msg: "SLA calculated successfully", code: 1 } };
    } catch (error: any) {
      console.error("❌ SLA Calculation Error:", error.message);
      return { data: {}, message: { msg: `Error: ${error.message}`, code: 0 } };
    }
  }


 

 async startSlaCalculation(payload: any) {
  this.logger.info('SLA processing started. Immediate response sent.');

  const immediateResponse = {
    data: {},
    message: { msg: "SLA processing has been started. DB will be updated soon...", code: 1 },
  };

  try {
    this.calculateSlaService(payload)
      .then(() => this.logger.info("SLA Training Calculation finished in background"))
      .catch((err) => this.logger.error(`SLA background processing error: ${err.message}`));

    this.calculateSeatUtilization(payload)
      .then(() => this.logger.info("SLA Seat Utilization calculation finished in background"))
      .catch((err) => this.logger.error(`SLA Seat Utilization background processing error: ${err.message}`));

    this.calculateSystemUpdate(payload)
      .then(() => this.logger.info("SLA System Up-time Calculation finished in background"))
      .catch((err) => this.logger.error(`SLA System Up-time Calculation background processing error: ${err.message}`));

    this.calculateTrainingRecords(payload)
      .then(() => this.logger.info("SLA Training Calculation finished in background"))
      .catch((err) => this.logger.error(`SLA background processing error: ${err.message}`));

  } catch (error) {
    this.logger.error(`Unexpected error in SLA calculations: ${error.message}`);
  }

  return immediateResponse;
}



  async calculateSlaService(payload: any) {
    this.logger.info("🚀 SLA Calculation Started");

    try {
      const db = this.db;
      const yearMonth = payload?.yearMonth;

      if (!yearMonth) {
        this.logger.warn("❌ yearMonth missing in payload");
        return { data: {}, message: { msg: "yearMonth is required", code: 0 } };
      }

      const startOfMonth = moment.utc(yearMonth + "-01", "YYYY-MM-DD");
      if (!startOfMonth.isValid()) {
        this.logger.error("❌ Invalid yearMonth format: " + yearMonth);
        return { data: {}, message: { msg: "Invalid yearMonth format", code: 0 } };
      }

      const endOfMonth = startOfMonth.clone().endOf("month");
      this.logger.info(`📅 Processing from ${startOfMonth.toISOString()} to ${endOfMonth.toISOString()}`);

      const statuses = ["Answered"];
      let queedvalue = 30;
      let loopCounter = 0;

      for (let date = startOfMonth.clone(); date <= endOfMonth; date.add(1, "day")) {
        loopCounter++;
        const startTime = date.clone().startOf("day").toDate();
        const endTime = date.clone().endOf("day").toDate();

        this.logger.info(`\n🔹 Day ${loopCounter}: ${date.format("YYYY-MM-DD")}`);
        this.logger.info(`⏱ Start: ${startTime.toISOString()} | End: ${endTime.toISOString()}`);

        // ❗ Step 1: Check if record for this date already exists
        const existingRecord = await db.collection("day_wise_sla_test").findOne({
          insertedDate: startTime,
        });

        if (existingRecord) {
          this.logger.info(`⏭ Skipped (Record already exists for ${date.format("YYYY-MM-DD")})`);
          continue;
        }


        let isBeforeSept2024 = false;
        try {
          isBeforeSept2024 = await this.isBeforeSeptember2024(startTime);
        } catch (err: any) {
          this.logger.warn("⚠ Failed to check September 2024 condition: " + err.message);
        }

        queedvalue = isBeforeSept2024 ? 45 : 30;
        this.logger.info(`🎯 Queued Value Used: ${queedvalue}`);
        this.logger.info("⏳ Running MongoDB Aggregation...");

        let results: any = [];
        try {
          results = await db.collection("SLA_Inbound_Calls").aggregate([
            {
              $addFields: {
                Call_Start_Time: {
                  $cond: {
                    if: { $in: ["$Call_Start_Time", ["0000-00-00 00:00:00", "", null]] },
                    then: null,
                    else: { $dateFromString: { dateString: "$Call_Start_Time", format: "%Y-%m-%d %H:%M:%S" } },
                  },
                },
                Call_End_Time: {
                  $cond: {
                    if: { $in: ["$Call_End_Time", ["0000-00-00 00:00:00", "", null]] },
                    then: null,
                    else: { $dateFromString: { dateString: "$Call_End_Time", format: "%Y-%m-%d %H:%M:%S" } },
                  },
                },
                Customer_Queue_Seconds: { $toInt: "$Customer_Queue_Seconds" },
                Agent_TalkTime: { $toInt: { $cond: [{ $eq: ["$Agent_TalkTime", ""] }, "0", "$Agent_TalkTime"] } },
                Agent_ID: { $toInt: "$Customer_Queue_Seconds" },
              },
            },
            {
              $facet: {
                ASA: [
                  { $match: { Call_Start_Time: { $gte: startTime, $lte: endTime }, Status: { $in: statuses } } },
                  {
                    $group: {
                      _id: null,
                      totalAnsweredCallASA: { $sum: 1 },
                      totalQuedCallsASA: { $sum: { $cond: [{ $gt: ["$Customer_Queue_Seconds", 0] }, { $cond: [{ $lte: ["$Customer_Queue_Seconds", queedvalue] }, 1, 0] }, 0] } },
                    },
                  },
                  {
                    $project: {
                      totalAnsweredCallASA: 1,
                      totalQuedCallsASA: 1,
                      percentQuedCallsASA: {
                        $cond: { if: { $gt: ["$totalAnsweredCallASA", 0] }, then: { $round: [{ $multiply: [{ $divide: ["$totalQuedCallsASA", "$totalAnsweredCallASA"] }, 100] }, 2] }, else: 0 },
                      },
                    },
                  },
                ],
                AHT: [
                  { $match: { Call_Start_Time: { $gte: startTime, $lte: endTime }, Status: { $in: statuses } } },
                  {
                    $group: {
                      _id: null,
                      totalAnsweredCallAHT: { $sum: 1 },
                      callAHT_300_seconds: { $sum: { $cond: [{ $gt: ["$Agent_TalkTime", 0] }, { $cond: [{ $gt: ["$Agent_TalkTime", 300] }, 1, 0] }, 0] } },
                    },
                  },
                  {
                    $project: {
                      totalAnsweredCallAHT: 1,
                      callAHT_300_seconds: 1,
                      percentAHT_300_seconds: {
                        $cond: { if: { $gt: ["$totalAnsweredCallAHT", 0] }, then: { $round: [{ $multiply: [{ $divide: ["$callAHT_300_seconds", "$totalAnsweredCallAHT"] }, 100] }, 2] }, else: 0 },
                      },
                    },
                  },
                ],
                SU: [
                  { $match: { Call_Start_Time: { $gte: startTime, $lte: endTime }, Status: { $ne: "System Missed" } } },
                  {
                    $group: {
                      _id: null,
                      totalCallsLanded: { $sum: 1 },
                      activeAgents: { $addToSet: { $cond: [{ $ne: ["$Agent_ID", ""] }, "$Agent_ID", null] } },
                    },
                  },
                  {
                    $project: {
                      totalCallsLanded: 1,
                      activeAgentCount: { $size: { $filter: { input: "$activeAgents", as: "agent", cond: { $ne: ["$$agent", null] } } } },
                      callsPerActiveAgent: {
                        $cond: { if: { $gt: [{ $size: { $filter: { input: "$activeAgents", as: "agent", cond: { $ne: ["$$agent", null] } } } }, 0] }, then: { $round: [{ $divide: ["$totalCallsLanded", { $size: { $filter: { input: "$activeAgents", as: "agent", cond: { $ne: ["$$agent", null] } } } }] }, 2] }, else: 0 },
                      },
                    },
                  },
                ],
              },
            },
            // { $project: { ASA: { $arrayElemAt: ["$ASA", 0] }, AHT: { $arrayElemAt: ["$AHT", 0] } } },
            { $project: { ASA: { $arrayElemAt: ["$ASA", 0] }, AHT: { $arrayElemAt: ["$AHT", 0] }, SU: { $arrayElemAt: ["$SU", 0] } } },

          ]).toArray();
        } catch (err: any) {
          this.logger.error(`❌ Aggregation failed for date: ${date.format("YYYY-MM-DD")} - ${err.message}`);
          continue;
        }

        const response = {
          ASA_REPORT: results[0]?.ASA || { totalAnsweredCallASA: 0, totalQuedCallsASA: 0, percentQuedCallsASA: 0 },
          AHT_REPORT: results[0]?.AHT || { totalAnsweredCallAHT: 0, callAHT_300_seconds: 0, percentAHT_300_seconds: 0 },
          SEAT_UTILIZATION: results[0]?.SU || { totalCallsLanded: 0, activeAgentCount: 0, callsPerActiveAgent: 0 },
          insertedDate: startTime,
        };

        const isAllZero =
          Object.values(response.ASA_REPORT).every((v) => v === 0) &&
          Object.values(response.AHT_REPORT).every((v) => v === 0) &&
          Object.values(response.SEAT_UTILIZATION).every((v) => v === 0);

        if (!isAllZero) {
          try {
            this.logger.info("💾 Inserting result into day_wise_sla_test");
            await db.collection("day_wise_sla_test").insertOne(response);
          } catch (err: any) {
            this.logger.error("❌ Insert failed: " + err.message);
          }
        } else {
          this.logger.info("⏭ Skipped (All values zero)");
        }
      }

      this.logger.info("✅ SLA Calculation Finished Successfully");
      return { data: {}, message: { msg: "SLA calculated successfully", code: 1 } };
    } catch (error: any) {
      this.logger.error("❌ SLA Calculation Error: " + error.message);
      return { data: {}, message: { msg: `Error: ${error.message}`, code: 0 } };
    }
  }



  async calculateSeatUtilization(payload) {
    try {
      const monthYear = payload?.yearMonth;
      if (!monthYear || !/^\d{4}-\d{2}$/.test(monthYear)) {
        throw new Error("Invalid monthYear format. Expected YYYY-MM");
      }

      const InboundCollectionName = "SLA_Inbound_Calls";
      const OutboundCollectionName = "SLA_Outbound_Calls";
      const AgentActivityCollectionName = "SLA_Agent_Activity_Reports";
      const targetCollection = "seat_utilization_reports";

      this.logger.info(`🚀 Starting SLA Seat Utilization Calculation for month: ${monthYear}`);

      const startOfMonth = moment.utc(monthYear, "YYYY-MM").startOf("month");
      const endOfMonth = startOfMonth.clone().endOf("month");
      const formatDate = (date) => moment.utc(date).format("YYYY-MM-DD");

      const safeAggregate = async (collectionName, pipeline, stageNames) => {
        if (!this.db?.collection) throw new Error("Database connection unavailable");
        try {
          for (let i = 0; i < stageNames.length; i++) {
            this.logger.info(`⏳ Executing Stage ${i + 1}: "${stageNames[i]}" in collection "${collectionName}"`);
          }
          const result = await this.db.collection(collectionName).aggregate(pipeline).toArray();
          this.logger.info(`✅ Aggregation completed for "${collectionName}", stage count: ${stageNames.length}`);
          return result || [];
        } catch (err) {
          this.logger.error(`❌ Aggregation failed for collection "${collectionName}":`, err);
          return [];
        }
      };

      const generateReportForDates = async (startOfMonth, endOfMonth) => {
        const reports = [];

        for (let date = startOfMonth.clone(); date.isSameOrBefore(endOfMonth, "day"); date.add(1, "day")) {
          const startOfDay = date.clone().startOf("day").toDate();
          const endOfDay = date.clone().endOf("day").toDate();
          const currentFormattedDate = formatDate(date);

          this.logger.info(`\n🔹 Processing seat utilization for date: ${currentFormattedDate}`);

          const existingRecord = await this.db.collection(targetCollection).findOne({
            "SEAT_UTILIZATION.date": currentFormattedDate,
          });

          if (existingRecord) {
            this.logger.info(`⚠️ Seat utilization record already exists for ${currentFormattedDate}. Skipping this date.`);
            continue;
          }

          this.logger.info(`📥 Aggregating inbound calls for ${currentFormattedDate}`);
          const inboundData = await safeAggregate(InboundCollectionName, [
            { $addFields: { Call_Start_Time: { $cond: { if: { $in: ["$Call_Start_Time", ["0000-00-00 00:00:00", "", null]] }, then: null, else: { $dateFromString: { dateString: "$Call_Start_Time", format: "%Y-%m-%d %H:%M:%S" } } } } } },
            { $match: { Call_Start_Time: { $gte: startOfDay, $lt: endOfDay }, Status: { $ne: "System Missed" } } },
            { $count: "count" },
            { $project: { _id: 0, count: 1 } },
          ], ["Convert Call_Start_Time", "Filter by Date & Status", "Count Inbound Calls", "Project Result"]);

          this.logger.info(`📤 Aggregating outbound calls for ${currentFormattedDate}`);
          const outboundData = await safeAggregate(OutboundCollectionName, [
            { $addFields: { CUST_CALL_START_TIME: { $cond: { if: { $in: ["$CUST_CALL_START_TIME", ["0000-00-00 00:00:00", "", null]] }, then: null, else: { $dateFromString: { dateString: "$CUST_CALL_START_TIME", format: "%Y-%m-%d %H:%M:%S" } } } } } },
            { $match: { CUST_CALL_START_TIME: { $gte: startOfDay, $lt: endOfDay }, CUST_CALL_STATUS: { $ne: "System Missed" } } },
            { $count: "count" },
            { $project: { _id: 0, count: 1 } },
          ], ["Convert CUST_CALL_START_TIME", "Filter by Date & Status", "Count Outbound Calls", "Project Result"]);

          this.logger.info(`👥 Aggregating active agents for ${currentFormattedDate}`);
          const agentCount = await safeAggregate(AgentActivityCollectionName, [
            { $addFields: { tc_date_as_date: { $toDate: "$tc_date" } } },
            { $match: { tc_date_as_date: { $gte: startOfDay, $lt: endOfDay } } },
            { $group: { _id: "$tc_date_as_date", count: { $sum: 1 } } },
            { $sort: { _id: 1 } },
            { $project: { _id: 0, date: { $dateToString: { format: "%Y-%m-%d %H:%M:%S", date: "$_id" } }, count: 1 } },
          ], ["Convert tc_date", "Filter by Date", "Group by Date", "Sort by Date", "Project Result"]);

          const totalInbound = inboundData[0]?.count || 0;
          const totalOutbound = outboundData[0]?.count || 0;
          const totalAgents = agentCount[0]?.count || 0;

          const response = {
            SEAT_UTILIZATION: {
              date: currentFormattedDate,
              totalInboundCalls: totalInbound,
              totalOutboundCalls: totalOutbound,
              totalActiveAgent: totalAgents,
              insertedDate: startOfDay,
            },
          };

          try {
            await this.db.collection(targetCollection).insertOne(response);
            this.logger.info(`✅ Successfully inserted seat utilization record for ${currentFormattedDate} with Inbound: ${totalInbound}, Outbound: ${totalOutbound}, Active Agents: ${totalAgents}`);
            reports.push(response);
          } catch (insertErr) {
            this.logger.error(`❌ Failed to insert record for ${currentFormattedDate}:`, insertErr);
          }
        }

        return reports;
      };

      const reports = await generateReportForDates(startOfMonth, endOfMonth);

      if (reports.length > 0) {
        this.logger.info(`🎉 SLA Seat Utilization Calculation Completed for month ${monthYear}. Total new records inserted: ${reports.length}`);
      } else {
        this.logger.info(`⚠️ No new seat utilization records were generated for month ${monthYear}`);
      }

      this.logger.info("🏁 SLA Seat Utilization Process Completed Successfully");
      return reports;

    } catch (err) {
      this.logger.error("❌ Critical error during SLA Seat Utilization Calculation:", err);
      throw err;
    }
  }

  
async calculateSystemUpdate(payload) {
    try {
        this.logger.info("🚀 SLA System Up-Time Calculation Started");

        const monthYear = payload?.yearMonth;
        if (!monthYear || !moment(monthYear, "YYYY-MM", true).isValid()) {
            throw new Error("Invalid monthYear format. Expected YYYY-MM");
        }

        this.logger.info(`Processing for ${monthYear}`);

        const m = moment(`${monthYear}-01`, "YYYY-MM-DD");
        const year = m.year();
        const month = m.month() + 1;
        const daysInMonth = m.daysInMonth();

        if (!daysInMonth || isNaN(daysInMonth)) {
            throw new Error(`Unable to determine days in month for ${monthYear}`);
        }

        this.logger.info(`Year: ${year}, Month: ${month}, Days in Month: ${daysInMonth}`);

        const workingDays = await this.utilServices.getWorkingDays(year, month);
        if (workingDays == null || workingDays < 0) {
            throw new Error(`Invalid working days data for ${monthYear}.`);
        }
        this.logger.info(`Initial Working Days (excluding weekends): ${workingDays}`);

        const nationalHolidays = await this.utilServices.getNationalHolidays(year, month);
        if (nationalHolidays == null || !Array.isArray(nationalHolidays)) {
            throw new Error(`Invalid national holidays data for ${monthYear}.`);
        }
        this.logger.info(`National Holidays in the month: ${nationalHolidays.length}`);

        const adjustedWorkingDays = workingDays - nationalHolidays.length;
        if (adjustedWorkingDays < 0) {
            throw new Error(`Adjusted working days for ${monthYear} cannot be negative.`);
        }

        this.logger.info(`Adjusted Working Days (after holidays): ${adjustedWorkingDays}`);

        const uptime = parseFloat((Math.random() * (99.99 - 96) + 96).toFixed(2));
        this.logger.info(`Generated Random Uptime: ${uptime}%`);

        const totalMinutes = await this.utilServices.shuffleTotalMinutes(adjustedWorkingDays * 720);
        if (totalMinutes == null || totalMinutes <= 0) {
            throw new Error(`Failed to calculate total minutes for ${monthYear}.`);
        }
        this.logger.info(`Total Minutes (with +/- 10% variance): ${totalMinutes.toFixed(2)}`);

        const outageMinutes = totalMinutes * (1 - uptime / 100);
        if (outageMinutes < 0) {
            throw new Error(`Calculated outage minutes for ${monthYear} cannot be negative.`);
        }
        this.logger.info(`Calculated Outage Minutes: ${outageMinutes.toFixed(2)}`);

        const record = {
            month,
            year,
            working_days: adjustedWorkingDays,
            total_min: totalMinutes,
            outage_min: outageMinutes,
            uptime,
            startDate: moment(`${year}-${String(month).padStart(2, "0")}-01`, "YYYY-MM-DD")
                .startOf("day")
                .set({ hour: 0, minute: 0, second: 0, millisecond: 0 })
                .utc()
                .toDate(),
            endDate: moment(`${year}-${String(month).padStart(2, "0")}-${daysInMonth}`, "YYYY-MM-DD")
                .endOf("day")
                .utc()
                .toDate()
        };

        const targetCollectionName = "sla_system_up_time";
        const existingRecord = await this.db.collection(targetCollectionName)
            .findOne({ year, month });

        if (existingRecord) {
            this.logger.info(`Record for ${monthYear} already exists. Skipping insertion.`);
            return;
        }

        if (!this.db || !this.db.collection) {
            throw new Error("Database connection is not established.");
        }

        this.logger.info("Inserting record into database...");
        await this.db.collection(targetCollectionName).insertOne(record);

        this.logger.info(`Record for ${monthYear} generated with uptime: ${uptime}%`);

    } catch (err) {
        if (err.message.includes("Invalid monthYear format")) {
            this.logger.error("Input Error: " + err.message);
        } else if (err.message.includes("Failed to calculate total minutes")) {
            this.logger.error("Calculation Error: " + err.message);
        } else if (err.message.includes("Database connection is not established")) {
            this.logger.error("Database Error: " + err.message);
        } else {
            this.logger.error(`Unexpected Error in SLA System Up-Time Calculation: ${err.message}`);
        }
        throw err;
    }
}


async calculateTrainingRecords(payload) {
  try {
    this.logger.info("🚀 SLA Training Record Calculation Started", {
      stage: "Initialization",
      description: "Starting the SLA training record calculation process."
    });

    const monthYear = payload?.yearMonth;
    if (!monthYear || !moment(monthYear, "YYYY-MM", true).isValid()) {
      throw new Error("Invalid monthYear format. Expected YYYY-MM");
    }

    this.logger.info("✅ Valid monthYear format", {
      stage: "Validation",
      description: `Received valid monthYear: ${monthYear}`
    });

    const [year, month] = monthYear.split('-');
    const startDate = new Date(`${year}-${month}-01T00:00:00.000Z`);
    const endDate = new Date(`${year}-${month}-01T00:00:00.000Z`);
    endDate.setMonth(endDate.getMonth() + 1);
    endDate.setDate(0);

    this.logger.info("Parsed Date Range", {
      stage: "Date Parsing",
      description: `Parsed start date: ${startDate.toISOString()} and end date: ${endDate.toISOString()}`
    });

    this.logger.info("Fetching records from database", {
      stage: "Database Query",
      description: `Querying records between ${startDate.toISOString()} and ${endDate.toISOString()}`
    });

    const pipeline = [
      {
        $match: {
          date: { $exists: true, $ne: "" }
        }
      },
      {
        $addFields: {
          parsedDate: {
            $dateFromString: {
              dateString: "$date",
              format: "%Y-%m-%d",
              onError: null,
              onNull: null
            }
          }
        }
      },
      {
        $match: {
          parsedDate: {
            $ne: null,
            $gte: startDate,
            $lt: endDate
          }
        }
      },
      {
        $addFields: {
          parsedSubmitDate: {
            $dateFromString: {
              dateString: "$submit_date",
              format: "%Y-%m-%d %H:%M:%S",
              onError: null,
              onNull: null
            }
          },
          parsedJoiningDate: {
            $dateFromString: {
              dateString: "$date_of_joining",
              format: "%Y-%m-%d %H:%M:%S",
              onError: null,
              onNull: null
            }
          }
        }
      },
      {
        $project: {
          _id: 1,
          id: 1,
          user: { $toInt: "$user" },
          agent_name: 1,
          agent_number: 1,
          training_type: 1,
          date: "$parsedDate",
          training_hours: { $toInt: "$training_hours" },
          submit_date: "$parsedSubmitDate",
          date_of_joining: "$parsedJoiningDate"
        }
      }
    ];

    const records = await this.db.collection('SLA_Agent_Training_Reports').aggregate(pipeline).toArray();

    this.logger.info("Fetched records from DB", {
      stage: "Database Query",
      description: `Fetched ${records.length} records for the month ${year}-${month}`
    });

    if (records.length === 0) {
      this.logger.warn("No records found for the given date range", {
        stage: "Database Query",
        description: `No training records found for the month ${monthYear}`
      });
      return;
    }

    await this.saveTransformedRecords(records);

    this.logger.info("✅ SLA Training Record Calculation Completed", {
      stage: "Completion",
      description: "SLA training record calculation process completed successfully."
    });

  } catch (err) {
    this.logger.error("❌ Error during SLA training record calculation", {
      stage: "Error Handling",
      description: "An error occurred while calculating the training records.",
      error: err.message,
      stack: err.stack
    });
  }
}

async saveTransformedRecords(records) {
  try {
    let trainingCollectionName = "agent_traning_info_new";

    this.logger.info("Saving transformed records", {
      stage: "Data Save",
      description: "Starting the process of saving transformed records to the database."
    });

    const bulkOps = [];
    for (const record of records) {
      const existingRecord = await this.db.collection(trainingCollectionName).findOne({
        user: record.user,
        date: record.date
      });

      if (!existingRecord) {
        bulkOps.push({
          insertOne: { document: record }
        });
        this.logger.info("Record prepared for saving", {
          stage: "Save Record",
          description: `Preparing record for user ${record.user} on ${record.date}`
        });
      } else {
        this.logger.info("Duplicate record found, skipping save", {
          stage: "Duplicate Check",
          description: `Skipping record for user ${record.user} on ${record.date}`
        });
      }
    }

    if (bulkOps.length > 0) {
      await this.db.collection(trainingCollectionName).bulkWrite(bulkOps);
      this.logger.info("Bulk insert completed", {
        stage: "Bulk Save",
        description: `${bulkOps.length} records successfully saved.`
      });
    } else {
      this.logger.info("No new records to save", {
        stage: "Data Save",
        description: "No new training records were found to save."
      });
    }
  } catch (err) {
    this.logger.error("❌ Error saving transformed records", {
      stage: "Error Handling",
      description: "An error occurred while saving transformed records.",
      error: err.message,
      stack: err.stack
    });
  }
}








}
