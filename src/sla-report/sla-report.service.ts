import { Inject, Injectable } from '@nestjs/common';
import { Db } from 'mongodb';
import { Sequelize } from 'sequelize';
import { RedisWrapper } from 'src/commonServices/redisWrapper';
import { UtilService } from 'src/commonServices/utilService';
import { MailService } from 'src/mail/mail.service';
import * as moment from "moment";
const Logger = require("../commonServices/logger");
import { parse } from 'csv-parse/sync';





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


 

 /* async startSlaCalculation(payload: any) {
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

    this.calculateCallQualityRecords(payload)
    .then(() => this.logger.info("SLA Call Quality Calculation finished in background"))
    .catch((err) => this.logger.error(`SLA Call Qualitybackground processing error: ${err.message}`));

  } catch (error) {
    this.logger.error(`Unexpected error in SLA calculations: ${error.message}`);
  }

  return immediateResponse;
} */


  async startSlaCalculation(payload: any) {
  this.logger.info('SLA processing started. Immediate response sent.');

  const immediateResponse = {
    data: {},
    message: { msg: "SLA processing has been started. DB will be updated soon...", code: 1 },
  };

  (async () => {
    const tasks = [
      { fn: this.calculateSlaService, name: "SLA Training" },
      { fn: this.calculateSeatUtilization, name: "SLA Seat Utilization" },
      { fn: this.calculateSystemUpdate, name: "SLA System Up-time" },
      { fn: this.calculateTrainingRecords, name: "SLA Training Records" },
      { fn: this.calculateCallQualityRecords, name: "SLA Call Quality" },
    ];

    for (const task of tasks) {
      try {
        await task.fn.call(this, payload); 
        this.logger.info(`${task.name} calculation finished in background`);
      } catch (err: any) {
        this.logger.error(`${task.name} background processing error: ${err.message}`);
      }
    }

    this.logger.info("All SLA calculations completed.");
  })();

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
       /*  try {
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
        } */

       try {
  results = await db.collection("SLA_Inbound_Calls").aggregate([
    {
      $addFields: {
        Call_Start_Time: {
          $cond: [
            { $in: [{ $type: "$Call_Start_Time" }, ["null", "missing"]] },
            null,
            {
              $cond: [
                { $eq: [{ $type: "$Call_Start_Time" }, "date"] },
                "$Call_Start_Time",
                {
                  $cond: [
                    { $in: ["$Call_Start_Time", ["0000-00-00 00:00:00", ""]] },
                    null,
                    {
                      $dateFromString: {
                        dateString: "$Call_Start_Time",
                        format: "%Y-%m-%d %H:%M:%S"
                      }
                    }
                  ]
                }
              ]
            }
          ]
        },

        Call_End_Time: {
          $cond: [
            { $in: [{ $type: "$Call_End_Time" }, ["null", "missing"]] },
            null,
            {
              $cond: [
                { $eq: [{ $type: "$Call_End_Time" }, "date"] },
                "$Call_End_Time",
                {
                  $cond: [
                    { $in: ["$Call_End_Time", ["0000-00-00 00:00:00", ""]] },
                    null,
                    {
                      $dateFromString: {
                        dateString: "$Call_End_Time",
                        format: "%Y-%m-%d %H:%M:%S"
                      }
                    }
                  ]
                }
              ]
            }
          ]
        },

        Customer_Queue_Seconds: {
          $cond: [
            { $in: ["$Customer_Queue_Seconds", ["", null]] },
            0,
            { $toInt: "$Customer_Queue_Seconds" }
          ]
        },

        Agent_TalkTime: {
          $cond: [
            { $in: ["$Agent_TalkTime", ["", null]] },
            0,
            { $toInt: "$Agent_TalkTime" }
          ]
        },

        Agent_ID: {
          $cond: [
            { $in: ["$Agent_ID", ["", null]] },
            null,
            { $toInt: "$Agent_ID" }
          ]
        }
      }
    },

    {
      $facet: {
        ASA: [
          { $match: { Call_Start_Time: { $gte: startTime, $lte: endTime }, Status: { $in: statuses } } },
          {
            $group: {
              _id: null,
              totalAnsweredCallASA: { $sum: 1 },
              totalQuedCallsASA: {
                $sum: {
                  $cond: [
                    { $gt: ["$Customer_Queue_Seconds", 0] },
                    { $cond: [{ $lte: ["$Customer_Queue_Seconds", queedvalue] }, 1, 0] },
                    0
                  ]
                }
              }
            }
          },
          {
            $project: {
              totalAnsweredCallASA: 1,
              totalQuedCallsASA: 1,
              percentQuedCallsASA: {
                $cond: {
                  if: { $gt: ["$totalAnsweredCallASA", 0] },
                  then: {
                    $round: [
                      { $multiply: [{ $divide: ["$totalQuedCallsASA", "$totalAnsweredCallASA"] }, 100] },
                      2
                    ]
                  },
                  else: 0
                }
              }
            }
          }
        ],

        AHT: [
          { $match: { Call_Start_Time: { $gte: startTime, $lte: endTime }, Status: { $in: statuses } } },
          {
            $group: {
              _id: null,
              totalAnsweredCallAHT: { $sum: 1 },
              callAHT_300_seconds: {
                $sum: {
                  $cond: [
                    { $gt: ["$Agent_TalkTime", 0] },
                    { $cond: [{ $gt: ["$Agent_TalkTime", 300] }, 1, 0] },
                    0
                  ]
                }
              }
            }
          },
          {
            $project: {
              totalAnsweredCallAHT: 1,
              callAHT_300_seconds: 1,
              percentAHT_300_seconds: {
                $cond: {
                  if: { $gt: ["$totalAnsweredCallAHT", 0] },
                  then: {
                    $round: [
                      { $multiply: [{ $divide: ["$callAHT_300_seconds", "$totalAnsweredCallAHT"] }, 100] },
                      2
                    ]
                  },
                  else: 0
                }
              }
            }
          }
        ],

        SU: [
          { $match: { Call_Start_Time: { $gte: startTime, $lte: endTime }, Status: { $ne: "System Missed" } } },
          {
            $group: {
              _id: null,
              totalCallsLanded: { $sum: 1 },
              activeAgents: {
                $addToSet: {
                  $cond: [{ $ne: ["$Agent_ID", null] }, "$Agent_ID", null]
                }
              }
            }
          },
          {
            $project: {
              totalCallsLanded: 1,
              activeAgentCount: {
                $size: {
                  $filter: {
                    input: "$activeAgents",
                    as: "agent",
                    cond: { $ne: ["$$agent", null] }
                  }
                }
              },
              callsPerActiveAgent: {
                $cond: {
                  if: {
                    $gt: [
                      {
                        $size: {
                          $filter: {
                            input: "$activeAgents",
                            as: "agent",
                            cond: { $ne: ["$$agent", null] }
                          }
                        }
                      },
                      0
                    ]
                  },
                  then: {
                    $round: [
                      {
                        $divide: [
                          "$totalCallsLanded",
                          {
                            $size: {
                              $filter: {
                                input: "$activeAgents",
                                as: "agent",
                                cond: { $ne: ["$$agent", null] }
                              }
                            }
                          }
                        ]
                      },
                      2
                    ]
                  },
                  else: 0
                }
              }
            }
          }
        ]
      }
    },

    {
      $project: {
        ASA: { $arrayElemAt: ["$ASA", 0] },
        AHT: { $arrayElemAt: ["$AHT", 0] },
        SU: { $arrayElemAt: ["$SU", 0] }
      }
    }
  ]).toArray();

} catch (err: any) {
  this.logger.error(
    `❌ Aggregation failed for date: ${date.format("YYYY-MM-DD")} - ${err.message}`
  );
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




 async calculateCallQualityRecordsPrevious(payload: any) {
  try {
    this.logger.info("Starting calculateCallQualityRecords function");

    const db = this.db;
    const yearMonth = payload?.yearMonth;
    const baseCollectionToInsert = "sla_records_audit_sept";

    if (!yearMonth || !/^(\d{4})-(\d{2})$/.test(yearMonth)) {
      this.logger.info("Invalid or missing 'yearMonth' in payload");
      return { data: {}, message: { msg: "Invalid or missing 'yearMonth'. Expected format: YYYY-MM", code: 0 } };
    }
    this.logger.info("YearMonth format validated");

    const [year, month] = yearMonth.split('-');
    
    if (isNaN(Number(year)) || isNaN(Number(month)) || Number(month) < 1 || Number(month) > 12) {
      this.logger.info("Invalid 'yearMonth' format. Year or Month is incorrect.");
      return { data: {}, message: { msg: "Invalid 'yearMonth' format. Year or Month is incorrect.", code: 0 } };
    }
    this.logger.info("Year and Month are valid numbers");

    const startDate = new Date(`${year}-${month}-01T00:00:00.000Z`);
    const endDate = new Date(`${year}-${month}-${new Date(Number(year), Number(month), 0).getDate()}T23:59:59.999Z`);
    
    this.logger.info(`Start date: ${startDate.toISOString()}, End date: ${endDate.toISOString()}`);

    const collectionName = `sla_call_quality_data_${year}_${month}`;
    this.logger.info(`Using collection: ${collectionName}`);

    const pipeline = [
      {
        $addFields: {
          call_date: { $toDate: "$call_date" },
        },
      },
      {
        $match: {
          call_date: {
            $gte: startDate,
            $lt: endDate,
          },
        },
      },
      {
        $addFields: {
          uniqueid_new: {
            $toInt: {
              $substrBytes: [
                "$uniqueid",
                1,
                { 
                  $subtract: [ 
                    { $indexOfBytes: ["$uniqueid", "."] }, 
                    1 
                  ] 
                },
              ]
            }
          },
          uniqueid: {
            $substrBytes: [
              "$uniqueid",
              1,
              { 
                $subtract: [ 
                  { $indexOfBytes: ["$uniqueid", "."] }, 
                  1 
                ] 
              },
            ]
          },
          agent_id_new: { $toInt: "$agent_id" },
          total_score_value: { $toInt: "$total_rating" }
        },
      }
    ];

    this.logger.info("Aggregation pipeline created");

    this.logger.info("Executing aggregation query...");
    const records = await db.collection(collectionName).aggregate(pipeline).toArray();
    this.logger.info(`Aggregation query executed. Records fetched: ${records.length}`);

    const recordCount = records.length;

    if (recordCount === 0) {
      this.logger.info(`No records found for ${yearMonth}`);
    } else {
      this.logger.info(`Fetched ${recordCount} records for ${yearMonth}`);
      this.logger.info(`First 5 records: ${JSON.stringify(records.slice(0, 5))}`);
    }

    return { data: records, message: { msg: "Success", code: 1 } };

  } catch (err) {
    console.error(err);
    this.logger.info("Error occurred while fetching call quality records");
    return { data: {}, message: { msg: "An unexpected error occurred while fetching records", code: 0 } };
  }
} 
 

async calculateCallQualityRecords(payload: any) {
  try {
    this.logger.info("Starting calculateCallQualityRecords function");

    const db = this.db;
    const yearMonth = payload?.yearMonth;
    const baseCollection = "sla_records_audit_sept";

    if (!yearMonth || !/^(\d{4})-(\d{2})$/.test(yearMonth)) {
      return {
        data: {},
        message: { msg: "Invalid or missing 'yearMonth'. Expected format: YYYY-MM", code: 0 }
      };
    }

    const [yearStr, monthStr] = yearMonth.split("-");
    const year = Number(yearStr);
    const month = Number(monthStr);
    if (isNaN(year) || isNaN(month) || month < 1 || month > 12) {
      return {
        data: {},
        message: { msg: "Invalid year or month values", code: 0 }
      };
    }

    const startDate = new Date(Date.UTC(year, month - 1, 1, 0, 0, 0, 0));
    const endDate = new Date(Date.UTC(year, month, 0, 23, 59, 59, 999));

    const collectionName = `sla_call_quality_data_${year}_${month.toString().padStart(2, "0")}`;

    const pipelineBase = [
      { $addFields: { call_date: { $toDate: "$call_date" } } },
      { $match: { call_date: { $gte: startDate, $lte: endDate } } },
      {
        $addFields: {
          uniqueid_new: {
            $toInt: {
              $substrBytes: [
                "$uniqueid",
                1,
                { $subtract: [{ $indexOfBytes: ["$uniqueid", "."] }, 1] }
              ]
            }
          },
          uniqueid: {
            $substrBytes: [
              "$uniqueid",
              1,
              { $subtract: [{ $indexOfBytes: ["$uniqueid", "."] }, 1] }
            ]
          },
          agent_id_new: { $toInt: "$agent_id" },
          total_score_value: { $toInt: "$total_rating" }
        }
      }
    ];

    const chunkSize = 1000;
    let skip = 0;
    let totalInserted = 0;

    while (true) {
      this.logger.info(`Fetching chunk (skip=${skip}, limit=${chunkSize})`);

      const records = await db.collection(collectionName)
        .aggregate([...pipelineBase, { $skip: skip }, { $limit: chunkSize }])
        .toArray();

      if (!Array.isArray(records) || records.length === 0) break;

      const bulkOps = records
        .filter(r => r.uniqueid_new != null)
        .map(r => ({
          updateOne: { filter: { uniqueid_new: r.uniqueid_new }, update: { $setOnInsert: r }, upsert: true }
        }));

      if (bulkOps.length > 0) {
        try {
          const result = await db.collection(baseCollection).bulkWrite(bulkOps, { ordered: false });
          totalInserted += result.upsertedCount || 0;
          this.logger.info(`Chunk processed. New records inserted: ${result.upsertedCount || 0}`);
        } catch (bulkErr) {
          this.logger.error("Error during bulkWrite", bulkErr);
        }
      }

      skip += chunkSize;
    }

    this.logger.info(`Processing completed. Total new records inserted: ${totalInserted}`);

    return { data: { inserted: totalInserted }, message: { msg: "Success", code: 1 } };

  } catch (err) {
    this.logger.error("Unexpected error in calculateCallQualityRecords", err);
    return { data: {}, message: { msg: "Unexpected error occurred", code: 0 } };
  }
}



async startCallQualityFileUploading(file: Express.Multer.File, yearMonth: string) {
  setImmediate(async () => {
    try {
      const result = await this.uploadCallQualityFileService(file, yearMonth);
      console.log('Background processing completed:', result);
    } catch (err) {
      console.error('Background job error:', err);
    }
  });

  return { message: 'File upload started. Processing in the background.', code: 1 };
}




async uploadCallQualityFileService(file: Express.Multer.File, yearMonth: string) {
  try {
    const db = this.db;

    if (!db) {
      this.logger.error("Database connection not available");
      return { data: {}, message: { msg: 'Database connection not available', code: -100 } };
    }

    if (!file) {
      this.logger.info("No file provided in request");
      return { data: {}, message: { msg: 'No file provided', code: 0 } };
    }

    if (!file.buffer || file.buffer.length === 0) {
      this.logger.info("File buffer is empty");
      return { data: {}, message: { msg: 'File buffer is empty', code: 0 } };
    }

    if (file.size > 50 * 1024 * 1024) {
      this.logger.info("File size exceeds 50MB limit");
      return { data: {}, message: { msg: 'File size exceeds maximum allowed limit', code: -5 } };
    }

    let fileContent: string;
    try {
      fileContent = file.buffer.toString('utf-8');
    } catch (encodingErr) {
      this.logger.error("File encoding error", { error: encodingErr });
      return { data: {}, message: { msg: 'Invalid file encoding', code: -6 } };
    }

    if (!fileContent || !fileContent.trim()) {
      this.logger.info("CSV file is empty after trimming");
      return { data: {}, message: { msg: 'CSV file is empty', code: 0 } };
    }

    if (!yearMonth || typeof yearMonth !== 'string') {
      this.logger.info("Year-month parameter missing or invalid type");
      return { data: {}, message: { msg: 'Year-month parameter is required', code: -4 } };
    }

    const yearMonthTrimmed = yearMonth.trim();
    if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(yearMonthTrimmed)) {
      this.logger.info("Invalid year-month format", { yearMonth: yearMonthTrimmed });
      return { data: {}, message: { msg: 'Invalid year-month format. Expected YYYY-MM', code: -4 } };
    }

    const [year, month] = yearMonthTrimmed.split('-').map(Number);
    const currentYear = new Date().getFullYear();
    if (year < 2000 || year > currentYear + 10) {
      this.logger.info("Year out of acceptable range", { year });
      return { data: {}, message: { msg: 'Year must be between 2000 and current year plus 10', code: -7 } };
    }

    let records: Record<string, any>[];
    try {
      this.logger.info("Parsing CSV content");
      records = parse(fileContent, { 
        columns: true, 
        skip_empty_lines: true, 
        trim: true,
        relax_column_count: true,
        skip_records_with_error: false
      });
    } catch (parseErr) {
      this.logger.error("CSV parsing error", { error: parseErr });
      return { data: {}, message: { msg: 'Invalid CSV format or structure', code: -2 } };
    }

    if (!Array.isArray(records)) {
      this.logger.error("Parsed records is not an array");
      return { data: {}, message: { msg: 'Invalid CSV parsing result', code: -2 } };
    }

    if (!records.length) {
      this.logger.info("CSV file contains no data records");
      return { data: {}, message: { msg: 'CSV file contains no records', code: 0 } };
    }

    const requiredFields = ['uniqueid', 'call_date'];
    const sampleRecord = records[0];
    const missingFields = requiredFields.filter(field => !(field in sampleRecord));
    
    if (missingFields.length > 0) {
      this.logger.info("CSV missing required fields", { missingFields });
      return { data: {}, message: { msg: `CSV missing required fields: ${missingFields.join(', ')}`, code: -8 } };
    }

    this.logger.info(`Year-Month received: ${yearMonthTrimmed}`);
    
    const startDate = moment(`${yearMonthTrimmed}-01`).startOf('day');
    const endDate = startDate.clone().endOf('month');
    
    if (!startDate.isValid() || !endDate.isValid()) {
      this.logger.error("Invalid date range calculation");
      return { data: {}, message: { msg: 'Failed to calculate date range', code: -9 } };
    }

    this.logger.info(`Filtering records for period: ${startDate.format()} to ${endDate.format()}`);

    const filteredRecords = records.filter(record => {
      if (!record || typeof record !== 'object') {
        return false;
      }

      if (!record.call_date) {
        return false;
      }

      const callDate = moment(record.call_date);
      if (!callDate.isValid()) {
        return false;
      }

      return callDate.isBetween(startDate, endDate, undefined, '[]');
    });

    if (!filteredRecords.length) {
      this.logger.info("No records match the selected month after filtering");
      return { data: {}, message: { msg: 'No records match the selected month', code: 0 } };
    }

    this.logger.info(`Filtered ${filteredRecords.length} records for the month`);

    const validRecords = filteredRecords.filter(record => {
      if (!record.uniqueid || typeof record.uniqueid !== 'string') {
        this.logger.warn("Record missing valid uniqueid", { record });
        return false;
      }
      return true;
    });

    if (!validRecords.length) {
      this.logger.info("No valid records with uniqueid found");
      return { data: {}, message: { msg: 'No valid records with uniqueid found', code: 0 } };
    }

    if (validRecords.length < filteredRecords.length) {
      this.logger.warn(`${filteredRecords.length - validRecords.length} records skipped due to missing uniqueid`);
    }

    const currentTimestamp = moment().toISOString();
    const rows = validRecords.map(record => ({
      ...record,
      InsertedDateTime: currentTimestamp,
    }));

    const collectionName = `sla_call_quality_data_${yearMonthTrimmed.replace('-', '_')}`;
    
    if (collectionName.length > 120) {
      this.logger.error("Collection name exceeds maximum length");
      return { data: {}, message: { msg: 'Collection name too long', code: -10 } };
    }

    this.logger.info(`Checking if collection ${collectionName} exists`);

    let collections;
    try {
      collections = await db.listCollections({ name: collectionName }).toArray();
    } catch (listErr) {
      this.logger.error("Failed to list collections", { error: listErr });
      return { data: {}, message: { msg: 'Failed to access database collections', code: -11 } };
    }

    if (!Array.isArray(collections)) {
      this.logger.error("Invalid collections list response");
      return { data: {}, message: { msg: 'Invalid database response', code: -11 } };
    }

    if (!collections.length) {
      this.logger.info(`Collection ${collectionName} does not exist, creating new collection`);
      try {
        await db.createCollection(collectionName);
      } catch (createErr) {
        this.logger.error("Failed to create collection", { error: createErr });
        return { data: {}, message: { msg: 'Failed to create database collection', code: -12 } };
      }
    } else {
      this.logger.info(`Collection ${collectionName} already exists`);
    }

    const chunkSize = 1000;
    const chunkedRecords = [];
    for (let i = 0; i < rows.length; i += chunkSize) {
      chunkedRecords.push(rows.slice(i, i + chunkSize));
    }

    this.logger.info(`Processing records in ${chunkedRecords.length} chunks`);

    let totalInserted = 0;
    let totalDuplicates = 0;
    let totalErrors = 0;

    for (let chunkIndex = 0; chunkIndex < chunkedRecords.length; chunkIndex++) {
      const chunk = chunkedRecords[chunkIndex];
      this.logger.info(`Processing chunk ${chunkIndex + 1}/${chunkedRecords.length} with ${chunk.length} records`);

      if (!chunk || !Array.isArray(chunk) || chunk.length === 0) {
        this.logger.warn(`Skipping invalid chunk at index ${chunkIndex}`);
        continue;
      }

      const uniqueRecords = [];
      
      for (const record of chunk) {
        try {
          if (!record || !record.uniqueid) {
            totalErrors++;
            continue;
          }

          const existingRecord = await db.collection(collectionName).findOne(
            { uniqueid: record.uniqueid },
            { projection: { _id: 1 } }
          );

          if (!existingRecord) {
            uniqueRecords.push(record);
          } else {
            totalDuplicates++;
          }
        } catch (findErr) {
          this.logger.error("Error checking record uniqueness", { error: findErr, uniqueid: record?.uniqueid });
          totalErrors++;
        }
      }

      if (uniqueRecords.length > 0) {
        this.logger.info(`Inserting ${uniqueRecords.length} unique records into database`);
        try {
          const insertResult = await db.collection(collectionName).insertMany(uniqueRecords, { ordered: false });
          
          if (insertResult && insertResult.insertedCount !== undefined) {
            totalInserted += insertResult.insertedCount;
          } else {
            totalInserted += uniqueRecords.length;
          }
        } catch (dbErr: any) {
          this.logger.error("Database insertion error", { error: dbErr, chunkIndex });
          
          if (dbErr.writeErrors && Array.isArray(dbErr.writeErrors)) {
            const successfulInserts = uniqueRecords.length - dbErr.writeErrors.length;
            totalInserted += successfulInserts;
            totalErrors += dbErr.writeErrors.length;
          } else {
            totalErrors += uniqueRecords.length;
          }
        }
      } else {
        this.logger.info("No unique records to insert in this chunk");
      }
    }

    if (totalInserted === 0 && totalDuplicates === 0 && totalErrors === 0) {
      this.logger.info("No records were processed");
      return { data: {}, message: { msg: 'No records were processed', code: 0 } };
    }

    const responseMessage = totalInserted > 0 
      ? `${totalInserted} records successfully inserted into ${collectionName}`
      : 'No new records inserted';

    const additionalInfo = [];
    if (totalDuplicates > 0) {
      additionalInfo.push(`${totalDuplicates} duplicates skipped`);
    }
    if (totalErrors > 0) {
      additionalInfo.push(`${totalErrors} errors encountered`);
    }

    const fullMessage = additionalInfo.length > 0 
      ? `${responseMessage}. ${additionalInfo.join(', ')}.`
      : `${responseMessage}.`;

    this.logger.info(fullMessage);

    return {
      data: { 
        insertedCount: totalInserted,
        duplicateCount: totalDuplicates,
        errorCount: totalErrors,
        totalProcessed: rows.length,
        collection: collectionName 
      },
      message: { 
        msg: fullMessage,
        code: totalInserted > 0 ? 1 : (totalErrors > 0 ? -13 : 0)
      },
    };
  } catch (err: any) {
    this.logger.error("Unexpected error in uploadCallQualityFileService", { 
      error: err,
      message: err?.message,
      stack: err?.stack 
    });
    return { data: {}, message: { msg: 'Unexpected error occurred', code: -99 } };
  }
}






}
