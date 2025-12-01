    import { Inject, Injectable } from '@nestjs/common';
    import { Db } from 'mongodb';
    import { UtilService } from 'src/commonServices/utilService';
    const Logger = require("../commonServices/logger");
    @Injectable()
    export class ProductiveCallingServices {
    private logger: InstanceType<typeof Logger>;

    constructor(
        @Inject('MONGO_DB') private readonly db: Db,
        private readonly utilServices: UtilService,
    ) {
        this.logger = new Logger("Productive_Calling.log");
    }


   async productiveCallingDayWise(db:any, year:any, month:any)  {
    return new Promise(async (resolve, reject) => {
        this.utilServices.cleanupCollection(db, 'krph_productive_status_daywise', { month: month, year: year })
        try {
            let pipeLine = [
                {
                    $addFields: {
                        day: { $dayOfMonth: "$Call_Start_Time" },
                        month: { $month: "$Call_Start_Time" },
                        year: { $year: "$Call_Start_Time" }
                    }
                },
                {
                    $match: {
                        month: month,
                        year: year,
                        Agent_TalkTime: { $ne: "" }
                    }
                },
                {
                    $addFields: {
                        Agent_TalkTime: { $toInt: "$Agent_TalkTime" },
                        Agent_ID: { $toInt: "$Agent_ID" }
                    }
                },
                {
                    $group: {
                        _id: {
                            Agent_ID: "$Agent_ID",
                            day: "$day",
                            month: "$month",
                            year: "$year"
                        },
                        totalTalkTimeSec: { $sum: "$Agent_TalkTime" },
                        totalCalls: { $sum: 1 },
                        avgTalkTimeSec: { $avg: "$Agent_TalkTime" }
                    }
                },
                {
                    $addFields: {
                        totalTalkTimeMin: {
                            $round: [{ $divide: ["$totalTalkTimeSec", 60] }, 2]
                        },
                        totalTalkTimeHour: {
                            $round: [{ $divide: ["$totalTalkTimeSec", 3600] }, 2]
                        },
                        avgTalkTimeMin: {
                            $round: [{ $divide: ["$avgTalkTimeSec", 60] }, 2]
                        }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        Agent_ID: "$_id.Agent_ID",
                        day: "$_id.day",
                        month: "$_id.month",
                        year: "$_id.year",
                        totalCalls: 1,
                        totalTalkTimeSec: 1,
                        totalTalkTimeMin: 1,
                        totalTalkTimeHour: 1,
                        avgTalkTimeSec: {
                            $round: ["$avgTalkTimeSec", 2]
                        },
                        avgTalkTimeMin: 1
                    }
                },
                {
                    $sort: {
                        Agent_ID: 1,
                        year: 1,
                        month: 1,
                        day: 1
                    }
                },
                {
                    $merge: {
                        into: 'krph_productive_status_daywise',
                        whenMatched: "merge",
                        whenNotMatched: "insert"
                    }
                }
            ]

            let result = await db.collection(`Agent_Performance_krph_historical_calls_${month}_${year}`).aggregate(pipeLine).toArray();
            resolve(result);
            this.logger.info(`completed the daywise krph_productive_status_daywise `);



        } catch (err) {
            console.log("err", err);
        }
    })
}



async productiveCallingMonthWise(db:any, year:any, month:any) {
    const collectionName = `krph_productive_status_monthwise_${month}_${year}`;
    const sourceCollection = "krph_productive_status_daywise";

    try {
        if (!db) {
            this.logger.error("productiveCallingMonthWise: Database instance missing");
            return false;
        }

        if (!year || !month || isNaN(year) || isNaN(month)) {
            this.logger.error("productiveCallingMonthWise: Invalid year or month");
            return false;
        }

        const numericMonth = Number(month);
        const numericYear = Number(year);

        if (numericMonth < 1 || numericMonth > 12) {
            this.logger.error("productiveCallingMonthWise: Month out of range");
            return false;
        }

        const existingRecords = await db.collection(collectionName).countDocuments({
            month: numericMonth,
            year: numericYear
        });

        const sourceRecords = await db.collection(sourceCollection).countDocuments({
            month: numericMonth,
            year: numericYear
        });

        if (existingRecords > 0 && existingRecords === sourceRecords) {
            this.logger.info(`productiveCallingMonthWise: Existing records match source; skipping regeneration for ${collectionName}`);
            return true;
        }

        await this.utilServices.cleanupCollection(
            db,
            collectionName,
            { month: numericMonth, year: numericYear }
        );

        this.logger.info(`productiveCallingMonthWise: Starting aggregation for ${collectionName}`);

        const pipeLine = [
            { $match: { year: numericYear, month: numericMonth } },
            {
                $group: {
                    _id: { Agent_ID: "$Agent_ID", day: "$day" },
                    dailyTalkTimeSec: { $sum: "$totalTalkTimeSec" },
                    dailyCalls: { $sum: "$totalCalls" }
                }
            },
            {
                $group: {
                    _id: { Agent_ID: "$_id.Agent_ID" },
                    totalTalkTimeSec: { $sum: "$dailyTalkTimeSec" },
                    totalCalls: { $sum: "$dailyCalls" },
                    totalDays: { $sum: 1 }
                }
            },
            {
                $addFields: {
                    totalTalkTimeMin: { $round: [{ $divide: ["$totalTalkTimeSec", 60] }, 2] },
                    totalTalkTimeHour: { $round: [{ $divide: ["$totalTalkTimeSec", 3600] }, 2] },
                    avgTalkTimeSec: { $round: [{ $divide: ["$totalTalkTimeSec", "$totalCalls"] }, 2] },
                    avgTalkTimeMin: {
                        $round: [
                            { $divide: [{ $divide: ["$totalTalkTimeSec", 60] }, "$totalCalls"] },
                            2
                        ]
                    },
                    avgTalkTimeHourPerDay: {
                        $round: [
                            { $divide: [{ $divide: ["$totalTalkTimeSec", 3600] }, "$totalDays"] },
                            2
                        ]
                    }
                }
            },
            {
                $project: {
                    Agent_ID: "$_id.Agent_ID",
                    totalTalkTimeSec: 1,
                    totalCalls: 1,
                    totalDays: 1,
                    totalTalkTimeMin: 1,
                    totalTalkTimeHour: 1,
                    avgTalkTimeSec: 1,
                    avgTalkTimeMin: 1,
                    avgTalkTimeHourPerDay: 1,
                    month: { $literal: numericMonth },
                    year: { $literal: numericYear },
                    _id: 0
                }
            },
            {
                $merge: {
                    into: collectionName,
                    whenMatched: "replace",
                    whenNotMatched: "insert"
                }
            }
        ];

        await db.collection(sourceCollection).aggregate(pipeLine).toArray();

        this.logger.info(`productiveCallingMonthWise: Completed aggregation for ${collectionName}`);

        return true;
    } catch (err) {
        this.logger.error(`productiveCallingMonthWise: Failed for month=${month}, year=${year}, error=${err.message}`);
        return false;
    }
}


    }

