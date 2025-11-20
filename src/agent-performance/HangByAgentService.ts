    import { Inject, Injectable } from '@nestjs/common';
    import { Db } from 'mongodb';
    import { UtilService } from 'src/commonServices/utilService';
    const Logger = require("../commonServices/logger");
    @Injectable()
    export class HangByAgentService {
    private logger: InstanceType<typeof Logger>;

    constructor(
        @Inject('MONGO_DB') private readonly db: Db,
        private readonly utilServices: UtilService,
    ) {
        this.logger = new Logger("Hang_By_Agent.log");
    }


    async hangByAgentDayWSise(db:any, year:any, month:any) {
    const collectionName = "HangUpByAgent_Daywise";
    const sourceCollection = `Agent_Performance_krph_historical_calls_${month}_${year}`;

    try {
        if (!db) {
            this.logger.error("hangByAgentDayWSise: Database instance missing");
            return false;
        }

        if (!year || !month || isNaN(year) || isNaN(month)) {
            this.logger.error("hangByAgentDayWSise: Invalid year or month");
            return false;
        }

        const numericMonth = Number(month);
        const numericYear = Number(year);

        if (numericMonth < 1 || numericMonth > 12) {
            this.logger.error("hangByAgentDayWSise: Month out of range");
            return false;
        }

        const existingRecords = await db.collection(collectionName).countDocuments({
            month: numericMonth,
            year: numericYear
        });

        const sourceRecords = await db.collection(sourceCollection).countDocuments({});

        if (existingRecords > 0 && existingRecords === sourceRecords) {
            this.logger.info(`hangByAgentDayWSise: Existing records match source; skipping regeneration`);
            return true;
        }

        await this.utilServices.cleanupCollection(
            db,
            collectionName,
            { month: numericMonth, year: numericYear }
        );

        this.logger.info(`hangByAgentDayWSise: Starting aggregation for ${numericMonth}-${numericYear}`);

        const pipeLine = [
            { $match: { Agent_ID: { $ne: "" } } },
            {
                $addFields: {
                    day: { $dayOfMonth: "$Call_Start_Time" },
                    month: { $month: "$Call_Start_Time" },
                    year: { $year: "$Call_Start_Time" },
                    date: { $dateToString: { format: "%Y-%m-%d", date: "$Call_Start_Time" } },
                    Agent_ID: { $toInt: "$Agent_ID" }
                }
            },
            { $match: { month: numericMonth, year: numericYear } },
            {
                $group: {
                    _id: {
                        Agent_ID: "$Agent_ID",
                        day: "$day",
                        month: "$month",
                        year: "$year",
                        date: "$date"
                    },
                    totalCalls: { $sum: 1 },
                    hangupByAgent: {
                        $sum: {
                            $cond: [{ $eq: ["$hangup_by", "Agent"] }, 1, 0]
                        }
                    }
                }
            },
            {
                $addFields: {
                    hangupPercentage: {
                        $multiply: [
                            { $divide: ["$hangupByAgent", "$totalCalls"] },
                            100
                        ]
                    },
                    tag: {
                        $switch: {
                            branches: [
                                { case: { $lte: ["$hangupPercentage", 0.01] }, then: "Best" },
                                {
                                    case: {
                                        $and: [
                                            { $gt: ["$hangupPercentage", 0.01] },
                                            { $lte: ["$hangupPercentage", 1] }
                                        ]
                                    },
                                    then: "Satisfactory"
                                },
                                {
                                    case: {
                                        $and: [
                                            { $gt: ["$hangupPercentage", 1] },
                                            { $lte: ["$hangupPercentage", 2] }
                                        ]
                                    },
                                    then: "Unsatisfactory"
                                },
                                { case: { $gt: ["$hangupPercentage", 2] }, then: "Low" }
                            ],
                            default: "Unknown"
                        }
                    }
                }
            },
            {
                $project: {
                    _id: 0,
                    Agent_ID: "$_id.Agent_ID",
                    date: "$_id.date",
                    day: "$_id.day",
                    month: "$_id.month",
                    year: "$_id.year",
                    totalCalls: 1,
                    hangupByAgent: 1,
                    hangupPercentage: 1,
                    tag: 1
                }
            },
            { $sort: { date: 1, Agent_ID: 1 } },
            {
                $merge: {
                    into: collectionName,
                    whenMatched: "replace",
                    whenNotMatched: "insert"
                }
            }
        ];

        await db.collection(sourceCollection).aggregate(pipeLine).toArray();

        this.logger.info(`hangByAgentDayWSise: Completed aggregation for ${numericMonth}-${numericYear}`);

        return true;
    } catch (err) {
        this.logger.error(`hangByAgentDayWSise: Failed for month=${month}, year=${year}, error=${err.message}`);
        return false;
    }
}



async hangByAgentMonthWise(db, year, month) {
    const collectionName = `HangUpByAgent_Monthwise_${month}_${year}`;
    const sourceCollection = "HangUpByAgent_Daywise";

    try {
        if (!db) {
            this.logger.error("hangByAgentMonthWise: Database instance missing");
            return false;
        }

        if (!year || !month || isNaN(year) || isNaN(month)) {
            this.logger.error("hangByAgentMonthWise: Invalid year or month");
            return false;
        }

        const numericMonth = Number(month);
        const numericYear = Number(year);

        if (numericMonth < 1 || numericMonth > 12) {
            this.logger.error("hangByAgentMonthWise: Month out of range");
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
            this.logger.info(`hangByAgentMonthWise: Existing records match source; skipping regeneration`);
            return true;
        }

        await this.utilServices.cleanupCollection(db, collectionName, {
            month: numericMonth,
            year: numericYear
        });

        this.logger.info(`hangByAgentMonthWise: Starting aggregation for ${numericMonth}-${numericYear}`);

        const pipeLine = [
            {
                $match: {
                    Agent_ID: { $ne: "" },
                    month: numericMonth,
                    year: numericYear
                }
            },
            {
                $group: {
                    _id: {
                        Agent_ID: "$Agent_ID",
                        month: "$month",
                        year: "$year"
                    },
                    totalCalls: { $sum: "$totalCalls" },
                    hangupByAgent: { $sum: "$hangupByAgent" }
                }
            },
            {
                $addFields: {
                    hangupPercentage: {
                        $cond: [
                            { $eq: ["$totalCalls", 0] },
                            0,
                            { $multiply: [{ $divide: ["$hangupByAgent", "$totalCalls"] }, 100] }
                        ]
                    }
                }
            },
            {
                $addFields: {
                    tag: {
                        $switch: {
                            branches: [
                                { case: { $eq: ["$hangupPercentage", 0] }, then: "Best" },
                                { case: { $lte: ["$hangupPercentage", 1] }, then: "Satisfactory" },
                                { case: { $lte: ["$hangupPercentage", 2] }, then: "Unsatisfactory" }
                            ],
                            default: "Low"
                        }
                    }
                }
            },
            {
                $project: {
                    _id: 0,
                    Agent_ID: "$_id.Agent_ID",
                    month: "$_id.month",
                    year: "$_id.year",
                    totalCalls: 1,
                    hangupByAgent: 1,
                    hangupPercentage: 1,
                    tag: 1
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

        this.logger.info(`hangByAgentMonthWise: Completed aggregation for ${numericMonth}-${numericYear}`);

        return true;
    } catch (err) {
        this.logger.error(`hangByAgentMonthWise: Failed for month=${month}, year=${year}, error=${err.message}`);
        return false;
    }
}


    }

