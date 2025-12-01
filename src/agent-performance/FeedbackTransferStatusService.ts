    import { Inject, Injectable } from '@nestjs/common';
    import { Db } from 'mongodb';
    import { UtilService } from 'src/commonServices/utilService';
    const Logger = require("../commonServices/logger");
    @Injectable()
    export class FeedbackTransferStatusService {
    private logger: InstanceType<typeof Logger>;

    constructor(
        @Inject('MONGO_DB') private readonly db: Db,
        private readonly utilServices: UtilService,
    ) {
        this.logger = new Logger("Feedback_Transfer_Status.log");
    }


async feedbackTransferStatusDayWise(db:any, year:any, month:any) {
    const targetCollection = "krph_feedback_status_day_wise";
    const sourceCollection = `Agent_Performance_krph_historical_calls_${month}_${year}`;

    try {
        if (!db) {
            this.logger.error("Database instance is missing");
            return false;
        }

        if (!year || !month || isNaN(year) || isNaN(month)) {
            this.logger.error(`Invalid parameters year=${year}, month=${month}`);
            return false;
        }

        await this.utilServices.cleanupCollection(db, targetCollection, { month, year });

        this.logger.info(`Starting day-wise feedback transfer status calculation for month=${month}, year=${year}`);

        const pipeline = [
            {
                $addFields: {
                    day: { $dayOfMonth: "$Call_Start_Time" },
                    month: { $month: "$Call_Start_Time" },
                    year: { $year: "$Call_Start_Time" }
                }
            },
            {
                $match: {
                    month: Number(month),
                    year: Number(year),
                    Feedback_status: { $ne: "" },
                    Agent_ID: { $ne: "" }
                }
            },
            {
                $addFields: {
                    Agent_ID: {
                        $convert: { input: "$Agent_ID", to: "int", onError: null, onNull: null }
                    }
                }
            },
            {
                $match: {
                    Agent_ID: { $ne: null }
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
                    totalFeedbacks: { $sum: 1 },
                    positiveFeedbacks: {
                        $sum: {
                            $cond: [
                                { $eq: [{ $toUpper: "$Feedback_status" }, "Y"] },
                                1,
                                0
                            ]
                        }
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
                    totalFeedbacks: 1,
                    positiveFeedbacks: 1,
                    positivePercentage: {
                        $round: [
                            {
                                $cond: [
                                    { $gt: ["$totalFeedbacks", 0] },
                                    {
                                        $multiply: [
                                            { $divide: ["$positiveFeedbacks", "$totalFeedbacks"] },
                                            100
                                        ]
                                    },
                                    0
                                ]
                            },
                            2
                        ]
                    }
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
                    into: targetCollection,
                    whenMatched: "merge",
                    whenNotMatched: "insert"
                }
            }
        ];

        const result = await db.collection(sourceCollection).aggregate(pipeline).toArray();

        this.logger.info(
            `Day-wise feedback transfer status calculation completed. Records processed: ${result?.length || 0}`
        );

        return true;

    } catch (err) {
        this.logger.error(
            `Error in feedbackTransferStatusDayWise for month=${month}, year=${year}: ${err.message}`,
            err
        );
        return false;
    }
}

async feedbackTransferStatusMonthWise(db:any, year:any, month:any) {
    const collectionName = `krph_feedback_status_month_wise_${month}_${year}`;
    const sourceCollection = "krph_feedback_status_day_wise";

    try {
        if (!db) {
            this.logger.error("feedbackTransferStatusMonthWise: Database instance missing");
            return false;
        }

        if (!year || !month || isNaN(year) || isNaN(month)) {
            this.logger.error("feedbackTransferStatusMonthWise: Invalid year or month");
            return false;
        }

        const numericMonth = Number(month);
        const numericYear = Number(year);

        if (numericMonth < 1 || numericMonth > 12) {
            this.logger.error("feedbackTransferStatusMonthWise: Month out of range");
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
            this.logger.info(`feedbackTransferStatusMonthWise: Existing records match source; skipping regeneration for ${collectionName}`);
            return true;
        }

        await this.utilServices.cleanupCollection(
            db,
            collectionName,
            { month: numericMonth, year: numericYear }
        );

        this.logger.info(`feedbackTransferStatusMonthWise: Starting aggregation for ${collectionName}`);

        const pipeLine = [
            { $match: { month: numericMonth, year: numericYear } },
            {
                $group: {
                    _id: { Agent_ID: "$Agent_ID", month: "$month", year: "$year" },
                    totalFeedbacks: { $sum: "$totalFeedbacks" },
                    positiveFeedbacks: { $sum: "$positiveFeedbacks" }
                }
            },
            {
                $addFields: {
                    positivePercentage: {
                        $round: [
                            {
                                $cond: [
                                    { $gt: ["$totalFeedbacks", 0] },
                                    { $multiply: [{ $divide: ["$positiveFeedbacks", "$totalFeedbacks"] }, 100] },
                                    0
                                ]
                            },
                            2
                        ]
                    }
                }
            },
            {
                $addFields: {
                    FeedbackPerformanceFlag: {
                        $switch: {
                            branches: [
                                { case: { $gt: ["$positivePercentage", 80] }, then: 4 },
                                {
                                    case: {
                                        $and: [
                                            { $gte: ["$positivePercentage", 70] },
                                            { $lte: ["$positivePercentage", 80] }
                                        ]
                                    },
                                    then: 3
                                },
                                {
                                    case: {
                                        $and: [
                                            { $gte: ["$positivePercentage", 50] },
                                            { $lt: ["$positivePercentage", 70] }
                                        ]
                                    },
                                    then: 2
                                },
                                { case: { $lt: ["$positivePercentage", 50] }, then: 1 }
                            ],
                            default: 0
                        }
                    }
                }
            },
            {
                $project: {
                    Agent_ID: "$_id.Agent_ID",
                    month: "$_id.month",
                    year: "$_id.year",
                    totalFeedbacks: 1,
                    positiveFeedbacks: 1,
                    positivePercentage: 1,
                    FeedbackPerformanceFlag: 1,
                    _id: 0
                }
            },
            { $sort: { Agent_ID: -1, year: 1, month: 1 } },
            {
                $merge: {
                    into: collectionName,
                    whenMatched: "merge",
                    whenNotMatched: "insert"
                }
            }
        ];

        await db.collection(sourceCollection).aggregate(pipeLine).toArray();

        this.logger.info(`feedbackTransferStatusMonthWise: Completed aggregation for ${collectionName}`);

        return true;
    } catch (error) {
        this.logger.error(`feedbackTransferStatusMonthWise: Failed for month=${month}, year=${year}, error=${error.message}`);
        return false;
    }
}

    }

