    import { Inject, Injectable } from '@nestjs/common';
    import { Db } from 'mongodb';
    import { UtilService } from 'src/commonServices/utilService';
    const Logger = require("../commonServices/logger");
    @Injectable()
    export class CustomerRatingService {
    private logger: InstanceType<typeof Logger>;

    constructor(
        @Inject('MONGO_DB') private readonly db: Db,
        private readonly utilServices: UtilService,
    ) {
        this.logger = new Logger("Customer_Rating.log");
    }

    async dayWiseCustomerRatingForAgent(db, year, month) {
    const targetCollection = "krph_agent_customer_rating_daywise";
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

        this.logger.info(`Starting day-wise customer rating calculation for month=${month}, year=${year}`);

        const pipeline = [
            {
                $addFields: {
                    month: { $month: "$Call_Start_Time" },
                    year: { $year: "$Call_Start_Time" }
                }
            },
            {
                $match: {
                    Feedback_status: "Y",
                    dtmf_1: { $ne: "" },
                    dtmf_2: { $ne: "" },
                    Agent_ID: { $ne: "" },
                    month: Number(month),
                    year: Number(year)
                }
            },
            {
                $addFields: {
                    Agent_ID: { $convert: { input: "$Agent_ID", to: "int", onError: null, onNull: null } },
                    dtmf_1: { $convert: { input: "$dtmf_1", to: "int", onError: null, onNull: null } },
                    dtmf_2: { $convert: { input: "$dtmf_2", to: "int", onError: null, onNull: null } },
                    Agent_TalkTime: { $convert: { input: "$Agent_TalkTime", to: "int", onError: 0, onNull: 0 } },
                    day: { $dayOfMonth: "$Call_Start_Time" },
                    date: { $dateToString: { format: "%Y-%m-%d", date: "$Call_Start_Time" } }
                }
            },
            {
                $match: {
                    Agent_ID: { $ne: null },
                    dtmf_1: { $ne: null },
                    dtmf_2: { $ne: null }
                }
            },
            {
                $group: {
                    _id: {
                        Agent_ID: "$Agent_ID",
                        year: "$year",
                        month: "$month",
                        day: "$day",
                        date: "$date"
                    },
                    totalCalls: { $sum: 1 },
                    avgRating: { $avg: { $avg: ["$dtmf_1", "$dtmf_2"] } },
                    totalTalkTimeSec: { $sum: "$Agent_TalkTime" },
                    avgTalkTimeSec: { $avg: "$Agent_TalkTime" }
                }
            },
            {
                $project: {
                    _id: 0,
                    Agent_ID: "$_id.Agent_ID",
                    year: "$_id.year",
                    month: "$_id.month",
                    day: "$_id.day",
                    date: "$_id.date",
                    totalCalls: 1,
                    avgRating: { $round: ["$avgRating", 2] },
                    totalTalkTimeSec: 1,
                    avgTalkTimeSec: { $round: ["$avgTalkTimeSec", 2] }
                }
            },
            {
                $sort: { Agent_ID: 1, date: 1 }
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
            `Day-wise customer rating calculation completed. Records processed: ${result?.length || 0}`
        );

        return true;

    } catch (err) {
        this.logger.error(
            `Error in dayWiseCustomerRatingForAgent for month=${month}, year=${year}: ${err.message}`,
            err
        );
        return false;
    }
}



async monthWiseCustomerRatingForAgent(db, year, month) {
    const targetCollection = `krph_agent_customer_rating_monthwise_${month}_${year}`;
    const sourceCollection = "krph_agent_customer_rating_daywise";

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

        this.logger.info(`Starting month-wise customer rating calculation for month=${month}, year=${year}`);

        const pipeline = [
            {
                $match: {
                    month: Number(month),
                    year: Number(year)
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
                    avgRatingOverall: { $avg: "$avgRating" }
                }
            },
            {
                $addFields: {
                    avgRatingOverall: {
                        $cond: [
                            { $eq: ["$totalCalls", 0] },
                            0,
                            "$avgRatingOverall"
                        ]
                    },
                    avgRatingPercentage: {
                        $round: [
                            {
                                $multiply: [
                                    { $divide: ["$avgRatingOverall", 5] },
                                    100
                                ]
                            },
                            2
                        ]
                    },
                    avgRatingRounded: { $round: ["$avgRatingOverall", 2] },
                    customerRatingFlag: {
                        $switch: {
                            branches: [
                                { case: { $gte: ["$avgRatingOverall", 4.8] }, then: 4 },
                                { case: { $gte: ["$avgRatingOverall", 4.0] }, then: 3 },
                                { case: { $gte: ["$avgRatingOverall", 3.0] }, then: 2 }
                            ],
                            default: 1
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
                    avgRatingOverall: "$avgRatingRounded",
                    avgRatingPercentage: 1,
                    customerRatingFlag: 1
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
            `Month-wise customer rating calculation completed. Records processed: ${result?.length || 0}`
        );

        return true;

    } catch (err) {
        this.logger.error(
            `Error in monthWiseCustomerRatingForAgent for month=${month}, year=${year}: ${err.message}`,
            err
        );
        return false;
    }
}



    }

