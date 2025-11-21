    import { Inject, Injectable } from '@nestjs/common';
    import { Db } from 'mongodb';
    import { UtilService } from 'src/commonServices/utilService';
    const Logger = require("../commonServices/logger");
    @Injectable()
    export class CRMTaggingCalcullationService {
    private logger: InstanceType<typeof Logger>;

    constructor(
        @Inject('MONGO_DB') private readonly db: Db,
        private readonly utilServices: UtilService,
    ) {
        this.logger = new Logger("CRM_Tagging_Calculation.log");
    }

 
    async dayWiseCRMTaggingCalculation(db:any, year:any, month:any) {
    const collectionName = "krph_agent_crm_tagging_day_wise";
    const sourceCollection = `krph_farmer_calling_history_for_agent_performance_${month}_${year}`;
    const lookupCollection = `Agent_Performance_krph_historical_calls_${month}_${year}`;

    try {
        if (!db) {
            this.logger.error("Database instance is missing");
            return false;
        }

        if (!year || !month || isNaN(year) || isNaN(month)) {
            this.logger.error(`Invalid input parameters year=${year}, month=${month}`);
            return false;
        }

        await this.utilServices.cleanupCollection(db, collectionName, { Year: year, Month: month });

        const pipeline = [
            {
                $lookup: {
                    from: lookupCollection,
                    localField: "Calling_ID",
                    foreignField: "Unique_ID",
                    as: "matchedCalls"
                }
            },
            {
                $unwind: "$matchedCalls"
            },
            {
                $match: {
                    $expr: {
                        $and: [
                            { $eq: [{ $month: "$matchedCalls.Call_Start_Time" }, Number(month)] },
                            { $eq: [{ $year: "$matchedCalls.Call_Start_Time" }, Number(year)] }
                        ]
                    }
                }
            },
            {
                $addFields: {
                    isTaggedCall: {
                        $and: [
                            { $ne: ["$State", null] },
                            { $ne: ["$District", null] },
                            { $ne: ["$State", ""] },
                            { $ne: ["$District", ""] }
                        ]
                    },
                    Call_Date: { $dateToString: { format: "%Y-%m-%d", date: "$matchedCalls.Call_Start_Time" } },
                    Call_Year: { $year: "$matchedCalls.Call_Start_Time" },
                    Call_Month: { $month: "$matchedCalls.Call_Start_Time" },
                    Call_Day: { $dayOfMonth: "$matchedCalls.Call_Start_Time" },
                    Agent_ID: {
                        $convert: {
                            input: "$matchedCalls.Agent_ID",
                            to: "int",
                            onError: null,
                            onNull: null
                        }
                    }
                }
            },
            {
                $match: { Agent_ID: { $ne: null } }
            },
            {
                $group: {
                    _id: {
                        Agent_ID: "$Agent_ID",
                        Date: "$Call_Date",
                        Day: "$Call_Day",
                        Month: "$Call_Month",
                        Year: "$Call_Year"
                    },
                    Total_Tagged_Calls: {
                        $sum: { $cond: ["$isTaggedCall", 1, 0] }
                    },
                    Total_Untagged_Calls: {
                        $sum: { $cond: ["$isTaggedCall", 0, 1] }
                    },
                    Total_Calls: { $sum: 1 }
                }
            },
            {
                $project: {
                    _id: 0,
                    Agent_ID: "$_id.Agent_ID",
                    Date: "$_id.Date",
                    Day: "$_id.Day",
                    Month: "$_id.Month",
                    Year: "$_id.Year",
                    Total_Tagged_Calls: 1,
                    Total_Untagged_Calls: 1,
                    Total_Calls: 1
                }
            },
            {
                $sort: {
                    Agent_ID: 1,
                    Year: 1,
                    Month: 1,
                    Day: 1
                }
            },
            {
                $merge: {
                    into: collectionName,
                    whenMatched: "merge",
                    whenNotMatched: "insert"
                }
            }
        ];

        this.logger.info(`Starting day-wise CRM tagging calculation for month=${month}, year=${year}`);

        const result = await db.collection(sourceCollection).aggregate(pipeline).toArray();

        this.logger.info(`Day-wise CRM tagging calculation completed. Records processed: ${result?.length || 0}`);

        return true;

    } catch (err) {
        this.logger.error(`Error in dayWiseCRMTaggingCalculation for month=${month}, year=${year}: ${err.message}`, err);
        return false;
    }
}




async monthWiseCRMTaggingCalculation(db:any, year:any, month:any) {
    const targetCollection = "krph_agent_crm_tagging_month_wise";
    const sourceCollection = "krph_agent_crm_tagging_day_wise";

    try {
        if (!db) {
            this.logger.error("Database instance is missing");
            return false;
        }

        if (!year || !month || isNaN(year) || isNaN(month)) {
            this.logger.error(`Invalid input parameters year=${year}, month=${month}`);
            return false;
        }

        await this.utilServices.cleanupCollection(db, targetCollection, { year, month });

        this.logger.info(`Starting month-wise CRM tagging calculation for month=${month}, year=${year}`);

        const pipeline = [
            {
                $match: {
                    Month: Number(month),
                    Year: Number(year)
                }
            },
            {
                $group: {
                    _id: {
                        Agent_ID: "$Agent_ID",
                        Month: "$Month",
                        Year: "$Year"
                    },
                    Total_Calls: { $sum: "$Total_Calls" },
                    Total_Tagged_Calls: { $sum: "$Total_Tagged_Calls" }
                }
            },
            {
                $addFields: {
                    Tagged_Percentage: {
                        $multiply: [
                            {
                                $cond: [
                                    { $eq: ["$Total_Calls", 0] },
                                    0,
                                    { $divide: ["$Total_Tagged_Calls", "$Total_Calls"] }
                                ]
                            },
                            100
                        ]
                    }
                }
            },
            {
                $addFields: {
                    performanceCRMTaggingFlag: {
                        $switch: {
                            branches: [
                                { case: { $gte: ["$Tagged_Percentage", 99] }, then: 4 },
                                { case: { $gte: ["$Tagged_Percentage", 95] }, then: 3 },
                                { case: { $gte: ["$Tagged_Percentage", 90] }, then: 2 }
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
                    month: "$_id.Month",
                    year: "$_id.Year",
                    Total_Calls: 1,
                    Total_Tagged_Calls: 1,
                    Tagged_Percentage: { $round: ["$Tagged_Percentage", 2] },
                    performanceCRMTaggingFlag: 1
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
            `Month-wise CRM tagging calculation completed. Records processed: ${result?.length || 0}`
        );

        return true;

    } catch (err) {
        this.logger.error(
            `Error in monthWiseCRMTaggingCalculation for month=${month}, year=${year}: ${err.message}`,
            err
        );
        return false;
    }
}




    }

