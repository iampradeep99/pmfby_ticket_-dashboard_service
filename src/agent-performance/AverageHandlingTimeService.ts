//     import { Inject, Injectable } from '@nestjs/common';
//     import { Db } from 'mongodb';
//     import { UtilService } from 'src/commonServices/utilService';
//     const Logger = require("../commonServices/logger");
//     @Injectable()
//     export class AverageHandlingTimeService {
//     private logger: InstanceType<typeof Logger>;

//     constructor(
//         @Inject('MONGO_DB') private readonly db: Db,
//         private readonly utilServices: UtilService,
//     ) {
//         this.logger = new Logger("AgentPerformanceRawFiles.log");
        

//     }

//     async agentAverageHandlingTime(db:any, year:any, month:any) {
//     const logPrefix = 'agentAverageHandlingTime';

//     try {
//         this.logger.info(`${logPrefix} started`);

//         if (!db) {
//             this.logger.error(`${logPrefix} failed: invalid database instance`);
//             throw new Error('Invalid database instance');
//         }

//         const y = Number(year);
//         const m = Number(month);

//         if (!y || !m || m < 1 || m > 12) {
//             this.logger.error(`${logPrefix} failed: invalid year or month`);
//             throw new Error('Invalid year or month');
//         }

//         const collectionName = `Agent_Performance_krph_historical_calls_${m}_${y}`;

//         this.logger.info(`${logPrefix} processing for ${collectionName}`);

//         const result = await this.logicAgentAverageHandlingTime(db, y, m, collectionName);

//         if (result !== true) {
//             this.logger.error(`${logPrefix} failed: processing returned false`);
//             throw new Error('Failed to process agent AHT data');
//         }

//         this.logger.info(`${logPrefix} completed successfully`);
//         return true;

//     } catch (err) {
//         this.logger.error(`Error in ${logPrefix}: ${err.message}`);
//         throw err;
//     }
// }


// async logicAgentAverageHandlingTime(db:any, year:any, month:any, collectionName:any) {
//     const logPrefix = 'logicAgentAverageHandlingTime';

//     try {
//         this.logger.info(`${logPrefix} started for ${collectionName} month=${month} year=${year}`);

//         if (!db) {
//             this.logger.error(`${logPrefix} failed: invalid database instance`);
//             throw new Error('Invalid database instance');
//         }

//         const y = Number(year);
//         const m = Number(month);

//         if (!y || !m || m < 1 || m > 12) {
//             this.logger.error(`${logPrefix} failed: invalid year or month`);
//             throw new Error('Invalid year or month');
//         }

//         const collectionNameForAht = 'krph_agent_daywise_AHT';
//         const ahtFilter = { month: m, year: y };

//         this.logger.info(`${logPrefix} cleaning previous records in ${collectionNameForAht}`);
//         await this.utilServices.cleanupCollection(db, collectionNameForAht, ahtFilter);

//         const pipeline = [
//             { $match: { Agent_ID: { $ne: '' }, Agent_TalkTime: { $ne: '' } } },
//             {
//                 $addFields: {
//                     Agent_TalkTime: { $toInt: '$Agent_TalkTime' },
//                     Agent_ID: { $toInt: '$Agent_ID' },
//                     Call_End_Date: { $toDate: '$Call_End_Time' }
//                 }
//             },
//             {
//                 $match: {
//                     $expr: {
//                         $and: [
//                             { $eq: [{ $year: '$Call_End_Date' }, y] },
//                             { $eq: [{ $month: '$Call_End_Date' }, m] }
//                         ]
//                     }
//                 }
//             },
//             {
//                 $group: {
//                     _id: {
//                         Agent_ID: '$Agent_ID',
//                         Date: { $dayOfMonth: '$Call_End_Date' },
//                         Month: { $month: '$Call_End_Date' },
//                         Year: { $year: '$Call_End_Date' }
//                     },
//                     avgTalkTimeSeconds: { $avg: '$Agent_TalkTime' },
//                     totalTalkTimeSeconds: { $sum: '$Agent_TalkTime' },
//                     totalCallsForDay: { $sum: 1 }
//                 }
//             },
//             {
//                 $addFields: {
//                     AHT_Seconds: { $round: ['$avgTalkTimeSeconds', 2] },
//                     AHT_Minutes: { $round: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 2] },
//                     totalTalkTimeMinutes: { $round: [{ $divide: ['$totalTalkTimeSeconds', 60] }, 2] },
//                     Category: {
//                         $switch: {
//                             branches: [
//                                 {
//                                     case: {
//                                         $and: [
//                                             { $gte: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 4] },
//                                             { $lt: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 5] }
//                                         ]
//                                     },
//                                     then: '4-5 minutes (Green)'
//                                 },
//                                 {
//                                     case: {
//                                         $and: [
//                                             { $gte: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 3] },
//                                             { $lt: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 4] }
//                                         ]
//                                     },
//                                     then: '3-4 minutes (Yellow)'
//                                 },
//                                 {
//                                     case: {
//                                         $and: [
//                                             { $gte: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 2] },
//                                             { $lt: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 3] }
//                                         ]
//                                     },
//                                     then: '2-3 minutes (Orange)'
//                                 }
//                             ],
//                             default: 'Outside Range (<2 or ≥5 minutes - Red)'
//                         }
//                     }
//                 }
//             },
//             {
//                 $project: {
//                     Agent_ID: '$_id.Agent_ID',
//                     date: '$_id.Date',
//                     month: '$_id.Month',
//                     year: '$_id.Year',
//                     AHT_Seconds: 1,
//                     AHT_Minutes: 1,
//                     Category: 1,
//                     totalCallsForDay: 1,
//                     totalTalkTimeSeconds: 1,
//                     totalTalkTimeMinutes: 1,
//                     _id: 0
//                 }
//             },
//             {
//                 $merge: {
//                     into: collectionNameForAht,
//                     whenMatched: 'merge',
//                     whenNotMatched: 'insert'
//                 }
//             }
//         ];

//         this.logger.info(`${logPrefix} executing aggregation pipeline on ${collectionName}`);
//         await db.collection(collectionName).aggregate(pipeline).toArray();

//         this.logger.info(`${logPrefix} completed successfully for month=${m}, year=${y}`);
//         return true;
//     } catch (err) {
//         this.logger.error(`${logPrefix} error: ${err.message} | collection=${collectionName} month=${month} year=${year}`);
//         throw err;
//     }
// }



// async logic_agentAvegrageHandlingTime_monthWise(db:any, year:any, month:any) {
//     const logPrefix = 'logic_agentAvegrageHandlingTime_monthWise';

//     try {
//         this.logger.info(`${logPrefix} started for month=${month} year=${year}`);

//         if (!db) {
//             this.logger.error(`${logPrefix} failed: invalid database instance`);
//             throw new Error('Invalid database instance');
//         }

//         const y = Number(year);
//         const m = Number(month);

//         if (!y || !m || m < 1 || m > 12) {
//             this.logger.error(`${logPrefix} failed: invalid year or month`);
//             throw new Error('Invalid year or month');
//         }

//         const targetCollection = 'krph_agent_moonthwise_AHT';
//         const filter = { month: m, year: y };

//         this.logger.info(`${logPrefix} cleaning existing records in ${targetCollection}`);
//         await this.utilServices.cleanupCollection(db, targetCollection, filter);

//         const pipeline = [
//             { $match: { month: m, year: y } },
//             {
//                 $group: {
//                     _id: {
//                         agent: '$Agent_ID',
//                         month: '$month',
//                         year: '$year'
//                     },
//                     averageAHTSeconds: { $avg: '$AHT_Seconds' },
//                     totalCallsForMonth: { $sum: 1 },
//                     totalCallSecondsForMonth: { $sum: '$AHT_Seconds' }
//                 }
//             },
//             {
//                 $addFields: {
//                     averageAHTMinutes: { $divide: ['$averageAHTSeconds', 60] },
//                     totalCallMinutesForMonth: { $divide: ['$totalCallSecondsForMonth', 60] }
//                 }
//             },
//             {
//                 $addFields: {
//                     PerformanceFlag: {
//                         $switch: {
//                             branches: [
//                                 {
//                                     case: { $and: [{ $gte: ['$averageAHTMinutes', 4] }, { $lt: ['$averageAHTMinutes', 5] }] },
//                                     then: 'Best performer'
//                                 },
//                                 {
//                                     case: { $and: [{ $gte: ['$averageAHTMinutes', 3] }, { $lt: ['$averageAHTMinutes', 4] }] },
//                                     then: 'Satisfactory'
//                                 },
//                                 {
//                                     case: { $and: [{ $gte: ['$averageAHTMinutes', 2] }, { $lt: ['$averageAHTMinutes', 3] }] },
//                                     then: 'Unsatisfactory'
//                                 },
//                                 {
//                                     case: {
//                                         $or: [
//                                             { $and: [{ $gte: ['$averageAHTMinutes', 5] }, { $lte: ['$averageAHTMinutes', 6] }] },
//                                             { $lt: ['$averageAHTMinutes', 2] },
//                                             { $gt: ['$averageAHTMinutes', 6] }
//                                         ]
//                                     },
//                                     then: 'Lowest performer'
//                                 }
//                             ],
//                             default: 'Unknown'
//                         }
//                     },
//                     performanceAHTFlag: {
//                         $switch: {
//                             branches: [
//                                 {
//                                     case: { $and: [{ $gte: ['$averageAHTMinutes', 4] }, { $lt: ['$averageAHTMinutes', 5] }] },
//                                     then: 4
//                                 },
//                                 {
//                                     case: { $and: [{ $gte: ['$averageAHTMinutes', 3] }, { $lt: ['$averageAHTMinutes', 4] }] },
//                                     then: 3
//                                 },
//                                 {
//                                     case: { $and: [{ $gte: ['$averageAHTMinutes', 2] }, { $lt: ['$averageAHTMinutes', 3] }] },
//                                     then: 2
//                                 },
//                                 {
//                                     case: {
//                                         $or: [
//                                             { $and: [{ $gte: ['$averageAHTMinutes', 5] }, { $lte: ['$averageAHTMinutes', 6] }] },
//                                             { $lt: ['$averageAHTMinutes', 2] },
//                                             { $gt: ['$averageAHTMinutes', 6] }
//                                         ]
//                                     },
//                                     then: 1
//                                 }
//                             ],
//                             default: 0
//                         }
//                     }
//                 }
//             },
//             {
//                 $project: {
//                     Agent_ID: '$_id.agent',
//                     month: '$_id.month',
//                     year: '$_id.year',
//                     PerformanceFlag: 1,
//                     performanceAHTFlag: 1,
//                     AverageAHTSeconds: '$averageAHTSeconds',
//                     AverageAHTMinutes: '$averageAHTMinutes',
//                     totalCallsForMonth: 1,
//                     totalCallSecondsForMonth: 1,
//                     totalCallMinutesForMonth: 1
//                 }
//             },
//             {
//                 $merge: {
//                     into: targetCollection,
//                     whenMatched: 'merge',
//                     whenNotMatched: 'insert'
//                 }
//             }
//         ];

//         this.logger.info(`${logPrefix} executing aggregation pipeline from krph_agent_daywise_AHT`);
//         await db.collection('krph_agent_daywise_AHT').aggregate(pipeline).toArray();

//         this.logger.info(`${logPrefix} completed successfully for month=${m}, year=${y}`);
//         return true;

//     } catch (err) {
//         this.logger.error(`${logPrefix} error: ${err.message} month=${month} year=${year}`);
//         throw err;
//     }
// }



//     }


import { Inject, Injectable } from '@nestjs/common';
import { Db } from 'mongodb';
import { UtilService } from 'src/commonServices/utilService';
const Logger = require('../commonServices/logger');

@Injectable()
export class AverageHandlingTimeService {
    private logger: InstanceType<typeof Logger>;

    constructor(
        @Inject('MONGO_DB') private readonly db: Db,
        private readonly utilServices: UtilService
    ) {
        this.logger = new Logger('AgentPerformanceRawFiles.log');
    }

    /**
     * Main wrapper to calculate average handling time for agents
     */
    async agentAverageHandlingTime(db: any, year: any, month: any) {
        const logPrefix = 'agentAverageHandlingTime';

        try {
            this.logger.info(`${logPrefix} started`);

            if (!db) throw new Error('Invalid database instance');

            const y = Number(year);
            const m = Number(month);

            if (!y || !m || m < 1 || m > 12)
                throw new Error('Invalid year or month');

            const collectionName = `Agent_Performance_krph_historical_calls_${m}_${y}`;

            this.logger.info(`${logPrefix} processing collection: ${collectionName}`);

            const result = await this.logicAgentAverageHandlingTime(
                db,
                y,
                m,
                collectionName
            );

            if (result !== true)
                throw new Error('AHT aggregation failed');

            this.logger.info(`${logPrefix} completed successfully`);
            return true;
        } catch (err: any) {
            this.logger.error(`${logPrefix} error: ${err.message}`);
            throw err;
        }
    }

    /**
     * Computes AHT Day-wise and stores into krph_agent_daywise_AHT
     */
    async logicAgentAverageHandlingTime(
        db: any,
        year: number,
        month: number,
        sourceCollectionName: string
    ) {
        const logPrefix = 'logicAgentAverageHandlingTime';

        try {
            this.logger.info(
                `${logPrefix} started for ${sourceCollectionName} [${month}/${year}]`
            );

            const collectionNameForAHT = 'krph_agent_daywise_AHT';
            const filter = { month, year };

            // Cleanup older processed records
            await this.utilServices.cleanupCollection(db, collectionNameForAHT, filter);

            const pipeline = [
                { $match: { Agent_ID: { $ne: '' }, Agent_TalkTime: { $ne: '' } } },
                {
                    $addFields: {
                        Agent_ID: { $toInt: '$Agent_ID' },
                        Agent_TalkTime: { $toInt: '$Agent_TalkTime' },
                        Call_End_Date: { $toDate: '$Call_End_Time' }
                    }
                },
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: [{ $year: '$Call_End_Date' }, year] },
                                { $eq: [{ $month: '$Call_End_Date' }, month] }
                            ]
                        }
                    }
                },
                {
                    $group: {
                        _id: {
                            Agent_ID: '$Agent_ID',
                            Date: { $dayOfMonth: '$Call_End_Date' },
                            Month: { $month: '$Call_End_Date' },
                            Year: { $year: '$Call_End_Date' }
                        },
                        avgTalkTimeSeconds: { $avg: '$Agent_TalkTime' },
                        totalTalkTimeSeconds: { $sum: '$Agent_TalkTime' },
                        totalCallsForDay: { $sum: 1 }
                    }
                },
                {
                    $addFields: {
                        AHT_Seconds: { $round: ['$avgTalkTimeSeconds', 2] },
                        AHT_Minutes: {
                            $round: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 2]
                        },
                        totalTalkTimeMinutes: {
                            $round: [{ $divide: ['$totalTalkTimeSeconds', 60] }, 2]
                        },
                        Category: {
                            $switch: {
                                branches: [
                                    {
                                        case: {
                                            $and: [
                                                { $gte: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 4] },
                                                { $lt: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 5] }
                                            ]
                                        },
                                        then: '4-5 minutes (Green)'
                                    },
                                    {
                                        case: {
                                            $and: [
                                                { $gte: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 3] },
                                                { $lt: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 4] }
                                            ]
                                        },
                                        then: '3-4 minutes (Yellow)'
                                    },
                                    {
                                        case: {
                                            $and: [
                                                { $gte: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 2] },
                                                { $lt: [{ $divide: ['$avgTalkTimeSeconds', 60] }, 3] }
                                            ]
                                        },
                                        then: '2-3 minutes (Orange)'
                                    }
                                ],
                                default: 'Outside Range (<2 or ≥5 minutes - Red)'
                            }
                        }
                    }
                },
                {
                    $project: {
                        Agent_ID: '$_id.Agent_ID',
                        date: '$_id.Date',
                        month: '$_id.Month',
                        year: '$_id.Year',
                        AHT_Seconds: 1,
                        AHT_Minutes: 1,
                        Category: 1,
                        totalCallsForDay: 1,
                        totalTalkTimeSeconds: 1,
                        totalTalkTimeMinutes: 1,
                        _id: 0
                    }
                },
                {
                    $merge: {
                        into: collectionNameForAHT,
                        whenMatched: 'merge',
                        whenNotMatched: 'insert'
                    }
                }
            ];

            await db.collection(sourceCollectionName).aggregate(pipeline).toArray();

            this.logger.info(
                `${logPrefix} completed successfully for [${month}/${year}]`
            );
            return true;
        } catch (err: any) {
            this.logger.error(
                `${logPrefix} error: ${err.message} | month=${month} year=${year}`
            );
            throw err;
        }
    }

    /**
     * Computes AHT Monthly summary from Day-wise AHT
     */
    async logic_agentAvegrageHandlingTime_monthWise(
        db: any,
        year: number,
        month: number
    ) {
        const logPrefix = 'logic_agentAvegrageHandlingTime_monthWise';

        try {
            this.logger.info(`${logPrefix} started for ${month}/${year}`);

            const targetCollection = 'krph_agent_moonthwise_AHT';
            const filter = { month, year };

            // Remove old month data
            await this.utilServices.cleanupCollection(db, targetCollection, filter);

            const pipeline = [
                { $match: { month, year } },
                {
                    $group: {
                        _id: {
                            Agent_ID: '$Agent_ID',
                            month: '$month',
                            year: '$year'
                        },
                        averageAHTSeconds: { $avg: '$AHT_Seconds' },
                        totalCallsForMonth: { $sum: 1 },
                        totalCallSecondsForMonth: { $sum: '$AHT_Seconds' }
                    }
                },
                {
                    $addFields: {
                        averageAHTMinutes: {
                            $round: [{ $divide: ['$averageAHTSeconds', 60] }, 2]
                        },
                        totalCallMinutesForMonth: {
                            $round: [{ $divide: ['$totalCallSecondsForMonth', 60] }, 2]
                        }
                    }
                },
                {
                    $addFields: {
                        PerformanceFlag: {
                            $switch: {
                                branches: [
                                    {
                                        case: {
                                            $and: [
                                                { $gte: ['$averageAHTMinutes', 4] },
                                                { $lt: ['$averageAHTMinutes', 5] }
                                            ]
                                        },
                                        then: 'Best performer'
                                    },
                                    {
                                        case: {
                                            $and: [
                                                { $gte: ['$averageAHTMinutes', 3] },
                                                { $lt: ['$averageAHTMinutes', 4] }
                                            ]
                                        },
                                        then: 'Satisfactory'
                                    },
                                    {
                                        case: {
                                            $and: [
                                                { $gte: ['$averageAHTMinutes', 2] },
                                                { $lt: ['$averageAHTMinutes', 3] }
                                            ]
                                        },
                                        then: 'Unsatisfactory'
                                    },
                                    {
                                        case: {
                                            $or: [
                                                {
                                                    $and: [
                                                        { $gte: ['$averageAHTMinutes', 5] },
                                                        { $lte: ['$averageAHTMinutes', 6] }
                                                    ]
                                                },
                                                { $lt: ['$averageAHTMinutes', 2] },
                                                { $gt: ['$averageAHTMinutes', 6] }
                                            ]
                                        },
                                        then: 'Lowest performer'
                                    }
                                ],
                                default: 'Unknown'
                            }
                        },
                        performanceAHTFlag: {
                            $switch: {
                                branches: [
                                    {
                                        case: {
                                            $and: [
                                                { $gte: ['$averageAHTMinutes', 4] },
                                                { $lt: ['$averageAHTMinutes', 5] }
                                            ]
                                        },
                                        then: 4
                                    },
                                    {
                                        case: {
                                            $and: [
                                                { $gte: ['$averageAHTMinutes', 3] },
                                                { $lt: ['$averageAHTMinutes', 4] }
                                            ]
                                        },
                                        then: 3
                                    },
                                    {
                                        case: {
                                            $and: [
                                                { $gte: ['$averageAHTMinutes', 2] },
                                                { $lt: ['$averageAHTMinutes', 3] }
                                            ]
                                        },
                                        then: 2
                                    },
                                    {
                                        case: {
                                            $or: [
                                                {
                                                    $and: [
                                                        { $gte: ['$averageAHTMinutes', 5] },
                                                        { $lte: ['$averageAHTMinutes', 6] }
                                                    ]
                                                },
                                                { $lt: ['$averageAHTMinutes', 2] },
                                                { $gt: ['$averageAHTMinutes', 6] }
                                            ]
                                        },
                                        then: 1
                                    }
                                ],
                                default: 0
                            }
                        }
                    }
                },
                {
                    $project: {
                        Agent_ID: '$_id.Agent_ID',
                        month: '$_id.month',
                        year: '$_id.year',
                        PerformanceFlag: 1,
                        performanceAHTFlag: 1,
                        AverageAHTSeconds: '$averageAHTSeconds',
                        AverageAHTMinutes: '$averageAHTMinutes',
                        totalCallsForMonth: 1,
                        totalCallSecondsForMonth: 1,
                        totalCallMinutesForMonth: 1
                    }
                },
                {
                    $merge: {
                        into: targetCollection,
                        whenMatched: 'merge',
                        whenNotMatched: 'insert'
                    }
                }
            ];

            await db.collection('krph_agent_daywise_AHT').aggregate(pipeline).toArray();

            this.logger.info(
                `${logPrefix} completed successfully for ${month}/${year}`
            );
            return true;
        } catch (err: any) {
            this.logger.error(
                `${logPrefix} error: ${err.message} | month=${month} year=${year}`
            );
            throw err;
        }
    }
}


