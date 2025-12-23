import { Inject, Injectable } from '@nestjs/common';
import { Db } from 'mongodb';
import { UtilService } from 'src/commonServices/utilService';
const Logger = require("../commonServices/logger");
@Injectable()
export class AgentPerformanceRwaFileService {
    private logger: InstanceType<typeof Logger>;

    constructor(
        @Inject('MONGO_DB') private readonly db: Db,
        private readonly utilServices: UtilService,
    ) {
        this.logger = new Logger("AgentPerformanceRawFiles.log");


    }


    async fetchInboundRawData(db: any, year: number, month: number) {
        console.log("enter this ")
        let isSuccess = false;
        // db = this.db
        console.log(`Starting fetchInboundRawData for Year: ${year}, Month: ${month}`);

        try {
            let fromDate = new Date(`${year}-${month}-1`);
            let toDate = new Date(year, month, 1);
            fromDate.setUTCHours(23, 59, 59, 999);
            toDate.setUTCHours(23, 59, 59, 999);
            this.logger.info(`Calculated date range: from ${fromDate.toISOString()} to ${toDate.toISOString()}`);

            const collectionName = `Agent_Performance_krph_historical_calls_${month}_${year}`;
            const filterConditions = { Call_Start_Time: { $gte: fromDate, $lte: toDate } };

            this.logger.info(`Cleaning up existing records in ${collectionName}...`);
            await this.utilServices.cleanupCollection(db, collectionName, filterConditions);
            this.logger.info(`Cleanup complete.`);

            this.logger.info(`Starting aggregation from SLA_Inbound_Calls into ${collectionName}...`);

            let pipeline = [


                {
                    $project: {
                        Farmer_Number: 1,
                        Agent_ID: 1,
                        Agent_Name: 1,
                        Campaign_Name: 1,
                        Status: 1,

                        Call_Start_Time: {
                            $cond: {
                                if: { $eq: [{ $type: "$Call_Start_Time" }, "string"] },
                                then: {
                                    $dateFromString: {
                                        dateString: "$Call_Start_Time",
                                        format: "%Y-%m-%d %H:%M:%S"
                                    }
                                },
                                else: "$Call_Start_Time"
                            }
                        },

                        Call_End_Time: 1,
                        Agent_Call_Start_Time: 1,
                        Agent_Call_End_Time: 1,

                        Customer_Call_Sec: {
                            $toInt: {
                                $cond: {
                                    if: { $eq: ["$Customer_Call_Sec", ""] },
                                    then: 0,
                                    else: "$Customer_Call_Sec"
                                }
                            }
                        },

                        Unique_ID: 1,
                        Agent_TalkTime: 1,
                        dtmf_1: 1,
                        dtmf_2: 1,
                        Feedback_status: 1,
                        hangup_by: 1
                    }
                }
                ,
                { $match: { Call_Start_Time: { $gte: fromDate, $lte: toDate } } },
                {
                    $merge: {
                        into: collectionName,
                        whenMatched: "replace",
                        whenNotMatched: "insert"
                    }
                }
            ]

            this.logger.info(JSON.stringify(pipeline));

            await db.collection('SLA_Inbound_Calls').aggregate(pipeline).toArray();
            this.logger.info(`Aggregation completed successfully.`);

            const count = await db.collection(collectionName).countDocuments();
            this.logger.info(`Total records in ${collectionName}: ${count}`);

            if (count > 0) {
                isSuccess = true;
                this.logger.info(`Inbound historical data fetch successful for ${year}-${month}`);
            } else {
                this.logger.info(`No records found for ${year}-${month}`);
            }

        } catch (err) {
            console.log(err)
            this.logger.error(`Error while getting Inbound historical data for ${year}-${month}`, err);
        }

        this.logger.info(`fetchInboundRawData process ended for ${year}-${month}`);
        return isSuccess;
    }



    async fetchCallQualityQAData(db: any, year: any, month: any) {
        let isSuccess = false;

        try {
            const parsedYear = parseInt(year);
            const parsedMonth = parseInt(month);

            if (isNaN(parsedYear) || parsedYear < 2000 || parsedYear > 2100) {
                this.logger.error(`Invalid year provided: ${year}`);
                return false;
            }

            if (isNaN(parsedMonth) || parsedMonth < 1 || parsedMonth > 12) {
                this.logger.error(`Invalid month provided: ${month}`);
                return false;
            }

            const fromDate = new Date(Date.UTC(parsedYear, parsedMonth - 1, 1, 0, 0, 0));
            const toDate = new Date(Date.UTC(parsedYear, parsedMonth, 1, 0, 0, 0));
            const collectionName = `Agent_Performance_krph_call_quality_qa_data_${parsedMonth}_${parsedYear}`;

            this.logger.info(
                `Starting QA Data Fetch for Year=${parsedYear}, Month=${parsedMonth}, Range=${fromDate.toISOString()} to ${toDate.toISOString()}`
            );

            const filterConditions = { call_date: { $gte: fromDate, $lt: toDate } };

            try {
                await this.utilServices.cleanupCollection(db, collectionName, filterConditions);
                this.logger.info(`Old records cleaned for collection: ${collectionName}`);
            } catch (cleanupErr) {
                this.logger.error(`Cleanup failed for ${collectionName}: ${cleanupErr.message}`);
            }

            const pipeline = [
                {
                    $addFields: {
                        call_date: {
                            $dateFromString: {
                                dateString: "$call_date",
                                format: "%Y-%m-%d %H:%M:%S",
                                onError: null,
                                onNull: null
                            }
                        },
                        total_rating: {
                            $convert: {
                                input: "$total_rating",
                                to: "int",
                                onError: 0,
                                onNull: 0
                            }
                        }
                    }
                },
                {
                    $match: {
                        call_date: { $gte: fromDate, $lt: toDate }
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

            this.logger.info(`Executing aggregation for QA data`);

            const cursor = await db.collection("SLA_QA_Raw_Records").aggregate(pipeline);
            await cursor.toArray();

            const totalRecords = await db.collection(collectionName).countDocuments();

            this.logger.info(
                `QA Data fetch completed. Processed Records: ${totalRecords} into collection ${collectionName}`
            );

            isSuccess = true;
        } catch (err) {
            this.logger.error(`Error occurred while fetching Call Quality QA data for ${year}-${month}: ${err.message}`);
        }

        return isSuccess;
    }


    async fetchFramerCallingData(db: any, year: any, month: any) {
        let isSuccess = false;

        try {
            this.logger.info(`Starting fetchFramerCallingData with year=${year}, month=${month}`);

            const y = Number(year);
            const m = Number(month);

            this.logger.info(`Parsed parameters y=${y}, m=${m}`);

            if (!y || !m || m < 1 || m > 12) {
                this.logger.error(`Invalid parameters received year=${year}, month=${month}`);
                return false;
            }

            const fromDate = new Date(Date.UTC(y, m - 1, 1, 0, 0, 0, 0));
            const toDate = new Date(Date.UTC(y, m, 1, 0, 0, 0, 0));

            this.logger.info(`Date range computed from=${fromDate.toISOString()} to=${toDate.toISOString()}`);

            const collectionName = `krph_farmer_calling_history_for_agent_performance_${m}_${y}`;

            this.logger.info(`Target collection name resolved: ${collectionName}`);

            const sourceCount = await db.collection('SLA_KRPH_Farmer_Calling_Master').countDocuments({
                InsertDateTime: { $gte: fromDate, $lt: toDate }
            });

            this.logger.info(`Source count fetched: ${sourceCount}`);

            const targetCount = await db.collection(collectionName).countDocuments({
                Created_At: { $gte: fromDate, $lt: toDate }
            });

            this.logger.info(`Target count fetched: ${targetCount}`);

            if (sourceCount === targetCount && sourceCount !== 0) {
                this.logger.info(`Counts already matched for ${collectionName}. Total ${sourceCount}. Skipping processing`);
                return true;
            }

            this.logger.info(`Cleanup started for ${collectionName}`);

            await this.utilServices.cleanupCollection(db, collectionName, {
                Created_At: { $gte: fromDate, $lt: toDate }
            });

            this.logger.info(`Cleanup completed for ${collectionName}`);

            this.logger.info(`Starting aggregation for ${collectionName}`);

            try {
                await db.collection('SLA_KRPH_Farmer_Calling_Master').aggregate([
                    {
                        $project: {
                            Calling_ID: "$CallingUniqueID",
                            Caller_Mobile_No: "$CallerMobileNumber",
                            Call_Status: "$CallStatus",
                            Farmer_Name: "$FarmerName",
                            State: "$FarmerStateName",
                            District: "$FarmerDistrictName",
                            Is_Registred: "$IsRegistered",
                            Created_At: "$InsertDateTime"
                        }
                    },
                    {
                        $match: {
                            Created_At: { $gte: fromDate, $lt: toDate }
                        }
                    },
                    {
                        $merge: {
                            into: collectionName,
                            whenMatched: "replace",
                            whenNotMatched: "insert"
                        }
                    }
                ]).toArray();

                this.logger.info(`Aggregation completed successfully for ${collectionName}`);
            } catch (aggErr) {
                this.logger.error(`Aggregation failed for ${collectionName}`, aggErr);
                return false;
            }

            const finalCount = await db.collection(collectionName).countDocuments({
                Created_At: { $gte: fromDate, $lt: toDate }
            });

            this.logger.info(`Final inserted count: ${finalCount}`);

            if (finalCount === 0) {
                this.logger.warn(`No records inserted into ${collectionName} for ${month}-${year}`);
                return false;
            }

            this.logger.info(`Farmer calling history processed successfully. Total records: ${finalCount}`);
            isSuccess = true;

        } catch (err) {
            this.logger.error(`Unexpected failure in fetchFramerCallingData for ${year}-${month}`, err);
            return false;
        }

        this.logger.info(`fetchFramerCallingData completed with status=${isSuccess}`);
        return isSuccess;
    }




    async fetchAgentAcitivityData(db: any, year: any, month: any) {
        let isSuccess = false;

        try {
            this.logger.info(`Starting getAgentAcitivityData with year=${year}, month=${month}`);

            const y = Number(year);
            const m = Number(month);

            this.logger.info(`Parsed inputs year=${y}, month=${m}`);

            if (!y || !m || m < 1 || m > 12) {
                this.logger.error(`Invalid input parameters year=${year}, month=${month}`);
                return false;
            }

            const fromDate = new Date(Date.UTC(y, m - 1, 1, 23, 59, 59, 999));
            const toDate = new Date(Date.UTC(y, m, 1, 23, 59, 59, 999));

            this.logger.info(`Computed date range from=${fromDate.toISOString()} to=${toDate.toISOString()}`);

            const filter = {
                tc_date: { $gte: fromDate, $lt: toDate }
            };

            this.logger.info(`Fetching source count from SLA_Agent_Activity_Reports`);
            const sourceCount = await db.collection('SLA_Agent_Activity_Reports').countDocuments({
                tc_date: { $gte: fromDate.toISOString().slice(0, 10), $lt: toDate.toISOString().slice(0, 10) }
            });

            this.logger.info(`Source count fetched: ${sourceCount}`);

            this.logger.info(`Fetching target count from all_agent_activity_records`);
            const targetCount = await db.collection('all_agent_activity_records').countDocuments(filter);

            this.logger.info(`Target count fetched: ${targetCount}`);

            if (sourceCount === targetCount && sourceCount !== 0) {
                this.logger.info(`Record counts match. No processing required. Skipping cleanup and aggregation.`);
                return true;
            }

            this.logger.info(`Counts do not match. Cleanup required. Starting cleanup for all_agent_activity_records`);
            await this.utilServices.cleanupCollection(db, 'all_agent_activity_records', filter);
            this.logger.info(`Cleanup completed for all_agent_activity_records`);

            this.logger.info(`Starting aggregation pipeline for SLA_Agent_Activity_Reports`);

            try {
                await db.collection('SLA_Agent_Activity_Reports').aggregate([
                    {
                        $addFields: {
                            user: { $toInt: "$user" },
                            tc_date: {
                                $dateFromString: {
                                    dateString: "$tc_date",
                                    format: "%Y-%m-%d"
                                }
                            }
                        }
                    },
                    {
                        $match: {
                            tc_date: { $gte: fromDate, $lt: toDate }
                        }
                    },
                    {
                        $merge: {
                            into: 'all_agent_activity_records',
                            whenMatched: "replace",
                            whenNotMatched: "insert"
                        }
                    }
                ]).toArray();

                this.logger.info(`Aggregation executed successfully for SLA_Agent_Activity_Reports`);
            } catch (aggError) {
                this.logger.error(`Aggregation failed for all_agent_activity_records`, aggError);
                return false;
            }

            this.logger.info(`Counting final inserted records in all_agent_activity_records`);
            const finalCount = await db.collection('all_agent_activity_records').countDocuments(filter);

            this.logger.info(`Final inserted count: ${finalCount}`);

            if (finalCount === 0) {
                this.logger.warn(`No agent activity data inserted for year=${year}, month=${month}`);
                return false;
            }

            this.logger.info(`Agent activity data processed successfully. Total records=${finalCount}`);
            isSuccess = true;

        } catch (err) {
            this.logger.error(`Unexpected error in getAgentAcitivityData for year=${year}, month=${month}`, err);
            return false;
        }

        this.logger.info(`getAgentAcitivityData completed with status=${isSuccess}`);
        return isSuccess;
    }


}

