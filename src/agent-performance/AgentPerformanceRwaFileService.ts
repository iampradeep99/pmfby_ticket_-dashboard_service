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

        let pipeline =    [
            {
            $project: {
                Farmer_Number: 1,
                Agent_ID: 1,
                Agent_Name: 1,
                Campaign_Name: 1,
                Status: 1,
                Call_Start_Time: {
                $dateFromString: { dateString: "$Call_Start_Time", format: "%Y-%m-%d %H:%M:%S" }
                },
                Call_End_Time: 1,
                Agent_Call_Start_Time: 1,
                Agent_Call_End_Time: 1,
                Customer_Call_Sec: {
  $toInt: {
    $cond: {
      if: { $eq: ["$Customer_Call_Sec", ""] },  // Check if the value is an empty string
      then: 0,  // If it is, return 0
      else: "$Customer_Call_Sec"  // Else, use the value and convert it to integer
    }
  }
}
,
                Unique_ID: 1,
                Agent_TalkTime: 1,
                dtmf_1: 1,
                dtmf_2: 1,
                Feedback_status: 1,
                hangup_by: 1
            }
            },
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
    }
