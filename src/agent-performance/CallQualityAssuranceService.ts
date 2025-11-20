    import { Inject, Injectable } from '@nestjs/common';
    import { Db } from 'mongodb';
    import { UtilService } from 'src/commonServices/utilService';
    const Logger = require("../commonServices/logger");
    @Injectable()
    export class CallQualityAssuranceService {
    private logger: InstanceType<typeof Logger>;

    constructor(
        @Inject('MONGO_DB') private readonly db: Db,
        private readonly utilServices: UtilService,
    ) {
        this.logger = new Logger("Call_Quality_Assurance_Service.log");
    }


    async callQualityAssuranceDayWise(db: any, year: number, month: number) {
  const collectionName = `Agent_Performance_krph_call_quality_qa_data_${month}_${year}`;
  const targetCollection = `callQualityDayWise`;

  try {
    // Validate input
    if (!db || !year || !month) {
      this.logger.error(`Invalid parameters => db:${!!db}, year:${year}, month:${month}`);
      return { status: false, message: "Invalid parameters" };
    }

    // Cleanup previous month/year data
    await this.utilServices.cleanupCollection(
      db,
      targetCollection,
      { month, year }
    );

    const pipeline = [
      {
        $addFields: {
          day: { $dayOfMonth: "$call_date" },
          month: { $month: "$call_date" },
          year: { $year: "$call_date" },
          date: {
            $dateToString: { format: "%Y-%m-%d", date: "$call_date" }
          },
          agent_id: { $toInt: "$agent_id" },
          total_rating: { $toDouble: "$total_rating" }
        }
      },
      {
        $match: { month, year }
      },
      {
        $group: {
          _id: {
            agent_id: "$agent_id",
            date: "$date",
            day: "$day",
            month: "$month",
            year: "$year",
            location: "$location"
          },
          averageRating: { $avg: "$total_rating" },
          totalEvaluations: { $sum: 1 }
        }
      },
      {
        $project: {
          _id: 0,
          Agent_ID: "$_id.agent_id",
          location: "$_id.location",
          date: "$_id.date",
          day: "$_id.day",
          month: "$_id.month",
          year: "$_id.year",
          averageRating: { $round: ["$averageRating", 2] },
          totalEvaluations: 1
        }
      },
      {
        $sort: { date: 1, Agent_ID: 1 }
      },
      {
        $merge: {
          into: targetCollection,
          whenMatched: "merge",
          whenNotMatched: "insert"
        }
      }
    ];

    // Run aggregation
    const result = await db.collection(collectionName).aggregate(pipeline).toArray();

    this.logger.info(
      `Call Quality Assurance Day Wise executed successfully | Records Processed: ${result.length}`
    );

    return { status: true, message: "Processed successfully", count: result.length };

  } catch (error) {
    this.logger.error("Error in callQualityAssuranceDayWise => ", error);
    return { status: false, message: "Internal server error" };
  }
}



async callQualityAssuranceMonthWise(db: any, year: number, month: number) {
  const sourceCollection = `Agent_Performance_krph_call_quality_qa_data_${month}_${year}`;
  const targetCollection = `Agent_Performance_krph_call_quality_qa_data_Monthwise_${month}_${year}`;

  try {
    // Validate inputs
    if (!db || !year || !month) {
      this.logger.error(`Invalid parameters | db:${!!db}, year:${year}, month:${month}`);
      return { status: false, message: "Invalid parameters" };
    }

    // Remove old month-year records
    await this.utilServices.cleanupCollection(db, targetCollection, { month, year });

    const pipeline = [
      {
        $addFields: {
          agent_id_int: { $toInt: "$agent_id" },
          month: { $month: "$call_date" },
          year: { $year: "$call_date" }
        }
      },
      {
        $match: {
          $expr: {
            $and: [
              { $eq: ["$month", month] },
              { $eq: ["$year", year] }
            ]
          }
        }
      },
      {
        $group: {
          _id: { agent_id: "$agent_id_int" },
          avg_total_rating: { $avg: "$total_rating" },
          doc_count: { $sum: 1 },
          agent_name: { $first: "$agent_name" },
          month: { $first: "$month" },
          year: { $first: "$year" }
        }
      },
      {
        $addFields: {
          performanceFlag: {
            $switch: {
              branches: [
                { case: { $gte: ["$avg_total_rating", 95] }, then: 4 },
                {
                  case: {
                    $and: [
                      { $gte: ["$avg_total_rating", 90] },
                      { $lt: ["$avg_total_rating", 95] }
                    ]
                  },
                  then: 3
                },
                {
                  case: {
                    $and: [
                      { $gte: ["$avg_total_rating", 80] },
                      { $lt: ["$avg_total_rating", 90] }
                    ]
                  },
                  then: 2
                },
                { case: { $lt: ["$avg_total_rating", 80] }, then: 1 }
              ],
              default: 0
            }
          }
        }
      },
      {
        $project: {
          _id: 0,
          agent_id: "$_id.agent_id",
          agent_name: 1,
          month: 1,
          year: 1,
          avg_total_rating: { $round: ["$avg_total_rating", 2] },
          total_days: "$doc_count",
          performanceFlag: 1
        }
      },
      {
        $sort: { avg_total_rating: -1 }
      },
      {
        $merge: {
          into: targetCollection,
          whenMatched: "replace",
          whenNotMatched: "insert"
        }
      }
    ];

    const result = await db.collection(sourceCollection).aggregate(pipeline).toArray();

    this.logger.info(
      `Call Quality Assurance Month Wise executed | Records Processed: ${result.length}`
    );

    return { status: true, message: "Processed successfully", count: result.length };

  } catch (error) {
    this.logger.error("Error in callQualityAssuranceMonthWise =>", error);
    return { status: false, message: "Internal server error" };
  }
}





    }

