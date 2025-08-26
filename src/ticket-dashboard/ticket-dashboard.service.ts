import { Injectable, Inject } from '@nestjs/common';
import { Db, Collection } from 'mongodb';
import * as NodeCache from 'node-cache';


@Injectable()
export class TicketDashboardService {
  private ticketCollection: Collection;
  private ticketDbCollection: Collection;
  private cache: NodeCache;

  constructor(@Inject('MONGO_DB') private readonly db: Db) {
    this.ticketCollection = this.db.collection('tickets');
    this.ticketDbCollection = this.db.collection('SLA_KRPH_SupportTickets_Records');

    this.cache = new NodeCache({ stdTTL: 300 }); 
  }

   async createTicket(ticketData: any): Promise<any> {
        const result = await this.ticketCollection.insertOne(ticketData);
        return {
            message: 'Ticket created successfully',
            ticketId: result.insertedId
        };
    }

  async fetchTickets(ticketInfo: any): Promise<any> {
    const cacheKey = 'ticket-stats';

    const cachedData = this.cache.get(cacheKey);
    if (cachedData) {
      return cachedData;
    }

    const pipeline = [
      {
        $facet: {
          Grievance: [
            { $match: { TicketHeaderID: 1 } },
            { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
            { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } }
          ],
          Information: [
            { $match: { TicketHeaderID: 2 } },
            {
              $group: {
                _id: {
                  status: "$TicketStatus",
                  head: "$TicketHeadName",
                  code: "$BMCGCode"
                },
                Total: { $sum: 1 }
              }
            },
            {
              $project: {
                _id: 0,
                TicketStatus: {
                  $cond: [
                    { $eq: ["$_id.code", 109025] },
                    { $concat: ["$_id.status", " (", "$_id.head", ")"] },
                    "$_id.status"
                  ]
                },
                Total: 1
              }
            }
          ],
          CropLoss: [
            { $match: { TicketHeaderID: 4 } },
            { $group: { _id: "$TicketStatus", Total: { $sum: 1 } } },
            { $project: { _id: 0, TicketStatus: "$_id", Total: 1 } }
          ]
        }
      }
    ];

    const result = await this.ticketDbCollection.aggregate(pipeline).toArray();
    const response = result[0];

    this.cache.set(cacheKey, response);

    return response;
  }
}
