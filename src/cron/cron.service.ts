import { Injectable, Inject } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { Db, Collection } from 'mongodb';
import { Sequelize } from 'sequelize-typescript';
import { MailService } from '../mail/mail.service';
import { QueryTypes } from 'sequelize'; // ✅ import QueryTypes

@Injectable()
export class CronService {
  constructor(
    @Inject('SEQUELIZE') private readonly sequelize: Sequelize,
    @Inject('MONGO_DB') private readonly db: Db,
    private mailService: MailService
  ) {}

  @Cron(CronExpression.EVERY_5_MINUTES)
  async handleCron() {
    console.log('⏰ Cron running every 30s');
    this.SupportTicketInsertCronForTicketListing()
      .then(msg => console.log(msg))
      .catch(err => console.error('❌ Cron failed:', err));
  }

  async SupportTicketInsertCronForTicketListing(): Promise<string> {
    const MYSQL_BATCH_SIZE = 1000000;
    const CHUNK_SIZE = 10000;
    const collection: Collection<any> = this.db.collection('SLA_Ticket_listing');

    return new Promise((resolve, reject) => {
      collection
        .createIndex({ SupportTicketID: 1 }, { unique: true, name: 'uniq_ticket_no' })
        .then(() =>
          this.sequelize.query<any>(`
            SELECT COUNT(*) as totalCount
            FROM krph_ticketview
            WHERE DATE(InsertDateTime) = CURDATE()
          `, { type: QueryTypes.SELECT })
        )
        .then((countResult: any[]) => {
          const totalRows = parseInt(countResult[0]?.totalCount || 0, 10);
          console.log(`📦 Total rows to process: ${totalRows}`);
          if (totalRows === 0) {
            console.log('✅ No new tickets to process today.');
            return Promise.resolve({ totalInserted: 0, totalSkipped: 0, insertedTicketNos: [] });
          }

          let offset = 0;
          let totalInserted = 0;
          let totalSkipped = 0;
          const insertedTicketNos: any[] = [];

          const processBatch = (): Promise<any> => {
            if (offset >= totalRows) {
              return Promise.resolve({ totalInserted, totalSkipped, insertedTicketNos });
            }

            return this.sequelize.query<any>(`
              SELECT * FROM krph_ticketview
              WHERE DATE(InsertDateTime) = CURDATE()
              LIMIT ${MYSQL_BATCH_SIZE} OFFSET ${offset}
            `, { type: QueryTypes.SELECT })
              .then((rows: any[]) => {
                if (!rows.length) return Promise.resolve({ totalInserted, totalSkipped, insertedTicketNos });
                console.log(`🚀 Processing batch at offset ${offset} with ${rows.length} rows`);

                return rows.reduce((chunkPromise: Promise<any>, _, idx: number) => {
                  if (idx % CHUNK_SIZE !== 0) return chunkPromise;

                  const chunk = rows.slice(idx, idx + CHUNK_SIZE);

                  return chunkPromise.then(() => {
                    const ops = chunk.map((record: any) => {
                      if ('id' in record) delete record.id;
                      return {
                        updateOne: {
                          filter: { SupportTicketID: record.SupportTicketID },
                          update: { $setOnInsert: record },
                          upsert: true
                        }
                      };
                    });

                    if (!ops.length) return Promise.resolve();

                    return collection.bulkWrite(ops, { ordered: false })
                      .then((result: any) => {
                        const insertedCount = result.upsertedCount || 0;
                        const skippedCount = ops.length - insertedCount;

                        totalInserted += insertedCount;
                        totalSkipped += skippedCount;

                        if (result.upsertedIds) {
                          Object.values(result.upsertedIds).forEach((_: any, i: number) => {
                            if (insertedTicketNos.length < 100) insertedTicketNos.push(chunk[i].SupportTicketID);
                          });
                        }

                        console.log(`✅ Inserted ${insertedCount}, ⏭️ Skipped ${skippedCount} in this chunk.`);

                        if (global.gc) global.gc();
                      })
                      .catch((err: any) => {
                        console.error('❌ Bulk write error:', err);
                      });
                  });
                }, Promise.resolve()).then(() => {
                  offset += MYSQL_BATCH_SIZE;
                  console.log(`📊 Processed offset: ${offset}/${totalRows}`);
                  return processBatch(); // recursive call
                });
              });
          };

          return processBatch();
        })
        .then(({ totalInserted, totalSkipped, insertedTicketNos }: any) => {
          console.log('🎉 Ticket sync completed.');
          console.log(`🟢 Total Inserted: ${totalInserted}`);
          console.log(`⏭️ Total Skipped: ${totalSkipped}`);

          const to = ['pmfbysystems@gmail.com'];
          const subject = '✅ Support Tickets Synced (Today)';
          const text = `
Hello,

The support ticket sync process completed successfully.

Criteria:
- InsertDateTime = Today

Summary:
- Total New Tickets Inserted: ${totalInserted}
- Total Skipped (already in MongoDB): ${totalSkipped}

Sample Inserted Ticket Numbers (first 100):
${insertedTicketNos.join(', ')}

Regards,
Automated System
          `;

          const html = `
<p><strong>The support ticket sync process completed successfully.</strong></p>
<p><strong>Summary:</strong></p>
<ul>
<li>Total New Tickets Inserted: <strong>${totalInserted}</strong></li>
<li>Total Skipped (already in MongoDB): <strong>${totalSkipped}</strong></li>
</ul>
<p><strong>Sample Inserted Ticket Numbers (first 100):</strong></p>
<pre>${insertedTicketNos.join(', ')}</pre>
<p>Regards,<br/>Automated System</p>
          `;

          return this.mailService.sendMail({ to, subject, text, html });
        })
        .then(() => resolve('✅ Support ticket sync completed successfully.'))
        .catch((err: any) => {
          console.error('❌ Fatal error during support ticket sync:', err);
          this.mailService.sendMail({
            to: ['pmfbysystems@gmail.com'],
            subject: '❌ Support Ticket Sync Failed',
            text: `An error occurred during the support ticket sync process:\n\n${err.stack}`,
            html: `<p>An error occurred during the support ticket sync process:</p><pre>${err.stack}</pre>`
          }).finally(() => reject(err));
        });
    });
  }
}
