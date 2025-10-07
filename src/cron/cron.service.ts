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

 @Cron('0 */15 * * * *') // every 15 minutes
async handleCron() {
  console.log('⏰ Cron running every 15 minutes');
  this.SupportTicketInsertCronForTicketListing()
    .then((msg) => {
      console.log(msg);
    })
    .catch(err => console.error('❌ Cron failed:', err));
}

  @Cron(CronExpression.EVERY_HOUR)
  async handleCronUpdate() {
    console.log('⏰ Cron running every 30s');
    this.supportTicketSyncingUpdateForTicketListing().then((response)=>{
            console.log(response)
        }) .catch(err => console.error('❌ Cron failed:', err));
    
   
  }

  @Cron(CronExpression.EVERY_2_HOURS)
  async handleCronUpdateDocketNumber() {
    console.log('⏰ Cron running every 30s');
    this.supportTicketSyncingUpdateForDocketNumber().then((response)=>{
            console.log(response)
        }) .catch(err => console.error('❌ Cron failed:', err));
    
   
  }






 async supportTicketSyncingUpdateForDocketNumber(): Promise<string> {
  const MYSQL_BATCH_SIZE = 100000;
  const CHUNK_SIZE = 1000;

  try {
    const collection: Collection<any> = this.db.collection('SLA_Ticket_listing');

    // Step 1: Count rows in MySQL
    const [countResult]: any = await this.sequelize.query(`
      SELECT COUNT(*) as totalCount
      FROM mergeticketlisting 
      WHERE TicketHeaderID = 4 AND TicketNCIPDocketNo IS NOT NULL
    `, { type: QueryTypes.SELECT });

    const totalRows: number = countResult?.totalCount || 0;
    console.log(`📦 Total rows to sync: ${totalRows}`);

    if (totalRows === 0) {
      console.log('✅ No rows to sync today.');
      return 'No rows to sync.';
    }

    let offset = 0;
    let totalUpdated = 0;
    let totalMissing = 0;

    while (offset < totalRows) {
      console.log("➡️ Processing batch with offset:", offset);

      const rows: any[] = await this.sequelize.query(`
        SELECT TicketNCIPDocketNo, TicketHeaderID, InsertDateTime, SupportTicketID, SupportTicketNo 
        FROM krishi_rakshak_pro.mergeticketlisting 
        WHERE TicketHeaderID = 4 AND TicketNCIPDocketNo IS NOT NULL
        LIMIT ${MYSQL_BATCH_SIZE} OFFSET ${offset}
      `, { type: QueryTypes.SELECT });

      console.log(`✅ Rows fetched in this batch: ${rows.length}`);

      if (!rows.length) break;

      for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
        const chunk: any[] = rows.slice(i, i + CHUNK_SIZE);

        for (const record of chunk) {
          const result = await collection.findOneAndUpdate(
            {
              TicketHeaderID: 4,
              TicketNCIPDocketNo: { $ne: null },
              SupportTicketID: record.SupportTicketID,

            },
            {
              $set: {
                TicketNCIPDocketNo: record.TicketNCIPDocketNo
              }
            },
            { returnDocument: 'after' }
          );

          if (result?.value) totalUpdated++;
          else totalMissing++;
        }

        // Optional: Force garbage collection to prevent memory bloat
        if (global.gc) global.gc();
      }

      offset += MYSQL_BATCH_SIZE;
      console.log(`✅ Processed offset: ${offset}/${totalRows}`);
    }

    console.log('🎉 Support ticket listing sync completed.');
    console.log(`🟢 Total Updated: ${totalUpdated}`);
    console.log(`🔴 Total Missing (not found in MongoDB): ${totalMissing}`);

    // Email report
    const to = ['pmfbysystems@gmail.com'];
    const subject = 'Support Ticket listing Data Update Completed';

    const text = `
Hello,

The Support Ticket listing data update process has completed.

Total Rows from MySQL: ${totalRows}
Total Existing Documents Updated: ${totalUpdated}
Total Missing (not found in MongoDB): ${totalMissing}

Regards,
Your Automation System
    `;

    const html = `
<p>Hello,</p>
<p><strong>The Support Ticket listing data update process has completed.</strong></p>
<p><strong>Total Rows from MySQL:</strong> ${totalRows}</p>
<p><strong>Total Existing Documents Updated:</strong> ${totalUpdated}</p>
<p><strong>Total Missing (not updated):</strong> ${totalMissing}</p>
<p>Regards,<br/>Your Automation System</p>
    `;

    await this.mailService.sendMail({ to, subject, text, html });

    return '✅ Support ticket sync completed successfully.';

  } catch (err: any) {
    console.error('❌ Error during supportTicketSyncing:', err);
    throw err;
  }
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
            FROM mergeticketlisting
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
              SELECT * FROM mergeticketlisting
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



 
  async supportTicketSyncingUpdateForTicketListing(): Promise<string> {
  const MYSQL_BATCH_SIZE = 100000;
  const CHUNK_SIZE = 1000;

  return new Promise(async (resolve, reject) => {
    try {
      const collection: Collection<any> = this.db.collection('SLA_Ticket_listing');

      // Count rows in MySQL
      const [countResult]: any = await this.sequelize.query(`
         SELECT COUNT(*) as totalCount
FROM mergeticketlisting 
          WHERE DATE(StatusUpdateTime) 
    BETWEEN DATE(CURDATE() - INTERVAL 1 DAY) 
    AND DATE(CURDATE() + INTERVAL 1 DAY)
      `, { type: QueryTypes.SELECT });

      const totalRows: number = countResult?.totalCount || 0;
      console.log(`📦 Total rows to sync: ${totalRows}`);

      if (totalRows === 0) {
        console.log('✅ No rows to sync today.');
        return resolve('No rows to sync.');
      }

      let offset = 0;
      let totalUpdated = 0;
      let totalMissing = 0;

      const processBatch = async (): Promise<void> => {
        if (offset >= totalRows) return;
        console.log("➡️ Processing batch with offset:", offset);

        const rows: any[] = await this.sequelize.query(`
          SELECT 
    InsertDateTime, 
    StatusUpdateTime, 
    TicketStatus, 
    TicketStatusID, 
    SupportTicketID,
    TicketReOpenDate, 
    TicketNCIPDocketNo, 
    SupportTicketNo
FROM mergeticketlisting 
WHERE DATE(StatusUpdateTime) 
    BETWEEN DATE(CURDATE() - INTERVAL 1 DAY) 
    AND DATE(CURDATE() + INTERVAL 1 DAY)
          LIMIT ${MYSQL_BATCH_SIZE} OFFSET ${offset}
        `, { type: QueryTypes.SELECT });

        console.log(`✅ Rows fetched in this batch: ${rows.length}`);

        if (!rows.length) return;

        for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
          const chunk: any[] = rows.slice(i, i + CHUNK_SIZE);

          for (const record of chunk) {
            const result = await collection.findOneAndUpdate(
              { SupportTicketID: record.SupportTicketID },
              {
                $set: {
                  InsertDateTime: record.InsertDateTime,
                  StatusUpdateTime: record.StatusUpdateTime,
                  TicketStatus: record.TicketStatus,
                  TicketStatusID: record.TicketStatusID,
                  TicketReOpenDate: record.TicketReOpenDate,
                  TicketNCIPDocketNo: record.TicketNCIPDocketNo,
                  SupportTicketNo: record.SupportTicketNo
                }
              },
              { returnDocument: 'after' }
            );
            console.log(result)

            if (result) totalUpdated++;
            else totalMissing++;
          }

          if (global.gc) global.gc();
        }

        offset += MYSQL_BATCH_SIZE;
        console.log(`✅ Processed offset: ${offset}/${totalRows}`);
        await processBatch();
      };

      await processBatch();

      console.log('🎉 Support ticket listing sync completed.');
      console.log(`🟢 Total Updated: ${totalUpdated}`);
      console.log(`🔴 Total Missing (not found in MongoDB): ${totalMissing}`);

      const to = ['pmfbysystems@gmail.com'];
      const subject = 'Support Ticket listing Data Update Completed';
      const text = `
Hello,

The Support Ticket listing data update process has completed.

Criteria:
- InsertDateTime ≠ Today
- StatusUpdateTime = Today

Total Rows from MySQL: ${totalRows}
Total Existing Documents Updated: ${totalUpdated}
Total Missing (not updated): ${totalMissing}

Regards,
Your Automation System
      `;
      const html = `
<p>Hello,</p>
<p><strong>The Support Ticket listing data update process has completed.</strong></p>
<p><strong>Criteria:</strong></p>
<ul>
  <li><code>InsertDateTime</code> ≠ Today</li>
  <li><code>StatusUpdateTime</code> = Today</li>
</ul>
<p><strong>Total Rows from MySQL:</strong> ${totalRows}</p>
<p><strong>Total Existing Documents Updated:</strong> ${totalUpdated}</p>
<p><strong>Total Missing (not updated):</strong> ${totalMissing}</p>
<p>Regards,<br/>Your Automation System</p>
      `;

      await this.mailService.sendMail({ to, subject, text, html });

      resolve('✅ Support ticket sync completed successfully.');

    } catch (err: any) {
      console.error('❌ Error during supportTicketSyncing:', err);
      reject(err);
    }
  });
}



}
