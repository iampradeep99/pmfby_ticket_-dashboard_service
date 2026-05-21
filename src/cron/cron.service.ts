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

@Cron('0 0-23/1 * * *')
async handleCronUpdateDocketNumber() {
  console.log('⏰ Cron running every 2 hours');
  try {
    const response = await this.supportTicketSyncingUpdateForDocketNumber();
    console.log(response);
  } catch (err) {
    console.error('❌ Cron failed:', err);
  }
}


  @Cron('0 * * * *')
  async handleCronUpdateDocketNumberForTicketHistory() {
    console.log('⏰ Cron running every 15 minutes');

    try {
      const response = await this.supportTicketSyncingUpdateForDocketNumberForTicketHistory();
      console.log(response);
    } catch (err) {
      console.error('❌ Cron failed:', err);
    }
  }

  @Cron('0 5 0 * * *')
  async cleanupOldSupportTicketDownloadLogs() {
    const collection = this.db.collection('support_ticket_download_logs');
    const startOfToday = new Date();
    startOfToday.setHours(0, 0, 0, 0);

    try {
      await collection.createIndex({ requestedAt: 1 }, { background: true, name: 'requestedAt_cleanup_idx' });
      await collection.createIndex({ createdAt: 1 }, { background: true, name: 'createdAt_cleanup_idx' });

      const result = await collection.deleteMany({
        status: { $nin: ['QUEUED', 'PROCESSING'] },
        $or: [
          { requestedAt: { $lt: startOfToday } },
          { requestedAt: { $exists: false }, createdAt: { $lt: startOfToday } },
        ],
      });

      console.log(`Deleted ${result.deletedCount} old support ticket download logs`);
    } catch (err) {
      console.error('Support ticket download log cleanup failed:', err);
    }
  }

  






 async supportTicketSyncingUpdateForDocketNumber(): Promise<string> {
  const MYSQL_BATCH_SIZE = 100000;
  const CHUNK_SIZE = 1000;

  try {
    const collection: Collection<any> = this.db.collection('SLA_Ticket_listing');

    // Step 1: Count rows in MySQL
    const [countResult]: any = await this.sequelize.query(`
         SELECT COUNT(*) AS totalCount
FROM mergeticketlisting
WHERE TicketHeaderID = 4
  AND TicketNCIPDocketNo IS NOT NULL
  AND InsertDateTime >= CURDATE() - INTERVAL 3 MONTH
  AND InsertDateTime < CURDATE() + INTERVAL 1 DAY
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
        SELECT TicketNCIPDocketNo, TicketHeaderID, SupportTicketID
        FROM krishi_rakshak_pro.mergeticketlisting 
        WHERE TicketHeaderID = 4 AND TicketNCIPDocketNo IS NOT NULL
         AND InsertDateTime >= CURDATE() - INTERVAL 3 MONTH
  AND InsertDateTime < CURDATE() + INTERVAL 1 DAY
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
    const subject = 'Docket Number Updation For SLA_ticket_listing';

    const text = `
Hello,
Docket Number Updation For SLA_ticket_listing has completed.

Total Rows from MySQL: ${totalRows}
Total Existing Documents Updated: ${totalUpdated}
Total Missing (not found in MongoDB): ${totalMissing}

Regards,
Your Automation System
    `;

    const html = `
<p>Hello,</p>
<p><strong>Docket Number Updation For SLA_ticket_listing has completed.
</strong></p>
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

async supportTicketSyncingUpdateForDocketNumberForTicketHistory(): Promise<string> {
  const MYSQL_BATCH_SIZE = 100000; // MySQL fetch size
  const CHUNK_SIZE = 1000;         // Mongo bulk update size

  try {
    console.log("🚀 Starting supportTicketSyncingUpdateForDocketNumberForTicketHistory...");

    const collection: Collection<any> = this.db.collection('SLA_KRPH_SupportTickets_Records');
    console.log("📂 Connected to MongoDB collection: SLA_KRPH_SupportTickets_Records");

    console.log("🧮 Counting total rows from MySQL...");

    const [countResult]: any = await this.sequelize.query(`
      SELECT COUNT(*) AS totalCount
      FROM mergeticketlisting
      WHERE TicketHeaderID = 4
        AND TicketNCIPDocketNo IS NOT NULL
        AND InsertDateTime >= CURDATE() - INTERVAL 3 MONTH
        AND InsertDateTime < CURDATE() + INTERVAL 1 DAY
    `, { type: QueryTypes.SELECT });

    const totalRows: number = countResult?.totalCount || 0;
    console.log(`📦 Total rows to sync: ${totalRows}`);

    if (totalRows === 0) {
      console.log('✅ No rows to sync today.');
      return 'No rows to sync.';
    }

    let offset = 0;
    let totalUpdated = 0;
    let totalSkipped = 0;

    console.log("🔁 Beginning main sync loop...");

    while (offset < totalRows) {
      console.log(`➡️ Fetching MySQL batch starting at offset: ${offset} (limit: ${MYSQL_BATCH_SIZE})`);

      const rows: any[] = await this.sequelize.query(`
        SELECT TicketNCIPDocketNo, TicketHeaderID, SupportTicketID
        FROM krishi_rakshak_pro.mergeticketlisting 
        WHERE TicketHeaderID = 4
          AND TicketNCIPDocketNo IS NOT NULL
          AND InsertDateTime >= CURDATE() - INTERVAL 3 MONTH
          AND InsertDateTime < CURDATE() + INTERVAL 1 DAY
        LIMIT ${MYSQL_BATCH_SIZE} OFFSET ${offset}
      `, { type: QueryTypes.SELECT });

      if (!rows.length) {
        console.log("⚠️ No more rows fetched, ending loop.");
        break;
      }

      console.log(`✅ Rows fetched in this batch: ${rows.length}`);

      for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
        const chunk = rows.slice(i, i + CHUNK_SIZE);
        console.log(`🧩 Processing chunk ${i / CHUNK_SIZE + 1} (${chunk.length} records)`);

        const bulkOps = chunk.map(record => ({
          updateOne: {
            filter: {
              TicketHeaderID: 4,
              SupportTicketID: record.SupportTicketID,
              TicketNCIPDocketNo: null // only update if null
            },
            update: {
              $set: { TicketNCIPDocketNo: record.TicketNCIPDocketNo }
            }
          }
        }));

        try {
          const result = await collection.bulkWrite(bulkOps, { ordered: false });

          const matched = result.matchedCount || 0;   // documents found for update
          const modified = result.modifiedCount || 0; // documents actually updated

          totalUpdated += modified;
          totalSkipped += chunk.length - matched;

          console.log(`✅ Chunk done → Matched: ${matched}, Updated: ${modified}, Skipped: ${chunk.length - matched}`);
        } catch (bulkErr) {
          console.error("❌ Bulk write error:", bulkErr.message);
        }

        if (global.gc) {
          global.gc();
        }
      }

      offset += MYSQL_BATCH_SIZE;
      console.log(`✅ Completed batch offset: ${offset}/${totalRows}`);
    }

    console.log("🎉 Sync completed successfully!");
    console.log(`🟢 Total Updated (filled nulls): ${totalUpdated}`);
    console.log(`🔵 Total Skipped (already had docket or missing in Mongo): ${totalSkipped}`);

    // Email summary report
    console.log("📧 Preparing email summary...");

    const to = ['pradeep.kumar@infodartmail.com'];
    const subject = 'Support Ticket Docket Number Update Report';

    const text = `
Hello Team,

The Docket Number update for Ticket History has been completed.

Summary:
- Total rows fetched from MySQL: ${totalRows}
- Total documents updated in Mongo (where null): ${totalUpdated}
- Total documents skipped (already had docket or missing in Mongo): ${totalSkipped}

Regards,
Automation System
    `;

    const html = `
<p>Hello Team,</p>
<p><strong>The Docket Number update for Ticket History has been completed.</strong></p>
<p><strong>Summary:</strong></p>
<ul>
  <li>Total rows fetched from MySQL: ${totalRows}</li>
  <li>Total documents updated in Mongo (where null): ${totalUpdated}</li>
  <li>Total documents skipped (already had docket or missing in Mongo): ${totalSkipped}</li>
</ul>
<p>Regards,<br/>Automation System</p>
    `;

    console.log("📨 Sending email report...");
    await this.mailService.sendMail({ to, subject, text, html });
    console.log("✅ Email sent successfully!");

    return '✅ Support ticket sync completed successfully.';
  } catch (err: any) {
    console.error('❌ Error during supportTicketSyncingUpdateForDocketNumberForTicketHistory:', err);
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
        this.sequelize.query<any>(
          `
          SELECT COUNT(*) as totalCount
          FROM mergeticketlisting
          WHERE DATE(InsertDateTime) = CURDATE()
        `,
          { type: QueryTypes.SELECT }
        )
      )
      .then((countResult: any[]) => {
        const totalRows = parseInt(countResult[0]?.totalCount || 0, 10);

        if (totalRows === 0) {
          return Promise.resolve({
            totalInserted: 0,
            totalSkipped: 0,
            insertedTicketNos: [],
          });
        }

        let offset = 0;
        let totalInserted = 0;
        let totalSkipped = 0;
        const insertedTicketNos: any[] = [];

        const processBatch = (): Promise<any> => {
          if (offset >= totalRows) {
            return Promise.resolve({
              totalInserted,
              totalSkipped,
              insertedTicketNos,
            });
          }

          return this.sequelize
            .query<any>(
              `
            SELECT * FROM mergeticketlisting
            WHERE DATE(InsertDateTime) = CURDATE()
            LIMIT ${MYSQL_BATCH_SIZE} OFFSET ${offset}
          `,
              { type: QueryTypes.SELECT }
            )
            .then((rows: any[]) => {
              if (!rows.length)
                return Promise.resolve({
                  totalInserted,
                  totalSkipped,
                  insertedTicketNos,
                });

              return rows
                .reduce((chunkPromise: Promise<any>, _: any, idx: number) => {
                  if (idx % CHUNK_SIZE !== 0) return chunkPromise;

                  const chunk = rows.slice(idx, idx + CHUNK_SIZE);

                  return chunkPromise.then(() => {
                    const ops = chunk.map((record: any) => {
                      if ('id' in record) delete record.id;
                      return {
                        updateOne: {
                          filter: { SupportTicketID: record.SupportTicketID },
                          update: { $setOnInsert: record },
                          upsert: true,
                        },
                      };
                    });

                    if (!ops.length) return Promise.resolve();

                    return collection
                      .bulkWrite(ops, { ordered: false })
                      .then((result: any) => {
                        const insertedCount = result.upsertedCount || 0;
                        const skippedCount = ops.length - insertedCount;

                        totalInserted += insertedCount;
                        totalSkipped += skippedCount;

                        if (result.upsertedIds) {
                          Object.values(result.upsertedIds).forEach(
                            (_: any, i: number) => {
                              if (insertedTicketNos.length < 100)
                                insertedTicketNos.push(chunk[i].SupportTicketID);
                            }
                          );
                        }

                        if (global.gc) global.gc();
                      })
                      .catch((err: any) => {
                        console.error('Bulk write error:', err);
                      });
                  });
                }, Promise.resolve())
                .then(() => {
                  offset += MYSQL_BATCH_SIZE;
                  return processBatch();
                });
            });
        };

        return processBatch();
      })
      .then(() => {
        resolve('Support ticket sync completed successfully.');
      })
      .catch((err: any) => {
        console.error('Error during support ticket sync:', err);
        reject(err);
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


  async syncTicketComments(): Promise<string> {
  const MYSQL_BATCH_SIZE = 100000;
  const CHUNK_SIZE = 1000;

  return new Promise(async (resolve, reject) => {
    try {
      const collection: Collection<any> = this.db.collection('ticket_comment_journey');

      // Count rows in MySQL
      const [countResult]: any = await this.sequelize.query(`
         SELECT COUNT(*) as totalCount FROM krishi_rakshak_pro.krph_ticketjourney
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
      SELECT * FROM krishi_rakshak_pro.krph_ticketjourney LIMIT ${MYSQL_BATCH_SIZE} OFFSET ${offset}
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
