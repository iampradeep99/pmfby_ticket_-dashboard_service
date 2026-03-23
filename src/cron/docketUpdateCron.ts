import { Injectable, Inject } from "@nestjs/common";
import { Cron, CronExpression } from "@nestjs/schedule";
import { Db, Collection } from "mongodb";
import { Sequelize } from "sequelize-typescript";
import { MailService } from "../mail/mail.service";
import { QueryTypes } from "sequelize"; // ✅ import QueryTypes

@Injectable()
export class DocketUpdateCron {
  constructor(
    @Inject("SEQUELIZE") private readonly sequelize: Sequelize,
    @Inject("MONGO_DB") private readonly db: Db,
    private mailService: MailService
  ) {}

  @Cron(CronExpression.EVERY_4_HOURS)
  // async handleCronUpdate() {
  //   console.log("⏰ Docket Update Cron Triggered");
  //   await this.docketUpdateTickets();
  // }

  async docketUpdateTickets() {
    const sourceCollection = "SLA_Ticket_listing";
    const BATCH_SIZE = 500;

    const query = {
      TicketHeaderID: 4,
      $or: [{ TicketNCIPDocketNo: "" }, { TicketNCIPDocketNo: null }],
      InsertDateTime: {
        $gte: new Date(new Date().setMonth(new Date().getMonth() - 3)),
      },
    };

    const startTime = Date.now();
    console.log("🚀 docketUpdateTickets started");
    console.log("📌 Mongo Query:", JSON.stringify(query));

    try {
      const totalRecords = await this.db
        .collection(sourceCollection)
        .countDocuments(query);

      if (!totalRecords) {
        console.log("ℹ️ No tickets found without docket number");
        return;
      }

      const totalBatches = Math.ceil(totalRecords / BATCH_SIZE);

      console.log(`📊 Total eligible tickets: ${totalRecords}`);
      console.log(`📦 Batch size: ${BATCH_SIZE}`);
      console.log(`📦 Total batches to process: ${totalBatches}`);

      const cursor = this.db
        .collection(sourceCollection)
        .find(query)
        .project({
          SupportTicketNo: 1,
          TicketNCIPDocketNo: 1,
          InsertDateTime: 1,
        })
        .batchSize(BATCH_SIZE);

      let batch: any[] = [];
      let processed = 0;
      let batchCount = 0;

      while (await cursor.hasNext()) {
        const doc = await cursor.next();
        if (!doc) continue;

        batch.push(doc);

        if (batch.length === BATCH_SIZE) {
          batchCount++;
          console.log(
            `\n▶️ Starting batch ${batchCount}/${totalBatches} | Records: ${batch.length}`
          );

          await this.processBatch(batch, batchCount, totalRecords, processed);

          processed += batch.length;
          const progress = ((processed / totalRecords) * 100).toFixed(2);
          console.log(
            `📈 Overall Progress: ${processed}/${totalRecords} (${progress}%)`
          );

          batch = [];
        }
      }

      if (batch.length) {
        batchCount++;
        console.log(
          `\n▶️ Starting FINAL batch ${batchCount}/${totalBatches} | Records: ${batch.length}`
        );

        await this.processBatch(batch, batchCount, totalRecords, processed);
        processed += batch.length;
      }

      const timeTaken = ((Date.now() - startTime) / 1000).toFixed(2);

      console.log("\n🏁 docketUpdateTickets completed");
      console.log(`📦 Total batches processed: ${batchCount}`);
      console.log(`📊 Total tickets processed: ${processed}`);
      console.log(`⏱️ Time taken: ${timeTaken}s`);
    } catch (err) {
      console.error("❌ docketUpdateTickets error:", err);
      throw err;
    }
  }

  async processBatch(
    tickets: any[],
    batchNo: number,
    totalRecords: number,
    processedSoFar: number
  ) {
    try {
      console.log(`\n   📤 Batch ${batchNo}: Preparing MySQL query`);

      // Deduplicate ticket numbers
      const supportTicketNos = [
        ...new Set(tickets.map((t) => t.SupportTicketNo)),
      ];
      console.log(
        `   📤 Batch ${batchNo}: Unique ticketNos = ${supportTicketNos.length}`
      );

      // Fetch docket numbers from MySQL
      const rows = await this.sequelize.query(
        `
      SELECT SupportTicketNo, TicketNCIPDocketNo
      FROM krishi_rakshak_pro.mergeticketlisting
      WHERE SupportTicketNo IN (:ticketNos)
        AND TicketNCIPDocketNo IS NOT NULL
        AND TicketNCIPDocketNo <> ''
      `,
        {
          replacements: { ticketNos: supportTicketNos },
          type: QueryTypes.SELECT,
        }
      );

      console.log(
        `   📥 Batch ${batchNo}: MySQL rows fetched = ${rows.length}`
      );

      if (!rows.length) {
        console.log(`   ⚠️ Batch ${batchNo}: No docket numbers found`);
        return;
      }

      // Map SupportTicketNo -> DocketNo
      const docketMap = new Map(
        (rows as any[]).map((r) => [r.SupportTicketNo, r.TicketNCIPDocketNo])
      );

      // Prepare MongoDB bulk updates
      const bulkUpdate = tickets
        .filter((t) => docketMap.has(t.SupportTicketNo))
        .map((t) => ({
          updateOne: {
            filter: {
              SupportTicketNo: t.SupportTicketNo,
              $or: [{ TicketNCIPDocketNo: "" }, { TicketNCIPDocketNo: null }],
            },
            update: {
              $set: {
                TicketNCIPDocketNo: docketMap.get(t.SupportTicketNo),
                docketUpdatedAt: new Date(),
              },
            },
          },
        }));

      console.log(
        `   🛠️ Batch ${batchNo}: Mongo updates prepared = ${bulkUpdate.length}`
      );

      if (bulkUpdate.length > 0) {
        const result = await this.db
          .collection("SLA_Ticket_listing")
          .bulkWrite(bulkUpdate, { ordered: false });

        console.log(
          `   ✅ Batch ${batchNo}: Mongo updated | matched=${result.matchedCount}, modified=${result.modifiedCount}`
        );
      } else {
        console.log(`   ⚠️ Batch ${batchNo}: Nothing to update in Mongo`);
      }

      // Log overall progress
      const processedAfterBatch = processedSoFar + tickets.length;
      const progress = ((processedAfterBatch / totalRecords) * 100).toFixed(2);
      console.log(
        `📈 Overall Progress after batch ${batchNo}: ${processedAfterBatch}/${totalRecords} (${progress}%)`
      );
    } catch (err) {
      console.error(`❌ Batch ${batchNo} failed`, err);
    }
  }
}
