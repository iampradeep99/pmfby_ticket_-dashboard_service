// import { Injectable, Inject } from "@nestjs/common";
// import { Cron, CronExpression } from "@nestjs/schedule";
// import { Db } from "mongodb";
// import { Sequelize } from "sequelize-typescript";
// import { UtilService } from "src/commonServices/utilService";

// @Injectable()
// export class TicketEscalationCron {
//   private readonly ROLE_CONFIG_COLLECTION = "Ticke_Escalation_User_Config";
//   private readonly TICKET_ASSIGN_COLLECTION = "Ticket_Assignment";
//   private readonly SLA_AUDIT_COLLECTION = "Ticket_SLA_Audit";

//   constructor(
//     @Inject("SEQUELIZE") private readonly sequelize: Sequelize,
//     @Inject("MONGO_DB") private readonly db: Db,
//     private readonly commonService: UtilService
//   ) {}

//   // ⚠️ For testing only → use EVERY_30_MINUTES in prod
//   @Cron(CronExpression.EVERY_10_SECONDS)
//   async handleCronUpdate() {
//     console.log("⏰ TicketEscalationCron started", new Date().toISOString());

//     try {
//       const roleConfigMap = await this.fetchRoleConfigMap();
//       await this.syncSlaAudit(roleConfigMap);
//       await this.processReminders(roleConfigMap);
//       await this.processEscalations(roleConfigMap);
//       await this.processAutoDispose(roleConfigMap);

//       console.log("✅ TicketEscalationCron finished");
//     } catch (err) {
//       console.error("❌ TicketEscalationCron error:", err);
//     }
//   }

//   // ---------- helpers ----------
//   getDateOnly(date: Date): string {
//     return date.toISOString().split("T")[0]; // YYYY-MM-DD
//   }

//   // ---------- role config ----------
//   async fetchRoleConfigMap(): Promise<Map<number, any>> {
//     const configs = await this.db
//       .collection(this.ROLE_CONFIG_COLLECTION)
//       .find({})
//       .toArray();

//     const map = new Map<number, any>();
//     for (const cfg of configs) {
//       map.set(cfg.RoleID, cfg);
//     }
//     return map;
//   }

//   async syncSlaAudit(roleConfigMap: Map<number, any>) {
//     const tickets = await this.db
//       .collection(this.TICKET_ASSIGN_COLLECTION)
//       .find({ TicketStatus: "In-Progress" })
//       .toArray();

//     for (const ticket of tickets) {
//       const exists = await this.db
//         .collection(this.SLA_AUDIT_COLLECTION)
//         .findOne({
//           SupportTicketID: ticket.SupportTicketID,
//           assignedTo: ticket.assignedTo,
//           AssignedDate: ticket.AssignedDate
//         });

//       if (exists) continue;

//       const config = roleConfigMap.get(ticket.AssigneeRoleID);
//       if (!config) continue;

//       const levelDueAt = new Date(
//         new Date(ticket.AssignedDate).getTime() +
//           config.AllowedDays * 24 * 60 * 60 * 1000
//       );

//       await this.db.collection(this.SLA_AUDIT_COLLECTION).insertOne({
//         SupportTicketID: ticket.SupportTicketID,
//         SupportTicketNo: ticket.SupportTicketNo,
//         assignedTo: ticket.assignedTo,
//         AssigneeRoleID: ticket.AssigneeRoleID,
//         AssigneRoleName: ticket.AssigneRoleName,
//         AssignedDate: ticket.AssignedDate,

//         levelDueAt,
//         nextReminderIndex: 0,
//         lastReminderDate: null,
//         escalatedAt: null,
//         autoDisposedAt: null,

//         createdAt: new Date(),
//         updatedAt: new Date()
//       });
//     }
//   }

//   async processReminders(roleConfigMap: Map<number, any>) {
//     const now = new Date();
//     const today = this.getDateOnly(now);

//     const records = await this.db
//       .collection(this.SLA_AUDIT_COLLECTION)
//       .find({ autoDisposedAt: null })
//       .toArray();

//     for (const record of records) {
//       const config = roleConfigMap.get(record.AssigneeRoleID);
//       if (!config || !config.ReminderDays?.length) continue;

//       const reminderIndex = record.nextReminderIndex ?? 0;
//       if (reminderIndex >= config.ReminderDays.length) continue;

//       if (record.lastReminderDate === today) continue;

//       const reminderDay = config.ReminderDays[reminderIndex];

//       const daysPassed =
//         (now.getTime() - new Date(record.AssignedDate).getTime()) /
//         (1000 * 60 * 60 * 24);

//       if (daysPassed >= reminderDay) {
//         const ticket = await this.db
//           .collection(this.TICKET_ASSIGN_COLLECTION)
//           .findOne({
//             SupportTicketID: record.SupportTicketID,
//             assignedTo: record.assignedTo
//           });

//         if (!ticket) continue;

//         await this.sendReminder(this.db, ticket, record.levelDueAt);

//         await this.db.collection(this.SLA_AUDIT_COLLECTION).updateOne(
//           { _id: record._id },
//           {
//             $set: {
//               nextReminderIndex: reminderIndex + 1,
//               lastReminderDate: today,
//               updatedAt: new Date()
//             }
//           }
//         );

//         console.log(
//           `🔔 Reminder ${reminderIndex + 1} sent for ticket ${ticket.SupportTicketNo} on ${today}`
//         );
//       }
//     }
//   }

//   async processEscalations(roleConfigMap: Map<number, any>) {
//     const now = new Date();

//     const records = await this.db
//       .collection(this.SLA_AUDIT_COLLECTION)
//       .find({ escalatedAt: null })
//       .toArray();

//     for (const record of records) {
//       const config = roleConfigMap.get(record.AssigneeRoleID);
//       if (!config?.AutoEscalateAfterDays) continue;

//       const escalateAt = new Date(
//         new Date(record.AssignedDate).getTime() +
//           config.AutoEscalateAfterDays * 24 * 60 * 60 * 1000
//       );

//       if (now >= escalateAt) {
//         const ticket = await this.db
//           .collection(this.TICKET_ASSIGN_COLLECTION)
//           .findOne({
//             SupportTicketID: record.SupportTicketID,
//             assignedTo: record.assignedTo
//           });

//         if (!ticket) continue;

//         await this.triggerEscalation(ticket);

//         await this.db.collection(this.SLA_AUDIT_COLLECTION).updateOne(
//           { _id: record._id },
//           {
//             $set: {
//               escalatedAt: new Date(),
//               updatedAt: new Date()
//             }
//           }
//         );
//       }
//     }
//   }

//   async processAutoDispose(roleConfigMap: Map<number, any>) {
//     const now = new Date();

//     const records = await this.db
//       .collection(this.SLA_AUDIT_COLLECTION)
//       .find({ autoDisposedAt: null })
//       .toArray();

//     for (const record of records) {
//       const config = roleConfigMap.get(record.AssigneeRoleID);
//       if (!config?.AutoDispose) continue;

//       const disposeAt = new Date(
//         new Date(record.AssignedDate).getTime() +
//           config.AllowedDays * 24 * 60 * 60 * 1000
//       );

//       if (now >= disposeAt) {
//         console.log(`🗑️ Auto disposing ticket ${record.SupportTicketNo}`);

//         await this.db.collection(this.SLA_AUDIT_COLLECTION).updateOne(
//           { _id: record._id },
//           {
//             $set: {
//               autoDisposedAt: new Date(),
//               updatedAt: new Date()
//             }
//           }
//         );
//       }
//     }
//   }

//   // ---------- ACTIONS ----------
//   async triggerEscalation(ticket: any) {
//     console.log(`🚀 Auto escalated ticket ${ticket.SupportTicketNo}`);
//   }

//   async sendReminder(db: any, ticket: any, dueAt: Date) {
//     const payload = {
//       Name: ticket.assignToName || "User",
//       ticket: ticket.SupportTicketNo,
//       mobileNO: ticket.AssigneeMobileNo
//     };

//     await this.commonService.sendSMSToUser(db, payload);
//   }
// }





import { Injectable, Inject } from "@nestjs/common";
import { Cron, CronExpression } from "@nestjs/schedule";
import { Db } from "mongodb";
import { Sequelize } from "sequelize-typescript";
import { UtilService } from "src/commonServices/utilService";

@Injectable()
export class TicketEscalationCron {
  private readonly ROLE_CONFIG_COLLECTION = "Ticke_Escalation_User_Config";
  private readonly TICKET_ASSIGN_COLLECTION = "Ticket_Assignment";
  private readonly SLA_AUDIT_COLLECTION = "Ticket_SLA_Audit";
  private readonly MILLISECONDS_IN_A_DAY = 24 * 60 * 60 * 1000;

  constructor(
    @Inject("SEQUELIZE") private readonly sequelize: Sequelize,
    @Inject("MONGO_DB") private readonly db: Db,
    private readonly commonService: UtilService
  ) {}

  @Cron(CronExpression.EVERY_MINUTE)
  async handleCronUpdate() {
    console.log("TicketEscalationCron started", new Date().toISOString());

    try {
      const roleConfigMap = await this.fetchRoleConfigMap();

      await this.syncSlaAudit(roleConfigMap);
      await this.processReminders(roleConfigMap);
      await this.processEscalations(roleConfigMap);
      await this.processAutoDispose(roleConfigMap);

      console.log("TicketEscalationCron finished");
    } catch (err) {
      console.error("TicketEscalationCron error:", err);
    }
  }

  getDateOnly(date: Date): string {
    return date.toISOString().split("T")[0];
  }

  async fetchRoleConfigMap(): Promise<Map<number, any>> {
    const configs = await this.db
      .collection(this.ROLE_CONFIG_COLLECTION)
      .find({})
      .toArray();

    const map = new Map<number, any>();
    for (const cfg of configs) map.set(cfg.RoleID, cfg);
    return map;
  }

  async syncSlaAudit(roleConfigMap: Map<number, any>) {
    const cursor = this.db
      .collection(this.TICKET_ASSIGN_COLLECTION)
      .find({ TicketStatus: "In-Progress" });

    while (await cursor.hasNext()) {
      const ticket = await cursor.next();
      if (!ticket) continue;

      const exists = await this.db
        .collection(this.SLA_AUDIT_COLLECTION)
        .findOne({
          SupportTicketID: ticket.SupportTicketID,
          assignedTo: ticket.assignedTo,
          AssignedDate: ticket.AssignedDate,
        });

      if (exists) continue;

      const config = roleConfigMap.get(ticket.AssigneeRoleID);
      if (!config) continue;

      const levelDueAt = new Date(
        new Date(ticket.AssignedDate).getTime() +
          config.AllowedDays * this.MILLISECONDS_IN_A_DAY
      );

      await this.db.collection(this.SLA_AUDIT_COLLECTION).insertOne({
        SupportTicketID: ticket.SupportTicketID,
        SupportTicketNo: ticket.SupportTicketNo,
        assignedTo: ticket.assignedTo,
        AssigneeRoleID: ticket.AssigneeRoleID,
        AssigneeRoleName: ticket.AssigneeRoleName,
        AssignedDate: ticket.AssignedDate,
        levelDueAt,
        nextReminderIndex: 0,
        lastReminderDate: null,
        escalatedAt: null,
        autoDisposedAt: null,
        createdAt: new Date(),
        updatedAt: new Date(),
      });
    }
  }

  async processReminders(roleConfigMap: Map<number, any>) {
    const now = new Date();
    const today = this.getDateOnly(now);

    const cursor = this.db
      .collection(this.SLA_AUDIT_COLLECTION)
      .find({ autoDisposedAt: null });

    while (await cursor.hasNext()) {
      const record = await cursor.next();
      if (!record) continue;

      const config = roleConfigMap.get(record.AssigneeRoleID);
      if (!config?.ReminderDays?.length) continue;

      const reminderIndex = record.nextReminderIndex ?? 0;
      if (reminderIndex >= config.ReminderDays.length) continue;
      if (record.lastReminderDate === today) continue;

      const reminderDay = config.ReminderDays[reminderIndex];
      const daysPassed =
        (now.getTime() - new Date(record.AssignedDate).getTime()) /
        this.MILLISECONDS_IN_A_DAY;

      if (daysPassed >= reminderDay) {
        const ticket = await this.getTicket(
          record.SupportTicketID,
          record.assignedTo
        );
        if (!ticket) continue;

        this.sendReminder(ticket, record.levelDueAt).catch(err =>
          console.error("Reminder error:", err)
        );

        await this.db.collection(this.SLA_AUDIT_COLLECTION).updateOne(
          { _id: record._id },
          {
            $set: {
              nextReminderIndex: reminderIndex + 1,
              lastReminderDate: today,
              updatedAt: new Date(),
            },
          }
        );

        console.log(
          `Reminder ${reminderIndex + 1} sent for ticket ${ticket.SupportTicketNo} on ${today}`
        );
      }
    }
  }

  async processEscalations(roleConfigMap: Map<number, any>) {
    const now = new Date();

    const cursor = this.db
      .collection(this.SLA_AUDIT_COLLECTION)
      .find({ escalatedAt: null });

    while (await cursor.hasNext()) {
      const record = await cursor.next();
      if (!record) continue;

      const config = roleConfigMap.get(record.AssigneeRoleID);
      if (!config?.AutoEscalateAfterDays) continue;

      const escalateAt = new Date(
        new Date(record.AssignedDate).getTime() +
          config.AutoEscalateAfterDays * this.MILLISECONDS_IN_A_DAY
      );

      if (now >= escalateAt) {
        const ticket = await this.getTicket(
          record.SupportTicketID,
          record.assignedTo
        );
        if (!ticket) continue;

        this.triggerEscalation(ticket).catch(err =>
          console.error("Escalation error:", err)
        );

        await this.db.collection(this.SLA_AUDIT_COLLECTION).updateOne(
          { _id: record._id },
          { $set: { escalatedAt: new Date(), updatedAt: new Date() } }
        );
      }
    }
  }

  // ---------------- Auto Dispose ----------------
  async processAutoDispose(roleConfigMap: Map<number, any>) {
    const now = new Date();

    const cursor = this.db
      .collection(this.SLA_AUDIT_COLLECTION)
      .find({ autoDisposedAt: null });

    while (await cursor.hasNext()) {
      const record = await cursor.next();
      if (!record) continue;

      const config = roleConfigMap.get(record.AssigneeRoleID);
      if (!config?.AutoDispose) continue;

      const disposeAt = new Date(
        new Date(record.AssignedDate).getTime() +
          config.AllowedDays * this.MILLISECONDS_IN_A_DAY
      );

      if (now >= disposeAt) {
        await this.db.collection(this.SLA_AUDIT_COLLECTION).updateOne(
          { _id: record._id },
          { $set: { autoDisposedAt: new Date(), updatedAt: new Date() } }
        );
      }
    }
  }

  // ---------------- Helpers ----------------
  async getTicket(SupportTicketID: string, assignedTo: string) {
    return this.db
      .collection(this.TICKET_ASSIGN_COLLECTION)
      .findOne({ SupportTicketID, assignedTo });
  }

  async triggerEscalation(ticket: any) {
    console.log(`Auto escalated ticket ${ticket.SupportTicketNo}`);
  }

  async sendReminder(ticket: any, dueAt: Date) {
    const payload = {
      Name: ticket.assignToName || "User",
      ticket: ticket.SupportTicketNo,
      mobileNO: ticket.AssigneeMobileNo,
    };

    await this.commonService.sendSMSToUser(this.db, payload);
  }
}
