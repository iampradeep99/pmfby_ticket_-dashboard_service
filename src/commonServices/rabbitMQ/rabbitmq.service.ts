



// import { Injectable, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
// import * as amqp from 'amqplib';
// import * as os from 'os';
// import { TicketDashboardService } from '../../ticket-dashboard/ticket-dashboard.service';

// @Injectable()
// export class RabbitMQService implements OnModuleInit, OnApplicationShutdown {
//   private readonly RABBITMQ_URL = 'amqp://user:password@10.128.60.11:5672';
//   private readonly QUEUE_NAME = 'support_ticket_download';
//   private connection: amqp.Connection;

//   private activeJobs = 0;
//   private readonly MAX_CONCURRENCY = 20;
//   private readonly MIN_CONCURRENCY = 2;
//   private readonly BACKLOG_FACTOR = 1;
//   private readonly NUM_CONSUMERS = 3; // number of consumers in same process
//   private shuttingDown = false;

//   constructor(private readonly ticketDashboardService: TicketDashboardService) {}

//   async onModuleInit() {
//     try {
//       console.log('[RabbitMQ] Connecting to RabbitMQ server...');
//       this.connection = await amqp.connect(this.RABBITMQ_URL);
//       console.log('[RabbitMQ] Connected to RabbitMQ');

//       // Create multiple consumers
//       for (let i = 0; i < this.NUM_CONSUMERS; i++) {
//         const channel = await this.connection.createChannel();
//         await channel.assertQueue(this.QUEUE_NAME, { durable: true });

//         // Each channel prefetch controls parallel delivery per consumer
//         channel.prefetch(this.MAX_CONCURRENCY);

//         console.log(`[RabbitMQ] Consumer ${i + 1} started`);
//         this.consumeMessages(channel);
//       }

//       this.monitorQueueAndAdjustConcurrency();
//     } catch (err) {
//       console.error('[RabbitMQ] Connection or channel error:', err);
//     }
//   }

//   async sendToQueue(message: any) {
//     const channel = await this.connection.createChannel();
//     await channel.assertQueue(this.QUEUE_NAME, { durable: true });

//     channel.sendToQueue(
//       this.QUEUE_NAME,
//       Buffer.from(JSON.stringify(message)),
//       { persistent: true }
//     );

//     await channel.close(); // optional cleanup
//     console.log('[RabbitMQ] Message sent to queue:', message);
//   }

//   private consumeMessages(channel: amqp.Channel) {
//     channel.consume(
//       this.QUEUE_NAME,
//       (msg) => {
//         if (!msg || this.shuttingDown) return;

//         const payload = JSON.parse(msg.content.toString());
//         this.activeJobs++;
//         console.log('[RabbitMQ] Received job:', payload, `Active: ${this.activeJobs}`);

//         (async () => {
//           try {
//             await this.ticketDashboardService.processTicketHistoryAndGenerateZip(payload);
//             channel.ack(msg);
//             console.log('[RabbitMQ] Job processed successfully');
//           } catch (err) {
//             console.error('[RabbitMQ] Job failed:', err);
//             channel.nack(msg, false, true);
//           } finally {
//             this.activeJobs--;
//             console.log('[RabbitMQ] Active jobs:', this.activeJobs);
//           }
//         })();
//       },
//       { noAck: false }
//     );
//   }

//   private async monitorQueueAndAdjustConcurrency() {
//     setInterval(async () => {
//       try {
//         const channel = await this.connection.createChannel();
//         const q = await channel.checkQueue(this.QUEUE_NAME);
//         const pendingMessages = q.messageCount;

//         const totalWorkload = pendingMessages + this.activeJobs;
//         console.log(
//           `[RabbitMQ] Pending: ${pendingMessages} | Active: ${this.activeJobs} | Total workload: ${totalWorkload}`
//         );

//         await channel.close();
//       } catch (err) {
//         console.error('[RabbitMQ] Error monitoring queue:', err);
//       }
//     }, 2000);
//   }

//   async onApplicationShutdown(signal?: string) {
//     console.log('[RabbitMQ] Shutting down, waiting for active jobs to finish...');
//     this.shuttingDown = true;

//     while (this.activeJobs > 0) {
//       await new Promise((resolve) => setTimeout(resolve, 100));
//     }

//     if (this.connection) await this.connection.close();
//     console.log('[RabbitMQ] Shutdown complete');
//   }
// }



import { Injectable, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
import * as amqp from 'amqplib';
import * as os from 'os';
import { TicketDashboardService } from '../../ticket-dashboard/ticket-dashboard.service';

@Injectable()
export class RabbitMQService implements OnModuleInit, OnApplicationShutdown {
  private readonly RABBITMQ_URL = 'amqp://user:password@10.128.60.11:5672';
  private readonly QUEUE_NAME = 'support_ticket_download';
  private connection: amqp.Connection;

  private activeJobs = 0;
  private readonly MAX_CONCURRENCY = 50;
  private readonly MIN_CONCURRENCY = 2;
  private readonly BACKLOG_FACTOR = 1;
  private readonly NUM_CONSUMERS = 3;
  private shuttingDown = false;

  constructor(private readonly ticketDashboardService: TicketDashboardService) {}

  async onModuleInit() {
    try {
      console.log('[RabbitMQ] Connecting to RabbitMQ server...');
      this.connection = await amqp.connect(this.RABBITMQ_URL);
      console.log('[RabbitMQ] Connected to RabbitMQ');

      for (let i = 0; i < this.NUM_CONSUMERS; i++) {
        const channel = await this.connection.createChannel();
        await channel.assertQueue(this.QUEUE_NAME, { durable: true });
        channel.prefetch(this.MAX_CONCURRENCY);

        console.log(`[RabbitMQ] Consumer ${i + 1} started`);
        this.consumeMessages(channel);
      }

      this.monitorQueueAndAdjustConcurrency();
    } catch (err) {
      console.error('[RabbitMQ] Connection or channel error:', err);
    }
  }

  async sendToQueue(message: any) {
    const channel = await this.connection.createChannel();
    await channel.assertQueue(this.QUEUE_NAME, { durable: true });

    channel.sendToQueue(
      this.QUEUE_NAME,
      Buffer.from(JSON.stringify(message)),
      { persistent: true }
    );

    await channel.close();
    console.log('[RabbitMQ] Message sent to queue:', message);
  }

//   private consumeMessages(channel: amqp.Channel) {
//     channel.consume(
//       this.QUEUE_NAME,
//       (msg) => {
//         if (!msg || this.shuttingDown) return;

//         const job = JSON.parse(msg.content.toString());
//         this.activeJobs++;
//         console.log('[RabbitMQ] Received job:', job, `Active jobs: ${this.activeJobs}`);

//         (async () => {
//           try {
//             if (job.type === 'ticket_history') {
//               await this.ticketDashboardService.processTicketHistoryAndGenerateZip(job.payload);
//             } else if (job.type === 'farmer_calling_history') {
//               await this.ticketDashboardService.farmerCallingHistoryDownloadReportAndZip(job.payload);
//             } else {
//               console.warn('[RabbitMQ] Unknown job type:', job.type);
//             }

//             channel.ack(msg);
//             console.log('[RabbitMQ] Job processed successfully');
//           } catch (err) {
//             console.error('[RabbitMQ] Job failed:', err);
//             channel.nack(msg, false, true); // Requeue
//           } finally {
//             this.activeJobs--;
//             console.log('[RabbitMQ] Active jobs:', this.activeJobs);
//           }
//         })();
//       },
//       { noAck: false }
//     );
//   }

private consumeMessages(channel: amqp.Channel) {
  channel.consume(
    this.QUEUE_NAME,
    (msg) => {
      if (!msg || this.shuttingDown) return;

      console.log('[RabbitMQ] Raw message content:', msg.content.toString());

      let job;
      try {
        job = JSON.parse(msg.content.toString());
      } catch (err) {
        console.error('[RabbitMQ] Failed to parse message:', err);
        channel.nack(msg, false, false); // Drop bad messages
        return;
      }

      this.activeJobs++;
      console.log('[RabbitMQ] Received job:', job, `Active jobs: ${this.activeJobs}`);

      (async () => {
        try {
          if (job.type === 'ticket_history') {
            console.log('[RabbitMQ] Processing ticket_history job...');
            await this.ticketDashboardService.processTicketHistoryAndGenerateZip(job.payload);
          } else if (job.type === 'farmer_calling_history') {
            console.log('[RabbitMQ] Processing farmer_calling_history job...');
            await this.ticketDashboardService.farmerCallingHistoryDownloadReportAndZip(job.payload);
          } else {
            console.warn('[RabbitMQ] Unknown job type:', job.type);
          }

          channel.ack(msg);
          console.log('[RabbitMQ] Job processed successfully');
        } catch (err) {
          console.error('[RabbitMQ] Job processing error:', err);
          channel.nack(msg, false, true);
        } finally {
          this.activeJobs--;
          console.log('[RabbitMQ] Active jobs:', this.activeJobs);
        }
      })();
    },
    { noAck: false }
  );
}


  private async monitorQueueAndAdjustConcurrency() {
    setInterval(async () => {
      try {
        const channel = await this.connection.createChannel();
        const q = await channel.checkQueue(this.QUEUE_NAME);
        const pendingMessages = q.messageCount;

        const totalWorkload = pendingMessages + this.activeJobs;
        console.log(
          `[RabbitMQ] Pending: ${pendingMessages} | Active: ${this.activeJobs} | Total workload: ${totalWorkload}`
        );

        await channel.close();
      } catch (err) {
        console.error('[RabbitMQ] Error monitoring queue:', err);
      }
    }, 2000);
  }

  async onApplicationShutdown(signal?: string) {
    console.log('[RabbitMQ] Shutting down, waiting for active jobs to finish...');
    this.shuttingDown = true;

    while (this.activeJobs > 0) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    if (this.connection) await this.connection.close();
    console.log('[RabbitMQ] Shutdown complete');
  }
}

