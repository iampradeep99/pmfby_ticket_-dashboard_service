// import { Injectable, OnModuleInit } from '@nestjs/common';
// import * as amqp from 'amqplib';
// import { TicketDashboardService } from '../../ticket-dashboard/ticket-dashboard.service';

// @Injectable()
// export class RabbitMQService implements OnModuleInit {
//   private readonly RABBITMQ_URL = 'amqp://user:password@10.128.60.11:5672';
//   private readonly QUEUE_NAME = 'support_ticket_download';
//   private connection: amqp.Connection;
//   private channel: amqp.Channel;

//   constructor(private readonly ticketDashboardService: TicketDashboardService) {}

//   async onModuleInit() {
//     console.log('[RabbitMQ] Connecting to RabbitMQ server...');
//     this.connection = await amqp.connect(this.RABBITMQ_URL);
//     this.channel = await this.connection.createChannel();
//     await this.channel.assertQueue(this.QUEUE_NAME, { durable: true });
//     console.log(`[RabbitMQ] Connected. Listening to queue "${this.QUEUE_NAME}"...`);

//     this.consumeMessages();
//   }

//   async sendToQueue(message: any) {
//     console.log('[RabbitMQ] Sending message to queue:', message);
//     await this.channel.sendToQueue(
//       this.QUEUE_NAME,
//       Buffer.from(JSON.stringify(message)),
//       { persistent: true }
//     );
//     console.log('[RabbitMQ] Message successfully sent to queue');
//   }

//   private async consumeMessages() {
//     console.log(`[RabbitMQ] Starting to consume messages from "${this.QUEUE_NAME}" queue...`);
//     this.channel.prefetch(1);
//     this.channel.consume(this.QUEUE_NAME, async (msg) => {
//       if (msg) {
//         const ticketPayload = JSON.parse(msg.content.toString());
//         console.log('[RabbitMQ] Received job from queue:', ticketPayload);

//         try {
//           await this.ticketDashboardService.processTicketHistoryAndGenerateZip(ticketPayload);
//           console.log('[RabbitMQ] Job processed successfully');
//           this.channel.ack(msg);
//         } catch (err) {
//           console.error('[RabbitMQ] Job processing failed:', err);
//           this.channel.nack(msg, false, true); // Requeue job in case of failure
//         }
//       }
//     });
//   }
// }



// import { Injectable, OnModuleInit } from '@nestjs/common';
// import * as amqp from 'amqplib';
// import * as os from 'os';
// import { TicketDashboardService } from '../../ticket-dashboard/ticket-dashboard.service';

// @Injectable()
// export class RabbitMQService implements OnModuleInit {
//   private readonly RABBITMQ_URL = 'amqp://user:password@10.128.60.11:5672';
//   private readonly QUEUE_NAME = 'support_ticket_download';
//   private connection: amqp.Connection;
//   private channel: amqp.Channel;

//   private activeJobs = 0;
//   private readonly MAX_CONCURRENCY = 15;
//   private readonly MIN_CONCURRENCY = 1; 
//   private readonly BACKLOG_FACTOR = 0.5; 

//   constructor(private readonly ticketDashboardService: TicketDashboardService) {}

//   async onModuleInit() {
//     try {
//       console.log('[RabbitMQ] Connecting to RabbitMQ server...');
//       this.connection = await amqp.connect(this.RABBITMQ_URL);
//       this.channel = await this.connection.createChannel();
//       await this.channel.assertQueue(this.QUEUE_NAME, { durable: true });

//       this.channel.prefetch(this.MIN_CONCURRENCY);

//       console.log(`[RabbitMQ] Connected. Listening to queue "${this.QUEUE_NAME}"...`);

//       this.consumeMessages();
//       this.monitorQueueAndAdjustConcurrency(); 
//     } catch (err) {
//       console.error('[RabbitMQ] Connection or channel error:', err);
//     }
//   }

//   async sendToQueue(message: any) {
//     this.channel.sendToQueue(
//       this.QUEUE_NAME,
//       Buffer.from(JSON.stringify(message)),
//       { persistent: true }
//     );
//     console.log('[RabbitMQ] Message sent to queue:', message);
//   }

//   private calculateDynamicConcurrency(pendingMessages: number): number {
//     const freeMemRatio = os.freemem() / os.totalmem();
//     const cpuLoad = os.loadavg()[0] / os.cpus().length;

//     let concurrency = Math.floor(this.MAX_CONCURRENCY * freeMemRatio * (1 - cpuLoad));

//     concurrency += Math.floor(pendingMessages * this.BACKLOG_FACTOR);

//     concurrency = Math.max(this.MIN_CONCURRENCY, Math.min(this.MAX_CONCURRENCY, concurrency));
//     return concurrency;
//   }

//   private async consumeMessages() {
//     await this.channel.consume(
//       this.QUEUE_NAME,
//       async (msg) => {
//         if (!msg) return;

//         const ticketPayload = JSON.parse(msg.content.toString());
//         console.log('[RabbitMQ] Received job:', ticketPayload);

//         let concurrencyLimit = this.MAX_CONCURRENCY; 
//         while (this.activeJobs >= concurrencyLimit) {
//           await new Promise(resolve => setTimeout(resolve, 50));
//         }

//         this.activeJobs++;
//         (async () => {
//           try {
//             await this.ticketDashboardService.processTicketHistoryAndGenerateZip(ticketPayload);
//             this.channel.ack(msg);
//             console.log('[RabbitMQ] Job processed successfully');
//           } catch (err) {
//             console.error('[RabbitMQ] Job failed:', err);
//             this.channel.nack(msg, false, true); 
//           } finally {
//             this.activeJobs--;
//           }
//         })();
//       },
//       { noAck: false }
//     );
//   }

//   private async monitorQueueAndAdjustConcurrency() {
//     setInterval(async () => {
//       try {
//         const q = await this.channel.checkQueue(this.QUEUE_NAME);
//         const pendingMessages = q.messageCount;

//         const newConcurrency = this.calculateDynamicConcurrency(pendingMessages);
//         this.channel.prefetch(newConcurrency);
//         console.log(`[RabbitMQ] Adjusted concurrency to ${newConcurrency} (pending: ${pendingMessages}, active: ${this.activeJobs})`);
//       } catch (err) {
//         console.error('[RabbitMQ] Error adjusting concurrency:', err);
//       }
//     }, 5000); 
//   }
// }


// latest working ocde

// import { Injectable, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
// import * as amqp from 'amqplib';
// import * as os from 'os';
// import { TicketDashboardService } from '../../ticket-dashboard/ticket-dashboard.service';

// @Injectable()
// export class RabbitMQService implements OnModuleInit, OnApplicationShutdown {
//   private readonly RABBITMQ_URL = 'amqp://user:password@10.128.60.11:5672';
//   private readonly QUEUE_NAME = 'support_ticket_download';
//   private connection: amqp.Connection;
//   private channel: amqp.Channel;

//   private activeJobs = 0;
//   private readonly MAX_CONCURRENCY = 20; // slightly higher max for backlog
//   private readonly MIN_CONCURRENCY = 2; 
//   private readonly BACKLOG_FACTOR = 1; // stronger weight on backlog
//   private shuttingDown = false;

//   constructor(private readonly ticketDashboardService: TicketDashboardService) {}

//   async onModuleInit() {
//     try {
//       console.log('[RabbitMQ] Connecting to RabbitMQ server...');
//       this.connection = await amqp.connect(this.RABBITMQ_URL);
//       this.channel = await this.connection.createChannel();
//       await this.channel.assertQueue(this.QUEUE_NAME, { durable: true });

//       this.channel.prefetch(this.MIN_CONCURRENCY);
//       console.log(`[RabbitMQ] Connected. Listening to queue "${this.QUEUE_NAME}"...`);

//       this.consumeMessages();
//       this.monitorQueueAndAdjustConcurrency();
//     } catch (err) {
//       console.error('[RabbitMQ] Connection or channel error:', err);
//     }
//   }

//   async sendToQueue(message: any) {
//     this.channel.sendToQueue(
//       this.QUEUE_NAME,
//       Buffer.from(JSON.stringify(message)),
//       { persistent: true }
//     );
//     console.log('[RabbitMQ] Message sent to queue:', message);
//   }

//   private calculateDynamicConcurrency(pendingMessages: number): number {
//     const freeMemRatio = os.freemem() / os.totalmem();
//     const cpuLoad = os.loadavg()[0] / os.cpus().length;

//     // Base concurrency from system resources
//     let concurrency = Math.floor(this.MAX_CONCURRENCY * freeMemRatio * (1 - cpuLoad));

//     // Increase concurrency proportional to queue backlog
//     concurrency += Math.floor(pendingMessages * this.BACKLOG_FACTOR);

//     // Clamp to min/max
//     concurrency = Math.max(this.MIN_CONCURRENCY, Math.min(this.MAX_CONCURRENCY, concurrency));

//     return concurrency;
//   }

//   private async consumeMessages() {
//     await this.channel.consume(
//       this.QUEUE_NAME,
//       (msg) => {
//         if (!msg || this.shuttingDown) return;

//         const ticketPayload = JSON.parse(msg.content.toString());
//         this.activeJobs++;
//         console.log('[RabbitMQ] Received job:', ticketPayload, `Active: ${this.activeJobs}`);

//         (async () => {
//           try {
//             await this.ticketDashboardService.processTicketHistoryAndGenerateZip(ticketPayload);
//             this.channel.ack(msg);
//             console.log('[RabbitMQ] Job processed successfully');
//           } catch (err) {
//             console.error('[RabbitMQ] Job failed:', err);
//             this.channel.nack(msg, false, true); // requeue failed job
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
//         const q = await this.channel.checkQueue(this.QUEUE_NAME);
//         const pendingMessages = q.messageCount;

//         // Calculate new concurrency dynamically
//         const newConcurrency = this.calculateDynamicConcurrency(pendingMessages);

//         // Only adjust if changed
//         if (newConcurrency !== this.channel.prefetch) {
//           this.channel.prefetch(newConcurrency);
//           console.log(
//             `[RabbitMQ] Adjusted concurrency to ${newConcurrency} | Pending: ${pendingMessages} | Active: ${this.activeJobs} | Total workload: ${pendingMessages + this.activeJobs}`
//           );
//         }
//       } catch (err) {
//         console.error('[RabbitMQ] Error adjusting concurrency:', err);
//       }
//     }, 2000); // faster interval for responsive backlog scaling
//   }

//   async onApplicationShutdown(signal?: string) {
//     console.log('[RabbitMQ] Shutting down, waiting for active jobs to finish...');
//     this.shuttingDown = true;

//     while (this.activeJobs > 0) {
//       await new Promise((resolve) => setTimeout(resolve, 100));
//     }

//     if (this.channel) await this.channel.close();
//     if (this.connection) await this.connection.close();

//     console.log('[RabbitMQ] Shutdown complete');
//   }
// }


// pradeep today date - 12-sept-2025
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
  private readonly MAX_CONCURRENCY = 20;
  private readonly MIN_CONCURRENCY = 2;
  private readonly BACKLOG_FACTOR = 1;
  private readonly NUM_CONSUMERS = 3; // number of consumers in same process
  private shuttingDown = false;

  constructor(private readonly ticketDashboardService: TicketDashboardService) {}

  async onModuleInit() {
    try {
      console.log('[RabbitMQ] Connecting to RabbitMQ server...');
      this.connection = await amqp.connect(this.RABBITMQ_URL);
      console.log('[RabbitMQ] Connected to RabbitMQ');

      // Create multiple consumers
      for (let i = 0; i < this.NUM_CONSUMERS; i++) {
        const channel = await this.connection.createChannel();
        await channel.assertQueue(this.QUEUE_NAME, { durable: true });

        // Each channel prefetch controls parallel delivery per consumer
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

    await channel.close(); // optional cleanup
    console.log('[RabbitMQ] Message sent to queue:', message);
  }

  private consumeMessages(channel: amqp.Channel) {
    channel.consume(
      this.QUEUE_NAME,
      (msg) => {
        if (!msg || this.shuttingDown) return;

        const payload = JSON.parse(msg.content.toString());
        this.activeJobs++;
        console.log('[RabbitMQ] Received job:', payload, `Active: ${this.activeJobs}`);

        (async () => {
          try {
            await this.ticketDashboardService.processTicketHistoryAndGenerateZip(payload);
            channel.ack(msg);
            console.log('[RabbitMQ] Job processed successfully');
          } catch (err) {
            console.error('[RabbitMQ] Job failed:', err);
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
