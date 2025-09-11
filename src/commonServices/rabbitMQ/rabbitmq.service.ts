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



import { Injectable, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import * as os from 'os';
import { TicketDashboardService } from '../../ticket-dashboard/ticket-dashboard.service';

@Injectable()
export class RabbitMQService implements OnModuleInit {
  private readonly RABBITMQ_URL = 'amqp://user:password@10.128.60.11:5672';
  private readonly QUEUE_NAME = 'support_ticket_download';
  private connection: amqp.Connection;
  private channel: amqp.Channel;

  private activeJobs = 0;
  private readonly MAX_CONCURRENCY = 15;
  private readonly MIN_CONCURRENCY = 1;
  private readonly BACKLOG_FACTOR = 0.5;

  constructor(private readonly ticketDashboardService: TicketDashboardService) {}

  async onModuleInit() {
    try {
      console.log('[RabbitMQ] Connecting to RabbitMQ server...');
      this.connection = await amqp.connect(this.RABBITMQ_URL);
      this.channel = await this.connection.createChannel();
      await this.channel.assertQueue(this.QUEUE_NAME, { durable: true });

      // Set initial prefetch
      this.channel.prefetch(this.MIN_CONCURRENCY);
      console.log(`[RabbitMQ] Connected. Listening to queue "${this.QUEUE_NAME}"...`);

      // Start consuming messages
      this.consumeMessages();

      // Start dynamic concurrency monitor
      this.monitorQueueAndAdjustConcurrency();
    } catch (err) {
      console.error('[RabbitMQ] Connection or channel error:', err);
    }
  }

  async sendToQueue(message: any) {
    this.channel.sendToQueue(
      this.QUEUE_NAME,
      Buffer.from(JSON.stringify(message)),
      { persistent: true }
    );
    console.log('[RabbitMQ] Message sent to queue:', message);
  }

  private calculateDynamicConcurrency(pendingMessages: number): number {
    const freeMemRatio = os.freemem() / os.totalmem();
    const cpuLoad = os.loadavg()[0] / os.cpus().length;

    let concurrency = Math.floor(this.MAX_CONCURRENCY * freeMemRatio * (1 - cpuLoad));
    concurrency += Math.floor(pendingMessages * this.BACKLOG_FACTOR);
    concurrency = Math.max(this.MIN_CONCURRENCY, Math.min(this.MAX_CONCURRENCY, concurrency));

    return concurrency;
  }

  private async consumeMessages() {
    await this.channel.consume(
      this.QUEUE_NAME,
      (msg) => {
        if (!msg) return;

        const ticketPayload = JSON.parse(msg.content.toString());
        console.log('[RabbitMQ] Received job:', ticketPayload);

        // Increment activeJobs counter for logging/monitoring
        this.activeJobs++;

        // Process message asynchronously
        (async () => {
          try {
            await this.ticketDashboardService.processTicketHistoryAndGenerateZip(ticketPayload);
            this.channel.ack(msg);
            console.log('[RabbitMQ] Job processed successfully');
          } catch (err) {
            console.error('[RabbitMQ] Job failed:', err);
            this.channel.nack(msg, false, true); // requeue on failure
          } finally {
            this.activeJobs--;
            console.log('[RabbitMQ] Active jobs:', this.activeJobs);
          }
        })();
      },
      { noAck: false } // manual ack
    );
  }

  private async monitorQueueAndAdjustConcurrency() {
    setInterval(async () => {
      try {
        const q = await this.channel.checkQueue(this.QUEUE_NAME);
        const pendingMessages = q.messageCount;

        const newConcurrency = this.calculateDynamicConcurrency(pendingMessages);
        this.channel.prefetch(newConcurrency);

        console.log(`[RabbitMQ] Adjusted concurrency to ${newConcurrency} (pending: ${pendingMessages}, active: ${this.activeJobs})`);
      } catch (err) {
        console.error('[RabbitMQ] Error adjusting concurrency:', err);
      }
    }, 5000); // adjust every 5 seconds
  }
}

