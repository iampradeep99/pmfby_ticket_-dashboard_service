


import { Injectable, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
import * as amqp from 'amqplib';
import { TicketDashboardService } from '../../ticket-dashboard/ticket-dashboard.service';
import { runWorker } from './worker-runner'; 

@Injectable()
export class RabbitMQService implements OnModuleInit, OnApplicationShutdown {
private readonly RABBITMQ_URL = 'amqp://user:password@10.128.60.11:5672';
// private readonly RABBITMQ_URL = 'amqp://myuser:mypassword@localhost:5672';
//  private readonly RABBITMQ_URL = process.env.RabbitmqURL;



  private readonly QUEUE_NAME = 'support_ticket_download';

  private connection: amqp.Connection;
  private consumerChannel: amqp.Channel;
  private producerChannel: amqp.Channel;
  private monitorChannel: amqp.Channel;

  private activeJobs = 0;
  private shuttingDown = false;

  // keep concurrency safe for heavy jobs
  private readonly PREFETCH = 5;

  constructor(private readonly ticketDashboardService: TicketDashboardService) {}

  async onModuleInit() {
    try {
      console.log('[RabbitMQ] Connecting...');
      this.connection = await amqp.connect(this.RABBITMQ_URL);

      // producer channel (for sending messages)
      this.producerChannel = await this.connection.createChannel();
      await this.producerChannel.assertQueue(this.QUEUE_NAME, { durable: true });

      // consumer channel
      this.consumerChannel = await this.connection.createChannel();
      await this.consumerChannel.assertQueue(this.QUEUE_NAME, { durable: true });
      this.consumerChannel.prefetch(this.PREFETCH);
      this.consumeMessages();

      // monitor channel (reuse, not recreate every 2s)
      this.monitorChannel = await this.connection.createChannel();

      console.log(`[RabbitMQ] Connected. Listening on "${this.QUEUE_NAME}"`);
      this.monitorQueueAndAdjustConcurrency();
    } catch (err) {
      console.error('[RabbitMQ] Init error:', err);
    }
  }

  async sendToQueue(message: any) {
    this.producerChannel.sendToQueue(
      this.QUEUE_NAME,
      Buffer.from(JSON.stringify(message)),
      { persistent: true }
    );
    console.log('[RabbitMQ] Message sent:', message);
  }

 private consumeMessages() {
  this.consumerChannel.consume(
    this.QUEUE_NAME,
    async (msg) => {
      if (!msg || this.shuttingDown) return;

      const payload = JSON.parse(msg.content.toString());
      this.activeJobs++;
      console.log(`[RabbitMQ] Job received | Active: ${this.activeJobs}`);

      try {
        // Run heavy job in worker thread
        await runWorker(payload);
        this.consumerChannel.ack(msg);
        console.log('[RabbitMQ] Job done');
      } catch (err) {
        console.error('[RabbitMQ] Job failed:', err);
        this.consumerChannel.nack(msg, false, true);
      } finally {
        this.activeJobs--;
      }
    },
    { noAck: false }
  );
}


  private async monitorQueueAndAdjustConcurrency() {
    setInterval(async () => {
      try {
        const q = await this.monitorChannel.checkQueue(this.QUEUE_NAME);
        console.log(
          `[RabbitMQ] Queue check | Pending: ${q.messageCount} | Active: ${this.activeJobs}`
        );
      } catch (err) {
        console.error('[RabbitMQ] Monitor error:', err);
      }
    }, 5000);
  }

  async onApplicationShutdown(signal?: string) {
    console.log('[RabbitMQ] Shutting down...');
    this.shuttingDown = true;

    while (this.activeJobs > 0) {
      await new Promise((r) => setTimeout(r, 100));
    }

    await this.consumerChannel?.close();
    await this.producerChannel?.close();
    await this.monitorChannel?.close();
    await this.connection?.close();

    console.log('[RabbitMQ] Shutdown complete');
  }
}


// import { Injectable, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
// import * as amqp from 'amqplib';
// import { TicketDashboardService } from '../../ticket-dashboard/ticket-dashboard.service';
// import { runWorker } from './worker-runner';
// import config from '../../environment/config'; // import dynamic config

// @Injectable()
// export class RabbitMQService implements OnModuleInit, OnApplicationShutdown {
//   // Dynamically pick RabbitMQ URL from config based on NODE_ENV
//   private readonly RABBITMQ_URL = config.rabbitmq;

 

//   private readonly QUEUE_NAME = 'support_ticket_download';

//   private connection: amqp.Connection;
//   private consumerChannel: amqp.Channel;
//   private producerChannel: amqp.Channel;
//   private monitorChannel: amqp.Channel;

//   private activeJobs = 0;
//   private shuttingDown = false;

//   private readonly PREFETCH = 5; // keep concurrency safe for heavy jobs

//   constructor(private readonly ticketDashboardService: TicketDashboardService) {}
  
//   async onModuleInit() {
//     try {
//       console.log('[RabbitMQ] Connecting to:', this.RABBITMQ_URL);
//       this.connection = await amqp.connect(this.RABBITMQ_URL);

//       // Producer channel
//       this.producerChannel = await this.connection.createChannel();
//       await this.producerChannel.assertQueue(this.QUEUE_NAME, { durable: true });

//       // Consumer channel
//       this.consumerChannel = await this.connection.createChannel();
//       await this.consumerChannel.assertQueue(this.QUEUE_NAME, { durable: true });
//       this.consumerChannel.prefetch(this.PREFETCH);
//       this.consumeMessages();

//       // Monitor channel
//       this.monitorChannel = await this.connection.createChannel();

//       console.log(`[RabbitMQ] Connected. Listening on "${this.QUEUE_NAME}"`);
//       this.monitorQueueAndAdjustConcurrency();
//     } catch (err) {
//       console.error('[RabbitMQ] Init error:', err);
//     }
//   }

//   async sendToQueue(message: any) {
//     this.producerChannel.sendToQueue(
//       this.QUEUE_NAME,
//       Buffer.from(JSON.stringify(message)),
//       { persistent: true }
//     );
//     console.log('[RabbitMQ] Message sent:', message);
//   }

//   private consumeMessages() {
//     this.consumerChannel.consume(
//       this.QUEUE_NAME,
//       async (msg) => {
//         if (!msg || this.shuttingDown) return;

//         const payload = JSON.parse(msg.content.toString());
//         this.activeJobs++;
//         console.log(`[RabbitMQ] Job received | Active: ${this.activeJobs}`);

//         try {
//           // Run heavy job in worker thread
//           await runWorker(payload);
//           this.consumerChannel.ack(msg);
//           console.log('[RabbitMQ] Job done');
//         } catch (err) {
//           console.error('[RabbitMQ] Job failed:', err);
//           this.consumerChannel.nack(msg, false, true);
//         } finally {
//           this.activeJobs--;
//         }
//       },
//       { noAck: false }
//     );
//   }

//   private async monitorQueueAndAdjustConcurrency() {
//     setInterval(async () => {
//       try {
//         const q = await this.monitorChannel.checkQueue(this.QUEUE_NAME);
//         console.log(
//           `[RabbitMQ] Queue check | Pending: ${q.messageCount} | Active: ${this.activeJobs}`
//         );
//       } catch (err) {
//         console.error('[RabbitMQ] Monitor error:', err);
//       }
//     }, 5000);
//   }

//   async onApplicationShutdown(signal?: string) {
//     console.log('[RabbitMQ] Shutting down...');
//     this.shuttingDown = true;

//     while (this.activeJobs > 0) {
//       await new Promise((r) => setTimeout(r, 100));
//     }

//     await this.consumerChannel?.close();
//     await this.producerChannel?.close();
//     await this.monitorChannel?.close();
//     await this.connection?.close();

//     console.log('[RabbitMQ] Shutdown complete');
//   }
// }
