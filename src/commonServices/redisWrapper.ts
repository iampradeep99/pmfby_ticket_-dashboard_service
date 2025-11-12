// import { Injectable } from '@nestjs/common';
// import * as Redis from 'lms-redis';

// const redisLoader = Redis();
// @Injectable()
// export class RedisWrapper {
//   private redisClient: any = null; // Replace `any` with actual client type if available

//   constructor() {
//     this.redisClient = null;
//   }

//   async connectionInit(options: Record<string, any> = {}): Promise<void> {
//     this.redisClient = await redisLoader.connectRedis(options);
//   }

//   getClient(): any { // Replace `any` with actual type if known
//     return redisLoader.getClient();
//   }

//   async setRedisCache(key: string, value: any, ttlInSec: number = 3600): Promise<'OK' | null> {
//     return redisLoader.setCache(key, value, ttlInSec);
//   }

//   async getRedisCache<T = unknown>(key: string): Promise<T | null> {
//     return redisLoader.getCache(key);
//   }

//   async deleteRedisCache(key: string): Promise<number> {
//     return redisLoader.deleteCache(key);
//   }
// }



// redis Code
import { Injectable } from '@nestjs/common';
import * as Redis from 'lms-redis';

const redisLoader = Redis();

@Injectable()
export class RedisWrapper {
  private redisClient: any = null;

  async connectionInit(options: Record<string, any> = {}): Promise<void> {
    this.redisClient = await redisLoader.connectRedis(options);
  }

  getClient(): any {
    return redisLoader.getClient();
  }

  async setRedisCache(key: string, value: any, ttlInSec: number = 3600): Promise<'OK' | null> {
    return redisLoader.setCache(key, value, ttlInSec);
  }

  async getRedisCache<T = unknown>(key: string): Promise<T | null> {
    return redisLoader.getCache(key);
  }

  async deleteRedisCache(key: string): Promise<number> {
    return redisLoader.deleteCache(key);
  }
}



// import { Injectable } from '@nestjs/common';
// import * as NodeCache from 'node-cache';

// @Injectable()
// export class RedisWrapper {
//   private redisClient: NodeCache;

//   constructor() {
//     this.redisClient = new NodeCache({ stdTTL: 3600, checkperiod: 120 });
//   }

//   getClient(): NodeCache {
//     return this.redisClient;
//   }

//   async setRedisCache(key: string, value: any, ttlInSec: number = 3600): Promise<'OK' | null> {
//     const success = this.redisClient.set(key, value, ttlInSec);
//     return success ? 'OK' : null;
//   }

//   async getRedisCache<T = unknown>(key: string): Promise<T | null> {
//     if (!this.redisClient) {
//       console.error('Redis client is not initialized!');
//       return null;
//     }
//     const value = this.redisClient.get<T>(key);
//     return value === undefined ? null : value;
//   }

//   async deleteRedisCache(key: string): Promise<number> {
//     if (!this.redisClient) {
//       console.error('Redis client is not initialized!');
//       return 0;
//     }
//     return this.redisClient.del(key);
//   }
// }
