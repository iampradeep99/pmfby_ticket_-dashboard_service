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
