import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import * as express from 'express';
import * as path from 'path';
import { RedisWrapper } from './commonServices/redisWrapper';
import * as crypto from 'crypto';
(global as any).crypto = crypto;

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.setGlobalPrefix('krphdashboard');

  app.use('/downloads', express.static(path.join(__dirname, '../downloads')));

  app.enableCors({
    origin: '*',                       
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS', 
    allowedHeaders: '*',            
  });

  const redis = new RedisWrapper();
  try {
    // await redis.connectionInit({ url: 'redis://10.128.60.9:6379' });
    console.log('✅ Redis connected and subscriber started');
  } catch (err) {
    console.error('❌ Redis init error:', err);
  }

  await app.listen(process.env.PORT ?? 5500);
}
bootstrap();









// // main.ts
// import { NestFactory } from '@nestjs/core';
// import { AppModule } from './app.module';
// import * as express from 'express';
// import * as path from 'path';
// import { RedisWrapper } from './commonServices/redisWrapper';

// async function bootstrap() {
//   const app = await NestFactory.create(AppModule);

//   // ✅ Global prefix
//   app.setGlobalPrefix('krphdashboard');

//   // ✅ Serve downloads
//   app.use('/downloads', express.static(path.join(__dirname, '../downloads')));

//   // ✅ Optional: Redis initialization
//   const redis = new RedisWrapper();
//   try {
//     await redis.connectionInit({ url: "redis://10.128.60.9:6379" });
//     console.log('✅ Redis connected and subscriber started');
//   } catch (err) {
//     console.error('❌ Redis init error:', err);
//   }

//   // ✅ Enable CORS if needed
//   app.enableCors();

//   await app.listen(process.env.PORT ?? 5500);
// }
// bootstrap();

