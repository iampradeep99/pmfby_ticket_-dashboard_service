import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import * as express from 'express';
import * as path from 'path';
import { RedisWrapper } from './commonServices/redisWrapper';

async function bootstrap() {
  // Create the NestJS application instance
  const app = await NestFactory.create(AppModule);

  // ✅ Set a global API prefix
  app.setGlobalPrefix('krphdashboard');

  // ✅ Serve static files from the "downloads" folder
  app.use('/downloads', express.static(path.join(__dirname, '../downloads')));

  // ✅ Enable CORS to allow requests from any origin (all ports, all headers)
  app.enableCors({
    origin: '*',                       // Allow any origin
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS', // Allow common HTTP methods
    allowedHeaders: '*',              // Allow any headers
  });

  // ✅ Initialize Redis connection
  const redis = new RedisWrapper();
  try {
    // await redis.connectionInit({ url: 'redis://10.128.60.9:6379' });
    console.log('✅ Redis connected and subscriber started');
  } catch (err) {
    console.error('❌ Redis init error:', err);
  }

  // ✅ Start listening on the configured port (default to 5500)
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

