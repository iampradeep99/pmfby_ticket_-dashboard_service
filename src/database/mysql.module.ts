import { Global, Module } from '@nestjs/common';
import { Sequelize } from 'sequelize-typescript';
@Global()
@Module({
  providers: [
    {
      provide: 'SEQUELIZE',
      useFactory: async () => {
        const sequelize = new Sequelize({
          dialect: 'mysql',
          host: process.env.DB_HOSTNAME || 'localhost',
          port: parseInt(process.env.DB_PORT) || 3306,
          username: process.env.DB_USERNAME || 'root',
          password: process.env.DB_PASSWORD || '',
          database: process.env.DB_NAME || 'test_db',
          logging: false,
        });

        await sequelize.authenticate();
        console.log('✅ MySQL Connected');
        return sequelize;
      },
    },
  ],
  exports: ['SEQUELIZE'],
})
export class MysqlModule {}
