// import { Global, Module } from '@nestjs/common';
// import { Sequelize } from 'sequelize-typescript';
// @Global()
// @Module({
//   providers: [
//     {
//       provide: 'SEQUELIZE',
//       useFactory: async () => {
//         const sequelize = new Sequelize({
//           dialect: 'mysql',
//           host: process.env.DB_HOSTNAME || 'localhost',
//           port: parseInt(process.env.DB_PORT) || 3306,
//           username: process.env.DB_USERNAME || 'root',
//           password: process.env.DB_PASSWORD || '',
//           database: process.env.DB_NAME || 'test_db',
//           logging: false,
//         });

//         await sequelize.authenticate();
//         console.log('✅ MySQL Connected');
//         return sequelize;
//       },
//     },
//   ],
//   exports: ['SEQUELIZE'],
// })
// export class MysqlModule {}



import { Global, Module, Provider } from '@nestjs/common';
import { Sequelize } from 'sequelize-typescript';
import config from '../environment/config'; // dynamic config

console.log('Using DB config:', config.mysql); // optional debug

@Global()
@Module({
  providers: [
    {
      provide: 'SEQUELIZE',
      useFactory: async () => {
        const mysqlConfig = config.mysql;

        const sequelize = new Sequelize({
          dialect: 'mysql',
          host: mysqlConfig.host,
          port: mysqlConfig.port,
          username: mysqlConfig.user,
          password: mysqlConfig.password,
          database: mysqlConfig.database,
          logging: false,
        });

        try {
          await sequelize.authenticate();
          console.log('✅ MySQL Connected');
        } catch (err) {
          console.error('❌ MySQL Connection Failed:', (err as Error).message);
          throw err;
        }

        return sequelize;
      },
    } as Provider<Sequelize>,
  ],
  exports: ['SEQUELIZE'],
})
export class MysqlModule {}
