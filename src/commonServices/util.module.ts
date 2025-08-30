// commonServices/util.module.ts
import { Module } from '@nestjs/common';
import { UtilService } from './utilService';

@Module({
  providers: [UtilService],
  exports: [UtilService], // Export it for other modules
})
export class UtilModule {}
