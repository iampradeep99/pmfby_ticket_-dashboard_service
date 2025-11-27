import { Module } from '@nestjs/common';
import { PdfGenerationService } from './pdf-generation/pdf-generation.service';
import { BrowserPoolService } from './pdf-generation/browser-pool.service';

@Module({
  providers: [PdfGenerationService,BrowserPoolService],
  exports: [PdfGenerationService],
})
export class QueueBrokerModule {}
