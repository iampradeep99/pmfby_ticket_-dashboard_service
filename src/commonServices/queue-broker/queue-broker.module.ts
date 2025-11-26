import { Module } from '@nestjs/common';
import { PdfGenerationService } from './pdf-generation/pdf-generation.service';

@Module({
  providers: [PdfGenerationService],
  exports: [PdfGenerationService],
})
export class QueueBrokerModule {}
