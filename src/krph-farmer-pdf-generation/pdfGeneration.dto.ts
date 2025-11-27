import { IsString, IsInt } from 'class-validator';

export class PdfGenerationPayload {
  @IsString()
  SupportTicketNo: string;

  @IsInt()
  SupportTicketID: number;
}
