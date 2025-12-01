import { IsString, IsInt, IsObject } from 'class-validator';

export class PdfGenerationPayload {
  @IsString()
  SupportTicketNo: string;

  @IsInt()
  SupportTicketID: number;

  @IsObject()
  objCommon: any;
}
