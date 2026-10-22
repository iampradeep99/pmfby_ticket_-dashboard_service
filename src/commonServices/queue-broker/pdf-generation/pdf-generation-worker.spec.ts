import axios from 'axios';
import { MongoClient } from 'mongodb';
import { PDFGenerationWorkerService } from './pdf-generation-worker';

jest.mock('../../../environment/config', () => ({
  __esModule: true,
  default: { mongodb: 'mongodb://localhost:27017' },
}));
jest.mock('../../../commonServices/logger', () => jest.fn());
jest.mock('mongodb', () => ({ MongoClient: jest.fn() }));

describe('WhatsApp PDF message logs', () => {
  const templateName = 'krph_ticket_resolved_pdf_21oct26_updated';
  const payload = {
    SupportTicketID: 123,
    SupportTicketNo: 'TICKET-123',
    RequestorMobileNo: '9876543210',
    TicketFileURl: 'https://example.com/ticket.pdf',
    fileName: 'ticket.pdf',
  };
  let service: PDFGenerationWorkerService;
  let post: jest.SpyInstance;
  let insertOne: jest.Mock;
  let collection: jest.Mock;
  let close: jest.Mock;

  beforeEach(() => {
    jest.useFakeTimers().setSystemTime(new Date('2026-10-22T18:30:00Z'));
    // Exercise the send flow without starting the browser or PDF queue.
    service = Object.create(PDFGenerationWorkerService.prototype);
    insertOne = jest.fn().mockResolvedValue({ acknowledged: true });
    collection = jest.fn().mockReturnValue({ insertOne });
    close = jest.fn().mockResolvedValue(undefined);
    (MongoClient as unknown as jest.Mock).mockImplementation(() => ({
      connect: jest.fn().mockResolvedValue(undefined),
      db: jest.fn().mockReturnValue({ collection }),
      close,
    }));
    post = jest.spyOn(axios, 'post');
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    jest.restoreAllMocks();
    jest.clearAllMocks();
    jest.useRealTimers();
  });

  it('saves each accepted message with its complete API response and IST date', async () => {
    const data = {
      messaging_product: 'whatsapp',
      contacts: [{ input: '919876543210', wa_id: '919876543210' }],
      messages: [{ id: 'wamid.test', message_status: 'accepted' }],
    };
    post.mockResolvedValue({ data });

    await expect(service.gupshupCallForPDFSend(payload)).resolves.toEqual(data);

    expect(collection).toHaveBeenCalledWith(
      'KRPH_WhatsApp_PDF_Resolved_Ticket_Message_Logs',
    );
    expect(insertOne).toHaveBeenCalledWith({
      SupportTicketID: 123,
      SupportTicketNo: 'TICKET-123',
      RequestorMobileNo: '919876543210',
      TemplateName: templateName,
      TicketFileURl: payload.TicketFileURl,
      FileName: payload.fileName,
      MessageId: 'wamid.test',
      MessageStatus: 'accepted',
      APIResponse: data,
      Date: '2026-10-23',
      Timezone: 'Asia/Kolkata',
      SentAt: new Date(),
      CreatedAt: new Date(),
    });
    expect(close).toHaveBeenCalledTimes(1);
  });

  it('saves separate records for messages sent to the same recipient', async () => {
    post
      .mockResolvedValueOnce({ data: { messages: [{ id: 'wamid.first' }] } })
      .mockResolvedValueOnce({ data: { messages: [{ id: 'wamid.second' }] } });

    await service.gupshupCallForPDFSend(payload);
    await service.gupshupCallForPDFSend(payload);

    expect(insertOne).toHaveBeenCalledTimes(2);
    expect(insertOne.mock.calls.map(([record]) => record.MessageId)).toEqual([
      'wamid.first',
      'wamid.second',
    ]);
    expect(insertOne.mock.calls[0][0]).not.toHaveProperty('SentCount');
  });

  it('keeps the previous IST day before midnight', async () => {
    jest.setSystemTime(new Date('2026-10-22T18:29:59Z'));
    post.mockResolvedValue({ data: { messages: [{ id: 'wamid.test' }] } });

    await service.gupshupCallForPDFSend(payload);

    expect(insertOne.mock.calls[0][0].Date).toBe('2026-10-22');
  });

  it.each([
    { error: { message: 'Rejected' } },
    { messages: [] },
    { messages: [{ id: 'wamid.test' }], error: { message: 'Rejected' } },
  ])(
    'does not save a sent-message record for an unconfirmed or rejected response: %j',
    async (data) => {
      post.mockResolvedValue({ data });

      await service.gupshupCallForPDFSend(payload);

      expect(insertOne).not.toHaveBeenCalled();
    },
  );

  it('does not save a sent-message record for a failed HTTP request', async () => {
    post.mockRejectedValue(new Error('Request failed'));

    await service.gupshupCallForPDFSend(payload);

    expect(insertOne).not.toHaveBeenCalled();
  });

  it('preserves a successful send if the response write fails', async () => {
    const data = {
      messaging_product: 'whatsapp',
      contacts: [{ input: '919876543210', wa_id: '919876543210' }],
      messages: [{ id: 'wamid.test', message_status: 'accepted' }],
    };
    post.mockResolvedValue({ data });
    insertOne.mockRejectedValue(new Error('Database unavailable'));

    await expect(service.gupshupCallForPDFSend(payload)).resolves.toEqual(data);

    expect(post).toHaveBeenCalledTimes(1);
    expect(close).toHaveBeenCalledTimes(1);
    expect(console.error).toHaveBeenCalledWith(
      'Failed to save WhatsApp PDF message response:',
      expect.objectContaining({ messageId: 'wamid.test' }),
    );
  });
});
