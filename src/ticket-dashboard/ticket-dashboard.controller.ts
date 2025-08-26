import { Body, Controller, Get, Post } from '@nestjs/common';
import { TicketDashboardService } from './ticket-dashboard.service';
import { CreateTicketDto } from 'src/DTOs/createTicket.dto';

@Controller('ticket-dashboard')
export class TicketDashboardController {
    constructor(private readonly dashboardService: TicketDashboardService) {}

    @Post('myticket')
    async createTicket(@Body() ticketData: CreateTicketDto) {
        return await this.dashboardService.createTicket(ticketData);
    }

    @Post()
    async fetchDashbaord(@Body() ticketInfo:any){
        return await this.dashboardService.fetchTickets(ticketInfo)
    }
}

