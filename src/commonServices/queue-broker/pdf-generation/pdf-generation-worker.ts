import axios, { type AxiosInstance, type AxiosError } from "axios"
import config from "../../../environment/config"
import * as pako from "pako"
const Logger = require("../../../commonServices/logger")
import * as moment from "moment"
import { MongoClient } from "mongodb"
import * as fs from 'fs';
import * as path from 'path';
// import * as  puppeteer from 'puppeteer';
import * as  FormData from "form-data";
import PQueue from 'p-queue';
import { chromium } from "playwright";


import { BrowserPoolService } from "./browser-pool.service"


var selectedData

const stepsTATJourneyCrpLs = [
    {
        tat: "TAT 1",
        color: "#0f99ef",
        text: "(10 Days) to respond the ticket missed ticket type changed to escalated ticket",
        date: "",
        icon: "",
        textId: "System Processed",
        agentName: "",
        ticket: "",
        status: "",
        ticketStatus: "",
        smsText: "",
        ageing: "0-10 days",
    },
    {
        tat: "TAT 2",
        color: "#eb0c7b",
        text: "(15 Days) to respond the ticket missed ticket type changed to escalated ticket",
        date: "",
        icon: "",
        textId: "System Processed",
        agentName: "",
        ticket: "",
        status: "",
        ticketStatus: "",
        smsText: "",
        ageing: "11-15 days",
    },
    {
        tat: "TAT 3",
        color: "#b94e00",
        text: "(20 Days) to respond the ticket missed ticket type changed to escalated ticket",
        date: "",
        icon: "",
        textId: "System Processed",
        agentName: "",
        ticket: "",
        status: "",
        ticketStatus: "",
        smsText: "",
        ageing: "16-20 days",
    },
    {
        tat: "TAT 4",
        color: "#f06d1a",
        text: "(20 > Days) to respond the ticket missed ticket type changed to escalated ticket",
        date: "",
        icon: "",
        textId: "System Processed",
        agentName: "",
        ticket: "",
        status: "",
        ticketStatus: "",
        smsText: "",
        ageing: "20> days",
    },
]

const stepsTATJourneyGrv = [
    {
        tat: "TAT 1",
        color: "#0f99ef",
        text: "(3 Days) to respond the ticket missed ticket type changed to escalated ticket",
        date: "",
        icon: "",
        textId: "System Processed",
        agentName: "",
        ticket: "",
        status: "",
        ticketStatus: "",
        smsText: "",
        ageing: "0-3 days",
    },
    {
        tat: "TAT 2",
        color: "#eb0c7b",
        text: "(7 Days) to respond the ticket missed ticket type changed to escalated ticket",
        date: "",
        icon: "",
        textId: "System Processed",
        agentName: "",
        ticket: "",
        status: "",
        ticketStatus: "",
        smsText: "",
        ageing: "4-7 days",
    },
    {
        tat: "TAT 3",
        color: "#b94e00",
        text: "(12 Days) to respond the ticket missed ticket type changed to escalated ticket",
        date: "",
        icon: "",
        textId: "System Processed",
        agentName: "",
        ticket: "",
        status: "",
        ticketStatus: "",
        smsText: "",
        ageing: "8-12 days",
    },
    {
        tat: "TAT 4",
        color: "#6908b1",
        text: "(15 Days) to respond the ticket missed ticket type changed to escalated ticket",
        date: "",
        icon: "",
        textId: "System Processed",
        agentName: "",
        ticket: "",
        status: "",
        ticketStatus: "",
        smsText: "",
        ageing: "13-15 days",
    },
    {
        tat: "TAT 5",
        color: "#f06d1a",
        text: "(16 > Days) to respond the ticket missed ticket type changed to escalated ticket",
        date: "",
        icon: "",
        textId: "System Processed",
        agentName: "",
        ticket: "",
        status: "",
        ticketStatus: "",
        smsText: "",
        ageing: "16> days",
    },
]

const logger = new Logger("worker-runner.log")



export class PDFGenerationWorkerService {
    private queue: PQueue;
    private client: AxiosInstance
    private readonly token = config.krphPDFTicketToken
    private processedCount = 0;



    constructor(private readonly browserPool: BrowserPoolService) {

        this.client = this.initializeClient()


        this.queue = new PQueue({
            concurrency: 100,
            timeout: 120000
        });


    }
    async connectDB() {
        const uri = config.mongodb
        const client = new MongoClient(uri)
        await client.connect()
        return client.db("krph_db")
    }




    private initializeClient(): AxiosInstance {
        return axios.create({
            baseURL: config.krphPDFTicketInfoURL,
            timeout: 5000,
        })
    }





    async sendFileToGCP(documentsPath: any) {
        const formData = new FormData();
        formData.append("filePath", "krph/farmer/tickets-pdf/");
        formData.append("uploadedBy", "KRPH");
        formData.append("documents", fs.createReadStream(documentsPath));

        const response = await this.client.post(
            "/GCPFileUploadForCDR",
            formData,
            {
                baseURL: "https://pmfby.gov.in/krphapi/FGMS", // override baseURL
                headers: { ...formData.getHeaders() },
                maxBodyLength: Infinity,
            }
        );

        return response.data;
    }


    async saveToDatabase(payload: object) {
        try {
            const database = await this.connectDB();
            const result = await database
                .collection('KRPH_Ticket_PDF_History_test')
                .insertOne(payload);

            if (result.acknowledged) {
                return { ...payload, _id: result.insertedId };
            } else {
                throw new Error("Failed to insert record into database");
            }
        } catch (err) {
            throw err;
        }
    }




    // async gupshupCallForPDFSend(payload) {
    //     try {
    //         let requestorMobileNo = payload?.RequestorMobileNo;
    //         if (requestorMobileNo && !requestorMobileNo.startsWith('91')) {
    //             requestorMobileNo = `91${requestorMobileNo}`;
    //         }
    //         requestorMobileNo = "919810110521"
    //         const requestData = {
    //             userid: config.gupshupConfig.userid,
    //             password: config.gupshupConfig.password,
    //             send_to: requestorMobileNo,
    //             v: config.gupshupConfig.version,
    //             format: config.gupshupConfig.format,
    //             msg_type: config.gupshupConfig.msg_type,
    //             method: config.gupshupConfig.method,
    //             caption: config.gupshupConfig.caption,
    //             media_url: `${payload?.TicketFileURl}`,
    //             filename:`${payload?.fileName}`

    //         };

    //         let apiUrl = config.gupshupConfig.gupshupAPIUrl;

    //         console.log(apiUrl)

    //         apiUrl = "https://mediaapi.smsgupshup.com/GatewayAPI/rest"

    //         const response = await this.client.post(apiUrl, requestData, {
    //             headers: {
    //                 "Content-Type": "application/json",
    //             },
    //         });

    //         console.log("Response:", response.data);
    //         return response.data;

    //     } catch (err) {
    //         console.error("Error:", err);
    //     }
    // }


    async gupshupCallForPDFSend(payload) {
        try {

            const allowedNumbers = [
                "919873382826",
                "919891651196",
                //  "916386236314",
                "919899499022",
                 "919215368699"
            ];

            const requestorMobileNo = allowedNumbers[Math.floor(Math.random() * allowedNumbers.length)];

            const requestData = {
                userid: config.gupshupConfig.userid,
                password: config.gupshupConfig.password,
                send_to: requestorMobileNo,
                v: config.gupshupConfig.version,
                format: config.gupshupConfig.format,
                msg_type: config.gupshupConfig.msg_type,
                method: config.gupshupConfig.method,
                caption: config.gupshupConfig.caption,
                media_url: payload?.TicketFileURl,
                filename: payload?.fileName
            };

            let apiUrl = "https://mediaapi.smsgupshup.com/GatewayAPI/rest";

            const response = await this.client.post(apiUrl, requestData, {
                headers: { "Content-Type": "application/json" },
            });

            console.log("Sent To:", requestorMobileNo);
            console.log("Response:", response.data);

            return response.data;

        } catch (err) {
            console.error("Error:", err);
        }
    }





    /*    async ProcessInformationForFarmer(payload: any): Promise<any> {
           try {
               return this.queue.add(async () => {
                   const startTime = Date.now();
                   let browser: any = null;
                   let context: any = null;
                   let page: any = null;
   
                   try {
                       console.log(`Processing ticket: ${payload.SupportTicketNo}`);
   
                       selectedData = await this.fetchSelectedData(payload?.SupportTicketNo);
                       console.log(selectedData, "test")
                       const ticketListDetails: any = await this.FetchTicketInformation(payload);
                       const prinHTML = await this.renderPMFBYTemplate(selectedData, ticketListDetails);
   
                       browser = await chromium.launch({ headless: true });
                       context = await browser.newContext();
                       page = await context.newPage();
   
                       await page.setContent(prinHTML, {
                           waitUntil: 'networkidle',
                           timeout: 30000
                       });
   
                       await page.waitForLoadState('domcontentloaded', { timeout: 10000 });
   
                       const tempDir = path.join(process.cwd(), "temp");
                       if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });
   
                       const timestamp = Date.now();
                       const safeName = selectedData.RequestorName.replace(/\s+/g, "_");
                       // const pdfPath = path.join(tempDir, `ticket_${payload.SupportTicketNo}.pdf`);
                       let pdfName = `Ticket_${payload.SupportTicketNo}_${safeName}_${timestamp}.pdf`
                       const pdfPath = path.join(tempDir, pdfName);
   
   
   
                       await page.pdf({
                           path: pdfPath,
                           format: "A4",
                           printBackground: true,
                           margin: { top: "10mm", right: "10mm", bottom: "10mm", left: "10mm" }
                       });
   
                       await page.close();
                       page = null;
   
                       const gcpResponse = await this.sendFileToGCP(pdfPath);
                       const fetchedGCPInfo = gcpResponse?.responseDynamic?.[0];
   
                       const finalPayloadToSave = {
                           SupportTicketID: selectedData?.SupportTicketID,
                           SupportTicketNo: selectedData?.SupportTicketNo,
                           TicketFileURl: fetchedGCPInfo?.gcsUrl,
                           TicketHistoryID: selectedData?.TicketHistoryID || "",
                           TicketStatusID: selectedData?.TicketStatusID || "",
                           TicketStatus: selectedData?.TicketStatus || "",
                           RequestorMobileNo: selectedData?.RequestorMobileNo || "",
                           GCSFileName: fetchedGCPInfo?.filename || "",
                           GCSId: fetchedGCPInfo?._id || "",
                           InsertedAtGCS: fetchedGCPInfo?.uploadedAt || "",
                           UpdateDateTime: Date.now(),
                           InsertedDateTime: selectedData?.Created || "",
                           fileName:pdfName || ""
                       };
   
                       if (
                           !finalPayloadToSave.SupportTicketID ||
                           !finalPayloadToSave.SupportTicketNo ||
                           !finalPayloadToSave.TicketFileURl
                       ) {
                           throw new Error("Missing required fields");
                       }
   
                       const savedInfo = await this.saveToDatabase(finalPayloadToSave);
   
                       if (savedInfo?._id) {
                           this.gupshupCallForPDFSend(finalPayloadToSave).catch(err =>
                               console.log(`Gupshup call failed for ${payload.SupportTicketNo}:`, err)
                           );
                       }
   
                       try {
                           fs.unlinkSync(pdfPath);
                       } catch (e) {
                           console.log(`Failed to delete temp file: ${pdfPath}`);
                       }
   
                       const processingTime = Date.now() - startTime;
                       console.log(`✓ Ticket ${payload.SupportTicketNo} processed in ${processingTime}ms`);
   
                       return {
                           success: true,
                           ticketNo: payload.SupportTicketNo,
                           processingTime,
                           gcsUrl: fetchedGCPInfo?.gcsUrl
                       };
   
                   } catch (err) {
                       console.log(`✗ Error processing ticket ${payload.SupportTicketNo}:`, err);
   
                       if (page) {
                           await page.close().catch(() => { });
                       }
   
                       // Return error object
                       const errorResult = {
                           success: false,
                           ticketNo: payload.SupportTicketNo,
                           processingTime: Date.now() - startTime,
                           error: err.message
                       };
   
                       return errorResult;
   
                   } finally {
                       if (context) await context.close().catch(() => { });
                       if (browser) await browser.close().catch(() => { });
                   }
               });
           } catch (err) {
               console.error('Queue error:', err);
   
               // If using this approach, you need to check result in runWorker
               return {
                   success: false,
                   ticketNo: payload.SupportTicketNo,
                   error: err.message || 'Unknown error'
               };
           }
       } */


    async ProcessInformationForFarmer(payload: any): Promise<any> {
        try {
            return this.queue.add(async () => {
                const startTime = Date.now();
                let browser: any = null;
                let context: any = null;
                let page: any = null;

                try {
                    console.log(`Processing ticket: ${payload.SupportTicketNo}`);

                    selectedData = await this.fetchSelectedData(payload?.SupportTicketNo);
                    const ticketHeaderId = Number(selectedData?.TicketHeaderID);
                    // if (selectedData?.TicketHeaderID !== 1) {
                    //     console.log(`Ticket ${payload.SupportTicketNo} skipped. TicketHeaderID (${selectedData?.TicketHeaderID}) != 1`);

                    //     return {
                    //         success: false,
                    //         ticketNo: payload.SupportTicketNo,
                    //         reason: "Processing skipped because TicketHeaderID is not 1.",
                    //         processingTime: Date.now() - startTime
                    //     };
                    // }

                    if (!ticketHeaderId || ticketHeaderId !== 1) {
                        console.log(
                            `Ticket ${payload.SupportTicketNo} skipped. Invalid or unsupported TicketHeaderID (${selectedData?.TicketHeaderID}).`
                        );

                        return {
                            success: false,
                            ticketNo: payload.SupportTicketNo,
                            reason: "Processing skipped because TicketHeaderID is missing, invalid, or not equal to 1.",
                            processingTime: Date.now() - startTime
                        };
                    }

                    const ticketListDetails: any = await this.FetchTicketInformation(payload);
                    const prinHTML = await this.renderPMFBYTemplate(selectedData, ticketListDetails);

                    browser = await chromium.launch({ headless: true });
                    context = await browser.newContext();
                    page = await context.newPage();

                    await page.setContent(prinHTML, {
                        waitUntil: 'networkidle',
                        timeout: 30000
                    });

                    await page.waitForLoadState('domcontentloaded', { timeout: 10000 });

                    const tempDir = path.join(process.cwd(), "temp");
                    if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });

                    const timestamp = Date.now();
                    const safeName = selectedData.RequestorName.replace(/\s+/g, "_");
                    const pdfName = `Ticket_${payload.SupportTicketNo}_${safeName}_${timestamp}.pdf`;
                    const pdfPath = path.join(tempDir, pdfName);

                    await page.pdf({
                        path: pdfPath,
                        format: "A4",
                        printBackground: true,
                        margin: { top: "10mm", right: "10mm", bottom: "10mm", left: "10mm" }
                    });

                    await page.close();
                    page = null;

                    const gcpResponse = await this.sendFileToGCP(pdfPath);
                    const fetchedGCPInfo = gcpResponse?.responseDynamic?.[0];

                    const finalPayloadToSave = {
                        SupportTicketID: selectedData?.SupportTicketID,
                        SupportTicketNo: selectedData?.SupportTicketNo,
                        TicketFileURl: fetchedGCPInfo?.gcsUrl,
                        TicketHistoryID: selectedData?.TicketHistoryID || "",
                        TicketStatusID: selectedData?.TicketStatusID || "",
                        TicketStatus: selectedData?.TicketStatus || "",
                        RequestorMobileNo: selectedData?.RequestorMobileNo || "",
                        GCSFileName: fetchedGCPInfo?.filename || "",
                        GCSId: fetchedGCPInfo?._id || "",
                        InsertedAtGCS: fetchedGCPInfo?.uploadedAt || "",
                        UpdateDateTime: Date.now(),
                        InsertedDateTime: selectedData?.Created || "",
                        fileName: pdfName || ""
                    };

                    if (!finalPayloadToSave.SupportTicketID ||
                        !finalPayloadToSave.SupportTicketNo ||
                        !finalPayloadToSave.TicketFileURl) {
                        throw new Error("Missing required fields");
                    }

                    const savedInfo = await this.saveToDatabase(finalPayloadToSave);

                    if (savedInfo?._id) {
                        this.gupshupCallForPDFSend(finalPayloadToSave).catch(err =>
                            console.log(`Gupshup call failed for ${payload.SupportTicketNo}:`, err)
                        );
                    }

                    try {
                        fs.unlinkSync(pdfPath);
                    } catch { }

                    const processingTime = Date.now() - startTime;
                    console.log(`Ticket ${payload.SupportTicketNo} processed in ${processingTime}ms`);

                    return {
                        success: true,
                        ticketNo: payload.SupportTicketNo,
                        processingTime,
                        gcsUrl: fetchedGCPInfo?.gcsUrl
                    };

                } catch (err: any) {
                    if (page) await page.close().catch(() => { });
                    return {
                        success: false,
                        ticketNo: payload.SupportTicketNo,
                        processingTime: Date.now() - startTime,
                        error: err.message
                    };
                } finally {
                    if (context) await context.close().catch(() => { });
                    if (browser) await browser.close().catch(() => { });
                }
            });
        } catch (err: any) {
            return {
                success: false,
                ticketNo: payload.SupportTicketNo,
                error: err.message || 'Unknown error'
            };
        }
    }

    async processBatch(payloads: any[]): Promise<any[]> {
        payloads = [payloads]
        console.log(`Starting batch processing: ${payloads?.length} tickets`);

        const startTime = Date.now();

        try {
            const results = await Promise.allSettled(
                payloads.map(payload => this.ProcessInformationForFarmer(payload))
            );

            const processedResults = results.map((result, index) => {
                const p = payloads[index] || {};
                const ticketNo = p.SupportTicketNo || null;
                const ticketId = p.SupportTicketID || null;

                if (result.status === 'fulfilled') {
                    return {
                        ...result.value,
                        ticketNo,
                        ticketId
                    };
                }

                return {
                    success: false,
                    ticketNo,
                    ticketId,
                    processingTime: 0,
                    error: result.reason?.message || 'Unknown error'
                };
            });

            const successful = processedResults.filter(r => r.success).length;
            const failed = processedResults.filter(r => !r.success).length;
            const totalTime = Date.now() - startTime;

            console.log(`Batch processing completed:`);
            console.log(`Successful: ${successful}`);
            console.log(`Failed: ${failed}`);
            console.log(`Total time: ${totalTime}ms`);
            console.log(`Average time: ${(totalTime / payloads.length).toFixed(2)}ms per PDF`);

            return processedResults;

        } catch (err) {
            console.log('Batch processing error:', err);
            throw err;
        }
    }



    async FetchTicketInformation(body: any): Promise<void> {
        if (!body) {
            throw new Error("Request body is required")
        }
        if (!this.token) {
            throw new Error("Missing token")
        }

        const { SupportTicketNo } = body
        if (!SupportTicketNo) {
            throw new Error("SupportTicketNo is required")
        }
        let TicketInfo

        const payload = {
            viewMode: "PDF",
            supportTicketNo: SupportTicketNo,
        }

        try {
            const response = await this.sendRequest(payload)

            if (response?.data?.responseDynamic) {
                const decodedResponse = await this.decompressResponse(response.data.responseDynamic)
                TicketInfo = await this.TicketDetailsBuildUp(decodedResponse)
                return TicketInfo
            } else {
                logger.warn(`FetchTicketInformation: No responseDynamic found for ticket ${SupportTicketNo}`)
            }
        } catch (err) {
            const error = err as AxiosError
            if (error.response) {
                throw new Error(`Request failed with status ${error.response.status}: ${JSON.stringify(error.response.data)}`)
            }
            if (error.request) {
                throw new Error("No response received from server")
            }
            throw new Error(error.message)
        }
    }

    private async sendRequest(payload: any) {
        return this.client.post("", payload, {
            headers: {
                Authorization: `Bearer ${this.token}`,
                "Content-Type": "application/json",
            },
        })
    }

    private async decompressResponse(compressedData: string | Uint8Array): Promise<any> {
        try {
            const dataBuffer =
                typeof compressedData === "string" ? Uint8Array.from(Buffer.from(compressedData, "base64")) : compressedData

            const decompressed = pako.inflate(dataBuffer, { to: "string" })
            return JSON.parse(decompressed)
        } catch (err) {
            throw err
        }
    }

    private async TicketDetailsBuildUp(data: any) {
        if (!data || !data.masterdatabinding || !Array.isArray(data.masterdatabinding)) {
            logger.warn("TicketDetailsBuildUp: No masterdatabinding found")
            return []
        }

        const [masterObj = {}, historyObj = {}, commentObj = {}] = data.masterdatabinding

        const masterTickets = Array.isArray(masterObj) ? masterObj : Object.values(masterObj || {})
        const histories = Array.isArray(historyObj) ? historyObj : Object.values(historyObj || {})
        const comments = Array.isArray(commentObj) ? commentObj : Object.values(commentObj || {})

        if (!masterTickets.length) {
            logger.warn("TicketDetailsBuildUp: No master tickets found")
            return []
        }

        const historyMap = new Map<number, any[]>()
        const commentMap = new Map<number, any[]>()

        for (const h of histories) {
            if (h && h.SupportTicketID != null) {
                const arr = historyMap.get(h.SupportTicketID) || []
                arr.push(h)
                historyMap.set(h.SupportTicketID, arr)
            }
        }

        for (const c of comments) {
            if (c && c.SupportTicketID != null) {
                const arr = commentMap.get(c.SupportTicketID) || []
                arr.push(c)
                commentMap.set(c.SupportTicketID, arr)
            }
        }

        const combinedTickets = masterTickets.map((ticket) => {
            if (!ticket || ticket.SupportTicketID == null) return ticket
            return {
                ...ticket,
                Histories: historyMap.get(ticket.SupportTicketID) || [],
                Comments: commentMap.get(ticket.SupportTicketID) || [],
            }
        })

        return combinedTickets
    }



    private isAgeingMatch(range: string, ageingValue: number): boolean {
        if (!range) return false

        range = range.replace(" days", "").trim()

        if (range.includes("-")) {
            const [min, max] = range.split("-").map(Number)
            return ageingValue >= min && ageingValue <= max
        }

        if (range.includes(">")) {
            const min = Number(range.replace(">", "").trim())
            return ageingValue > min
        }

        return false
    }

    private async getStatusWiseTemplate(
        pStatusID?: any,
        pData?: any,
        pselectedData?: any,
        pticketHistoryData?: any,
        pticketStatus?: any,
    ) {
        try {
            let rtnStatusWiseTemplate: any = []

            switch (pStatusID) {
                case 109301:
                    rtnStatusWiseTemplate = [
                        {
                            tat: "",
                            id: 1,
                            color: "#f06d1a",
                            text: `Farmer request received from  ( ${pData && pData.CreatedBY ? pData.CreatedBY : ""} )`,
                            date:
                                pData && pData.CreatedAt
                                    ? await this.dateToSpecificFormat(
                                        `${pData.CreatedAt.split("T")[0]} ${await this.Convert24FourHourAndMinute(
                                            pData.CreatedAt.split("T")[1],
                                        )}`,
                                        "DD-MM-YYYY HH:mm",
                                    )
                                    : null,
                            icon: `<img src="${"Ticket"}" width="24" height="24" />`,
                            textId: "",
                            agentName:
                                pData && pData.CreatedBY === "Agent"
                                    ? `Agent Name : ${pData && pData.AgentName ? pData.AgentName : null}`
                                    : `User Name : ${pData.CreatedBY}`,
                            ticket: "",
                            status: "",
                            ticketStatus:
                                pStatusID === 109301
                                    ? "Open"
                                    : pStatusID === 109302
                                        ? "In-Progress"
                                        : pStatusID === 109303
                                            ? "Resolved"
                                            : pStatusID === 109304
                                                ? "Re-Open"
                                                : "",
                            smsText: "",
                            ageing: "",
                        },
                        {
                            tat: "",
                            id: 2,
                            color: "#0f99ef",
                            text: "Farmer ticket created",
                            date:
                                pData && pData.CreatedAt
                                    ? await this.dateToSpecificFormat(
                                        `${pData.CreatedAt.split("T")[0]} ${await this.Convert24FourHourAndMinute(
                                            pData.CreatedAt.split("T")[1],
                                        )}`,
                                        "DD-MM-YYYY HH:mm",
                                    )
                                    : null,
                            icon: `<img src="${"Ticket"}" width="24" height="24" />`,
                            textId: "",
                            agentName:
                                pData && pData.CreatedBY === "Agent"
                                    ? `Agent Name : ${pData && pData.AgentName ? pData.AgentName : null}`
                                    : `User Name : ${pData.CreatedBY}`,
                            ticket: "Ticket assigned to IC User",
                            status: "",
                            ticketStatus:
                                pStatusID === 109301
                                    ? "Open"
                                    : pStatusID === 109302
                                        ? "In-Progress"
                                        : pStatusID === 109303
                                            ? "Resolved"
                                            : pStatusID === 109304
                                                ? "Re-Open"
                                                : "",
                            smsText: "",
                            ageing: "",
                        },
                        {
                            tat: "",
                            id: 3,
                            color: "#6908b1",
                            text: "SMS sent to farmer with ticket number",
                            date: "",
                            icon: `<span class="sms-icon">📩</span>`,
                            textId: `${""}`,
                            agentName: "",
                            ticket: "",
                            status: "SMS sent successfully",
                            ticketStatus:
                                pStatusID === 109301
                                    ? "Open"
                                    : pStatusID === 109302
                                        ? "In-Progress"
                                        : pStatusID === 109303
                                            ? "Resolved"
                                            : pStatusID === 109304
                                                ? "Re-Open"
                                                : "",
                            smsText: `प्रिय किसान ... ${pData.SupportTicketNo} ...`,
                            ageing: "",
                        },
                    ]
                    break

                case 109302:
                    rtnStatusWiseTemplate = [
                        {
                            tat: "",
                            id: 4,
                            color: "#dd5c9cff",
                            text: "Ticket responded by IC User",
                            date:
                                pData && pData.TicketHistoryDate
                                    ? await this.dateToSpecificFormat(
                                        `${pData.TicketHistoryDate.split("T")[0]} ${await this.Convert24FourHourAndMinute(
                                            pData.TicketHistoryDate.split("T")[1],
                                        )}`,
                                        "DD-MM-YYYY HH:mm",
                                    )
                                    : null,
                            icon: `<img src="${"Ticket"}" width="24" height="24" />`,
                            textId: "",
                            agentName:
                                pData && pData.CreatedBY === "Agent"
                                    ? `Agent Name : ${pData && pData.AgentName ? pData.AgentName : null}`
                                    : `User Name : ${pData.CreatedBY}`,
                            ticket: "",
                            status: "",
                            ticketStatus:
                                pStatusID === 109301
                                    ? "Open"
                                    : pStatusID === 109302
                                        ? "In-Progress"
                                        : pStatusID === 109303
                                            ? "Resolved"
                                            : pStatusID === 109304
                                                ? "Re-Open"
                                                : "",
                            smsText: "",
                            ageing: "",
                        },
                    ]
                    break

                case 109303:
                    let pAgeiing = ""
                    const pReOpenStatus =
                        pticketStatus === "Resolved"
                            ? "Resolved"
                            : pticketStatus === "Resolved1"
                                ? "ReOpen"
                                : pticketStatus === "Resolved2"
                                    ? "ReOpen1"
                                    : pticketStatus === "Resolved3"
                                        ? "ReOpen2"
                                        : ""

                    if (pticketStatus === "Resolved") {
                        pAgeiing = await this.getTicketAgeing(
                            pselectedData.CreatedAt,
                            pticketHistoryData,
                            pselectedData.TicketHeaderID,
                            pticketStatus,
                        )
                    } else if (pticketStatus === "Resolved1" || pticketStatus === "Resolved2" || pticketStatus === "Resolved3") {
                        const filterDataReOpen: any = Object.values(pticketHistoryData).find(
                            (entry: any) => entry.TicketStatus === pReOpenStatus,
                        )

                        if (filterDataReOpen) {
                            if (Object.values(filterDataReOpen).length > 0) {
                                pAgeiing = await this.getTicketAgeing(
                                    filterDataReOpen.TicketHistoryDate,
                                    pticketHistoryData,
                                    selectedData.TicketHeaderID,
                                    pticketStatus,
                                )
                            }
                        }
                    }

                    if (pselectedData.TicketHeaderID === 1) {
                        for (let i = 0; i < stepsTATJourneyGrv.length; i++) {
                            if (!this.isAgeingMatch(stepsTATJourneyGrv[i].ageing, Number(pAgeiing))) {
                                rtnStatusWiseTemplate.push(stepsTATJourneyGrv[i])
                            } else break
                        }
                    } else if (pselectedData.TicketHeaderID === 4) {
                        for (let i = 0; i < stepsTATJourneyCrpLs.length; i++) {
                            if (!this.isAgeingMatch(stepsTATJourneyCrpLs[i].ageing, Number(pAgeiing))) {
                                rtnStatusWiseTemplate.push(stepsTATJourneyCrpLs[i])
                            } else break
                        }
                    }

                    rtnStatusWiseTemplate.push(
                        {
                            tat: "",
                            id: 5,
                            color: "#eb0c7b",
                            text: "Ticket responded by IC User",
                            date:
                                pData && pData.TicketHistoryDate
                                    ? await this.dateToSpecificFormat(
                                        `${pData.TicketHistoryDate.split("T")[0]} ${await this.Convert24FourHourAndMinute(
                                            pData.TicketHistoryDate.split("T")[1],
                                        )}`,
                                        "DD-MM-YYYY HH:mm",
                                    )
                                    : null,
                            icon: `<img src="${"Ticket"}" width="24" height="24" />`,
                            textId: "",
                            agentName:
                                pData && pData.UserType === "CSC"
                                    ? `Agent Name : ${pData && pData.AgentName ? pData.AgentName : null}`
                                    : `User Name : ${pData.CreatedBY}`,
                            ticket: "Ticket assigned to IC Admin for response verification",
                            status: "",
                            ticketStatus:
                                pStatusID === 109301
                                    ? "Open"
                                    : pStatusID === 109302
                                        ? "In-Progress"
                                        : pStatusID === 109303
                                            ? "Resolved"
                                            : pStatusID === 109304
                                                ? "Re-Open"
                                                : "",
                            smsText: "",
                            ageing: `(${pAgeiing != null ? pAgeiing : 0} days)`,
                        },
                        {
                            tat: "",
                            id: 6,
                            color: "#01b981",
                            text: "Notification sent to farmer...",
                            date: "",
                            icon: `<span class="sms-icon">📩</span>`,
                            textId: `${""}`,
                            agentName: "",
                            ticket: "",
                            status: "SMS sent successfully",
                            ticketStatus:
                                pStatusID === 109301
                                    ? "Open"
                                    : pStatusID === 109302
                                        ? "In-Progress"
                                        : pStatusID === 109303
                                            ? "Resolved"
                                            : pStatusID === 109304
                                                ? "Re-Open"
                                                : "",
                            smsText: `प्रिय किसान ...`,
                            ageing: "",
                        },
                    )

                    break

                case 109304:
                    rtnStatusWiseTemplate = [
                        {
                            tat: "",
                            id: 7,
                            color: "#b94e00",
                            text: "Ticket reponed by farmer",
                            date:
                                pData && pData.TicketHistoryDate
                                    ? await this.dateToSpecificFormat(
                                        `${pData.TicketHistoryDate.split("T")[0]} ${await this.Convert24FourHourAndMinute(
                                            pData.TicketHistoryDate.split("T")[1],
                                        )}`,
                                        "DD-MM-YYYY HH:mm",
                                    )
                                    : null,
                            icon: `<img src="${"Ticket"}" width="24" height="24" />`,
                            textId: "",
                            agentName:
                                pData && pData.UserType === "CSC"
                                    ? `Agent Name : ${pData && pData.CreatedBY ? pData.CreatedBY : null}`
                                    : `User Name : ${pData.CreatedBY}`,
                            ticket: "Ticket assigned to IC User",
                            status: "",
                            ticketStatus:
                                pStatusID === 109301
                                    ? "Open"
                                    : pStatusID === 109302
                                        ? "In-Progress"
                                        : pStatusID === 109303
                                            ? "Resolved"
                                            : pStatusID === 109304
                                                ? "Re-Open"
                                                : "",
                            smsText: "",
                            ageing: "",
                        },
                        {
                            tat: "",
                            id: 8,
                            color: "#f06d1a",
                            text: "SMS sent to farmer with ticket number...",
                            date: "",
                            icon: `<span class="sms-icon">📩</span>`,
                            textId: `${""}`,
                            agentName: "",
                            ticket: "",
                            status: "SMS sent successfully",
                            ticketStatus:
                                pStatusID === 109301
                                    ? "Open"
                                    : pStatusID === 109302
                                        ? "In-Progress"
                                        : pStatusID === 109303
                                            ? "Resolved"
                                            : pStatusID === 109304
                                                ? "Re-Open"
                                                : "",
                            smsText: `प्रिय किसान ...`,
                            ageing: "",
                        },
                    ]
                    break

                default:
                    rtnStatusWiseTemplate = []
            }

            const finalResolved = await Promise.all(rtnStatusWiseTemplate.map(async (item) => ({ ...item })))

            return finalResolved
        } catch (err) {
            console.log(err)
            return []
        }
    }

    private async dateToSpecificFormat(date: any, format = "DD-MM-YYYY HH:mm") {
        try {
            const d = new Date(date)
            const convertedDate = moment(d).format(format)
            return convertedDate
        } catch {
            return null
        }
    }

    private async Convert24FourHourAndMinute(timeString = "") {
        const [hour, minute] = timeString.split(":")
        const formattedHour = Number(hour)
        return `${formattedHour}:${minute}`
    }

    async getTicketAgeing(createdDate: any, ticketHistory: any, ticketHeaderID: any, pResolvedStatus: any) {
        if (!createdDate || !ticketHistory) return null

        const resolvedEntry: any = Object.values(ticketHistory).find(
            (entry: any) => entry?.TicketStatus === pResolvedStatus,
        )

        if (!resolvedEntry) return null

        const resolvedDate = moment(resolvedEntry.TicketHistoryDate)
        const created = moment(createdDate)

        const diffDays = resolvedDate.diff(created, "days")

        if (ticketHeaderID === 1) {
            if (diffDays >= 0 && diffDays <= 3) return "0-3"
            if (diffDays >= 4 && diffDays <= 7) return "4-7"
            if (diffDays >= 8 && diffDays <= 12) return "8-12"
            if (diffDays >= 13 && diffDays <= 15) return "13-15"
            if (diffDays >= 16) return "16>"
        } else if (ticketHeaderID === 4) {
            if (diffDays >= 0 && diffDays <= 10) return "0-10"
            if (diffDays >= 11 && diffDays <= 15) return "11-15"
            if (diffDays >= 16 && diffDays <= 20) return "16-20"
            if (diffDays >= 20) return "20>"
        }

        return null
    }



    async getCaseHistoryStepByStep(commentsTickets: any, pdata: any) {
        debugger
        let rtnCommentsList = []
        if (commentsTickets.length === 0) {
            const statusTemplate = await this.getStatusWiseTemplate(pdata.TicketStatusID, pdata, "", "")
            rtnCommentsList = statusTemplate
        } else if (commentsTickets.length > 0) {
            const allTicketStatusTemplate = []

            const statusTemplate = await this.getStatusWiseTemplate(109301, pdata, "", "")
            allTicketStatusTemplate.push(...statusTemplate)

            for (const value of commentsTickets) {
                if (value.TicketStatusID === 109302) {
                    const statusTemplate = await this.getStatusWiseTemplate(value.TicketStatusID, value, pdata, "", "")
                    allTicketStatusTemplate.push(...statusTemplate)
                } else if (value.TicketStatusID === 109304) {
                    const statusTemplate = await this.getStatusWiseTemplate(value.TicketStatusID, value, pdata, "", "")
                    allTicketStatusTemplate.push(...statusTemplate)
                } else if (value.TicketStatusID === 109303) {
                    const statusTemplate = await this.getStatusWiseTemplate(
                        value.TicketStatusID,
                        value,
                        pdata,
                        commentsTickets,
                        value.TicketStatus,
                    )
                    allTicketStatusTemplate.push(...statusTemplate)
                }
            }

            rtnCommentsList = allTicketStatusTemplate
        }

        return rtnCommentsList
    }


    async fetchSelectedData(ticketNumber: string) {
        try {
            const db = await this.connectDB()
            const collection = db.collection("SLA_Ticket_listing")
            const record = await collection.findOne({ SupportTicketNo: ticketNumber })

            if (!record) {
                return {
                    data: {},
                    message: { msg: "No Record Found Associated With This Ticket No.", code: 1 },
                }
            }

            return record
        } catch (err) {
            return { data: {}, message: { msg: "Internal Server Error", code: 0 } }
        }
    }



    async renderPMFBYTemplate(selectedData: any = {}, ticketListDetails: any[] = [], helpers: any = {}) {
        const s = (path: any, def: any = "") => {
            try {
                if (path === undefined || path === null) {
                    return def
                }
                return path
            } catch (e) {
                return def
            }
        }

        let ticketHtml = ""

        for (let idx = 0; idx < (ticketListDetails || []).length; idx++) {
            const data: any = ticketListDetails[idx]

            //   console.log(data, "test")

            let historiesHtml = ""
            const historiesArray = data && data.Histories ? data.Histories : []

            if (Array.isArray(historiesArray)) {
                for (let i = 0; i < historiesArray.length; i++) {
                    const v: any = historiesArray[i]

                    let formattedHistoryDate = ""
                    if (v && v.TicketHistoryDate) {
                        const splitParts = (v.TicketHistoryDate || "").split("T")
                        const datePart = splitParts[0] || ""
                        const timePartRaw = splitParts[1] || ""
                        const timePart = await this.Convert24FourHourAndMinute(timePartRaw)
                        const combined = datePart + " " + timePart
                        formattedHistoryDate = await this.dateToSpecificFormat(combined, "DD-MM-YYYY HH:mm")
                    }

                    let ticketDescription = ""
                    if (v && v.TicketDescription) {
                        ticketDescription = await this.parseHtml(v.TicketDescription)
                    }

                    let auditButton = ""
                    if (v && v.isAudit === 1 && v.TicketStatusID === 109303) {
                        auditButton =
                            '<button style="background-color:#55d464ff;color:#ffffff;border:1px solid #1ce447ff;border-radius:10px;padding:5px 15px;font-size:14px;font-weight:400;">Audited</button>'
                    }

                    let reasonHtml = ""
                    if (
                        v &&
                        data &&
                        data.isSatisfied === 0 &&
                        v.AuditRemarks &&
                        v.AuditRemarks !== "" &&
                        v.TicketStatusID === 109303
                    ) {
                        reasonHtml = "<strong>Reason : </strong>"
                        reasonHtml += "<div>" + (v && v.AuditRemarks ? v.AuditRemarks : "") + "</div>"
                    }

                    let userTypeDisplay = ""
                    if (v && v.UserType === "CSC") {
                        if (v && v.CallingUserID) {
                            userTypeDisplay = "Agent ID : " + v.CallingUserID
                        } else {
                            userTypeDisplay = "Agent ID : NA"
                        }
                    } else {
                        userTypeDisplay = data && data.UserType ? data.UserType : ""
                    }

                    let createdByNameFirst = ""
                    if (v && v.CreatedBY) {
                        const nameParts = (v.CreatedBY || "").split(" ")
                        if (nameParts.length > 0) {
                            createdByNameFirst = nameParts[0]
                        } else {
                            createdByNameFirst = v.CreatedBY || ""
                        }
                    }

                    historiesHtml +=
                        "" +
                        '<div class="accordion" style="margin-bottom:8px;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,0.1);overflow:hidden;">' +
                        '  <div class="accordion-summary" style="background:#4a90e2;color:white;padding:8px;display:flex;justify-content:space-between;align-items:center;">' +
                        '    <div style="font-weight:bold;">Activity ' +
                        (i + 2) +
                        " : " +
                        (v && v.TicketStatus ? v.TicketStatus : "") +
                        "</div>" +
                        '    <div style="text-align:center;font-weight:bold;">' +
                        formattedHistoryDate +
                        "</div>" +
                        '    <div style="font-weight:bold;">Name : ' +
                        createdByNameFirst +
                        "</div>" +
                        '    <div style="font-weight:bold;">' +
                        userTypeDisplay +
                        "</div>" +
                        "  </div>" +
                        '  <div class="accordion-details" style="background:#f5f7fa;padding:12px;">' +
                        '    <div style="background:white;padding:12px;border-radius:8px;">' +
                        '      <div style="font-weight:bold;margin-bottom:6px;">Description :</div>' +
                        '      <div style="color:#666;">' +
                        ticketDescription +
                        "</div>" +
                        auditButton +
                        reasonHtml +
                        "    </div>" +
                        "  </div>" +
                        "</div>"
                }
            }

            let commentsHtml = ""

            if (Array.isArray(data && data.Comments ? data.Comments : []) && (data.Comments || []).length > 0) {
                const steps = await this.getCaseHistoryStepByStep(data.Comments, data)

                // console.log(steps, "stepsstepsstepssteps")

                if (Array.isArray(steps)) {
                    for (let i = 0; i < steps.length; i++) {
                        const step = steps[i]

                        let iconHtml = ""
                        if (step.icon !== undefined && step.icon !== null && step.icon !== "") {
                            iconHtml = step.icon
                        } else {
                            iconHtml = '<img src="/img/ticket.svg" width="24" height="24"/>'
                        }

                        let smsHoverHtml = ""
                        if (step.smsText !== undefined && step.smsText !== null && String(step.smsText).trim() !== "") {
                            smsHoverHtml = '<div class="hover-cardSendPdf"><p>' + step.smsText + "</p></div>"
                        }

                        commentsHtml +=
                            "" +
                            '<div class="card-tooltipSendPdf" key="' +
                            step.id +
                            "-" +
                            i +
                            '" style="position:relative;margin:12px 0;">' +
                            '  <div class="pillSendPdf top" style="border:1px solid ' +
                            (step.color || "") +
                            ";background:linear-gradient(to bottom, " +
                            (step.color || "") +
                            ' 25%, #fff 20%);margin-bottom:250px;">' +
                            '    <div class="circleSendPdf circle-topSendPdf" style="border:1px solid ' +
                            (step.color || "") +
                            ';">' +
                            '      <p class="iconSendPdf">' +
                            iconHtml +
                            "</p>" +
                            "    </div>" +
                            '    <div class="text-partSendPdf text-topSendPdf">' +
                            "      <strong>" +
                            (step.tat || "") +
                            "</strong>" +
                            "      <p>" +
                            (step.text || "") +
                            "</p>" +
                            '      <strong style="font-size:12px;">' +
                            (step.ticketStatus || "") +
                            "</strong>" +
                            '      <p><strong style="font-size:12px;">' +
                            (step.ageing || "") +
                            "</strong></p>" +
                            "    </div>" +
                            '    <div class="connectorSendPdf connector-bottomSendPdf"><span class="dot" style="background:' +
                            (step.color || "") +
                            '"></span></div>' +
                            '    <div class="detailsSendPdf details-bottomSendPdf">' +
                            "      <p>" +
                            (step.textId || "") +
                            "</p>" +
                            "      <p>" +
                            (step.agentName || "") +
                            "</p>" +
                            "      <p>" +
                            (step.date || "") +
                            "</p>" +
                            "      <strong>" +
                            (step.ticket || "") +
                            "</strong>" +
                            '      <p class="successSendPdf">' +
                            (step.status || "") +
                            "</p>" +
                            "    </div>" +
                            "  </div>" +
                            smsHoverHtml +
                            "</div>"
                    }
                }
            }

            let createdAtFormatted = ""
            if (data && data.CreatedAt) {
                const splitCreatedAt = (data.CreatedAt || "").split("T")
                const createdDatePart = splitCreatedAt[0] || ""
                const createdTimePartRaw = splitCreatedAt[1] || ""
                const createdTimePart = await this.Convert24FourHourAndMinute(createdTimePartRaw)
                createdAtFormatted = await this.dateToSpecificFormat(
                    createdDatePart + " " + createdTimePart,
                    "DD-MM-YYYY HH:mm",
                )
            }

            let statusClass = ""
            if (data && data.TicketStatus) {
                const ticketStatusLower = (data.TicketStatus || "").toLowerCase()
                statusClass = ticketStatusLower.split("-").join("")
            }

            const ticketNumberHtml = data && data.SupportTicketNo ? data.SupportTicketNo : ""
            const ticketTypeHtml =
                (data && data.TicketHeadName ? data.TicketHeadName : "") +
                " → " +
                (data && data.TicketCategoryName ? data.TicketCategoryName : "") +
                " → " +
                (data && data.TicketSubCategoryName ? data.TicketSubCategoryName : "")
            const sourceHtml = data && data.CreatedBY ? data.CreatedBY : ""

            let requestorContactHtml = ""
            if (data && data.RequestorMobileNo) {
                requestorContactHtml = "+91 " + data.RequestorMobileNo
            }

            let seasonLabel = ""
            if (data && data.RequestSeason === 1) {
                seasonLabel = "Kharif"
            } else if (data && data.RequestSeason === 2) {
                seasonLabel = "Rabi"
            } else {
                seasonLabel = ""
            }

            ticketHtml +=
                "" +
                '<div class="containerhistory">' +
                '  <div class="left-panel">' +
                '    <div class="ticketcard">' +
                '      <div class="ticketheader" style="display:flex;justify-content:space-between;align-items:center;">' +
                '        <div class="leftpanel">' +
                "          <h1><strong>Ticket Number : </strong> " +
                ticketNumberHtml +
                "</h1>" +
                "          <h1><strong>Ticket Type : </strong> " +
                ticketTypeHtml +
                "</h1>" +
                "          <h1><strong>Source : </strong> " +
                sourceHtml +
                "</h1>" +
                "        </div>" +
                '        <div class="rightpanel">' +
                '          <span class="status ' +
                statusClass +
                '">' +
                (data && data.TicketStatus ? data.TicketStatus : "") +
                "</span>" +
                "        </div>" +
                "      </div>" +
                "    </div>" +
                '    <div class="accordion" style="border-radius:8px;box-shadow:0 2px 6px rgba(0,0,0,0.12);overflow:hidden;margin-top:8px;">' +
                '      <div class="accordion-summary" style="background:#4a90e2;color:white;padding:8px;display:flex;gap:12px;align-items:center;">' +
                '        <div style="flex:1;font-weight:bold;">Activity 1 : Ticket Created</div>' +
                '        <div style="text-align:center;font-weight:bold;">' +
                createdAtFormatted +
                "</div>" +
                '        <div style="font-weight:bold;">Name : ' +
                (data && data.AgentName ? data.AgentName : "") +
                "</div>" +
                '        <div style="font-weight:bold;">' +
                (data && data.CreatedType === "Agent"
                    ? "Agent ID: " + (data && data.CallingUserID ? data.CallingUserID : "")
                    : data && data.CreatedType
                        ? data.CreatedType
                        : "") +
                "        </div>" +
                "      </div>" +
                '      <div class="accordion-details" style="background:#f5f7fa;padding:12px;">' +
                '        <div style="background:white;padding:12px;border-radius:8px;">' +
                '          <div style="font-weight:bold;">Description :</div>' +
                '          <div style="color:#666;">' +
                (data && data.TicketDescription ? await this.parseHtml(data.TicketDescription) : "") +
                "</div>" +
                "        </div>" +
                "      </div>" +
                "    </div>" +
                historiesHtml +
                "  </div>" +
                '  <div class="right-panel">' +
                '    <div id="pdf-last-section" class="CustomerBox">' +
                '      <div class="Heading">' +
                '        <div class="ReqInfo">' +
                // '          <img src="/img/customer-avatar.png" alt="Customer" />' +
                '<span style="font-size: 34px;">👤</span>' +

                "          <h3>" +
                (data && data.RequestorName ? data.RequestorName : "") +
                "</h3>" +
                "          <br />" +
                "          <p>" +
                requestorContactHtml +
                "</p>" +
                "        </div>" +
                '        <div class="ActionBox">' +
                "          <span>⋮</span>" +
                '          <span title="Download Farmer Information" style="cursor:pointer;">⬇︎</span>' +
                "        </div>" +
                "      </div>" +
                '      <div class="MainBox">' +
                '        <div class="InfoBox" id="iwant_flex">' +
                '          <div class="SubBox">' +
                '            <p>Season - Year : <span id="spnSeasonYear">' +
                seasonLabel +
                " - " +
                (data && data.RequestYear ? data.RequestYear : "") +
                "</span></p>" +
                "          </div>" +
                '          <div class="SubBox">' +
                '            <p>Residential Location : <span id="spnInsStateDistrict">' +
                (data && data.StateMasterName ? data.StateMasterName : "") +
                (data && data.DistrictMasterName ? ", " + data.DistrictMasterName.trim() : "") +
                (data && data.SubDistrictName ? ", " + data.SubDistrictName : "") +
                (data && data.VillageName ? ", " + data.VillageName : "") +
                "</span></p>" +
                "          </div>" +
                '          <div class="SubBox">' +
                '            <p>Land Location : <span id="spnLandDistrictVillage">' +
                (data && data.PlotStateName ? data.PlotStateName : "") +
                (data && data.PlotDistrictName ? ", " + data.PlotDistrictName.trim() : "") +
                (data && data.PlotVillageName ? ", " + data.PlotVillageName : "") +
                "</span></p>" +
                "          </div>" +
                '          <div class="SubBox">' +
                '            <p>Ins Company : <span id="spnInsCompany">' +
                (data && data.InsuranceCompany ? data.InsuranceCompany : "") +
                "</span></p>" +
                "          </div>" +
                '          <div class="SubBox">' +
                '            <p>Policy No : <span id="spnPolicyNo">' +
                (data && data.InsurancePolicyNo ? data.InsurancePolicyNo : "") +
                "</span></p>" +
                "          </div>" +
                '          <div class="SubBox">' +
                '            <p>Application No : <span id="spnApplicationNo">' +
                (data && data.ApplicationNo ? data.ApplicationNo : "") +
                "</span></p>" +
                "          </div>" +
                '          <div class="SubBox">' +
                '            <p>Land Survey Or Land Division Number : <span id="spnLandSurveyDivision">' +
                (data && data.LandSurveyNumber ? data.LandSurveyNumber : "") +
                " Or " +
                (data && data.LandDivisionNumber ? data.LandDivisionNumber : "") +
                "</span></p>" +
                "          </div>" +
                '          <div class="SubBox">' +
                '            <p>Area : <span id="spnArea">' +
                (data && data.PolicyArea ? data.PolicyArea : "") +
                "</span></p>" +
                "          </div>" +
                '          <div class="SubBox">' +
                '            <p>Crop Name : <span id="spnCropName">' +
                (data && data.ApplicationCropName ? data.ApplicationCropName : "") +
                "</span></p>" +
                "          </div>" +
                '          <div class="SubBox">' +
                '            <p>Premium Amount : <span id="spnPremiumAmount">' +
                (data && data.PolicyPremium ? data.PolicyPremium : "") +
                "</span></p>" +
                "          </div>" +
                '          <div class="SubBox">' +
                '            <p>Scheme : <span id="spnScheme">' +
                (data && data.SchemeName ? data.SchemeName : "") +
                "</span></p>" +
                "          </div>" +
                "        </div>" +
                "      </div>" +
                "    </div>" +
                "  </div>" +
                "</div>" +
                "<hr />" +
                // '<div id="case_history_ticket_details">' +
                // "  <h6>Case History</h6>" +
                // commentsHtml +
                // "</div>" +
                "<br />" +
                "<br />" +
                "<br />" +
                "<br />" +
                "<br />" +
                "<br />" +
                "<hr />"
        }

        let createdAtOfSelectedFormatted = ""
        if (selectedData && selectedData.CreatedAt) {
            const parts = (selectedData.CreatedAt || "").split("T")
            const datePart = parts[0] || ""
            const timePartRaw = parts[1] || ""
            const timePart = await this.Convert24FourHourAndMinute(timePartRaw)
            createdAtOfSelectedFormatted = await this.dateToSpecificFormat(datePart + " " + timePart, "DD-MM-YYYY HH:mm")
        }

        let selectedSeasonLabel = ""
        if (selectedData && selectedData.RequestSeason === 1) {
            selectedSeasonLabel = "Kharif"
        } else if (selectedData && selectedData.RequestSeason === 2) {
            selectedSeasonLabel = "Rabi"
        } else {
            selectedSeasonLabel = ""
        }

        const html =
            "" +
            "<!doctype html>" +
            '<html lang="en">' +
            "<head>" +
            '  <meta charset="utf-8" />' +
            '  <meta name="viewport" content="width=device-width, initial-scale=1" />' +
            "  <title>PMFBY - Policy</title>" +
            "<style>" +
            "  .main-box { width: 1055px; padding: 10px; }" +
            "  .header { display: flex; align-items: center; justify-content: flex-start; border: 1px solid #000; padding: 8px 12px; margin-bottom: 10px; }" +
            "  .header img { height: 60px; }" +
            "  .divider { width: 1px; height: 50px; background-color: #000; margin: 0 12px; }" +
            "  .header-text { margin-left: 10px; }" +
            "  .header-text h2 { font-size: 18px; margin: 0; font-weight: 700; }" +
            "  .header-text p { font-size: 13px; margin: 0; font-weight: 500; }" +
            "  .policy-bar { background-color: #f4c430; color: #000; border: 1px solid #000; padding: 6px 12px; display: flex; justify-content: space-between; align-items: center; font-size: 13px; margin-bottom: 10px; }" +
            "  .policy-bar b { font-weight: 700; }" +
            "  .info-box { display: flex; align-items: stretch; border: 1px solid #000; width: 100%; background: #fff; margin-bottom: 10px; }" +
            "  .left-qr, .right-qr { background-color: #eaf8e5; display: flex; align-items: center; justify-content: center; width: 180px; padding: 5px; }" +
            "  .left-qr { border-right: 1px solid #000; }" +
            "  .right-qr { border-left: 1px solid #000; flex-direction: row; position: relative; }" +
            "  .right-qr-content { display: flex; flex-direction: column; align-items: center; }" +
            "  .left-qr img, .right-qr img { width: 140px; height: 140px; }" +
            "  .center-info { flex: 1; padding: 10px 20px; font-size: 12px; }" +
            "  .center-info table { width: 100%; border-collapse: collapse; border: none; }" +
            "  .center-info td { padding: 4px 0; text-align: left; border: none; }" +
            "  .center-info td:first-child { color: #444; }" +
            "  .center-info td:last-child { font-weight: bold; }" +
            "  .whatsapp-label { writing-mode: vertical-rl; transform: rotate(180deg); font-weight: bold; color: #000; padding: 0 4px; text-align: center; }" +
            "  .whatsapp-number { font-weight: bold; color: #075e54; margin-top: 4px; }" +
            "  table { width: 100%; border-collapse: collapse; font-size: 12px; }" +
            "  td, th { border: 1px solid #000; padding: 6px 10px; text-align: center; }" +
            "  .Farmer-info { margin-bottom: 10px; border: 1px solid #000; }" +
            "  .Farmer-info table { width: 100%; border-collapse: collapse; border: 1px solid #000; }" +
            "  .Farmer-info td { padding: 4px 0; text-align: center; border: 1px solid #000; }" +
            "  .section-title { font-weight: bold; padding: 6px; border: 1px solid #000; margin-bottom: 2px; }" +
            "  .section-title-table { margin-bottom: 15px; border: 1px solid #000; }" +
            "  .section-title-table table { width: 100%; border-collapse: collapse; border: 1px solid #000; font-size: 12px; }" +
            "  .section-title-table td { padding: 4px; text-align: center; border: 1px solid #000; font-weight: 600; }" +
            "  .section-title-table th { padding: 4px 0; text-align: center; border: 1px solid #000; background-color: #fff; font-weight: normal; color: #000; }" +
            "  .summary-table.no-inner-border { border: 1px solid #000; border-collapse: collapse; margin-bottom: 15px; }" +
            "  .summary-table.no-inner-border td { border: none; padding: 6px 10px; text-align: left; }" +
            "  .summary-table.no-inner-border tr td:first-child { border-right: 1px solid transparent; }" +
            "  .currency { font-family: 'DejaVu Sans', Arial, sans-serif; }" +
            "  .h6Tag { font-weight: 600; font-size: 14px; }" +
            "  .pTag { font-size: 12.5px; text-align: left; }" +
            "  .hrTag { font-weight: bold; }" +
            "  .containerhistory { display: flex; flex-direction: row; justify-content: space-between; align-items: flex-start; gap: 10px; width: 100%; font-size: 12px; }" +
            "  .left-panel { flex: 2.2; background: #fff; border-radius: 10px; padding: 0px; width: 550px; }" +
            "  .ticket-header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #e6e6e6; padding-bottom: 10px; margin-bottom: 20px; }" +
            "  .ticket-header h2 { font-size: 18px; color: #222; }" +
            "  .ticket-status { background-color: #e6fff0; color: #007a33; border: 1px solid #007a33; border-radius: 20px; padding: 6px 14px; font-size: 14px; font-weight: 600; display: inline-block; text-align: center; margin-bottom: 8px; }" +
            "  .ticket-info { line-height: 1.8; font-size: 15px; }" +
            "  .activity { background: #fff; border-radius: 10px; margin-top: 15px; border: 1px solid #ddd; overflow: hidden; }" +
            "  .activity-header { background-color: #2b78e4; color: white; padding: 10px 15px; font-weight: bold; display: flex; justify-content: space-between; align-items: center; }" +
            "  .activity-content { background: #f9f9f9; padding: 15px; }" +
            "  .activity-content p { margin-bottom: 8px; }" +
            "  .right-panel { flex: 1; background: #fff; border-radius: 10px; padding: 5px; width: 450px; height: fit-content; }" +
            "  .ticketcard { width: 100%; background: #fff; border-radius: 12px; box-shadow: 0 6px 18px rgba(0,0,0,0.08); border: 1px solid #e5e7eb; overflow: hidden; margin-bottom: 15px; }" +
            "  .ticketheader { display: flex; justify-content: space-between; align-items: center; padding: 6px 12px; background: linear-gradient(135deg, #f9fafb, #ffffff); border-bottom: 1px solid #e5e7eb; }" +
            "  .leftpanel h1 { font-size: 14px; color: #374151; }" +
            "  .leftpanel strong { color: #111827; }" +
            "  .status { padding: 6px 14px; border-radius: 20px; font-size: 13px; font-weight: 600; border: 1px solid #10b981; background: #ecfdf5; color: #065f46; }" +
            "  .status.open { background: #fffbeb; color: #92400e; border-color: #f59e0b; }" +
            "  .status.reopen { background: #fef2f2; color: #991b1b; border-color: #ef4444; }" +
            "  .status.inprogress { background: #fffbeb; color: #92400e; border-color: #f59e0b; }" +
            "  .status.resolved { background: #ecfdf5; color: #065f46; border-color: #10b981; }" +
            "  .rightpanel { min-width: 120px; text-align: -webkit-center; display: inline-block; }" +
            "  .Event1panel { display: grid; width: 100%; padding: 10px 12px 5px 12px; }" +
            "  .pillSendPdf { width: 120px; min-height: 235px; border: 5px solid black; border-radius: 150px; position: relative; margin: 45px 5px; transition: all 0.3s ease; }" +
            "  .pillSendPdf.top { align-self: flex-start; }" +
            "  .pillSendPdf.bottom { align-self: flex-end; }" +
            "  .circleSendPdf { width: 40px; height: 40px; border-radius: 50%; background: #fff; border: 3px solid; position: absolute; left: 50%; transform: translateX(-50%); display: flex; align-items: center; justify-content: center; font-size: 22px; }" +
            "  .iconSendPdf { margin-top: 13px; }" +
            "  .circle-topSendPdf { top: 13%; }" +
            "  .circle-bottomSendPdf { bottom: 13%; }" +
            "  .text-partSendPdf { width: 100%; padding: 5px 10px; font-size: 10px; font-weight: 600; text-align: center; white-space: normal; position: absolute; left: 50%; transform: translateX(-50%); line-height: 1.4; }" +
            "  .text-topSendPdf { top: 35%; }" +
            "  .text-bottomSendPdf { bottom: 35%; }" +
            "  .connectorSendPdf { width: 2px; height: 30px; background: #ccc; position: absolute; left: 50%; transform: translateX(-50%); }" +
            "  .connector-bottomSendPdf { bottom: -19%; }" +
            "  .connector-topSendPdf { top: -19%; }" +
            "  .connectorSendPdf .dot { width: 12px; height: 12px; border-radius: 50%; position: absolute; left: 50%; transform: translateX(-50%); }" +
            "  .connector-bottomSendPdf .dot { bottom: -12px; }" +
            "  .connector-topSendPdf .dot { top: -12px; }" +
            "  .detailsSendPdf { font-size: 11px; color: #444; text-align: left; width: 100%; position: absolute; text-wrap: wrap; top: 315px; }" +
            "  .details-bottomSendPdf { width: 100%; left: 5%; }" +
            "  .details-topSendPdf { width: 100%; top: -95%; left: 5%; }" +
            "  .detailsSendPdf p { margin: 2px 0; }" +
            "  .successSendPdf { color: green; font-weight: bold; }" +
            "  .card-tooltipSendPdf { position: relative; display: inline-block; }" +
            "  .hover-cardSendPdf { position: absolute; bottom: 5%; left: 50%; transform: translateX(-50%) scale(0.95); min-width: 300px; max-width: 220px; padding: 8px 10px; background: #fff; color: #333; border-radius: 8px; font-size: 11px; line-height: 1.4; text-align: justify; box-shadow: 0 4px 10px rgba(0,0,0,0.25); opacity: 0; visibility: hidden; transition: all 0.25s ease; z-index: 2000; }" +
            "  .card-tooltipSendPdf:hover .hover-cardSendPdf { opacity: 1; visibility: visible; transform: translateX(-50%) scale(1); }" +
            "  .hover-cardSendPdf::after { content: ''; position: absolute; top: 100%; left: 50%; margin-left: -6px; border-width: 6px; border-style: solid; border-color: #fff transparent transparent transparent; }" +
            "</style>" +
            "</head>" +
            "<body>" +
            '  <div class="main-box">' +
            '    <div class="header">' +
            '      <img src="https://upload.wikimedia.org/wikipedia/commons/5/55/Emblem_of_India.svg" alt="India Emblem" />' +
            '      <div class="divider"></div>' +
            '      <img src="https://pmfby.amnex.co.in/pmfby/public/img/logo-product.svg" alt="PMFBY Logo" />' +
            '      <div class="header-text">' +
            "        <h2>Pradhan Mantri Fasal Bima Yojana</h2>" +
            '        <p class="pTag">MINISTRY OF AGRICULTURE & FARMERS WELFARE</p>' +
            "      </div>" +
            "    </div>" +
            '    <div class="policy-bar">' +
            "      <div>Policy number: <b>" +
            s(selectedData && selectedData.InsurancePolicyNo ? selectedData.InsurancePolicyNo : "", "") +
            "</b></div>" +
            "      <div>Application Status: <b>Approved By GOI</b></div>" +
            "    </div>" +
            '    <div class="info-box">' +
            '      <div class="left-qr"><img src="https://pmfby.amnex.co.in/pmfby/public/krph/documents//leftQR.jpg" alt="Left QR" /></div>' +
            '      <div class="center-info">' +
            "        <table>" +
            "          <tr><td>State</td><td>: " +
            s(selectedData && selectedData.StateMasterName ? selectedData.StateMasterName : "", "") +
            "</td></tr>" +
            "          <tr><td>Scheme</td><td>: " +
            s(selectedData && selectedData.SchemeName ? selectedData.SchemeName : "", "") +
            "</td></tr>" +
            "          <tr><td>Year</td><td>: " +
            s(selectedData && selectedData.RequestYear ? selectedData.RequestYear : "", "") +
            "</td></tr>" +
            "          <tr><td>Season</td><td>: " +
            selectedSeasonLabel +
            "</td></tr>" +
            "          <tr><td>Created By</td><td>: " +
            s(selectedData && selectedData.CreatedBY ? selectedData.CreatedBY : "", "") +
            "</td></tr>" +
            "          <tr><td>Created At</td><td>: " +
            (createdAtOfSelectedFormatted ? createdAtOfSelectedFormatted : "") +
            "</td></tr>" +
            "        </table>" +
            "      </div>" +
            '      <div class="right-qr">' +
            '        <div class="right-qr-content">' +
            '          <img src="https://pmfby.amnex.co.in/pmfby/public/img/whatsapp-chatbot-scanner.jpg" alt="Right QR" />' +
            '          <div class="whatsapp-number">7065514447</div>' +
            "        </div>" +
            "      </div>" +
            "    </div>" +
            '    <table class="Farmer-info">' +
            "      <tr><td><b>Farmer Name</b></td><td>" +
            s(selectedData && selectedData.RequestorName ? selectedData.RequestorName : "", "") +
            "</td></tr>" +
            "      <tr><td><b>Register Mobile number</b></td><td>" +
            s(selectedData && selectedData.RequestorMobileNo ? selectedData.RequestorMobileNo : "", "") +
            "</td></tr>" +
            "      <tr><td><b>Policy Number</b></td><td>" +
            s(selectedData && selectedData.InsurancePolicyNo ? selectedData.InsurancePolicyNo : "", "") +
            "</td></tr>" +
            "      <tr><td><b>Season & Year</b></td><td>" +
            selectedSeasonLabel +
            " - " +
            s(selectedData && selectedData.RequestYear ? selectedData.RequestYear : "", "") +
            "</td></tr>" +
            "      <tr><td><b>State</b></td><td>" +
            s(selectedData && selectedData.PlotStateName ? selectedData.PlotStateName : "", "") +
            "</td></tr>" +
            "      <tr><td><b>District</b></td><td>" +
            s(selectedData && selectedData.PlotDistrictName ? selectedData.PlotDistrictName : "", "") +
            "</td></tr>" +
            "      <tr><td><b>Insurance Company</b></td><td>" +
            s(selectedData && selectedData.InsuranceCompany ? selectedData.InsuranceCompany : "", "") +
            "</td></tr>" +
            "      <tr><td><b>Crop Name</b></td><td>" +
            s(selectedData && selectedData.ApplicationCropName ? selectedData.ApplicationCropName : "", "") +
            "</td></tr>" +
            "    </table>" +
            '    <div class="section-title">Crop Details</div>' +
            '    <table class="section-title-table">' +
            "      <thead>" +
            "        <tr>" +
            "          <th>District</th><th>Village</th><th>Crop</th><th>Survey No</th><th>Area Insured (Hect./Plants)</th><th>Farmer Share (₹)</th>" +
            "        </tr>" +
            "      </thead>" +
            "      <tbody>" +
            "        <tr>" +
            "          <td>" +
            s(selectedData && selectedData.PlotDistrictName ? selectedData.PlotDistrictName : "", "") +
            "</td>" +
            "          <td>" +
            s(selectedData && selectedData.PlotVillageName ? selectedData.PlotVillageName : "", "") +
            "</td>" +
            "          <td>" +
            s(selectedData && selectedData.ApplicationCropName ? selectedData.ApplicationCropName : "", "") +
            "</td>" +
            "          <td>" +
            s(selectedData && selectedData.LandSurveyNumber ? selectedData.LandSurveyNumber : "", "") +
            "</td>" +
            "          <td>" +
            s(selectedData && selectedData.AREA ? selectedData.AREA : "", "") +
            "</td>" +
            "          <td>" +
            s(selectedData && selectedData.FarmerShare ? selectedData.FarmerShare : "", "") +
            "</td>" +
            "        </tr>" +
            "      </tbody>" +
            "    </table>" +
            '    <table class="summary-table no-inner-border" style="display:none;">' +
            "      <tr>" +
            "        <td><b>Total Area Insured (Hect./Plants):</b><br/></td>" +
            '        <td><b>Total Premium Paid:</b><br/><span class="currency">₹</span></td>' +
            '        <td><b>Total Sum Insured:</b><br/><span class="currency">₹</span></td>' +
            "      </tr>" +
            "    </table>" +
            ticketHtml +
            "    <br />" +
            "    <div>" +
            '      <h6 class="h6Tag">Important Note: </h6>' +
            '      <p class="pTag">The last page of this document contains verification points and general advisory related to the farmer\'s enrollment and insurance coverage. Applicants must adhere to the terms and conditions of the scheme.</p>' +
            '      <h6 class="h6Tag">Disclaimer: </h6>' +
            '      <p class="pTag">This document is only for payment of the insurance premium by the farmer. As per the operational guidelines of the Pradhan Mantri Fasal Bima Yojana (PMFBY), the farmer\'s participation in the scheme will be determined after verification of the required documents.</p>' +
            '      <hr class="hrTag" />' +
            '      <h6 class="h6Tag"> Important Points to Consider Before Participating in the Scheme:</h6>' +
            '      <p class="pTag">(a) After receiving the registration receipt, the information entered by the Walker Agent / CSC-VLE / Bank / Intermediary should be cross-checked. Land details, bank account number, insured crop, insured area, and premium amount must be verified again at the time of enrollment.</p>' +
            '      <p class="pTag">(b) The authenticity of this registration receipt can be verified by scanning the QR code printed on page 1 of the receipt. The applicant farmer should verify the land and related details obtained through the QR scan.</p>' +
            '      <p class="pTag">(c) In case of any discrepancy in the registration details, the applicant farmer is advised to immediately report it to the Walker Agent / CSC Center / Bank / Intermediary for correction.</p>' +
            '      <hr class="hrTag" />' +
            '      <h6 class="h6Tag">General Instructions for Applicant Farmers: </h6>' +
            '      <p class="pTag">(a) The applicant farmer is not required to pay any additional service or processing fee for enrollment through any mode. Only the farmer\'s premium amount is payable.</p>' +
            '      <p class="pTag">(b) If any incorrect information is found in the portal data or the attached documents, the respective application may be rejected.</p>' +
            '      <p class="pTag">(c) As per the scheme guidelines, in case of natural calamities (hailstorm, landslide, flood, cloudburst, lightning) or post-harvest losses (storm, unseasonal rain, etc.), the farmer must inform the bank or concerned department within 72 hours through the Crop Insurance App.</p>' +
            '      <p class="pTag">(d) The claim amount under the scheme is determined based on the shortfall in average yield as assessed through CCEs (Crop Cutting Experiments) in the notified insurance area. Data declared by any other department or institution on drought or flood conditions will not be considered.</p>' +
            '      <p class="pTag">(e) Farmers can track the status of their applications through the Aadhaar-based Crop Insurance App, available on the Google Play Store and at <a href="https://pmfby.gov.in/" target="_blank">www.pmfby.gov.in</a>.</p>' +
            "    </div>" +
            "  </div>" +
            "</body>" +
            "</html>"

        return html
    }

    async parseHtml(str = "") {
        if (!str) return ""

        return str
            .replace(/<\/?[^>]+(>|$)/g, " ")
            .replace(/&nbsp;/g, " ")
            .replace(/&amp;/g, "&")
            .replace(/&lt;/g, "<")
            .replace(/&gt;/g, ">")
            .replace(/&quot;/g, '"')
            .replace(/&#39;/g, "'")
            .replace(/\s+/g, " ")
            .trim()
    }
}
