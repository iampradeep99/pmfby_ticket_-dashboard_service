import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { chromium, Browser, BrowserContext } from 'playwright';

@Injectable()
export class BrowserPoolService implements OnModuleInit, OnModuleDestroy {
    private readonly logger = new Logger(BrowserPoolService.name);
    private browsers: Browser[] = [];
    private contexts: BrowserContext[] = [];
    private currentContextIndex = 0;
    private isInitialized = false;

    async onModuleInit() {
        await this.initialize();
    }

    async onModuleDestroy() {
        await this.cleanup();
    }

    private async initialize() {
        this.logger.log('Initializing browser pool...');

        // 5 browsers × 20 contexts = 100 concurrent capacity
        for (let i = 0; i < 5; i++) {
            const browser = await chromium.launch({
                headless: true,
                args: ['--no-sandbox', '--disable-setuid-sandbox']
            });
            this.browsers.push(browser);

            for (let j = 0; j < 20; j++) {
                const context = await browser.newContext();
                this.contexts.push(context);
            }
        }

        this.isInitialized = true;
        this.logger.log(`Browser pool ready: ${this.contexts.length} contexts`);
    }

    getContext(): BrowserContext {
        const context = this.contexts[this.currentContextIndex];
        this.currentContextIndex = (this.currentContextIndex + 1) % this.contexts.length;
        return context;
    }

    public async reinitialize() {
    this.logger.warn('Reinitializing browser pool...');

    await this.cleanup();
    this.currentContextIndex = 0;
    await this.initialize();

    this.logger.log('Browser pool successfully reinitialized.');
}


    private async cleanup() {
        for (const context of this.contexts) {
            await context.close().catch(() => {});
        }
        for (const browser of this.browsers) {
            await browser.close().catch(() => {});
        }
        this.contexts = [];
        this.browsers = [];
    }
}