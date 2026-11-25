// Flow Rider — entrypoint. The strategy lives in strategy.ts; this file only starts it.
// See strategy.ts for the idea and the harness terms (hello, batch, orders, slot).
import { runBot } from './vendor/makefun-bot.ts';
import { createBot } from './strategy.ts';

await runBot(createBot());
