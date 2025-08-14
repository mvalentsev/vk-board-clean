// env: GROUP_TOKEN=токен_сообщества  USER_TOKEN=токен_админа
// Назначение: поиск соседних дублей комментариев в теме и их удаление.

import 'dotenv/config';
import { VK } from 'vk-io';

// ----------------------------- Constants & Utils -----------------------------

const DEFAULTS = {
    tail: 300,
    full: false,
    ignoreAtts: false,
    dryRun: false,
    concurrency: 2,         // безопасная умеренная параллельность
    maxRetries: 6,          // для 6 (Too many requests) и временных ошибок
    baseBackoffMs: 300,     // старт задержки
    maxBackoffMs: 3000,     // потолок backoff
    planPreview: 20,        // сколько ID показать в dry-run
};

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function clamp(n, min, max) {
    return Math.min(max, Math.max(min, n));
}

// Экспоненциальный backoff с джиттером
async function backoff(attempt, base = DEFAULTS.baseBackoffMs, max = DEFAULTS.maxBackoffMs) {
    const exp = Math.min(max, base * Math.pow(2, attempt));
    // full jitter: [0..exp]
    const delay = Math.floor(Math.random() * exp);
    await sleep(delay);
}

// ----------------------------- CLI & ENV -------------------------------------

function printUsageAndExit() {
    console.error('usage: node index.js <group_id> <topic_id> [--full] [--tail=300] [--ignore-atts] [--dry-run] [--concurrency=2] [--max-retries=6] [--backoff-ms=300]');
    process.exit(2);
}

function parseArgs() {
    const args = process.argv.slice(2);
    if (args.length < 2 || args.includes('--help') || args.includes('-h')) {
        printUsageAndExit();
    }
    const group_id = Number(args[0]);
    const topic_id = Number(args[1]);
    if (!Number.isFinite(group_id) || !Number.isFinite(topic_id)) {
        console.error('group_id и topic_id должны быть числами.');
        printUsageAndExit();
    }

    const opts = { ...DEFAULTS };
    for (const a of args.slice(2)) {
        if (a === '--full') opts.full = true;
        else if (a.startsWith('--tail=')) opts.tail = clamp(Number(a.split('=')[1] || DEFAULTS.tail), 1, 1000000);
        else if (a === '--ignore-atts') opts.ignoreAtts = true;
        else if (a === '--dry-run') opts.dryRun = true;
        else if (a.startsWith('--concurrency=')) opts.concurrency = clamp(Number(a.split('=')[1] || DEFAULTS.concurrency), 1, 8);
        else if (a.startsWith('--max-retries=')) opts.maxRetries = clamp(Number(a.split('=')[1] || DEFAULTS.maxRetries), 0, 20);
        else if (a.startsWith('--backoff-ms=')) opts.baseBackoffMs = clamp(Number(a.split('=')[1] || DEFAULTS.baseBackoffMs), 50, 10000);
        else {
            console.error('Неизвестный аргумент:', a);
            printUsageAndExit();
        }
    }
    return { group_id, topic_id, opts };
}

function requireEnv(name) {
    const v = process.env[name];
    if (!v) {
        console.error(`Отсутствует переменная окружения ${name}`);
        process.exit(2);
    }
    return v;
}

// ----------------------------- Normalizers -----------------------------------

/**
 * Нормализация текста для сравнения дублей
 * - нижний регистр
 * - убирает zero-width символы
 * - нормализует пробелы, nbsp
 * - схлопывает пустые строки и пробелы вокруг переносов
 */
function normalizeText(s) {
    return (s || '')
        .replace(/\u00A0/g, ' ')                  // nbsp -> space
        .toLowerCase()
        .replace(/[\u200B\u200C\u200D\uFEFF]/g, '') // zero-width
        .replace(/[ \t]+/g, ' ')
        .replace(/\s*\n\s*/g, '\n')
        .trim();
}

/**
 * Строит «сигнатуру» вложений для сравнения дублей
 * Стараемся быть устойчивыми к пустым/частично заполненным объектам
 */
function attsHash(atts = [], ignore = false) {
    if (ignore || !Array.isArray(atts) || atts.length === 0) return '';
    try {
        return atts
            .map((a) => {
                const t = a && a.type;
                if (!t) return 'unknown:';
                const obj = a[t] || {};
                // типовые поля: owner_id / id, иногда access_key может мешать сравнивать — не учитываем
                const o = obj.owner_id != null ? String(obj.owner_id) : '';
                const id = obj.id != null ? String(obj.id) : '';
                // на случай sticker/market/audio_message и т.п. полей без owner_id
                const stickerId = obj.sticker_id != null ? `st:${obj.sticker_id}` : '';
                const url = obj.url || obj.link || '';
                return `${t}:${o}:${id}:${stickerId}:${url}`;
            })
            .sort()
            .join('|');
    } catch {
        return '';
    }
}

// ----------------------------- VK clients & API helpers ----------------------

const vkGroup = new VK({ token: requireEnv('GROUP_TOKEN') }); // delete/restore
const vkUser  = new VK({ token: requireEnv('USER_TOKEN') });  // read

/**
 * Универсальный вызов VK API с ретраями
 * @param {() => Promise<any>} fn функция, вызывающая vk.api.XXX
 * @param {object} [cfg]
 * @param {number} [cfg.maxRetries]
 * @param {number} [cfg.baseBackoffMs]
 */
async function withRetries(fn, cfg = {}) {
    const maxRetries = cfg.maxRetries != null ? cfg.maxRetries : DEFAULTS.maxRetries;
    const base = cfg.baseBackoffMs != null ? cfg.baseBackoffMs : DEFAULTS.baseBackoffMs;

    let attempt = 0;
    while (true) {
        try {
            return await fn();
        } catch (e) {
            const code = e && e.code;
            const message = e && (e.message || e.error_msg || String(e));
            // 6: Too many requests per second — всегда ретраим
            // 10/9/5xx/temporaries — имеет смысл ретраить
            const retriable = code === 6 || code === 10 || (code >= 500 && code < 600);
            if (!retriable || attempt >= maxRetries) {
                // Логируем и пробрасываем
                throw new Error(`VK API failed: code=${code}, msg=${message}, attempt=${attempt}`);
            }
            attempt++;
            await backoff(attempt, base, DEFAULTS.maxBackoffMs);
        }
    }
}

/**
 * Получить общее число комментариев
 */
async function getTotal({ group_id, topic_id }) {
    const { count = 0 } = await withRetries(() => vkUser.api.board.getComments({
        group_id, topic_id, count: 1, offset: 0, sort: 'desc'
    }));
    return count;
}

/**
 * Итератор по комментариям снизу вверх (asc) батчами по 100
 */
async function* iterAsc({ group_id, topic_id, startOffset = 0 }) {
    const page = 100;
    let offset = startOffset;
    while (true) {
        const { items = [] } = await withRetries(() => vkUser.api.board.getComments({
            group_id, topic_id, count: page, offset, sort: 'asc'
        }));
        if (!items.length) break;
        for (const it of items) yield it;
        offset += items.length;
        if (items.length < page) break;
    }
}

/**
 * План удаления дублей: удаляем только соседние дубли (один за другим)
 */
async function planDeletionsAsc({ group_id, topic_id, startOffset = 0, ignoreAtts = false }) {
    const toDelete = [];
    let prevKept = null;

    for await (const cur of iterAsc({ group_id, topic_id, startOffset })) {
        if (!prevKept) { prevKept = cur; continue; }

        const sameAuthor = prevKept.from_id === cur.from_id;
        const sameText   = normalizeText(prevKept.text) === normalizeText(cur.text);
        const sameAtts   = attsHash(prevKept.attachments, ignoreAtts) === attsHash(cur.attachments, ignoreAtts);

        if (sameAuthor && sameText && sameAtts) {
            // соседний дубль → удаляем текущий, prevKept не двигаем
            toDelete.push(cur.id);
        } else {
            prevKept = cur;
        }
    }
    return toDelete;
}

// Ограниченная параллельность выполнения массива задач
async function pMap(items, mapper, { concurrency = 2 } = {}) {
    concurrency = Math.max(1, Number(concurrency) || 1);
    let i = 0, active = 0, resolved = 0;
    const results = new Array(items.length);

    return await new Promise((resolve, reject) => {
        const next = () => {
            if (resolved === items.length) return resolve(results);
            while (active < concurrency && i < items.length) {
                const curIdx = i++;
                active++;
                Promise.resolve(mapper(items[curIdx], curIdx))
                    .then((r) => { results[curIdx] = r; })
                    .catch(reject)
                    .finally(() => {
                        active--;
                        resolved++;
                        next();
                    });
            }
        };
        next();
    });
}

/**
 * Удаление пачки комментариев с ограниченной параллельностью и ретраями
 */
async function deleteBatch({ group_id, topic_id, ids, concurrency, maxRetries, baseBackoffMs }) {
    let ok = 0;
    const startedAt = Date.now();
    const total = ids.length;
    let processed = 0;

    await pMap(ids, async (comment_id) => {
        try {
            await withRetries(() => vkGroup.api.board.deleteComment({ group_id, topic_id, comment_id }), {
                maxRetries,
                baseBackoffMs
            });
            ok++;
        } catch (e) {
            const code = e && e.code;
            const msg = e && (e.message || e.error_msg || e.toString());
            // остальные ошибки логируем и идём дальше
            console.error('delete failed', comment_id, code, msg);
        } finally {
            processed++;
            if (processed % 20 === 0 || processed === total) {
                const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
                console.log(`Progress: ${processed}/${total}, deleted=${ok}, elapsed=${elapsed}s`);
            }
        }
    }, { concurrency });

    return ok;
}

// ----------------------------- Main ------------------------------------------

async function sanityCheck() {
    // Пробный лёгкий вызов, чтобы ранне поймать проблемы с токенами/доступом
    try {
        await withRetries(() => vkUser.api.utils.getServerTime());
        // у группового токена тоже короткую проверку сделаем: безвредный метод
        await withRetries(() => vkGroup.api.utils.getServerTime());
    } catch (e) {
        console.error('Проверка токенов не прошла:', e && (e.message || e));
        process.exit(2);
    }
}

let shuttingDown = false;
function setupSignals() {
    const handler = (sig) => {
        if (shuttingDown) return;
        shuttingDown = true;
        console.log(`Получен сигнал ${sig}. Завершаем…`);
        // даём шанс текущим операциям корректно завершиться
        setTimeout(() => process.exit(130), 1500);
    };
    process.on('SIGINT', handler);
    process.on('SIGTERM', handler);
}

async function main() {
    setupSignals();
    const t0 = Date.now();
    const { group_id, topic_id, opts } = parseArgs();
    await sanityCheck();

    let startOffset = 0;
    if (!opts.full) {
        const total = await getTotal({ group_id, topic_id });
        // берём хвост +1 элемент слева, чтобы учесть стык
        startOffset = Math.max(0, total - (opts.tail + 1));
        console.log(`Total=${total}, scanning tail from offset ${startOffset}`);
    } else {
        console.log('Scanning FULL thread (ascending)…');
    }

    const plan = await planDeletionsAsc({
        group_id, topic_id,
        startOffset,
        ignoreAtts: opts.ignoreAtts
    });

    if (opts.dryRun) {
        console.log(`DRY-RUN: would delete ${plan.length} comments`);
        if (plan.length) {
            const head = plan.slice(0, Math.min(opts.planPreview, plan.length));
            const tail = plan.slice(Math.max(plan.length - opts.planPreview, 0));
            console.log(`First ${head.length}:`, head);
            if (tail.length && tail !== head) console.log(`Last ${tail.length}:`, tail);
        }
        const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
        console.log(`Done (dry-run) in ${elapsed}s`);
        process.exit(0);
    }

    if (!plan.length) {
        console.log('Нечего удалять. Выходим.');
        return;
    }

    const deleted = await deleteBatch({
        group_id, topic_id, ids: plan,
        concurrency: opts.concurrency,
        maxRetries: opts.maxRetries,
        baseBackoffMs: opts.baseBackoffMs
    });

    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    console.log(`Deleted: ${deleted}/${plan.length}, took ${elapsed}s`);
}

main().catch(e => { console.error(e); process.exit(1); });