import type { ChannelAdapter, ImMedia } from '../engine/types.js';
/** 飞书一张卡片最多 5 个 table 元素，超了整个写入会被拒（230099 / ErrCode 11310），旧卡片内容会卡住。 */
export declare const FEISHU_CARD_MAX_TABLES = 5;
/** 按表格数量拆正文，保证每张卡不超上限，且不会把表格切一半。 */
export declare function splitByTableLimit(text: string, maxTables?: number): string[];
interface FeishuContentMessage {
    message_id?: string;
    message_type?: string;
    content?: string;
}
interface ResourceClient {
    im: {
        messageResource: {
            get(opts: {
                path: {
                    message_id: string;
                    file_key: string;
                };
                params: {
                    type: string;
                };
            }): Promise<{
                getReadableStream(): import('node:stream').Readable;
                headers?: Record<string, string>;
            }>;
        };
    };
}
/** SDK messageResource.get is the receive-side API, not im.image.get. */
export declare function resolveFeishuContent(client: ResourceClient, message: FeishuContentMessage, signal?: AbortSignal): Promise<{
    text: string;
    media: ImMedia[];
}>;
interface FeishuConfig {
    appId?: string;
    appSecret?: string;
    domain?: 'feishu' | 'lark';
    ownerOpenId?: string;
    groupTriggerMode?: FeishuGroupTriggerMode;
    host?: {
        get(name: string): unknown;
    };
}
interface FeishuMention {
    key?: string;
    id?: {
        open_id?: string;
    };
}
export type FeishuGroupTriggerMode = 'bot-mention' | 'owner-or-bot';
export declare const FEISHU_GROUP_CONTEXT_MAX_MESSAGES = 30;
export declare const FEISHU_GROUP_CONTEXT_TTL_MS: number;
export declare function normalizeFeishuGroupTriggerMode(value: unknown): FeishuGroupTriggerMode;
export declare function isFeishuUserMentioned(mentions: FeishuMention[] | undefined, openId: string): boolean;
/** 群消息只有明确 mention 当前机器人本身才算 addressed；@ 其他成员不触发。 */
export declare function isFeishuBotMentioned(mentions: FeishuMention[] | undefined, botOpenId: string): boolean;
interface FeishuGroupContextEntry {
    at: number;
    senderId: string;
    text: string;
}
/**
 * 普通群消息只进入这个有界内存缓冲，不进入 Gateway，也不会创建会话或触发模型。
 * 进程停止即清空，避免把完整群聊历史长期落盘。
 */
export declare class FeishuGroupContextBuffer {
    private readonly maxMessages;
    private readonly ttlMs;
    private readonly chats;
    constructor(maxMessages?: number, ttlMs?: number);
    add(chatId: string, senderId: string | undefined, text: string, now?: number): void;
    recent(chatId: string, now?: number): FeishuGroupContextEntry[];
    clear(): void;
}
/** 不下载资源地提取普通群消息摘要，供滚动上下文使用。 */
export declare function feishuContextText(message: FeishuContentMessage): string;
export declare function prependFeishuGroupContext(current: string, entries: FeishuGroupContextEntry[]): string;
export declare function createFeishuChannel(id: 'feishu' | 'lark', config: FeishuConfig, log: (line: string) => void, loadSdk?: () => Promise<typeof import('@larksuiteoapi/node-sdk')>): ChannelAdapter | undefined;
export {};
//# sourceMappingURL=feishu.d.ts.map