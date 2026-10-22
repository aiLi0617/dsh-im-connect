import { channelNotice } from '../engine/command-locale.js'
import { choiceSendError, ChoiceSendError } from '../engine/choice-delivery.js'
import { afterPartialDelivery, DeliveryRejected } from '../engine/deferred-delivery.js'
import { diagnosticJson, platformResult, probe, requireDiagnostic } from './diagnostics.js'
import { fileOperation } from './file-send.js'
import type { ChannelAdapter, ImMedia, ImMessage } from '../engine/types.js'
import { quietSdkLogger } from '../engine/quiet-logger.js'
import { KeyedSerialQueue } from '../engine/keyed-queue.js'
import { timeoutSignal } from '../engine/abort.js'
import { fileMedia, MAX_CHANNEL_IMAGES } from './channel-image-download.js'
import { markdownTableOffsets } from '../engine/split.js'

/** 飞书一张卡片最多 5 个 table 元素，超了整个写入会被拒（230099 / ErrCode 11310），旧卡片内容会卡住。 */
export const FEISHU_CARD_MAX_TABLES = 5

/** 按表格数量拆正文，保证每张卡不超上限，且不会把表格切一半。 */
export function splitByTableLimit(text: string, maxTables = FEISHU_CARD_MAX_TABLES): string[] {
  const value = text ?? ''
  const starts = markdownTableOffsets(value)
  if (starts.length <= maxTables) return value ? [value] : []
  const parts: string[] = []
  let from = 0
  for (let index = maxTables; index < starts.length; index += maxTables) {
    parts.push(value.slice(from, starts[index]))
    from = starts[index]!
  }
  parts.push(value.slice(from))
  return parts.filter((part) => part.trim() !== '')
}

/** 飞书限流：消息接口 230020 / 11232，通用频控 99991400（HTTP 429，旧接口为 400）。 */
const FEISHU_THROTTLE_CODES = new Set([230020, 11232, 99991400])

function isThrottleCode(code: unknown): boolean {
  return FEISHU_THROTTLE_CODES.has(Number(code))
}

/** SDK 在 HTTP 非 2xx 时抛 AxiosError：429，或响应体带限流码，说明请求被限流、未被受理。 */
function throttledResponse(error: unknown): boolean {
  const response = (error as { response?: { status?: number; data?: { code?: unknown } } } | undefined)?.response
  return response?.status === 429 || isThrottleCode(response?.data?.code)
}

/** 诊断只保留受限长度的数字状态码，禁止输出响应正文、请求配置或凭据。 */
function httpFailureSummary(error: unknown): string {
  const response = (error as { response?: { status?: unknown; data?: { code?: unknown } } } | undefined)?.response
  const status = response?.status
  const code = response?.data?.code
  const safeStatus = typeof status === 'number' && Number.isInteger(status) && status >= 100 && status <= 599 ? status : 'unknown'
  const safeCode = typeof code === 'number' && Number.isSafeInteger(code) && code >= 0 && code <= 999_999_999
    ? code : typeof code === 'string' && /^\d{1,9}$/.test(code) ? code : 'unknown'
  return `http=${safeStatus} code=${safeCode}`
}

// 沿用 SDK 允许缺省业务码的响应，同时兼容数字和字符串零码。
function isRejectedCode(code: unknown): boolean {
  return code !== undefined && code !== 0 && code !== '0'
}

interface FeishuContentMessage {
  message_id?: string
  message_type?: string
  content?: string
}
interface ResourceClient {
  im: { messageResource: { get(opts: { path: { message_id: string; file_key: string }; params: { type: string } }): Promise<{ getReadableStream(): import('node:stream').Readable; headers?: Record<string, string> }> } }
}
const MAX_IMAGE_BYTES = 20 * 1024 * 1024
// Shared across all images in one message, not renewed for each resource.
const MAX_MESSAGE_IMAGE_BYTES = MAX_IMAGE_BYTES

/** SDK messageResource.get is the receive-side API, not im.image.get. */
export async function resolveFeishuContent(client: ResourceClient, message: FeishuContentMessage, signal: AbortSignal = AbortSignal.timeout(30_000)): Promise<{ text: string; media: ImMedia[] }> {
  const content = JSON.parse(message.content ?? '{}')
  const keys: string[] = []
  const texts: string[] = []
  if (message.message_type === 'text') texts.push(content.text ?? '')
  else if (message.message_type === 'image') {
    if (!content.image_key) throw new Error('图片资源缺失')
    keys.push(content.image_key)
  } else if (message.message_type === 'file') {
    if (!content.file_key) throw new Error('文件资源缺失')
    keys.push(content.file_key)
  } else if (message.message_type === 'post') {
    const post = content.content ? content : content.zh_cn ?? content.en_us ?? Object.values(content)[0]
    if (!post || !Array.isArray(post.content)) throw new Error('富文本内容无效')
    if (post.title) texts.push(post.title)
    for (const row of post.content) {
      for (const item of row) {
        if (item.tag === 'img') {
          if (!item.image_key) throw new Error('图片资源缺失')
          keys.push(item.image_key)
        } else if (typeof item.text === 'string') texts.push(item.text)
      }
    }
  } else throw new Error('暂不支持该消息类型，请发送文字或图片。')
  if (keys.length > MAX_CHANNEL_IMAGES) throw new Error('图片数量超过上限')
  const media: ImMedia[] = []
  let totalBytes = 0
  for (const key of keys) {
    if (totalBytes >= MAX_MESSAGE_IMAGE_BYTES) throw new Error('图片累计大小超过上限')
    if (!message.message_id) throw new Error('图片消息 ID 缺失')
    signal.throwIfAborted()
    let stream: import('node:stream').Readable | undefined
    let abort: () => void = () => {}
    const aborted = new Promise<never>((_, reject) => {
      abort = () => { stream?.destroy(new Error('图片下载已取消或超时')); reject(new Error('图片下载已取消或超时')) }
      signal.addEventListener('abort', abort, { once: true })
    })
    try {
      const resource = await Promise.race([
        client.im.messageResource.get({ path: { message_id: message.message_id, file_key: key }, params: { type: message.message_type === 'file' ? 'file' : 'image' } }).then(resource => {
          stream = resource.getReadableStream()
          if (signal.aborted) { stream.destroy(); throw new Error('图片下载已取消或超时') }
          return resource
        }), aborted,
      ])
      const consume = async () => {
        const chunks: Buffer[] = []
        let size = 0
        if (Number(resource.headers?.['content-length']) > MAX_IMAGE_BYTES) throw new Error('图片超过 20 MB 上限')
        if (Number(resource.headers?.['content-length']) > MAX_MESSAGE_IMAGE_BYTES - totalBytes) throw new Error('图片累计大小超过上限')
        for await (const chunk of stream!) {
          const bytes = Buffer.from(chunk)
          size += bytes.length
          totalBytes += bytes.length
          if (totalBytes > MAX_MESSAGE_IMAGE_BYTES) throw new Error('图片累计大小超过上限')
          if (size > MAX_IMAGE_BYTES) throw new Error('图片超过 20 MB 上限')
          chunks.push(bytes)
        }
        if (!size) throw new Error('图片为空')
        if (message.message_type === 'file') return fileMedia(Buffer.concat(chunks), content.file_name)
        return { kind: 'image' as const, data: Buffer.concat(chunks), mediaType: resource.headers?.['content-type']?.split(';')[0] }
      }
      media.push(await Promise.race([consume(), aborted]))
    } finally {
      signal.removeEventListener('abort', abort)
      stream?.destroy()
    }
  }
  return { text: texts.join('\n'), media }
}

interface FeishuConfig {
  appId?: string
  appSecret?: string
  domain?: 'feishu' | 'lark'
  ownerOpenId?: string
  groupTriggerMode?: FeishuGroupTriggerMode
  host?: { get(name: string): unknown }
}

interface FeishuMention {
  key?: string
  id?: { open_id?: string }
}

export type FeishuGroupTriggerMode = 'bot-mention' | 'owner-or-bot'

export const FEISHU_GROUP_CONTEXT_MAX_MESSAGES = 30
export const FEISHU_GROUP_CONTEXT_TTL_MS = 30 * 60_000
const FEISHU_GROUP_CONTEXT_MESSAGE_MAX_CHARS = 2_000

export function normalizeFeishuGroupTriggerMode(value: unknown): FeishuGroupTriggerMode {
  return value === 'owner-or-bot' ? 'owner-or-bot' : 'bot-mention'
}

export function isFeishuUserMentioned(mentions: FeishuMention[] | undefined, openId: string): boolean {
  if (!openId || !Array.isArray(mentions)) return false
  return mentions.some((mention) => mention.id?.open_id === openId)
}

/** 群消息只有明确 mention 当前机器人本身才算 addressed；@ 其他成员不触发。 */
export function isFeishuBotMentioned(mentions: FeishuMention[] | undefined, botOpenId: string): boolean {
  return isFeishuUserMentioned(mentions, botOpenId)
}

interface FeishuGroupContextEntry {
  at: number
  senderId: string
  text: string
}

/**
 * 普通群消息只进入这个有界内存缓冲，不进入 Gateway，也不会创建会话或触发模型。
 * 进程停止即清空，避免把完整群聊历史长期落盘。
 */
export class FeishuGroupContextBuffer {
  private readonly chats = new Map<string, FeishuGroupContextEntry[]>()

  constructor(
    private readonly maxMessages = FEISHU_GROUP_CONTEXT_MAX_MESSAGES,
    private readonly ttlMs = FEISHU_GROUP_CONTEXT_TTL_MS,
  ) {}

  add(chatId: string, senderId: string | undefined, text: string, now = Date.now()): void {
    const normalized = text.trim().slice(0, FEISHU_GROUP_CONTEXT_MESSAGE_MAX_CHARS)
    if (!chatId || !normalized) return
    const entries = this.recent(chatId, now)
    entries.push({ at: now, senderId: senderId?.trim() || 'unknown', text: normalized })
    if (entries.length > this.maxMessages) entries.splice(0, entries.length - this.maxMessages)
    this.chats.set(chatId, entries)
  }

  recent(chatId: string, now = Date.now()): FeishuGroupContextEntry[] {
    const cutoff = now - this.ttlMs
    const entries = (this.chats.get(chatId) ?? []).filter((entry) => entry.at >= cutoff)
    if (entries.length) this.chats.set(chatId, entries)
    else this.chats.delete(chatId)
    return entries.map((entry) => ({ ...entry }))
  }

  clear(): void { this.chats.clear() }
}

/** 不下载资源地提取普通群消息摘要，供滚动上下文使用。 */
export function feishuContextText(message: FeishuContentMessage): string {
  let content: Record<string, unknown>
  try {
    const parsed = JSON.parse(message.content ?? '{}') as unknown
    content = parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {}
  } catch { return '' }
  if (message.message_type === 'text') return typeof content.text === 'string' ? content.text : ''
  if (message.message_type === 'image') return '[图片]'
  if (message.message_type === 'file') return `[文件${typeof content.file_name === 'string' && content.file_name ? `: ${content.file_name}` : ''}]`
  if (message.message_type !== 'post') return message.message_type ? `[${message.message_type}消息]` : ''
  const localized = content.content ? content : Object.values(content).find((value) => value && typeof value === 'object' && !Array.isArray(value))
  if (!localized || typeof localized !== 'object') return ''
  const post = localized as { title?: unknown; content?: unknown }
  const texts: string[] = typeof post.title === 'string' && post.title ? [post.title] : []
  if (Array.isArray(post.content)) {
    for (const row of post.content) {
      if (!Array.isArray(row)) continue
      for (const item of row) {
        if (!item || typeof item !== 'object') continue
        const node = item as { tag?: unknown; text?: unknown }
        if (typeof node.text === 'string') texts.push(node.text)
        else if (node.tag === 'img') texts.push('[图片]')
      }
    }
  }
  return texts.join('\n')
}

export function prependFeishuGroupContext(current: string, entries: FeishuGroupContextEntry[]): string {
  if (!entries.length) return current
  const history = entries.map((entry) => `- [${entry.senderId}] ${entry.text}`).join('\n')
  return [
    '<group_context>',
    '以下是触发消息之前的群聊上下文，仅用于理解对话背景：',
    history,
    '</group_context>',
    '<current_message>',
    current,
    '</current_message>',
  ].join('\n')
}

export function createFeishuChannel(id: 'feishu' | 'lark', config: FeishuConfig, log: (line: string) => void, loadSdk?: () => Promise<typeof import('@larksuiteoapi/node-sdk')>): ChannelAdapter | undefined {
  const appId = config.appId?.trim()
  const appSecret = config.appSecret?.trim()
  if (!appId || !appSecret) return undefined

  let handler: ((msg: ImMessage) => void | Promise<void>) | undefined
  let client: ResourceClient & {
    request(opts: { url: string; method: 'GET' }): Promise<unknown>
    im: { messageReaction: { create(opts: unknown): Promise<{ code?: number; data?: { reaction_id?: string } }>; delete(opts: unknown): Promise<{ code?: number }> }; message: { create(opts: unknown): Promise<unknown>; patch(opts: unknown): Promise<unknown> }; file: { create(opts: unknown): Promise<{ file_key?: string } | null> } }
  } | undefined
  let ws: { close(opts?: { force?: boolean }): void } | undefined
  let statusText = '未连接'
  let lifecycle: AbortController | undefined
  const label = id === 'lark' ? 'Lark' : '飞书'

  return {
    id,
    label,
    maxMessageLength: 4000,
    async start() {
      lifecycle?.abort()
      const generation = new AbortController()
      lifecycle = generation
      const current = () => lifecycle === generation && !generation.signal.aborted
      const incoming = new KeyedSerialQueue()
      let sdk: typeof import('@larksuiteoapi/node-sdk')
      try {
        sdk = await (loadSdk ? loadSdk() : import('@larksuiteoapi/node-sdk'))
      } catch {
        throw new Error('缺少依赖 @larksuiteoapi/node-sdk')
      }
      const domain = id === 'lark' || config.domain === 'lark' ? 'https://open.larksuite.com' : undefined
      const logger = quietSdkLogger(() => log(`[${id}] SDK 请求或连接异常（敏感详情已隐藏）`), id)
      const loggerLevel = sdk.LoggerLevel?.error ?? 1
      const httpInstance = Object.create(sdk.defaultHttpInstance)
      httpInstance.request = (opts: Record<string, unknown>) => sdk.defaultHttpInstance.request({ ...opts, timeout: 30_000, maxRedirects: 0 })
      client = new sdk.Client({ appId, appSecret, logger, loggerLevel, httpInstance, ...(domain ? { domain } : {}) }) as unknown as typeof client
      const identity = await client!.request({ url: '/open-apis/bot/v3/info', method: 'GET' }) as {
        bot?: { open_id?: string }
      }
      const botOpenId = identity.bot?.open_id?.trim() ?? ''
      if (!botOpenId) throw new Error(`${id}: 无法获取机器人 open_id`)
      const ownerOpenId = config.ownerOpenId?.trim() ?? ''
      const groupTriggerMode = normalizeFeishuGroupTriggerMode(config.groupTriggerMode)
      const groupContexts = new FeishuGroupContextBuffer()
      if (groupTriggerMode === 'owner-or-bot' && !ownerOpenId) {
        log(`[${id}] 群聊扩展触发已启用，但账号缺少 ownerOpenId；当前只能由 @机器人 触发`)
      }
      const dispatcher = new sdk.EventDispatcher({}).register({
        'card.action.trigger': async (data: { operator?: { open_id?: string }; action?: { value?: { token?: string; group?: boolean } }; context?: { open_chat_id?: string }; event_id?: string }) => {
          const token = data.action?.value?.token
          const chatId = data.context?.open_chat_id
          if (current() && token && chatId && data.operator?.open_id) void Promise.resolve(handler?.({ chatId, userId: data.operator.open_id,
            text: '', actionToken: token, kind: data.action?.value?.group ? 'group' : 'dm', addressed: true,
          })).catch(() => log(`[${id}] 按钮操作失败`))
          return {}
        },
        'im.message.receive_v1': async (data: {
          sender?: { sender_id?: { open_id?: string } }
          message?: { message_id?: string; chat_id?: string; chat_type?: string; message_type?: string; content?: string; mentions?: FeishuMention[] }
        }) => {
          const message = data.message
          if (!message) return
          const group = message.chat_type === 'group'
          const senderId = data.sender?.sender_id?.open_id
          if (group && senderId === botOpenId) return
          const botMentioned = isFeishuBotMentioned(message.mentions, botOpenId)
          const ownerMentioned = groupTriggerMode === 'owner-or-bot' && isFeishuUserMentioned(message.mentions, ownerOpenId)
          const addressed = !group || botMentioned || ownerMentioned
          if (!message.chat_id || !current()) return
          const chatId = message.chat_id
          return incoming.run(chatId, async () => {
            if (!current()) return
            if (group && groupTriggerMode === 'owner-or-bot' && !addressed) {
              groupContexts.add(chatId, senderId, feishuContextText(message))
              return
            }
            if (!addressed) return
            const priorContext = group && groupTriggerMode === 'owner-or-bot' ? groupContexts.recent(chatId) : []
            let resolved: { text: string; media: ImMedia[] }
            try {
              resolved = await resolveFeishuContent(client!, message, timeoutSignal(30_000, generation.signal))
            } catch {
              if (!current()) return
              log(`[${id}] 图片或消息读取失败`)
              await client?.im.message.create({
                params: { receive_id_type: 'chat_id' },
                data: { receive_id: message.chat_id, msg_type: 'text', content: JSON.stringify({ text: channelNotice(config.host, '图片或消息读取失败，请重发图片或发送文字。') }) },
              }).then(result => {
                const code = (result as { code?: number | string } | undefined)?.code
                if (isRejectedCode(code)) throw new Error('media-notice-rejected')
              }).catch(() => log(`[${id}] 媒体提示发送失败`))
              return
            }
            if (!current()) return
            const currentText = feishuContextText(message) || resolved.text
            if (group && groupTriggerMode === 'owner-or-bot') groupContexts.add(chatId, senderId, currentText)
            await handler?.({
              chatId,
              userId: senderId,
              text: group && groupTriggerMode === 'owner-or-bot'
                ? prependFeishuGroupContext(resolved.text, priorContext)
                : resolved.text,
              ...(resolved.media.length ? { media: resolved.media } : {}),
              messageId: message.message_id,
              kind: group ? 'group' : 'dm',
              addressed,
            })
          }).catch(() => { if (current()) log(`[${id}] 消息处理失败`) })
        },
      })
      const wsClient = new sdk.WSClient({ appId, appSecret, logger, loggerLevel, ...(domain ? { domain } : {}) })
      ws = wsClient
      await wsClient.start({ eventDispatcher: dispatcher })
      statusText = '长连接已建立'
      log(`[${id}] WebSocket 长连接已启动`)
    },
    async stop() {
      lifecycle?.abort()
      lifecycle = undefined
      ws?.close({ force: true })
      ws = undefined
      statusText = '已停止'
    },
    canDeliverDeferred() { return statusText === '长连接已建立' },
    async send(chatId, text) {
      if (!client) throw new Error(`${id}: 尚未连接`)
      const sender = client
      // 普通回复也走卡片 markdown，标题、列表和表格才会被渲染；按表格数量拆卡。
      let delivered = false
      for (const part of splitByTableLimit(text)) {
        try {
          // 仅明确业务拒绝或 HTTP 400 回退；网络结果未知时不重发，避免重复投递。
          // 飞书要渲染 Markdown 表格，卡片必须是 Schema 2.0（body.elements）
          const card = await sender.im.message.create({
            params: { receive_id_type: 'chat_id' },
            data: {
              receive_id: chatId,
              msg_type: 'interactive',
              content: JSON.stringify({ schema: '2.0', config: { wide_screen_mode: true }, body: { elements: [{ tag: 'markdown', content: part }] } }),
            },
          }).catch((error: unknown) => {
            const response = (error as { response?: { status?: number } } | undefined)?.response
            // 旧接口也可能用 HTTP 400 返回限流，必须优先保留上层退避语义。
            if (response?.status !== 400 || throttledResponse(error)) throw error
            log(`[${id}] 卡片发送被拒，回退纯文本 ${httpFailureSummary(error)}`)
            return { code: 'http-400' }
          }) as { code?: number | string } | undefined
          if (!isRejectedCode(card?.code)) { delivered = true; continue }
          // 被限流时改发纯文本只会再被限流，直接交给上层退避重试。
          if (isThrottleCode(card?.code)) throw new DeliveryRejected(`${id}: text-send-throttled code=${card?.code}`)
          const rejected = card?.code
          // 卡片被拒（内容过大、接口不支持等）时退回纯文本，不丢正文。
          const plain = await sender.im.message.create({
            params: { receive_id_type: 'chat_id' },
            data: { receive_id: chatId, msg_type: 'text', content: JSON.stringify({ text: part }) },
          }).catch((error: unknown) => {
            log(`[${id}] 纯文本回退发送失败 ${httpFailureSummary(error)}`)
            throw error
          }) as { code?: number | string } | undefined
          if (isThrottleCode(plain?.code)) throw new DeliveryRejected(`${id}: text-send-throttled code=${plain?.code}`)
          if (isRejectedCode(plain?.code)) throw new Error(`${id}: text-send-rejected code=${plain?.code} card=${String(rejected)}`)
          delivered = true
        } catch (error) {
          const normalized = error instanceof DeliveryRejected || !throttledResponse(error)
            ? error : new DeliveryRejected(`${id}: text-send-throttled http=${(error as { response?: { status?: number } }).response?.status}`)
          throw delivered ? afterPartialDelivery(normalized) : normalized
        }
      }
    },
    async sendChoices(message, text, buttons) {
      if (!client) throw new Error('channel-unavailable')
      const result = await client.im.message.create({ params: { receive_id_type: 'chat_id' }, data: {
        receive_id: message.chatId, msg_type: 'interactive', content: JSON.stringify({ config: { wide_screen_mode: true }, elements: [
          { tag: 'markdown', content: text },
          ...buttons.map(button => ({ tag: 'action', actions: [{ tag: 'button', text: { tag: 'plain_text', content: button.label.slice(0, 60) },
            type: 'default', value: { token: button.token, group: message.kind === 'group' } }] })),
        ] }),
      } }).catch(error => { throw choiceSendError(error) }) as { code?: number; data?: { message_id?: string } } | undefined
      if (isRejectedCode(result?.code)) throw new ChoiceSendError('rejected')
      const messageId = result?.data?.message_id
      const sender = client
      if (!messageId) return
      return { close: async (status: string) => {
        const updated = await sender.im.message.patch({ path: { message_id: messageId }, data: { content: JSON.stringify({ config: { wide_screen_mode: true }, elements: [
          { tag: 'markdown', content: text }, { tag: 'div', text: { tag: 'plain_text', content: status } },
        ] }) } }) as { code?: number } | undefined
        if (isRejectedCode(updated?.code)) throw new Error('card-update-failed')
      } }
    },
    async addStatusReaction(message, state, _label, signal) {
      if (!client || !message.messageId || state === 'cancelled') return
      const emoji = (state === 'success' || state === 'ended') ? 'DONE' : state === 'error' ? 'ERROR' : 'OnIt'
      signal.throwIfAborted()
      const result = await fileOperation(client.im.messageReaction.create({
        path: { message_id: message.messageId }, data: { reaction_type: { emoji_type: emoji } },
      }), signal)
      if (isRejectedCode(result.code) || !result.data?.reaction_id) throw new Error('reaction-rejected')
      return result.data.reaction_id
    },
    async removeStatusReaction(message, reaction, signal) {
      if (!client) return
      signal.throwIfAborted()
      const result = await fileOperation(client.im.messageReaction.delete({ path: { message_id: message.messageId, reaction_id: reaction } }), signal)
      if (isRejectedCode(result.code)) throw new Error('reaction-rejected')
    },
    async sendFile(chatId, file, signal) {
      if (!client || !lifecycle) throw new Error(`${id}: 尚未连接`)
      const active = client
      const scope = AbortSignal.any([timeoutSignal(120_000, lifecycle.signal), ...(signal ? [signal] : [])])
      scope.throwIfAborted()
      const uploaded = await fileOperation(active.im.file.create({ data: { file_type: 'stream', file_name: file.name, file: Buffer.from(file.data) } }), scope)
      scope.throwIfAborted()
      if (!uploaded?.file_key) throw new Error('file-upload-rejected')
      const sent = await active.im.message.create({
        params: { receive_id_type: 'chat_id' },
        data: { receive_id: chatId, msg_type: 'file', content: JSON.stringify({ file_key: uploaded.file_key }) },
      }) as { code?: number }
      if (isRejectedCode(sent.code)) throw new Error('file-send-rejected')
    },
    setMessageHandler(h) { handler = h },
    async diagnose(signal) {
      const domain = id === 'lark' || config.domain === 'lark' ? 'https://open.larksuite.com' : 'https://open.feishu.cn'
      let token: string
      const auth = await probe('credentials', signal, async () => {
        const data = await diagnosticJson(`${domain}/open-apis/auth/v3/tenant_access_token/internal`, signal, { app_id: appId, app_secret: appSecret })
        platformResult(data.code, [10003, 10014])
        requireDiagnostic(typeof data.tenant_access_token === 'string' && data.tenant_access_token)
        token = data.tenant_access_token
      })
      if (auth.status !== 'passed') return [auth]
      return [auth, await probe('bot', signal, async () => {
        const data = await diagnosticJson(`${domain}/open-apis/bot/v3/info`, signal, undefined, { Authorization: `Bearer ${token}` })
        platformResult(data.code)
        const bot = data.bot as { open_id?: unknown } | undefined
        requireDiagnostic(typeof bot?.open_id === 'string' && bot.open_id)
      })]
    },
    status() { return statusText },
  }
}


