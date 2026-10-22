import assert from 'node:assert/strict'
import test from 'node:test'
import {
  FeishuGroupContextBuffer,
  createFeishuChannel,
  feishuContextText,
  isFeishuBotMentioned,
  isFeishuUserMentioned,
  normalizeFeishuGroupTriggerMode,
} from '../lib/channels/feishu.js'

for (const code of [undefined, 0, '0', 230001, '230001']) test(`飞书发送入口统一业务码：${JSON.stringify(code)}`, async t => {
  let currentCode = code
  const result = () => ({ code: currentCode, data: { message_id: 'm', reaction_id: 'r' } })
  const sdk = { defaultHttpInstance: {}, Client: class {
    request = async () => ({ bot: { open_id: 'bot' } })
    im = { message: { create: async () => result(), patch: async () => result() },
      messageReaction: { create: async () => result(), delete: async () => result() },
      file: { create: async () => ({ file_key: 'f' }) } }
  }, EventDispatcher: class { register() { return this } }, WSClient: class { async start() {} close() {} } }
  const channel = createFeishuChannel('feishu', { appId: 'test', appSecret: 'test' }, () => {}, async () => sdk)
  t.after(() => channel.stop())
  await channel.start()
  const message = { chatId: 'c', messageId: 'm' }, signal = new AbortController().signal
  currentCode = 0
  const card = await channel.sendChoices(message, '选择', [{ label: '一', token: 't:0' }])
  currentCode = code
  for (const action of [
    () => channel.send('c', '正文'),
    () => channel.sendChoices(message, '选择', [{ label: '一', token: 't:0' }]),
    () => card.close('结束'),
    () => channel.addStatusReaction(message, 'success', '', signal),
    () => channel.removeStatusReaction(message, 'r', signal),
    () => channel.sendFile('c', { name: 'a.txt', data: new Uint8Array([65]) }, signal),
  ]) {
    if (code === 230001 || code === '230001') await assert.rejects(action)
    else await action()
  }
})

test('飞书群聊只把 @ 当前机器人视为 addressed', () => {
  const mentions = [
    { key: '@_user_1', id: { open_id: 'ou-colleague' } },
    { key: '@_user_2', id: { open_id: 'ou-bot' } },
  ]
  assert.equal(isFeishuBotMentioned(mentions, 'ou-bot'), true)
  assert.equal(isFeishuBotMentioned(mentions, 'ou-other-bot'), false)
  assert.equal(isFeishuBotMentioned(undefined, 'ou-bot'), false)
  assert.equal(isFeishuUserMentioned(mentions, 'ou-colleague'), true)
  assert.equal(normalizeFeishuGroupTriggerMode('owner-or-bot'), 'owner-or-bot')
  assert.equal(normalizeFeishuGroupTriggerMode('unknown'), 'bot-mention')
})

test('飞书群聊上下文有界、按群隔离并淘汰过期消息', () => {
  const context = new FeishuGroupContextBuffer(2, 1_000)
  context.add('chat-a', 'u1', 'one', 1_000)
  context.add('chat-a', 'u2', 'two', 1_100)
  context.add('chat-a', 'u3', 'three', 1_200)
  context.add('chat-b', 'u4', 'other', 1_200)
  assert.deepEqual(context.recent('chat-a', 1_300).map(item => item.text), ['two', 'three'])
  assert.deepEqual(context.recent('chat-b', 1_300).map(item => item.text), ['other'])
  assert.deepEqual(context.recent('chat-a', 2_201), [])
})

test('飞书普通消息摘要不下载资源并覆盖文字、富文本和附件', () => {
  assert.equal(feishuContextText({ message_type: 'text', content: '{"text":"hello"}' }), 'hello')
  assert.equal(feishuContextText({ message_type: 'image', content: '{"image_key":"key"}' }), '[图片]')
  assert.equal(feishuContextText({ message_type: 'file', content: '{"file_name":"report.pdf"}' }), '[文件: report.pdf]')
  assert.equal(feishuContextText({ message_type: 'post', content: JSON.stringify({ zh_cn: { title: '标题', content: [[{ tag: 'text', text: '正文' }, { tag: 'img' }]] } }) }), '标题\n正文\n[图片]')
})

test('飞书扩展群聊模式缓存普通消息，由 @本人或 @机器人触发且只投递一次', async t => {
  let callback
  const received = []
  const sdk = {
    defaultHttpInstance: {},
    Client: class {
      request = async () => ({ bot: { open_id: 'ou-bot' } })
      im = { message: { create: async () => ({ code: 0 }) }, messageResource: { get: async () => { throw new Error('不应下载普通消息资源') } } }
    },
    EventDispatcher: class { register(events) { callback = events['im.message.receive_v1']; return this } },
    WSClient: class { async start() {} close() {} },
  }
  const channel = createFeishuChannel('feishu', {
    appId: 'test', appSecret: 'test', ownerOpenId: 'ou-owner', groupTriggerMode: 'owner-or-bot',
  }, () => {}, async () => sdk)
  channel.setMessageHandler(message => received.push(message))
  t.after(() => channel.stop())
  await channel.start()

  const text = (message_id, sender, value, mentions) => callback({
    sender: { sender_id: { open_id: sender } },
    message: { message_id, chat_id: 'chat', chat_type: 'group', message_type: 'text', content: JSON.stringify({ text: value }), mentions },
  })
  await text('m1', 'ou-a', '背景一')
  await text('m2', 'ou-b', '背景二')
  assert.equal(received.length, 0)

  await text('m3', 'ou-c', '@你 请处理', [{ id: { open_id: 'ou-owner' } }])
  assert.equal(received.length, 1)
  assert.equal(received[0].addressed, true)
  assert.match(received[0].text, /<group_context>[\s\S]*\[ou-a\] 背景一[\s\S]*\[ou-b\] 背景二[\s\S]*<current_message>[\s\S]*@你 请处理/)

  await text('m4', 'ou-d', '@两者', [{ id: { open_id: 'ou-owner' } }, { id: { open_id: 'ou-bot' } }])
  assert.equal(received.length, 2, '同时 @本人和机器人只能触发一次')
  await text('m5', 'ou-bot', '机器人自己的消息', [{ id: { open_id: 'ou-owner' } }])
  assert.equal(received.length, 2, '机器人自己的消息不能触发循环')
})

test('飞书默认模式保持旧行为：普通消息和仅 @本人都不进入 Gateway', async t => {
  let callback
  const received = []
  const sdk = {
    defaultHttpInstance: {},
    Client: class {
      request = async () => ({ bot: { open_id: 'ou-bot' } })
      im = { message: { create: async () => ({ code: 0 }) }, messageResource: { get: async () => { throw new Error('unused') } } }
    },
    EventDispatcher: class { register(events) { callback = events['im.message.receive_v1']; return this } },
    WSClient: class { async start() {} close() {} },
  }
  const channel = createFeishuChannel('feishu', { appId: 'test', appSecret: 'test', ownerOpenId: 'ou-owner' }, () => {}, async () => sdk)
  channel.setMessageHandler(message => received.push(message))
  t.after(() => channel.stop())
  await channel.start()
  const base = { chat_id: 'chat', chat_type: 'group', message_type: 'text', content: '{"text":"hello"}' }
  await callback({ sender: { sender_id: { open_id: 'ou-user' } }, message: { ...base, message_id: 'm1' } })
  await callback({ sender: { sender_id: { open_id: 'ou-user' } }, message: { ...base, message_id: 'm2', mentions: [{ id: { open_id: 'ou-owner' } }] } })
  assert.equal(received.length, 0)
  await callback({ sender: { sender_id: { open_id: 'ou-user' } }, message: { ...base, message_id: 'm3', mentions: [{ id: { open_id: 'ou-bot' } }] } })
  assert.equal(received.length, 1)
  assert.equal(received[0].text, 'hello')
})
