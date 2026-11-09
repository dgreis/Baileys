import NodeCache from '@cacheable/node-cache'
import { jest } from '@jest/globals'
import { EventEmitter } from 'events'
import P from 'pino'
import type { CacheStore } from '../../Types'
import type { BinaryNode } from '../../WABinary'

// An always-open socket that never touches the network
class FakeWebSocketClient extends EventEmitter {
	readonly isOpen = true
	readonly isClosed = false
	readonly isClosing = false
	readonly isConnecting = false
	connect() {}
	async close() {}
	send(_data: string | Uint8Array, cb?: (err?: Error) => void) {
		cb?.()
		return true
	}
}

jest.unstable_mockModule('../../Socket/Client/websocket', () => ({ WebSocketClient: FakeWebSocketClient }))

// imported after the mock is registered, so the socket picks up the fake client
const { default: makeWASocket } = await import('../../Socket')
const { DEFAULT_CONNECTION_CONFIG } = await import('../../Defaults')
const { makeSession } = await import('../TestUtils/session')

// a LID sender avoids the PN->LID USync lookup during decryption, which would wait on a server reply
const PEER_JID = '98765432101234@lid'

type MessageHandler = (node: BinaryNode) => Promise<void>

const makeMessageNode = (id: string, child: BinaryNode): BinaryNode => ({
	tag: 'message',
	attrs: {
		id,
		from: PEER_JID,
		type: 'text',
		t: Math.floor(Date.now() / 1000).toString()
	},
	content: [child]
})

// enc payload with no Signal session for the sender, so decryption fails
const undecryptableNode = (id: string) =>
	makeMessageNode(id, { tag: 'enc', attrs: { type: 'msg', v: '2' }, content: Buffer.from('not-a-signal-message') })

// message that arrives without any encrypted content (e.g. CTWA ads)
const unavailableNode = (id: string) => makeMessageNode(id, { tag: 'unavailable', attrs: {} })

const waitFor = async (check: () => boolean, timeoutMs = 4_000) => {
	const start = Date.now()
	while (!check()) {
		if (Date.now() - start > timeoutMs) {
			throw new Error('condition not met in time')
		}

		await new Promise(resolve => setTimeout(resolve, 20))
	}
}

const setup = async (enablePlaceholderResend?: boolean) => {
	const { state, clear } = await makeSession()
	state.creds.me = { id: '1234567890:1@s.whatsapp.net', name: 'Test User' }

	const placeholderResendCache = new NodeCache<number>({ stdTTL: 3600, useClones: false }) as CacheStore
	const resendSet = jest.spyOn(placeholderResendCache, 'set')
	const msgRetryCounterCache = new NodeCache<number>({ stdTTL: 3600, useClones: false }) as CacheStore

	const sock = makeWASocket({
		...DEFAULT_CONNECTION_CONFIG,
		logger: P({ level: 'silent' }),
		auth: state,
		placeholderResendCache,
		msgRetryCounterCache,
		// immediate (unscheduled) phone request path, so the resend is observable without timers
		enableRecentMessageCache: false,
		retryRequestDelayMs: 0,
		...(enablePlaceholderResend === undefined ? {} : { enablePlaceholderResend })
	})

	const handler = sock.ws.listeners('CB:message')[0] as MessageHandler | undefined
	if (!handler) {
		throw new Error('CB:message handler not registered')
	}

	const teardown = async () => {
		// clearing pending requests makes the delayed resend resolve early instead of sending the PDO
		for (const [id] of resendSet.mock.calls) {
			await placeholderResendCache.del(id)
		}

		await sock.end(new Error('Test completed'))
		await clear()
	}

	return { handler, resendSet, msgRetryCounterCache, teardown }
}

describe('enablePlaceholderResend', () => {
	it('defaults to true', () => {
		expect(DEFAULT_CONNECTION_CONFIG.enablePlaceholderResend).toBe(true)
	})

	describe('decrypt failure', () => {
		it('requests a placeholder resend from the phone by default', async () => {
			const { handler, resendSet, teardown } = await setup()
			void handler(undecryptableNode('DECRYPT_FAIL_DEFAULT'))

			await waitFor(() => resendSet.mock.calls.some(([id]) => id === 'DECRYPT_FAIL_DEFAULT'))

			await teardown()
		})

		it('still sends the retry receipt but no placeholder resend when disabled', async () => {
			const { handler, resendSet, msgRetryCounterCache, teardown } = await setup(false)
			await handler(undecryptableNode('DECRYPT_FAIL_DISABLED'))

			expect(await msgRetryCounterCache.get<number>('DECRYPT_FAIL_DISABLED:undefined')).toBe(1)
			expect(resendSet).not.toHaveBeenCalled()

			await teardown()
		})
	})

	describe('message without content', () => {
		it('requests a placeholder resend from the phone by default', async () => {
			const { handler, resendSet, teardown } = await setup()
			void handler(unavailableNode('UNAVAILABLE_DEFAULT'))

			await waitFor(() => resendSet.mock.calls.some(([id]) => id === 'UNAVAILABLE_DEFAULT'))

			await teardown()
		})

		it('does not request a placeholder resend when disabled', async () => {
			const { handler, resendSet, teardown } = await setup(false)
			await handler(unavailableNode('UNAVAILABLE_DISABLED'))

			expect(resendSet).not.toHaveBeenCalled()

			await teardown()
		})
	})
})
