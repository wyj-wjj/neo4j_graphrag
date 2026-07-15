import { API_BASE, APIError, authenticateDev, authorizationHeaders, type ChatResponse } from './client'

export interface StreamEvent {
  event: 'start' | 'delta' | 'citation' | 'status' | 'error' | 'end'
  data: Record<string, unknown>
}

function parseBlock(block: string): StreamEvent | null {
  let event = ''
  let data = ''
  for (const line of block.split('\n')) {
    if (line.startsWith('event:')) event = line.slice(6).trim()
    if (line.startsWith('data:')) data += line.slice(5).trim()
  }
  if (!event || !data) return null
  return {event: event as StreamEvent['event'], data: JSON.parse(data) as Record<string, unknown>}
}

export async function streamChat(
  query: string,
  sessionId: string | undefined,
  signal: AbortSignal,
  onEvent: (event: StreamEvent) => void,
): Promise<Partial<ChatResponse>> {
  await authenticateDev()
  const response = await fetch(`${API_BASE}/api/v1/chat/stream`, {
    method: 'POST',
    headers: authorizationHeaders(),
    body: JSON.stringify({query, session_id: sessionId}),
    signal,
  })
  if (!response.ok || !response.body) throw new APIError(response.status, 'stream_failed', '流式连接失败')
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  try {
    while (true) {
      const {done, value} = await reader.read()
      buffer += decoder.decode(value, {stream: !done}).replace(/\r\n/g, '\n')
      const blocks = buffer.split('\n\n')
      buffer = blocks.pop() ?? ''
      for (const block of blocks) {
        const parsed = parseBlock(block)
        if (parsed) onEvent(parsed)
      }
      if (done) break
    }
  } finally {
    reader.releaseLock()
  }
  return {}
}
