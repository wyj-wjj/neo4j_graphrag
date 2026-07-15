import { useRef, useState } from 'react'
import { Alert, Button, Card, Input, Space, Tag, Typography } from 'antd'
import type { Citation } from '../api/client'
import { streamChat, type StreamEvent } from '../api/sse'

export default function ChatPage() {
  const [query, setQuery] = useState('')
  const [answer, setAnswer] = useState('')
  const [status, setStatus] = useState('idle')
  const [intent, setIntent] = useState('')
  const [source, setSource] = useState('')
  const [citations, setCitations] = useState<Citation[]>([])
  const [error, setError] = useState('')
  const [isStreaming, setIsStreaming] = useState(false)
  const controller = useRef<AbortController | null>(null)

  const handleEvent = (message: StreamEvent) => {
    if (message.event === 'delta') setAnswer((value) => value + String(message.data.content ?? ''))
    if (message.event === 'citation') setCitations((value) => [...value, message.data.citation as Citation])
    if (message.event === 'status') {
      setStatus(String(message.data.status ?? ''))
      setIntent(String(message.data.intent ?? ''))
      setSource(String(message.data.source ?? ''))
    }
    if (message.event === 'error') setError(String(message.data.message ?? '回答失败'))
  }

  const send = async () => {
    if (!query.trim() || isStreaming) return
    setAnswer(''); setCitations([]); setError(''); setStatus('streaming')
    setIsStreaming(true)
    controller.current = new AbortController()
    try { await streamChat(query.trim(), undefined, controller.current.signal, handleEvent) }
    catch (reason) { if ((reason as Error).name !== 'AbortError') setError((reason as Error).message) }
    finally { controller.current = null; setIsStreaming(false) }
  }

  const cancel = () => { controller.current?.abort(); controller.current = null; setIsStreaming(false); setStatus('cancelled') }

  return (
    <section aria-labelledby="chat-title">
      <Typography.Title id="chat-title" level={2}>智能对话</Typography.Title>
      <Card className="answer-card" aria-live="polite">
        {answer || <Typography.Text type="secondary">回答将在这里增量显示。知识回答必须带引用；业务结果会明确标记 Fake。</Typography.Text>}
        <Space wrap className="status-row">
          {status !== 'idle' && <Tag>{status}</Tag>}{intent && <Tag color="blue">{intent}</Tag>}{source && <Tag color={source === 'fake' ? 'orange' : 'green'}>{source}</Tag>}
        </Space>
        {error && <Alert type="error" showIcon message={error} />}
      </Card>
      {citations.map((item) => <Card size="small" key={item.citation_id} className="citation"><b>[{item.citation_id}]</b> {item.document_title} · v{item.document_version} · {item.source_location}</Card>)}
      <label htmlFor="question" className="field-label">你的问题</label>
      <Input.TextArea id="question" value={query} onChange={(event) => setQuery(event.target.value)} maxLength={4000} autoSize={{minRows: 3, maxRows: 8}} onPressEnter={(event) => {if (!event.shiftKey) {event.preventDefault(); void send()}}} />
      <Space className="actions"><Button type="primary" onClick={() => void send()} disabled={!query.trim() || isStreaming}>发送</Button><Button onClick={cancel} disabled={!isStreaming}>取消</Button></Space>
    </section>
  )
}
