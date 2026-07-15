import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { Alert, Button, Card, Input, Table, Tag, Typography } from 'antd'
import { api } from '../api/client'

export default function DebugPage() {
  const [query, setQuery] = useState('')
  const debug = useMutation({mutationFn: api.debug})
  return <section aria-labelledby="debug-title"><Typography.Title id="debug-title" level={2}>检索调试</Typography.Title><Alert type="info" showIcon message="仅管理员可见；结果只显示授权后的候选摘要，不返回完整敏感原文。" /><Input.Search aria-label="调试问题" value={query} onChange={(event) => setQuery(event.target.value)} enterButton="分析" onSearch={(value) => value.trim() && debug.mutate(value.trim())} loading={debug.isPending} />{debug.isError && <Alert type="error" showIcon message={debug.error.message} />}{debug.data && <Card className="answer-card" title={`Rewrite：${debug.data.rewritten_query}`}><div>{Object.entries(debug.data.branches).map(([name,value]) => <Tag key={name}>{name}: {value}</Tag>)}</div><Table rowKey="chunk_id" pagination={false} dataSource={debug.data.candidates} columns={[{title:'引用',dataIndex:'citation_id'},{title:'文档',dataIndex:'document_title'},{title:'Chunk',dataIndex:'chunk_id'},{title:'来源',dataIndex:'sources',render:(value:string[])=>value.join(', ')},{title:'融合分',dataIndex:'score'}]} /></Card>}<Button className="sr-only" aria-hidden>占位</Button></section>
}
