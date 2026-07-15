import { useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { Alert, Button, Card, Descriptions, Input, Space, Typography } from 'antd'
import { api } from '../api/client'

export default function TasksPage() {
  const [draft, setDraft] = useState('')
  const [taskId, setTaskId] = useState('')
  const query = useQuery({queryKey: ['task', taskId], queryFn: () => api.task(taskId), enabled: Boolean(taskId), refetchInterval: (item) => item.state.data && !['completed','failed','partial_failed'].includes(item.state.data.status) ? 1000 : false})
  const retry = useMutation({mutationFn: () => api.retryTask(taskId), onSuccess: () => query.refetch()})
  return <section aria-labelledby="tasks-title"><Typography.Title id="tasks-title" level={2}>入库任务</Typography.Title><Space.Compact block><Input aria-label="任务 ID" value={draft} onChange={(event) => setDraft(event.target.value)} /><Button type="primary" onClick={() => setTaskId(draft.trim())}>查询</Button></Space.Compact>{query.isError && <Alert type="error" showIcon message={query.error.message} />}{query.data && <Card className="answer-card"><Descriptions column={1} items={[{key:'status',label:'状态',children:query.data.status},{key:'attempt',label:'尝试次数',children:query.data.attempt},{key:'error',label:'错误',children:query.data.error_message ?? '无'}]} /><Button onClick={() => retry.mutate()} disabled={!['failed','partial_failed'].includes(query.data.status)}>重试</Button></Card>}</section>
}
