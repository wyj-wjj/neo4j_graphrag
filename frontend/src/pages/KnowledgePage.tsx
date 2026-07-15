import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, Card, Input, Popconfirm, Progress, Select, Space, Table, Tag, Typography, Upload } from 'antd'
import { InboxOutlined } from '@ant-design/icons'
import { api, type TaskResponse } from '../api/client'

const allowed = ['pdf','docx','xlsx','pptx','html','htm','txt','md','png','jpg','jpeg']

export default function KnowledgePage() {
  const queryClient = useQueryClient()
  const [file, setFile] = useState<File | null>(null)
  const [versionFile, setVersionFile] = useState<File | null>(null)
  const [title, setTitle] = useState('')
  const [task, setTask] = useState<TaskResponse | null>(null)
  const [documentId, setDocumentId] = useState('')
  const documents = useQuery({queryKey: ['documents'], queryFn: api.documents})
  const versions = useQuery({queryKey: ['versions', documentId], queryFn: () => api.versions(documentId), enabled: Boolean(documentId)})
  const taskQuery = useQuery({
    queryKey: ['task', task?.task_id],
    queryFn: () => api.task(task!.task_id),
    enabled: Boolean(task?.task_id),
    refetchInterval: (item) => item.state.data && !['completed','failed','partial_failed'].includes(item.state.data.status) ? 500 : false,
  })
  const refreshDocuments = () => queryClient.invalidateQueries({queryKey: ['documents']})
  const mutation = useMutation({mutationFn: api.upload, onSuccess: (value) => {setTask(value); void refreshDocuments()}})
  const versionMutation = useMutation({
    mutationFn: ({id, form}: {id: string, form: FormData}) => api.uploadVersion(id, form),
    onSuccess: (value) => {setTask(value); void queryClient.invalidateQueries({queryKey: ['versions', documentId]})},
  })
  const inactivate = useMutation({mutationFn: api.inactivateDocument, onSuccess: () => {setDocumentId(''); void refreshDocuments()}})

  const submit = () => {
    if (!file || !title.trim()) return
    const form = new FormData(); form.set('title', title.trim()); form.set('file', file)
    mutation.mutate(form)
  }

  const submitVersion = () => {
    if (!versionFile || !documentId) return
    const form = new FormData(); form.set('file', versionFile)
    versionMutation.mutate({id: documentId, form})
  }

  const uploadProps = (setter: (value: File) => void) => ({
    beforeUpload: (candidate: File) => {const suffix = candidate.name.split('.').pop()?.toLowerCase() ?? ''; if (!allowed.includes(suffix) || candidate.size > 25 * 1024 * 1024) return Upload.LIST_IGNORE; setter(candidate); return false as const},
    maxCount: 1,
    accept: allowed.map((item) => `.${item}`).join(','),
  })
  const currentTask = taskQuery.data ?? task

  return (
    <section aria-labelledby="knowledge-title">
      <Typography.Title id="knowledge-title" level={2}>知识入库</Typography.Title>
      <Card>
        <label htmlFor="document-title" className="field-label">文档标题</label>
        <Input id="document-title" value={title} onChange={(event) => setTitle(event.target.value)} maxLength={500} />
        <Upload.Dragger {...uploadProps(setFile)}>
          <p className="ant-upload-drag-icon"><InboxOutlined /></p><p>选择或拖入安全文档（最大 25 MB）</p>
        </Upload.Dragger>
        <Button type="primary" onClick={submit} loading={mutation.isPending} disabled={!file || !title.trim()} className="actions">创建入库任务</Button>
        {mutation.isError && <Alert type="error" message={mutation.error.message} showIcon />}
        {currentTask && <Card size="small" title={`任务 ${currentTask.task_id}`}><Progress aria-label="文档入库任务进度" percent={currentTask.status === 'completed' ? 100 : 20} status={currentTask.status.includes('failed') ? 'exception' : 'active'} /><Typography.Text>状态：{currentTask.status}</Typography.Text></Card>}
      </Card>
      <Card title="文档与版本" className="answer-card">
        {documents.isError && <Alert type="error" message={documents.error.message} showIcon />}
        <Select
          aria-label="选择文档"
          placeholder="选择文档查看版本"
          value={documentId || undefined}
          onChange={setDocumentId}
          loading={documents.isLoading}
          options={documents.data?.items.map((item) => ({value: item.document_id, label: `${item.title} (${item.status})`}))}
          style={{minWidth: 280}}
        />
        {documentId && <>
          <Space wrap className="actions">
            <Upload {...uploadProps(setVersionFile)}><Button>选择新版本文件</Button></Upload>
            <Button type="primary" onClick={submitVersion} loading={versionMutation.isPending} disabled={!versionFile}>上传新版本</Button>
            <Popconfirm title="确认下线该文档？" onConfirm={() => inactivate.mutate(documentId)}><Button danger loading={inactivate.isPending}>下线文档</Button></Popconfirm>
          </Space>
          {versionMutation.isError && <Alert type="error" message={versionMutation.error.message} showIcon />}
          <Table
            rowKey="version_id"
            loading={versions.isLoading}
            pagination={false}
            dataSource={versions.data?.items ?? []}
            columns={[
              {title: '版本', dataIndex: 'version', key: 'version'},
              {title: '类型', dataIndex: 'mime_type', key: 'mime_type'},
              {title: '大小', dataIndex: 'size_bytes', key: 'size_bytes', render: (value: number) => `${value} B`},
              {title: '状态', key: 'status', render: (_, row) => row.valid_until ? <Tag>已失效</Tag> : <Tag color="green">当前</Tag>},
              {title: '创建时间', dataIndex: 'created_at', key: 'created_at', render: (value: string) => new Date(value).toLocaleString()},
            ]}
          />
        </>}
      </Card>
    </section>
  )
}
