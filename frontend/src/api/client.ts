import type { components } from './schema'

export type ChatResponse = components['schemas']['ChatResponse']
export type Citation = components['schemas']['Citation']
export type TaskResponse = components['schemas']['IngestionTaskResponse']
export type DebugResponse = components['schemas']['DebugResponse']
export type DocumentListResponse = components['schemas']['DocumentListResponse']
export type DocumentVersionListResponse = components['schemas']['DocumentVersionListResponse']

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? ''
let accessToken = ''

export class APIError extends Error {
  constructor(public readonly status: number, public readonly code: string, message: string) {
    super(message)
  }
}

export function setAccessToken(token: string) { accessToken = token }

export async function authenticateDev(roles: string[] = ['user', 'admin']) {
  const response = await fetch(`${API_BASE}/api/v1/auth/dev-token`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({user_id: 'demo-user', roles, expires_minutes: 60}),
  })
  if (!response.ok) throw new APIError(response.status, 'authentication_failed', '无法获取开发身份')
  const payload = await response.json() as components['schemas']['TokenResponse']
  accessToken = payload.access_token
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  if (!accessToken) await authenticateDev()
  const headers = new Headers(init?.headers)
  headers.set('Authorization', `Bearer ${accessToken}`)
  headers.set('X-Request-ID', crypto.randomUUID())
  if (init?.body && !(init.body instanceof FormData)) headers.set('Content-Type', 'application/json')
  const response = await fetch(`${API_BASE}${path}`, {...init, headers})
  if (!response.ok) {
    const error = await response.json().catch(() => ({})) as Partial<components['schemas']['ErrorBody']>
    throw new APIError(response.status, error.error_code ?? 'request_failed', error.message ?? '请求失败')
  }
  if (response.status === 204) return undefined as T
  return response.json() as Promise<T>
}

export const api = {
  createSession: () => request<components['schemas']['SessionCreateResponse']>('/api/v1/sessions', {method: 'POST'}),
  history: (sessionId: string) => request<components['schemas']['HistoryResponse']>(`/api/v1/sessions/${sessionId}/history`),
  upload: (form: FormData) => request<TaskResponse>('/api/v1/documents', {method: 'POST', body: form}),
  documents: () => request<DocumentListResponse>('/api/v1/documents'),
  versions: (documentId: string) => request<DocumentVersionListResponse>(`/api/v1/documents/${documentId}/versions`),
  uploadVersion: (documentId: string, form: FormData) => request<TaskResponse>(`/api/v1/documents/${documentId}/versions`, {method: 'POST', body: form}),
  inactivateDocument: (documentId: string) => request<void>(`/api/v1/documents/${documentId}`, {method: 'DELETE'}),
  task: (taskId: string) => request<TaskResponse>(`/api/v1/ingestion-tasks/${taskId}`),
  retryTask: (taskId: string) => request<TaskResponse>(`/api/v1/ingestion-tasks/${taskId}/retry`, {method: 'POST'}),
  debug: (query: string) => request<DebugResponse>('/api/v1/admin/retrieval-debug', {method: 'POST', body: JSON.stringify({query})}),
}

export function authorizationHeaders(): HeadersInit {
  return {'Authorization': `Bearer ${accessToken}`, 'X-Request-ID': crypto.randomUUID(), 'Content-Type': 'application/json'}
}

export { API_BASE }
