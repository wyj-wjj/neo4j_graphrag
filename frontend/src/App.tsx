import { Component, lazy, Suspense, type ErrorInfo, type ReactNode } from 'react'
import { Alert, Button, Result, Spin } from 'antd'
import { Navigate, Route, Routes } from 'react-router-dom'
import WorkbenchLayout from './components/WorkbenchLayout'

const ChatPage = lazy(() => import('./pages/ChatPage'))
const DebugPage = lazy(() => import('./pages/DebugPage'))
const KnowledgePage = lazy(() => import('./pages/KnowledgePage'))
const TasksPage = lazy(() => import('./pages/TasksPage'))

interface BoundaryState { failed: boolean }

class ErrorBoundary extends Component<{children: ReactNode}, BoundaryState> {
  state: BoundaryState = {failed: false}

  static getDerivedStateFromError(): BoundaryState { return {failed: true} }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('render boundary', error.name, info.componentStack)
  }

  render() {
    if (this.state.failed) {
      return <Result status="error" title="页面暂时无法显示" extra={<Button onClick={() => location.reload()}>重新加载</Button>} />
    }
    return this.props.children
  }
}

export default function App() {
  return (
    <ErrorBoundary>
      <Suspense fallback={<Spin fullscreen tip="加载工作台" />}>
        <Routes>
          <Route element={<WorkbenchLayout />}>
            <Route index element={<Navigate to="/chat" replace />} />
            <Route path="chat" element={<ChatPage />} />
            <Route path="knowledge" element={<KnowledgePage />} />
            <Route path="tasks" element={<TasksPage />} />
            <Route path="debug" element={<DebugPage />} />
          </Route>
          <Route path="*" element={<Alert type="warning" showIcon message="页面不存在" />} />
        </Routes>
      </Suspense>
    </ErrorBoundary>
  )
}
