import { BugOutlined, DatabaseOutlined, MessageOutlined, UnorderedListOutlined } from '@ant-design/icons'
import { Layout, Menu, Typography } from 'antd'
import { Link, Outlet, useLocation } from 'react-router-dom'

const items = [
  {key: '/chat', icon: <MessageOutlined />, label: <Link to="/chat">智能对话</Link>},
  {key: '/knowledge', icon: <DatabaseOutlined />, label: <Link to="/knowledge">知识入库</Link>},
  {key: '/tasks', icon: <UnorderedListOutlined />, label: <Link to="/tasks">任务状态</Link>},
  {key: '/debug', icon: <BugOutlined />, label: <Link to="/debug">检索调试</Link>},
]

export default function WorkbenchLayout() {
  const location = useLocation()
  return (
    <Layout className="app-shell">
      <Layout.Sider breakpoint="lg" collapsedWidth="0" aria-label="主导航">
        <Typography.Title level={4} className="brand">GraphRAG 工作台</Typography.Title>
        <Menu theme="dark" mode="inline" selectedKeys={[location.pathname]} items={items} />
      </Layout.Sider>
      <Layout>
        <Layout.Header className="topbar"><Typography.Text strong>企业智能客服 · 阶段一</Typography.Text></Layout.Header>
        <Layout.Content className="content"><Outlet /></Layout.Content>
      </Layout>
    </Layout>
  )
}
