import { fireEvent, render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import App from './App'

describe('workbench routes', () => {
  it('renders the chat route and accessible input', async () => {
    render(<MemoryRouter initialEntries={['/chat']}><App /></MemoryRouter>)
    expect(await screen.findByRole('heading', {name: '智能对话'})).toBeInTheDocument()
    const question = screen.getByLabelText('你的问题')
    const send = screen.getByRole('button', {name: '发送'})
    expect(question).toBeInTheDocument()
    expect(send).toBeDisabled()
    fireEvent.change(question, {target: {value: '测试问题'}})
    expect(send).toBeEnabled()
  })

  it('renders a not-found result', () => {
    render(<MemoryRouter initialEntries={['/missing']}><App /></MemoryRouter>)
    expect(screen.getByText('页面不存在')).toBeInTheDocument()
  })
})
