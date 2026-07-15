import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import App from './App'

describe('workbench routes', () => {
  it('renders the chat route and accessible input', async () => {
    render(<MemoryRouter initialEntries={['/chat']}><App /></MemoryRouter>)
    expect(await screen.findByRole('heading', {name: '智能对话'})).toBeInTheDocument()
    expect(screen.getByLabelText('你的问题')).toBeInTheDocument()
  })

  it('renders a not-found result', () => {
    render(<MemoryRouter initialEntries={['/missing']}><App /></MemoryRouter>)
    expect(screen.getByText('页面不存在')).toBeInTheDocument()
  })
})
