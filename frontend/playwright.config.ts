import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './e2e',
  use: {baseURL: 'http://127.0.0.1:5173', trace: 'retain-on-failure'},
  webServer: [
    {
      command: 'cd .. && uv run uvicorn graphrag.main:app --host 127.0.0.1 --port 8000',
      url: 'http://127.0.0.1:8000/api/v1/health/live',
      reuseExistingServer: true,
      env: {
        APP_ENV: 'test',
        USE_FAKE_EXTERNAL_CLIENTS: 'true',
        UPLOAD_DIR: '/tmp/graphrag-playwright-uploads',
        EMBEDDING_DIMENSION: '32',
        JWT_DEV_SECRET: 'playwright-jwt-secret-material-long-enough',
        FAKE_APPROVAL_SECRET: 'playwright-approval-secret-material-long',
      },
    },
    {command: 'pnpm dev', url: 'http://127.0.0.1:5173', reuseExistingServer: true},
  ],
})
