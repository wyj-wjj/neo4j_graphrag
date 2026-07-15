import { expect, test } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

async function expectNoSeriousAccessibilityIssues(page: import('@playwright/test').Page) {
  const results = await new AxeBuilder({page}).analyze()
  expect(
    results.violations.filter((item) => ['serious', 'critical'].includes(item.impact ?? '')),
  ).toEqual([])
}

test('upload, ingest, cite and navigate the complete workbench', async ({page}) => {
  await page.goto('/knowledge')
  await expect(page.getByRole('heading', {name: '知识入库'})).toBeVisible()
  await page.getByLabel('文档标题').fill('星河保温杯保修政策')
  await page.locator('input[type="file"]').first().setInputFiles({
    name: 'policy.txt',
    mimeType: 'text/plain',
    buffer: Buffer.from('星河保温杯提供两年保修，非人为损坏可以免费换新。'),
  })
  await page.getByRole('button', {name: '创建入库任务'}).click()
  await expect(page.getByText('状态：completed')).toBeVisible({timeout: 15_000})
  await expectNoSeriousAccessibilityIssues(page)

  await page.goto('/chat')
  await page.getByLabel('你的问题').fill('星河保温杯保修政策是什么')
  await page.getByRole('button', {name: '发送'}).click()
  await expect(page.getByText('answered', {exact: true})).toBeVisible({timeout: 15_000})
  await expect(page.getByText('[C1]', {exact: false})).toBeVisible()
  await expectNoSeriousAccessibilityIssues(page)

  for (const [path, heading] of [['/tasks','入库任务'],['/debug','检索调试']]) {
    await page.goto(path)
    await expect(page.getByRole('heading', {name: heading})).toBeVisible()
    await expectNoSeriousAccessibilityIssues(page)
  }
})
