# 猎聘招聘爬虫

自动抓取猎聘网指定公司的招聘岗位，写入 PostgreSQL，并通过 REST API 对外提供查询和智能分析接口。

## 功能

- 列表页翻页抓取 + 详情页 JD 全文提取
- 增量模式（跳过已存在记录）/ 全量模式可切换
- 代理池轮换 + 反爬虫规避
- SSH 隧道模式（无需暴露数据库端口）
- REST API：岗位查询、关键词搜索、趋势统计、触发抓取
- JD 智能分析：调用 DeepSeek API 提取职能分类、级别、技能列表、摘要

## 安装

```bash
pip install -r requirements.txt
playwright install chromium
```

## 配置

编辑 `liepin_recruitment_spider.py` 顶部的用户配置区，或通过环境变量覆盖：

| 环境变量 | 说明 | 示例 |
|----------|------|------|
| `DATABASE_URL` | 数据库连接串，设置后自动跳过 SSH 隧道 | `postgresql+psycopg2://user:pass@host:5432/db` |
| `SSH_HOST` | SSH 跳板机地址 | `192.168.1.100` |
| `SSH_USER` | SSH 用户名 | `ubuntu` |
| `SSH_PASSWORD` | SSH 密码 | `secret` |
| `SSH_PORT` | SSH 端口，默认 `22` | `22` |
| `HEADLESS` | 无头模式，`1` 开启 | `1` |
| `CHROMIUM_PATH` | 指定 Chromium 路径 | `/usr/bin/chromium` |
| `INCREMENTAL` | `0` 关闭增量去重，默认开启 | `0` |
| `FETCH_DETAIL` | `0` 跳过详情页抓取，默认开启 | `0` |
| `DEEPSEEK_API_KEY` | DeepSeek API Key，用于 JD 智能分析 | `sk-xxx` |

目标公司在 `COMPANIES` 列表中配置：

```python
COMPANIES = [
    {'vendor': '公司A', 'keyword': '公司A搜索词'},
    {'vendor': '公司B', 'keyword': '公司B搜索词'},
]
```

## 使用

### 直接运行爬虫

```bash
# 直连数据库，无头模式
DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/db HEADLESS=1 python liepin_recruitment_spider.py

# SSH 隧道模式（不设 DATABASE_URL 时自动启用）
python liepin_recruitment_spider.py

# 跳过详情页，只抓列表（速度更快）
FETCH_DETAIL=0 python liepin_recruitment_spider.py
```

### 启动 API 服务

```bash
uvicorn api:app --port 8000

# 生产环境
uvicorn api:app --host 0.0.0.0 --port 8000 --workers 2
```

Swagger 文档：`http://localhost:8000/docs`

### JD 智能分析

```bash
# 分析全部尚未处理的岗位
DEEPSEEK_API_KEY=sk-xxx python jd_analyzer.py

# 指定公司，最多处理 50 条
python jd_analyzer.py --api-key sk-xxx --vendor 公司A --limit 50
```

## API 端点

### 招聘数据

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/jobs` | 查询岗位列表 |
| `GET` | `/jobs/{id}` | 岗位详情 |
| `GET` | `/vendors` | 公司列表及岗位数 |
| `GET` | `/stats/trend` | 近 8 周各公司新增趋势 |
| `POST` | `/scrape` | 触发后台抓取任务 |
| `GET` | `/scrape/{task_id}` | 查询抓取任务状态 |

### JD 智能分析

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/jobs/{id}/analysis` | 获取某岗位的 AI 分析结果 |
| `POST` | `/analyze` | 触发批量 JD 分析任务 |
| `GET` | `/analyze/{task_id}` | 查询分析任务状态 |
| `GET` | `/stats/skills` | 全量技能词频排行 |

### 查询示例

```bash
# 关键词搜索
GET /jobs?q=模拟工程师&page=1&page_size=20

# 按公司 + 城市筛选
GET /jobs?vendor=公司A&city=上海

# 按发布日期区间
GET /jobs?date_from=2026-01-01&date_to=2026-06-01

# 触发抓取（仅指定公司，关闭增量）
POST /scrape
{
  "incremental": false,
  "fetch_detail": true,
  "vendors": ["公司A", "公司B"]
}

# 通过 API 触发 JD 分析（api_key 不填则读环境变量）
POST /analyze
{
  "api_key": "sk-xxx",
  "vendor": "公司A",
  "limit": 100
}

# 技能词频，只看公司A，取前 10
GET /stats/skills?vendor=公司A&top_n=10
```

## 数据库表结构

表名：`competitor_recruitment`

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID | 主键 |
| `vendor` | String | 公司标识（配置中的 vendor） |
| `brand_name` | String | 猎聘显示的公司全名 |
| `job_title` | String | 岗位名称 |
| `salary` | String | 薪资范围 |
| `city` | String | 工作城市 |
| `experience` | String | 经验要求 |
| `education` | String | 学历要求 |
| `job_url` | String | 岗位链接（唯一） |
| `description` | Text | JD 全文 |
| `posted_at` | Date | 岗位发布日期 |
| `crawled_at` | DateTime | 抓取时间 |

表名：`jd_analysis`

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID | 主键 |
| `job_id` | UUID | 关联 `competitor_recruitment.id` |
| `job_function` | String | 岗位职能分类 |
| `seniority` | String | 级别（junior/mid/senior/lead） |
| `skills` | JSONB | 技能列表 |
| `summary` | Text | 50 字内核心职责摘要 |
| `analyzed_at` | DateTime | 分析时间 |

## 依赖

- [Playwright](https://playwright.dev/python/) — 浏览器自动化
- [SQLAlchemy](https://www.sqlalchemy.org/) — ORM
- [FastAPI](https://fastapi.tiangolo.com/) — API 框架
- [Paramiko](https://www.paramiko.org/) — SSH 隧道
- [DeepSeek API](https://platform.deepseek.com/) — JD 智能分析
