# -*- coding: utf-8 -*-
"""
求职匹配工具 - 根据简历自动找猎聘匹配岗位
=====================================================================

功能：
    1. 用 DeepSeek API 解析简历，提取技能、目标岗位、推荐搜索词
    2. 按关键词+目标城市爬取猎聘职位（不依赖数据库，结果直接返回）
    3. 用 DeepSeek API 对每个职位打匹配分，返回排序结果

用法：
    python job_matcher.py --resume resume.txt --city 北京
    python job_matcher.py --resume resume.txt --city 上海 --keywords "Python" "数据分析"
    python job_matcher.py --resume resume.txt --city 深圳 --intern --top 30 --json

环境变量：
    DEEPSEEK_API_KEY   DeepSeek API Key（必须）
    HEADLESS=1         无头模式运行浏览器
"""

import argparse
import json
import os
import random
import re
import time
from typing import Optional

import requests as _requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

from liepin_recruitment_spider import (
    ProxyPool,
    _UA_POOL,
    _ANTI_DETECT_JS,
)


# ══════════════════════════════════════════════════════════════════════════════
# ── 城市代码映射（猎聘城市参数） ────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

CITY_CODES: dict[str, str] = {
    '北京': '010',
    '上海': '021',
    '广州': '020',
    '深圳': '0755',
    '杭州': '0571',
    '成都': '028',
    '武汉': '027',
    '南京': '025',
    '西安': '029',
    '重庆': '023',
    '苏州': '0512',
    '厦门': '0592',
    '天津': '022',
    '长沙': '0731',
    '郑州': '0371',
    '青岛': '0532',
    '合肥': '0551',
    '济南': '0531',
    '宁波': '0574',
    '东莞': '0769',
    '佛山': '0757',
    '珠海': '0756',
    '无锡': '0510',
    '大连': '0411',
    '沈阳': '024',
    '哈尔滨': '0451',
    '长春': '0431',
    '南昌': '0791',
    '昆明': '0871',
    '贵阳': '0851',
    '南宁': '0771',
    '福州': '0591',
    '太原': '0351',
    '石家庄': '0311',
    '海口': '0898',
    '乌鲁木齐': '0991',
}


# ══════════════════════════════════════════════════════════════════════════════
# ── 职位搜索爬虫（不依赖数据库，结果 in-memory） ───────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class JobSearchSpider:
    """
    按关键词 + 城市搜索猎聘职位，结果不写库，直接返回列表。
    复用 LiepinSpider 的浏览器 / 代理 / 反爬基础设施。
    """

    def __init__(self, playwright, proxy_pool=None, headless=None):
        self.playwright  = playwright
        self.proxy_pool  = proxy_pool
        if headless is None:
            headless = os.environ.get('HEADLESS', '').lower() in ('1', 'true', 'yes')

        self.browser = playwright.chromium.launch(
            executable_path=os.environ.get('CHROMIUM_PATH') or None,
            headless=headless,
            slow_mo=50,
            args=['--no-sandbox', '--disable-dev-shm-usage'],
        )
        self.context = None
        self.page    = None
        self._current_proxy = None
        self._make_context()

    def _make_context(self, proxy_url=None):
        if self.context:
            try:
                self.context.close()
            except Exception:
                pass
        ctx_opts = {
            'viewport':   {'width': 1920, 'height': 1080},
            'user_agent': random.choice(_UA_POOL),
            'locale':     'zh-CN',
        }
        if proxy_url:
            ctx_opts['proxy'] = {'server': proxy_url}
            print(f"[Proxy] 使用代理: {proxy_url}")
        else:
            print("[Proxy] 直连模式")
        self.context = self.browser.new_context(**ctx_opts)
        self.context.add_init_script(_ANTI_DETECT_JS)
        self.page = self.context.new_page()
        self._current_proxy = proxy_url

    def _human_scroll(self, times: int = 3, page=None):
        p = page or self.page
        for _ in range(times):
            p.mouse.wheel(0, random.randint(200, 400))
            time.sleep(random.uniform(0.3, 0.7))

    def open_homepage(self):
        """访问首页以建立 cookie，减少反爬触发。"""
        self.page.goto('https://www.liepin.com/', wait_until='domcontentloaded')
        time.sleep(random.uniform(2, 3))
        print("[Spider] 首页就绪，开始搜索")

    def search(self, keyword: str, city: str = None, max_pages: int = 3) -> list[dict]:
        """
        搜索职位，返回 dict 列表。
        每条包含: job_title, company, salary, city, experience, education, job_url
        """
        city_code = CITY_CODES.get(city or '', '')
        base_url  = f'https://www.liepin.com/zhaopin/?key={keyword}'
        if city_code:
            base_url += f'&city={city_code}'

        print(f"\n[Spider] 关键词={keyword}  城市={city or '全国'}")

        try:
            self.page.goto(base_url + '&curPage=0', wait_until='domcontentloaded', timeout=30_000)
        except PWTimeoutError:
            print("  首页加载超时，跳过")
            return []

        time.sleep(random.uniform(2, 3))
        self._human_scroll(random.randint(3, 5))
        time.sleep(random.uniform(1, 2))

        results: list[dict] = []
        seen_urls: set[str] = set()

        for page_num in range(1, max_pages + 1):
            try:
                self.page.wait_for_selector('.job-detail-box', timeout=10_000)
            except PWTimeoutError:
                print(f"  第{page_num}页未找到职位列表，停止")
                break

            jobs     = self._extract_page_jobs()
            new_jobs = [j for j in jobs if j['job_url'] not in seen_urls]

            for j in new_jobs:
                seen_urls.add(j['job_url'])
                results.append(j)

            print(f"  第{page_num}页: {len(new_jobs)} 条新职位（累计 {len(results)}）")

            if page_num >= max_pages or not self._has_next_page():
                break

            try:
                self.page.locator('li.ant-pagination-next').click()
                time.sleep(random.uniform(3, 5))
                self._human_scroll(random.randint(2, 4))
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                print(f"  翻页失败: {e}")
                break

        return results

    def fetch_description(self, job_url: str) -> str:
        """在新标签页抓取职位详情 JD 正文。"""
        detail_page = self.context.new_page()
        description = ''
        try:
            detail_page.goto(job_url, wait_until='domcontentloaded', timeout=30_000)
            time.sleep(random.uniform(1.5, 2.5))
            self._human_scroll(2, page=detail_page)

            for sel in [
                '.job-detail-content', '.job-description',
                '.newjob-intro', '.job-item-content',
                '[class*="job-deta"]', '.desc-container',
            ]:
                try:
                    el = detail_page.locator(sel).first
                    if el.count():
                        text = el.inner_text().strip()
                        if len(text) > 30:
                            description = text
                            break
                except Exception:
                    pass
        except PWTimeoutError:
            print(f"  详情页超时: {job_url}")
        except Exception as e:
            print(f"  详情页失败: {e}")
        finally:
            try:
                detail_page.close()
            except Exception:
                pass
        return description

    def _extract_page_jobs(self) -> list[dict]:
        jobs = []
        for card in self.page.locator('.job-detail-box').all():
            try:
                job = self._parse_card(card)
                if job:
                    jobs.append(job)
            except Exception as e:
                print(f"  解析 card 失败: {e}")
        return jobs

    def _parse_card(self, card) -> Optional[dict]:
        link = card.locator('a[href*="/job/"]').first
        if not link.count():
            return None
        href = link.get_attribute('href') or ''
        if not href:
            return None

        job_url = href.split('?')[0]
        if not job_url.startswith('http'):
            job_url = 'https://www.liepin.com' + job_url

        title_el  = link.locator('div.ellipsis-1').first
        job_title = ''
        if title_el.count():
            job_title = title_el.get_attribute('title') or title_el.inner_text().strip()

        salary = ''
        for sp in link.locator('span').all():
            t = sp.inner_text().strip()
            if re.search(r'\d+.*[kK万]', t):
                salary = t
                break

        city_text   = ''
        city_spans  = link.locator('span.ellipsis-1').all()
        if city_spans:
            city_text = city_spans[0].inner_text().strip()

        experience = education = ''
        child_divs = link.locator(':scope > div').all()
        if child_divs:
            for sp in child_divs[-1].locator('span').all():
                t = sp.inner_text().strip()
                if re.search(r'[年届]|经验|不限', t) and not experience:
                    experience = t
                elif re.search(r'本科|硕士|博士|大专|学历', t) and not education:
                    education = t

        company = ''
        company_sec = card.locator('[data-nick="job-detail-company-info"]')
        if company_sec.count():
            comp_span = company_sec.locator('span.ellipsis-1').first
            if comp_span.count():
                company = comp_span.inner_text().strip()

        return {
            'job_title':  job_title,
            'company':    company,
            'salary':     salary,
            'city':       city_text,
            'experience': experience,
            'education':  education,
            'job_url':    job_url,
            'description': '',
        }

    def _has_next_page(self) -> bool:
        try:
            nxt = self.page.locator('li.ant-pagination-next')
            if nxt.count() > 0:
                return nxt.get_attribute('aria-disabled') != 'true'
        except Exception:
            pass
        return False

    def close(self):
        try:
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# ── Prompt ────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_EXTRACT_PROMPT = """你是一个求职辅助 AI。根据以下简历，提取求职者的核心信息，返回严格的 JSON 对象，不含其他文字：
{{
  "name": "姓名（若有，否则空字符串）",
  "education": "最高学历，如本科/硕士/博士",
  "major": "专业方向",
  "skills": ["技能1", "技能2"],
  "experience_years": 0,
  "target_roles": ["目标岗位1", "目标岗位2"],
  "search_keywords": ["猎聘搜索词1", "猎聘搜索词2", "猎聘搜索词3"],
  "highlights": "50字以内的求职者核心亮点"
}}

说明：
- search_keywords 是用于在猎聘搜索框输入的关键词，结合技能和目标岗位给出 2-4 个最精准的词。
- 对于校招生/实习生，search_keywords 中可包含"实习"字样的岗位搜索词。
- experience_years 为 0 表示在校生/应届生。

简历内容：
{resume}"""

_SCORE_PROMPT = """你是一个招聘匹配 AI。根据求职者的简历摘要，为以下职位列表打匹配分，返回严格的 JSON 对象，不含其他文字。

求职者简历摘要：
{profile}

职位列表（JSON 数组）：
{jobs}

返回格式（JSON 对象，results 数组与输入职位一一对应）：
{{
  "results": [
    {{
      "index": 0,
      "match_score": 85,
      "match_reason": "简短说明匹配原因（不超过40字）"
    }}
  ]
}}

打分标准：
- 90-100：非常匹配，技能和岗位高度吻合
- 70-89：较匹配，大部分要求满足
- 50-69：一般，有部分技能差距
- 30-49：较低，岗位方向基本匹配但技能差距大
- 0-29：不匹配

对于校招生/实习生，适当降低经验要求的权重，提高技能和学历的权重。"""


# ══════════════════════════════════════════════════════════════════════════════
# ── 简历解析 & 匹配打分 ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"


class ResumeMatcher:
    """
    基于 DeepSeek API 的简历解析与职位匹配打分。

    参数：
        api_key - DeepSeek API Key；不传则读环境变量 DEEPSEEK_API_KEY
        model   - 模型名，默认 deepseek-chat
    """

    def __init__(self, api_key: str = None, model: str = "deepseek-chat"):
        self.api_key = api_key or os.environ.get('DEEPSEEK_API_KEY', '')
        self.model   = model
        if not self.api_key:
            raise ValueError(
                "需要 DeepSeek API Key（环境变量 DEEPSEEK_API_KEY 或 --api-key 参数）"
            )

    def _call(self, prompt: str, max_tokens: int = 1024) -> str:
        resp = _requests.post(
            _DEEPSEEK_API_URL,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model":           self.model,
                "messages":        [{"role": "user", "content": prompt}],
                "response_format": {"type": "json_object"},
                "temperature":     0.1,
                "max_tokens":      max_tokens,
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]

    def parse_resume(self, resume_text: str) -> dict:
        """解析简历，返回结构化 profile dict。"""
        prompt  = _EXTRACT_PROMPT.format(resume=resume_text[:4000])
        content = self._call(prompt, max_tokens=1024)
        return json.loads(content)

    def score_jobs(self, profile: dict, jobs: list[dict], batch_size: int = 15) -> list[dict]:
        """
        为职位列表打匹配分（批量调用，避免单次 token 超限）。
        返回带 match_score 和 match_reason 字段的职位列表，按分数降序。
        """
        if not jobs:
            return []

        profile_text = json.dumps(profile, ensure_ascii=False)
        scored_jobs: list[dict] = []

        for batch_start in range(0, len(jobs), batch_size):
            batch = jobs[batch_start:batch_start + batch_size]
            jobs_summary = [
                {
                    "index":      idx,
                    "job_title":  j.get("job_title", ""),
                    "company":    j.get("company", ""),
                    "experience": j.get("experience", ""),
                    "education":  j.get("education", ""),
                    "description": (j.get("description") or "")[:300],
                }
                for idx, j in enumerate(batch)
            ]
            prompt = _SCORE_PROMPT.format(
                profile=profile_text,
                jobs=json.dumps(jobs_summary, ensure_ascii=False),
            )
            try:
                content = self._call(prompt, max_tokens=2048)
                data    = json.loads(content)
                scores  = data.get("results", [])

                for s in scores:
                    idx = s.get("index", 0)
                    if 0 <= idx < len(batch):
                        job = dict(batch[idx])
                        job["match_score"]  = s.get("match_score", 0)
                        job["match_reason"] = s.get("match_reason", "")
                        scored_jobs.append(job)

            except Exception as e:
                print(f"  [打分] 批次失败: {e}，该批次默认 0 分")
                for j in batch:
                    job = dict(j)
                    job["match_score"]  = 0
                    job["match_reason"] = "打分失败"
                    scored_jobs.append(job)

        return sorted(scored_jobs, key=lambda x: x.get("match_score", 0), reverse=True)


# ══════════════════════════════════════════════════════════════════════════════
# ── 主流程 ────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def find_matching_jobs(
    resume_text: str,
    city: str,
    keywords: list[str] = None,
    top_n: int = 20,
    api_key: str = None,
    max_pages_per_keyword: int = 2,
    fetch_detail: bool = False,
    headless: bool = None,
    use_proxy: bool = True,
    intern_mode: bool = False,
) -> dict:
    """
    主流程：给定简历和城市，返回匹配度排序的职位列表。

    参数：
        resume_text            - 简历纯文本
        city                   - 目标城市（中文，如"北京"）
        keywords               - 手动指定搜索词列表，None 则 AI 自动提取
        top_n                  - 返回前 N 名
        api_key                - DeepSeek API Key
        max_pages_per_keyword  - 每个关键词最多爬几页
        fetch_detail           - 是否进入详情页抓 JD 全文（更慢但匹配更准）
        headless               - 浏览器是否无头，None 读环境变量
        use_proxy              - 是否启用代理池
        intern_mode            - 校招/实习模式（搜索词自动补充"实习"变体）

    返回：
        {
            "profile":       { ... },   # 解析的简历摘要
            "jobs":          [ ... ],   # 匹配排序的职位列表（含 match_score / match_reason）
            "total_crawled": N,         # 抓取去重后总数
        }
    """
    matcher = ResumeMatcher(api_key=api_key)

    # ── 1. 解析简历 ──────────────────────────────────────────────────────────
    print("[简历解析] 正在用 AI 分析简历...")
    profile = matcher.parse_resume(resume_text)
    print(f"  姓名:     {profile.get('name') or '（未识别）'}")
    print(f"  学历:     {profile.get('education', '')} · {profile.get('major', '')}")
    print(f"  技能:     {', '.join(profile.get('skills', [])[:6])}")
    print(f"  目标岗位: {', '.join(profile.get('target_roles', []))}")
    print(f"  AI搜索词: {', '.join(profile.get('search_keywords', []))}")

    # ── 2. 确定搜索关键词 ─────────────────────────────────────────────────────
    base_kws = keywords or profile.get('search_keywords', [])
    if not base_kws:
        raise ValueError("无法从简历提取搜索词，请手动指定 --keywords")

    search_kws = list(base_kws)
    if intern_mode:
        # 实习模式：追加带"实习"后缀的变体，去重
        intern_kws = [kw + '实习' for kw in base_kws if '实习' not in kw]
        search_kws = list(dict.fromkeys(search_kws + intern_kws))
        print(f"  [实习模式] 最终搜索词: {', '.join(search_kws)}")

    # ── 3. 爬取职位 ───────────────────────────────────────────────────────────
    all_jobs:  list[dict] = []
    seen_urls: set[str]   = set()

    with sync_playwright() as pw:
        proxy_pool = ProxyPool(min_size=2) if use_proxy else None
        spider     = JobSearchSpider(pw, proxy_pool=proxy_pool, headless=headless)
        try:
            spider.open_homepage()

            for kw in search_kws:
                jobs = spider.search(kw, city=city, max_pages=max_pages_per_keyword)

                for j in jobs:
                    if j['job_url'] in seen_urls:
                        continue
                    seen_urls.add(j['job_url'])

                    if fetch_detail:
                        print(f"  [详情] {j['job_url']}")
                        j['description'] = spider.fetch_description(j['job_url'])
                        time.sleep(random.uniform(1.5, 3))

                    all_jobs.append(j)

                if len(search_kws) > 1:
                    time.sleep(random.uniform(5, 10))
        finally:
            spider.close()

    print(f"\n[匹配打分] 共抓到 {len(all_jobs)} 条去重职位，开始打分...")

    # ── 4. 打分排序 ───────────────────────────────────────────────────────────
    scored = matcher.score_jobs(profile, all_jobs)
    top_jobs = scored[:top_n]

    return {
        "profile":       profile,
        "jobs":          top_jobs,
        "total_crawled": len(all_jobs),
    }


# ══════════════════════════════════════════════════════════════════════════════
# ── 结果展示 ──────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def print_results(result: dict, output_json: bool = False):
    if output_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    profile  = result["profile"]
    jobs     = result["jobs"]
    total    = result["total_crawled"]

    print("\n" + "=" * 72)
    print("  简历解析结果")
    print("=" * 72)
    print(f"  姓名:     {profile.get('name') or '（未识别）'}")
    print(f"  学历:     {profile.get('education', '')} · {profile.get('major', '')}")
    print(f"  技能:     {', '.join(profile.get('skills', []))}")
    print(f"  目标岗位: {', '.join(profile.get('target_roles', []))}")
    print(f"  亮点:     {profile.get('highlights', '')}")
    print(f"\n共抓取 {total} 条职位，展示 Top {len(jobs)} 匹配结果：")
    print("=" * 72)

    for i, job in enumerate(jobs, 1):
        score  = job.get("match_score", 0)
        filled = round(score / 10)
        bar    = "█" * filled + "░" * (10 - filled)
        print(f"\n#{i:2d}  [{bar}] {score:3d}分  {job.get('job_title', '')}")
        print(f"      公司: {job.get('company', '')}  | 薪资: {job.get('salary') or '面议'}")
        print(f"      城市: {job.get('city', '')}  经验: {job.get('experience', '')}  学历: {job.get('education', '')}")
        print(f"      理由: {job.get('match_reason', '')}")
        print(f"      链接: {job.get('job_url', '')}")

    print("\n" + "=" * 72)


# ══════════════════════════════════════════════════════════════════════════════
# ── CLI 入口 ──────────────────────────────════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="根据简历自动搜索猎聘匹配岗位",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
示例：
  python job_matcher.py --resume resume.txt --city 北京
  python job_matcher.py --resume resume.txt --city 上海 --keywords Python 数据分析
  python job_matcher.py --resume resume.txt --city 深圳 --intern --top 30 --json
  python job_matcher.py --resume resume.txt --city 杭州 --fetch-detail --no-proxy

支持城市：{' / '.join(CITY_CODES.keys())}
        """,
    )
    parser.add_argument('--resume',       required=True,        help="简历文件路径（UTF-8 纯文本）")
    parser.add_argument('--city',         required=True,        help="目标城市，如：北京、上海、深圳")
    parser.add_argument('--keywords',     nargs='+',            help="搜索关键词（不填则 AI 自动提取）")
    parser.add_argument('--top',          type=int, default=20, help="返回 Top N 结果，默认 20")
    parser.add_argument('--pages',        type=int, default=2,  help="每个关键词最多爬几页，默认 2")
    parser.add_argument('--api-key',      default=None,         help="DeepSeek API Key")
    parser.add_argument('--intern',       action='store_true',  help="校招/实习模式：搜索词自动补充实习变体")
    parser.add_argument('--fetch-detail', action='store_true',  help="抓取详情页 JD 全文（更慢但匹配更准）")
    parser.add_argument('--no-proxy',     action='store_true',  help="禁用代理池，直连运行")
    parser.add_argument('--headless',     action='store_true',  help="无头模式运行浏览器（适合服务器）")
    parser.add_argument('--json',         action='store_true',  help="以 JSON 格式输出完整结果")
    args = parser.parse_args()

    if args.city not in CITY_CODES:
        print(f"[警告] '{args.city}' 不在城市列表中，将以无城市过滤模式搜索")

    with open(args.resume, 'r', encoding='utf-8') as f:
        resume_text = f.read()

    result = find_matching_jobs(
        resume_text=resume_text,
        city=args.city,
        keywords=args.keywords,
        top_n=args.top,
        api_key=args.api_key,
        max_pages_per_keyword=args.pages,
        fetch_detail=args.fetch_detail,
        headless=args.headless if args.headless else None,
        use_proxy=not args.no_proxy,
        intern_mode=args.intern,
    )

    print_results(result, output_json=args.json)


if __name__ == '__main__':
    main()
