# -*- coding: utf-8 -*-
"""
猎聘招聘爬虫 
=====================================================================

功能：
    自动搜索猎聘网上指定公司的招聘职位，翻页抓取全量，写入 PostgreSQL 数据库。
    支持代理池轮换（proxy.scdn.io），内置反爬虫检测规避。

依赖安装：
    pip install playwright sqlalchemy psycopg2-binary paramiko requests
    playwright install chromium

数据库：
    脚本会自动创建 competitor_recruitment 表（如不存在）。

配置方式（按优先级）：

    1. 直接修改下方 ── 用户配置区 ── 中的常量
    2. 环境变量（见各变量说明）

用法示例：
    # 直连数据库
    DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/dbname python liepin_recruitment_spider.py

    # SSH 隧道模式（不设 DATABASE_URL 时自动启用）
    python liepin_recruitment_spider.py

    # 无头模式（服务器上运行）
    HEADLESS=1 DATABASE_URL=... python liepin_recruitment_spider.py

    # 指定 Chromium 路径
    CHROMIUM_PATH=/usr/bin/chromium python liepin_recruitment_spider.py
"""

import os
import re
import select
import socket
import sys
import threading
import time
import uuid
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone

import paramiko
import requests as _requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from sqlalchemy import (
    Column, String, Date, DateTime, UniqueConstraint, Index,
    create_engine, text
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import declarative_base, sessionmaker


# ══════════════════════════════════════════════════════════════════════════════
# ── 用户配置区（按需修改）────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

# 目标公司列表：vendor=数据库标识（唯一），keyword=猎聘搜索词
COMPANIES = [
    {'vendor': '公司A', 'keyword': '公司A'},
    {'vendor': '公司B', 'keyword': '公司B关键词'},
]

# SSH 隧道配置（仅当 DATABASE_URL 环境变量未设置时使用）
SSH_HOST     = os.environ.get('SSH_HOST', 'your_ssh_host')
SSH_USER     = os.environ.get('SSH_USER', 'your_ssh_user')
SSH_PASSWORD = os.environ.get('SSH_PASSWORD', 'your_ssh_password')
SSH_PORT     = int(os.environ.get('SSH_PORT', '22'))
DB_REMOTE_PORT   = 5432   # 服务器上 PostgreSQL 监听端口
TUNNEL_LOCAL_PORT = 15433  # 本地转发端口（避免与其他进程冲突）

# 数据库连接（优先读环境变量 DATABASE_URL）
_USE_TUNNEL = not bool(os.environ.get('DATABASE_URL'))
DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    f'postgresql+psycopg2://your_db_user:your_db_password'
    f'@127.0.0.1:{TUNNEL_LOCAL_PORT}/your_db_name',
)

# ══════════════════════════════════════════════════════════════════════════════
# ── 数据库模型（自包含，自动建表）────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

Base = declarative_base()


class CompetitorRecruitment(Base):
    __tablename__ = 'competitor_recruitment'

    id         = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    vendor     = Column(String(64),  nullable=False, index=True)   # 公司简称
    brand_name = Column(String(128), nullable=True)                # 猎聘显示的公司全名
    job_title  = Column(String(256), nullable=False)
    salary     = Column(String(64),  nullable=True)
    city       = Column(String(64),  nullable=True)
    experience = Column(String(64),  nullable=True)
    education  = Column(String(64),  nullable=True)
    job_url    = Column(String(512), nullable=False)
    description= Column(String,      nullable=True)
    posted_at  = Column(Date,        nullable=True, index=True)
    crawled_at = Column(DateTime,    nullable=True, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('vendor', 'job_url', name='uq_recruitment_url'),
    )


# ══════════════════════════════════════════════════════════════════════════════
# ── 数据库会话管理 ────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_Session = None


def _init_db(url: str):
    """初始化数据库引擎，自动创建表（如不存在）。"""
    global _Session
    engine = create_engine(url, pool_pre_ping=True, pool_size=3)
    Base.metadata.create_all(engine)
    _Session = sessionmaker(autocommit=False, autoflush=False,
                            expire_on_commit=False, bind=engine)


@contextmanager
def get_db():
    db = _Session()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# ── SSH 隧道（可选，仅 _USE_TUNNEL=True 时启用）──────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_transport   = None
_tunnel_server = None


def _forward_channel(local_sock, channel):
    while True:
        try:
            r, _, _ = select.select([local_sock, channel], [], [], 5)
        except Exception:
            break
        if local_sock in r:
            data = local_sock.recv(4096)
            if not data:
                break
            channel.sendall(data)
        if channel in r:
            data = channel.recv(4096)
            if not data:
                break
            local_sock.sendall(data)
    local_sock.close()
    channel.close()


def start_db_tunnel():
    global _transport, _tunnel_server
    transport = paramiko.Transport((SSH_HOST, SSH_PORT))
    transport.connect(username=SSH_USER, password=SSH_PASSWORD)
    _transport = transport

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
    srv.bind(('127.0.0.1', TUNNEL_LOCAL_PORT))
    srv.listen(20)
    srv.settimeout(1)
    _tunnel_server = srv

    def accept_loop():
        while True:
            try:
                sock, addr = srv.accept()
            except socket.timeout:
                if not transport.is_active():
                    break
                continue
            except Exception:
                break
            try:
                chan = transport.open_channel(
                    'direct-tcpip', ('127.0.0.1', DB_REMOTE_PORT), addr
                )
            except Exception as e:
                print(f"[DB] 隧道 channel 失败: {e}")
                sock.close()
                continue
            threading.Thread(
                target=_forward_channel, args=(sock, chan), daemon=True
            ).start()

    threading.Thread(target=accept_loop, daemon=True).start()
    time.sleep(0.5)
    print(f"[DB] SSH 隧道已建立 (127.0.0.1:{TUNNEL_LOCAL_PORT} → {SSH_HOST}:{DB_REMOTE_PORT})")


def stop_db_tunnel():
    global _tunnel_server, _transport
    if _tunnel_server:
        try:
            _tunnel_server.close()
        except Exception:
            pass
    if _transport:
        try:
            _transport.close()
        except Exception:
            pass
    print("[DB] 隧道已关闭")


# ══════════════════════════════════════════════════════════════════════════════
# ── 代理池 ────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class ProxyPool:
    """
    从 proxy.scdn.io 获取免费代理，并发验证可用性后维护一个可用池。
    每次 get() 返回一个 'protocol://ip:port' 字符串，失效时调用 remove_bad() 触发补充。
    """
    _API = "https://proxy.scdn.io/api/get_proxy.php"

    def __init__(self, min_size: int = 3):
        self._min_size = min_size
        self._pool: list[str] = []
        self._lock = threading.Lock()
        self._refresh()

    def _fetch_raw(self, count: int = 20):
        for protocol in ("socks5", "http"):
            try:
                resp = _requests.get(
                    self._API,
                    params={"protocol": protocol, "count": count},
                    timeout=6,
                )
                proxies = resp.json()["data"]["proxies"]
                if proxies:
                    print(f"[Proxy] 获取 {len(proxies)} 个 {protocol} 代理")
                    return [(p, protocol) for p in proxies]
            except Exception as e:
                print(f"[Proxy] 获取代理失败({protocol}): {e}")
        return []

    @staticmethod
    def _check(addr: str, protocol: str, timeout: int = 7):
        proxy_url = f"{protocol}://{addr}"
        try:
            r = _requests.get(
                "https://www.baidu.com",
                proxies={"http": proxy_url, "https": proxy_url},
                timeout=timeout,
                allow_redirects=True,
            )
            if r.status_code < 400:
                return f"{protocol}://{addr}"
        except Exception:
            pass
        return None

    def _refresh(self):
        raw = self._fetch_raw(20)
        if not raw:
            print("[Proxy] 无法获取代理，将直连运行")
            return
        valid = []
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(self._check, addr, proto): addr for addr, proto in raw}
            for f in as_completed(futures):
                result = f.result()
                if result:
                    valid.append(result)
        with self._lock:
            self._pool = valid
        print(f"[Proxy] 可用代理: {len(valid)}/{len(raw)}")
        if not valid:
            print("[Proxy] 全部验证失败，将直连运行")

    def get(self):
        """返回一个可用的代理 URL，无可用时返回 None。"""
        with self._lock:
            if not self._pool:
                return None
            proxy = self._pool.pop(0)
            self._pool.append(proxy)
        return proxy

    def remove_bad(self, proxy_url: str):
        """标记某代理不可用，池低于 min_size 时自动补充。"""
        with self._lock:
            if proxy_url in self._pool:
                self._pool.remove(proxy_url)
            remaining = len(self._pool)
        print(f"[Proxy] 移除失效代理 {proxy_url}，剩余 {remaining}")
        if remaining < self._min_size:
            threading.Thread(target=self._refresh, daemon=True).start()

    def empty(self) -> bool:
        with self._lock:
            return len(self._pool) == 0


# ══════════════════════════════════════════════════════════════════════════════
# ── 工具函数 ──────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def parse_liepin_date(raw: str):
    """
    解析猎聘常见发布时间格式：
      '刚刚' / '3小时前' / '2天前' / '2026-05-30' / '05-30'
    返回 date 对象，无法解析时返回 None。
    """
    if not raw:
        return None
    raw = raw.strip()
    today = date.today()
    if raw == '刚刚':
        return today
    m = re.match(r'(\d+)\s*小时前', raw)
    if m:
        return today
    m = re.match(r'(\d+)\s*天前', raw)
    if m:
        return today - timedelta(days=int(m.group(1)))
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', raw)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = re.match(r'(\d{2})-(\d{2})$', raw)
    if m:
        return date(today.year, int(m.group(1)), int(m.group(2)))
    return None


# ══════════════════════════════════════════════════════════════════════════════
# ── 爬虫主体 ──────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_ANTI_DETECT_JS = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
    Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
"""

_UA_POOL = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
]


class LiepinSpider:
    """
    猎聘招聘爬虫。

    参数：
        playwright   - sync_playwright() 上下文
        proxy_pool   - ProxyPool 实例（可选，None 则直连）
        companies    - 目标公司列表，格式同模块顶部 COMPANIES 常量
        headless     - 是否无头模式（True=无头，False=有头）
    """

    def __init__(self, playwright, proxy_pool=None, companies=None, headless=None,
                 incremental=True, fetch_detail=True):
        self.playwright  = playwright
        self.proxy_pool  = proxy_pool
        self.companies   = companies or COMPANIES
        self.grand_total = 0  # 本次运行累计新增条数

        # headless 优先用参数，其次读环境变量，最后默认有头（方便调试）
        if headless is None:
            headless = os.environ.get('HEADLESS', '').lower() in ('1', 'true', 'yes')

        # incremental: True=跳过已存在记录，False=全量重试写入
        # 环境变量 INCREMENTAL=0 可在不改代码的情况下关闭
        _env_inc = os.environ.get('INCREMENTAL', '')
        self.incremental = incremental if _env_inc == '' else _env_inc.lower() not in ('0', 'false', 'no')

        # fetch_detail: True=进入详情页抓 JD 和发布日期，False=仅保存列表页信息
        # 环境变量 FETCH_DETAIL=0 可关闭
        _env_fd = os.environ.get('FETCH_DETAIL', '')
        self.fetch_detail = fetch_detail if _env_fd == '' else _env_fd.lower() not in ('0', 'false', 'no')

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

    # ── 浏览器 context 管理 ───────────────────────────────────────────────────

    def _make_context(self, proxy_url=None):
        if self.context:
            try:
                self.context.close()
            except Exception:
                pass
        ctx_opts = {
            'viewport':    {'width': 1920, 'height': 1080},
            'user_agent':  random.choice(_UA_POOL),
            'locale':      'zh-CN',
        }
        if proxy_url:
            ctx_opts['proxy'] = {'server': proxy_url}
            print(f"[Proxy] 使用代理: {proxy_url}")
        else:
            print("[Proxy] 直连模式（无代理）")
        self.context = self.browser.new_context(**ctx_opts)
        self.context.add_init_script(_ANTI_DETECT_JS)
        self.page = self.context.new_page()
        self._current_proxy = proxy_url

    def _rotate_proxy(self):
        if self._current_proxy and self.proxy_pool:
            self.proxy_pool.remove_bad(self._current_proxy)
        new_proxy = self.proxy_pool.get() if self.proxy_pool else None
        self._make_context(new_proxy)

    # ── 页面操作 ──────────────────────────────────────────────────────────────

    def open_homepage(self):
        print("正在打开猎聘首页...")
        self.page.goto('https://www.liepin.com/', wait_until='domcontentloaded')
        time.sleep(random.uniform(2, 3))
        print("页面就绪，开始抓取")

    def _human_scroll(self, times: int = 3, page=None):
        p = page or self.page
        for _ in range(times):
            p.mouse.wheel(0, random.randint(200, 400))
            time.sleep(random.uniform(0.3, 0.7))

    # ── 核心抓取逻辑 ──────────────────────────────────────────────────────────

    def _fetch_job_description(self, job_url: str) -> tuple:
        """
        在新标签页打开职位详情页，提取 JD 正文和发布日期。
        返回 (description: str, posted_at: date | None)。
        """
        detail_page = self.context.new_page()
        description = ''
        posted_at = None
        try:
            detail_page.goto(job_url, wait_until='domcontentloaded', timeout=30_000)
            time.sleep(random.uniform(1.5, 2.5))
            self._human_scroll(random.randint(2, 3), page=detail_page)

            # 依次尝试猎聘详情页常见 JD 容器 selector
            for sel in [
                '.job-detail-content',
                '.job-description',
                '.newjob-intro',
                '.job-item-content',
                '[class*="job-deta"]',
                '.desc-container',
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

            # 发布日期：先找 <time>，再找含日期关键词的 span/p
            for sel in ['time', '[class*="time"]', '[class*="date"]', 'span', 'p']:
                if posted_at:
                    break
                try:
                    for el in detail_page.locator(sel).all():
                        raw = el.inner_text().strip()
                        d = parse_liepin_date(raw)
                        if d:
                            posted_at = d
                            break
                except Exception:
                    pass

        except PWTimeoutError:
            print(f"    详情页加载超时: {job_url}")
        except Exception as e:
            print(f"    详情页抓取失败: {e}")
        finally:
            try:
                detail_page.close()
            except Exception:
                pass

        return description, posted_at

    def search_company_jobs(self, vendor: str, keyword: str, max_proxy_retries: int = 2) -> int:
        """搜索某公司招聘，翻页抓取全量，返回本次新增条数。代理失败时自动轮换重试。"""
        print(f"\n=== 开始抓取：{vendor}（搜索词：{keyword}）===")
        for attempt in range(max_proxy_retries + 1):
            if attempt > 0:
                print(f"  [Proxy] 第 {attempt} 次代理轮换重试...")
                self._rotate_proxy()
            result = self._search_with_current_context(vendor, keyword)
            if result is not None:
                return result
            if not self.proxy_pool or self.proxy_pool.empty():
                break
        print(f"  {vendor} 所有重试均失败，跳过")
        return 0

    def _search_with_current_context(self, vendor: str, keyword: str):
        """用当前 context 执行一次完整抓取，失败返回 None，成功返回新增条数。"""
        total_inserted = 0
        total_skipped  = 0
        seen_urls: set = set()

        url = f'https://www.liepin.com/zhaopin/?key={keyword}&companyName={keyword}&curPage=0'
        print(f"  第 1 页 → {url}")
        try:
            self.page.goto(url, wait_until='domcontentloaded', timeout=30_000)
        except PWTimeoutError:
            print("  页面加载超时（可能代理问题）")
            return None

        time.sleep(random.uniform(2, 3))
        self._human_scroll(random.randint(3, 5))
        time.sleep(random.uniform(1, 2))

        page_num = 1
        while True:
            try:
                self.page.wait_for_selector('.job-detail-box', timeout=10_000)
            except PWTimeoutError:
                print("  未找到职位列表，停止")
                break

            jobs_on_page = self._extract_jobs_from_page(keyword)
            new_jobs = [j for j in jobs_on_page if j['job_url'] not in seen_urls]

            if not new_jobs:
                print("  本页无新职位，停止翻页")
                break

            new_count = 0
            for job in new_jobs:
                job_url = job['job_url']
                seen_urls.add(job_url)

                # ① 查重（incremental=True 时跳过已存在的记录）
                if self.incremental:
                    try:
                        with get_db() as db:
                            if db.query(CompetitorRecruitment).filter_by(
                                vendor=vendor, job_url=job_url
                            ).first():
                                total_skipped += 1
                                continue
                    except Exception as e:
                        print(f"  查库异常: {e}")
                        continue

                # ② 抓详情页（fetch_detail=True 时进入岗位页面获取 JD 和发布日期）
                if self.fetch_detail:
                    print(f"    抓详情: {job_url}")
                    description, posted_at = self._fetch_job_description(job_url)
                    time.sleep(random.uniform(2, 4))
                else:
                    description, posted_at = '', None

                # ③ 写库
                try:
                    with get_db() as db:
                        db.add(CompetitorRecruitment(
                            id=uuid.uuid4(),
                            vendor=vendor,
                            brand_name=job.get('brand_name', ''),
                            job_title=job.get('job_title', ''),
                            salary=job.get('salary', ''),
                            city=job.get('city', ''),
                            experience=job.get('experience', ''),
                            education=job.get('education', ''),
                            job_url=job_url,
                            description=description,
                            posted_at=posted_at,
                            crawled_at=datetime.now(timezone.utc),
                        ))
                        new_count += 1
                        total_inserted += 1
                except Exception as e:
                    print(f"  写库异常: {e}")

            print(f"  第{page_num}页解析 {len(jobs_on_page)} 条，"
                  f"新增 {new_count} 条（累计 {len(seen_urls)}，跳过 {total_skipped}）")

            if not self._has_next_page():
                print("  已到最后一页")
                break

            try:
                self.page.locator('li.ant-pagination-next').click()
                page_num += 1
                print(f"  → 翻到第 {page_num} 页，等待加载...")
                time.sleep(random.uniform(3, 5))
                self._human_scroll(random.randint(2, 4))
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                print(f"  点击下一页失败: {e}")
                break

        print(f"  {vendor} 完成，共新增 {total_inserted} 条")
        return total_inserted

    def _extract_jobs_from_page(self, keyword: str) -> list:
        jobs = []
        for card in self.page.locator('.job-detail-box').all():
            try:
                job = self._parse_job_card(card, keyword)
                if job:
                    jobs.append(job)
            except Exception as e:
                print(f"    解析 card 失败: {e}")
        return jobs

    def _parse_job_card(self, card, keyword: str):
        link = card.locator('a[href*="/job/"]').first
        if link.count() == 0:
            return None
        href = link.get_attribute('href') or ''
        if not href:
            return None
        job_url = href.split('?')[0]
        if not job_url.startswith('http'):
            job_url = 'https://www.liepin.com' + job_url

        # 职位名（优先 title 属性，避免 ellipsis 截断）
        title_el = link.locator('div.ellipsis-1').first
        job_title = ''
        if title_el.count():
            job_title = title_el.get_attribute('title') or title_el.inner_text().strip()

        # 薪资（含数字和 k/K/万 的 span）
        salary = ''
        for sp in link.locator('span').all():
            t = sp.inner_text().strip()
            if re.search(r'\d+.*[kK万]', t):
                salary = t
                break

        # 城市
        city = ''
        city_spans = link.locator('span.ellipsis-1').all()
        if city_spans:
            city = city_spans[0].inner_text().strip()

        # 经验 & 学历
        experience = education = ''
        child_divs = link.locator(':scope > div').all()
        if child_divs:
            for sp in child_divs[-1].locator('span').all():
                t = sp.inner_text().strip()
                if re.search(r'[年届]|经验|不限', t) and not experience:
                    experience = t
                elif re.search(r'本科|硕士|博士|大专|学历', t) and not education:
                    education = t

        # 公司名
        brand_name = keyword
        company_sec = card.locator('[data-nick="job-detail-company-info"]')
        if company_sec.count():
            comp_span = company_sec.locator('span.ellipsis-1').first
            if comp_span.count():
                brand_name = comp_span.inner_text().strip()

        return {
            'job_title':   job_title,
            'brand_name':  brand_name,
            'salary':      salary,
            'city':        city,
            'experience':  experience,
            'education':   education,
            'job_url':     job_url,
            'description': '',
            'posted_at':   None,
        }

    def _has_next_page(self) -> bool:
        try:
            nxt = self.page.locator('li.ant-pagination-next')
            if nxt.count() > 0:
                return nxt.get_attribute('aria-disabled') != 'true'
        except Exception:
            pass
        return False

    # ── 主运行入口 ────────────────────────────────────────────────────────────

    def run(self):
        try:
            if _USE_TUNNEL:
                start_db_tunnel()
            if _Session is None:
                _init_db(DATABASE_URL)
            self.open_homepage()

            grand_total = 0
            for i, co in enumerate(self.companies):
                if self.proxy_pool:
                    proxy = self.proxy_pool.get()
                    self._make_context(proxy)
                n = self.search_company_jobs(co['vendor'], co['keyword'])
                grand_total += n
                if i < len(self.companies) - 1:
                    wait = random.uniform(15, 25)
                    print(f"  等待 {wait:.0f}s 后处理下一家...")
                    time.sleep(wait)

            self.grand_total = grand_total
            print(f"\n全部完成，共写入 {grand_total} 条到 competitor_recruitment 表")
        except KeyboardInterrupt:
            print("\n用户中断")
        finally:
            try:
                self.context.close()
                self.browser.close()
            except Exception:
                pass
            if _USE_TUNNEL:
                stop_db_tunnel()


# ══════════════════════════════════════════════════════════════════════════════
# ── 入口 ─────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def main():
    with sync_playwright() as pw:
        proxy_pool = ProxyPool(min_size=2)
        spider = LiepinSpider(pw, proxy_pool=proxy_pool)
        spider.run()


if __name__ == '__main__':
    main()
