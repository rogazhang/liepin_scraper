# -*- coding: utf-8 -*-
"""
猎聘竞品招聘 REST API
=====================================================================

启动：
    uvicorn api:app --reload --port 8000

Swagger 文档：
    http://localhost:8000/docs
"""

import threading
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, or_, text

import jd_analyzer as _jd_mod  # 注册 JDAnalysis 到 Base.metadata（必须在 _init_db 前导入）
from jd_analyzer import JDAnalysis, JDAnalyzer
from liepin_recruitment_spider import (
    COMPANIES,
    DATABASE_URL,
    LiepinSpider,
    ProxyPool,
    CompetitorRecruitment,
    _USE_TUNNEL,
    _init_db,
    _Session,
    get_db,
    start_db_tunnel,
    stop_db_tunnel,
)
from playwright.sync_api import sync_playwright


# ══════════════════════════════════════════════════════════════════════════════
# ── 应用生命周期 ──────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    if _USE_TUNNEL:
        start_db_tunnel()
    _init_db(DATABASE_URL)
    yield
    if _USE_TUNNEL:
        stop_db_tunnel()


app = FastAPI(
    title="猎聘竞品招聘 API",
    description="查询竞品公司在猎聘的招聘数据，支持关键词搜索和趋势统计。",
    version="1.0.0",
    lifespan=lifespan,
)


# ══════════════════════════════════════════════════════════════════════════════
# ── Pydantic 模型 ─────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class JobOut(BaseModel):
    id: uuid.UUID
    vendor: str
    brand_name: Optional[str] = None
    job_title: str
    salary: Optional[str] = None
    city: Optional[str] = None
    experience: Optional[str] = None
    education: Optional[str] = None
    job_url: str
    description: Optional[str] = None
    posted_at: Optional[date] = None
    crawled_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class JobListOut(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[JobOut]


class VendorOut(BaseModel):
    vendor: str
    job_count: int


class TrendItem(BaseModel):
    vendor: str
    week: str
    count: int


class ScrapeRequest(BaseModel):
    incremental: bool = True
    fetch_detail: bool = True
    vendors: Optional[list[str]] = None  # None = 全部公司


class ScrapeStatus(BaseModel):
    task_id: str
    status: str          # pending / running / done / error
    inserted: Optional[int] = None
    error: Optional[str] = None


class AnalysisOut(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    job_function: Optional[str] = None
    seniority: Optional[str] = None
    skills: Optional[list[str]] = None
    summary: Optional[str] = None
    analyzed_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class AnalyzeRequest(BaseModel):
    api_key: Optional[str] = None   # 不传则读环境变量 DEEPSEEK_API_KEY
    vendor: Optional[str] = None    # None = 全部公司
    limit: Optional[int] = None     # None = 全部未分析岗位


class AnalyzeStatus(BaseModel):
    task_id: str
    status: str                      # pending / running / done / error
    analyzed: Optional[int] = None
    error: Optional[str] = None


# ══════════════════════════════════════════════════════════════════════════════
# ── 后台任务状态（进程内，重启后清空） ────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_scrape_tasks: dict[str, dict] = {}
_analyze_tasks: dict[str, dict] = {}


def _run_analyze(task_id: str, api_key: Optional[str], vendor: Optional[str], limit: Optional[int]):
    _analyze_tasks[task_id]["status"] = "running"
    try:
        analyzer = JDAnalyzer(api_key=api_key)
        n = analyzer.run_batch(limit=limit, vendor=vendor)
        _analyze_tasks[task_id].update(status="done", analyzed=n)
    except Exception as e:
        _analyze_tasks[task_id].update(status="error", error=str(e))


def _run_scrape(task_id: str, incremental: bool, fetch_detail: bool, companies: list):
    _scrape_tasks[task_id]["status"] = "running"
    try:
        with sync_playwright() as pw:
            spider = LiepinSpider(
                pw,
                proxy_pool=ProxyPool(min_size=2),
                companies=companies,
                incremental=incremental,
                fetch_detail=fetch_detail,
            )
            spider.run()
            _scrape_tasks[task_id].update(status="done", inserted=spider.grand_total)
    except Exception as e:
        _scrape_tasks[task_id].update(status="error", error=str(e))


# ══════════════════════════════════════════════════════════════════════════════
# ── 端点 ─────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/jobs", response_model=JobListOut, summary="查询岗位列表")
def list_jobs(
    vendor: Optional[str] = Query(None, description="公司标识，精确匹配"),
    city:   Optional[str] = Query(None, description="城市，模糊匹配"),
    q:      Optional[str] = Query(None, description="关键词，匹配岗位名称和 JD 正文"),
    date_from: Optional[date] = Query(None, description="发布日期起（含）"),
    date_to:   Optional[date] = Query(None, description="发布日期止（含）"),
    page:      int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页条数"),
):
    with get_db() as db:
        query = db.query(CompetitorRecruitment)
        if vendor:
            query = query.filter(CompetitorRecruitment.vendor == vendor)
        if city:
            query = query.filter(CompetitorRecruitment.city.ilike(f"%{city}%"))
        if q:
            query = query.filter(or_(
                CompetitorRecruitment.job_title.ilike(f"%{q}%"),
                CompetitorRecruitment.description.ilike(f"%{q}%"),
            ))
        if date_from:
            query = query.filter(CompetitorRecruitment.posted_at >= date_from)
        if date_to:
            query = query.filter(CompetitorRecruitment.posted_at <= date_to)

        total = query.count()
        items = (
            query.order_by(CompetitorRecruitment.crawled_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )
        return JobListOut(total=total, page=page, page_size=page_size, items=items)


@app.get("/jobs/{job_id}", response_model=JobOut, summary="岗位详情")
def get_job(job_id: uuid.UUID):
    with get_db() as db:
        job = db.query(CompetitorRecruitment).filter_by(id=job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="岗位不存在")
        return job


@app.get("/vendors", response_model=list[VendorOut], summary="公司列表及岗位数")
def list_vendors():
    with get_db() as db:
        rows = (
            db.query(
                CompetitorRecruitment.vendor,
                func.count().label("job_count"),
            )
            .group_by(CompetitorRecruitment.vendor)
            .order_by(func.count().desc())
            .all()
        )
        return [VendorOut(vendor=r.vendor, job_count=r.job_count) for r in rows]


@app.get("/stats/trend", response_model=list[TrendItem], summary="近 8 周各公司新增趋势")
def stats_trend():
    with get_db() as db:
        rows = db.execute(text("""
            SELECT
                vendor,
                TO_CHAR(DATE_TRUNC('week', crawled_at), 'YYYY-MM-DD') AS week,
                COUNT(*) AS count
            FROM competitor_recruitment
            WHERE crawled_at >= NOW() - INTERVAL '8 weeks'
            GROUP BY vendor, DATE_TRUNC('week', crawled_at)
            ORDER BY week DESC, vendor
        """)).fetchall()
        return [TrendItem(vendor=r.vendor, week=r.week, count=r.count) for r in rows]


@app.post("/scrape", response_model=ScrapeStatus, status_code=202, summary="触发抓取任务")
def trigger_scrape(req: ScrapeRequest):
    if req.vendors:
        companies = [c for c in COMPANIES if c["vendor"] in req.vendors]
        if not companies:
            raise HTTPException(status_code=400, detail="未找到指定公司，请检查 vendor 名称")
    else:
        companies = COMPANIES

    task_id = str(uuid.uuid4())
    _scrape_tasks[task_id] = {"status": "pending", "inserted": None, "error": None}

    threading.Thread(
        target=_run_scrape,
        args=(task_id, req.incremental, req.fetch_detail, companies),
        daemon=True,
    ).start()

    return ScrapeStatus(task_id=task_id, status="pending")


@app.get("/scrape/{task_id}", response_model=ScrapeStatus, summary="查询抓取任务状态")
def get_scrape_status(task_id: str):
    task = _scrape_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    return ScrapeStatus(task_id=task_id, **task)


# ══════════════════════════════════════════════════════════════════════════════
# ── JD 分析端点 ───────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/jobs/{job_id}/analysis", response_model=AnalysisOut, summary="获取岗位的 AI 分析结果")
def get_job_analysis(job_id: uuid.UUID):
    with get_db() as db:
        record = db.query(JDAnalysis).filter_by(job_id=job_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="该岗位暂无分析结果")
        return record


@app.post("/analyze", response_model=AnalyzeStatus, status_code=202, summary="触发 JD 批量分析任务")
def trigger_analyze(req: AnalyzeRequest):
    task_id = str(uuid.uuid4())
    _analyze_tasks[task_id] = {"status": "pending", "analyzed": None, "error": None}

    threading.Thread(
        target=_run_analyze,
        args=(task_id, req.api_key, req.vendor, req.limit),
        daemon=True,
    ).start()

    return AnalyzeStatus(task_id=task_id, status="pending")


@app.get("/analyze/{task_id}", response_model=AnalyzeStatus, summary="查询分析任务状态")
def get_analyze_status(task_id: str):
    task = _analyze_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    return AnalyzeStatus(task_id=task_id, **task)


@app.get("/stats/skills", summary="全量技能词频统计")
def stats_skills(
    vendor: Optional[str] = Query(None, description="按公司过滤"),
    top_n:  int           = Query(20, ge=1, le=100, description="返回前 N 个技能"),
):
    with get_db() as db:
        query = db.query(JDAnalysis)
        if vendor:
            job_ids = [
                r.id for r in db.query(CompetitorRecruitment.id)
                .filter(CompetitorRecruitment.vendor == vendor)
                .all()
            ]
            query = query.filter(JDAnalysis.job_id.in_(job_ids))
        records = query.filter(JDAnalysis.skills.isnot(None)).all()

    counter: dict[str, int] = {}
    for r in records:
        for skill in (r.skills or []):
            counter[skill] = counter.get(skill, 0) + 1

    ranked = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:top_n]
    return [{"skill": s, "count": c} for s, c in ranked]
