# -*- coding: utf-8 -*-
"""
JD 智能分析模块（基于 DeepSeek API）
=====================================================================

功能：
    调用 DeepSeek API 对职位 JD 进行结构化解析，提取岗位职能分类、
    级别、技能列表、摘要，结果写入 jd_analysis 表。

用法：
    # 分析全部未处理岗位
    DEEPSEEK_API_KEY=sk-xxx python jd_analyzer.py

    # 只分析指定公司，最多 50 条
    python jd_analyzer.py --api-key sk-xxx --vendor 公司A --limit 50
"""

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone

import requests as _requests
from sqlalchemy import Column, String, DateTime, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB

from liepin_recruitment_spider import (
    Base,
    CompetitorRecruitment,
    DATABASE_URL,
    _USE_TUNNEL,
    _init_db,
    get_db,
    start_db_tunnel,
    stop_db_tunnel,
)


# ══════════════════════════════════════════════════════════════════════════════
# ── 数据库模型 ────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class JDAnalysis(Base):
    __tablename__ = 'jd_analysis'

    id           = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id       = Column(PG_UUID(as_uuid=True), nullable=False, index=True)
    job_function = Column(String(64),  nullable=True)   # 岗位职能分类
    seniority    = Column(String(32),  nullable=True)   # junior/mid/senior/lead
    skills       = Column(JSONB,       nullable=True)   # 技能列表
    summary      = Column(Text,        nullable=True)   # 岗位摘要（50 字内）
    analyzed_at  = Column(DateTime,    nullable=False,
                          default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('job_id', name='uq_jd_analysis_job_id'),
    )


# ══════════════════════════════════════════════════════════════════════════════
# ── Prompt ────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_PROMPT = """分析以下招聘JD，返回严格的JSON，不含其他任何文字：
{{
  "job_function": "从以下选项选一个：硬件设计/软件开发/算法/测试/销售/市场/运营/管理/其他",
  "seniority": "从 junior/mid/senior/lead/unknown 选一个",
  "skills": ["技能或工具1", "技能或工具2"],
  "summary": "50字以内的核心职责摘要"
}}

JD：
{description}"""


# ══════════════════════════════════════════════════════════════════════════════
# ── 分析器 ────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class JDAnalyzer:
    """
    使用 DeepSeek API 对职位 JD 进行结构化分析。

    参数：
        api_key   - DeepSeek API Key；不传则读环境变量 DEEPSEEK_API_KEY
        model     - 模型名，默认 deepseek-chat
        rpm_limit - 每分钟最大请求数，用于限流，默认 30
    """

    _API_URL = "https://api.deepseek.com/chat/completions"

    def __init__(self, api_key: str = None, model: str = "deepseek-chat", rpm_limit: int = 30):
        self.api_key   = api_key or os.environ.get("DEEPSEEK_API_KEY", "")
        self.model     = model
        self._interval = 60.0 / rpm_limit
        if not self.api_key:
            raise ValueError(
                "需要提供 DeepSeek API Key（参数 api_key 或环境变量 DEEPSEEK_API_KEY）"
            )

    def analyze(self, description: str) -> dict:
        """对单条 JD 文本调用 DeepSeek，返回结构化 dict。"""
        if not description or not description.strip():
            return {"job_function": None, "seniority": "unknown", "skills": [], "summary": ""}

        prompt = _PROMPT.format(description=description[:3000])
        resp = _requests.post(
            self._API_URL,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model":           self.model,
                "messages":        [{"role": "user", "content": prompt}],
                "response_format": {"type": "json_object"},
                "temperature":     0.1,
                "max_tokens":      512,
            },
            timeout=30,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        return json.loads(content)

    def run_batch(self, limit: int = None, vendor: str = None) -> int:
        """
        批量分析数据库中尚未分析的岗位。

        参数：
            limit  - 本次最多处理条数，None 表示全部
            vendor - 只处理指定公司，None 表示全部

        返回：成功分析条数
        """
        with get_db() as db:
            analyzed_ids = {r.job_id for r in db.query(JDAnalysis.job_id).all()}
            query = (
                db.query(CompetitorRecruitment)
                .filter(
                    CompetitorRecruitment.description.isnot(None),
                    CompetitorRecruitment.description != '',
                    ~CompetitorRecruitment.id.in_(analyzed_ids),
                )
            )
            if vendor:
                query = query.filter(CompetitorRecruitment.vendor == vendor)
            if limit:
                query = query.limit(limit)
            jobs = query.all()

        total = len(jobs)
        print(f"[Analyzer] 待分析岗位：{total} 条")
        success = 0

        for i, job in enumerate(jobs, 1):
            try:
                result = self.analyze(job.description)
                with get_db() as db:
                    db.merge(JDAnalysis(
                        id=uuid.uuid4(),
                        job_id=job.id,
                        job_function=result.get("job_function"),
                        seniority=result.get("seniority"),
                        skills=result.get("skills", []),
                        summary=result.get("summary", ""),
                        analyzed_at=datetime.now(timezone.utc),
                    ))
                success += 1
                print(
                    f"  [{i}/{total}] {job.vendor} · {job.job_title}"
                    f" → {result.get('job_function')} / {result.get('seniority')}"
                )
            except Exception as e:
                print(f"  [{i}/{total}] 失败 ({job.job_title}): {e}")

            time.sleep(self._interval)

        print(f"[Analyzer] 完成，成功 {success}/{total} 条")
        return success


# ══════════════════════════════════════════════════════════════════════════════
# ── CLI 入口 ──────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="JD 智能分析（DeepSeek）")
    parser.add_argument("--api-key", default=None,
                        help="DeepSeek API Key（也可用环境变量 DEEPSEEK_API_KEY）")
    parser.add_argument("--model",   default="deepseek-chat", help="模型名，默认 deepseek-chat")
    parser.add_argument("--limit",   type=int, default=None,  help="本次最多处理条数")
    parser.add_argument("--vendor",  default=None,            help="只处理指定公司")
    args = parser.parse_args()

    if _USE_TUNNEL:
        start_db_tunnel()

    import jd_analyzer  # noqa: 确保 JDAnalysis 注册到 Base.metadata
    _init_db(DATABASE_URL)

    analyzer = JDAnalyzer(api_key=args.api_key, model=args.model)
    analyzer.run_batch(limit=args.limit, vendor=args.vendor)

    if _USE_TUNNEL:
        stop_db_tunnel()
