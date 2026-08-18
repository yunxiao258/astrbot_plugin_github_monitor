"""AstrBot GitHub 仓库监控插件：监控仓库更新（提交/发布/标签），自动推送到群聊与私聊"""

import asyncio
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import aiohttp

from astrbot.api import AstrBotConfig, logger
from astrbot.api.all import MessageChain, MessageEventResult
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star, register

# 插件元数据
PLUGIN_NAME = "astrbot_plugin_github_monitor"
PLUGIN_AUTHOR = "Administrator"
PLUGIN_DESC = "监控 GitHub 仓库更新，自动推送到群聊与私聊"
PLUGIN_VERSION = "1.3.0"

# GitHub API 基础地址与请求头
GITHUB_API = "https://api.github.com"
GITHUB_USER_AGENT = "astrbot-plugin-github-monitor"
TIMEZONE_BJ = timezone(timedelta(hours=8))  # 北京时间 UTC+8
DEFAULT_TIMEOUT = 15  # HTTP 请求超时（秒）
# AI 摘要并发限流：防止多仓库同时调用 LLM 打爆 provider
_SUMMARY_SEM = asyncio.Semaphore(2)
# API 拉取条数：固定较大值避免两轮检查间隔内更新超过 per_page 导致基线外更新丢失
# （max_events_per_check 仅用于控制通知条数）
_FETCH_PER_PAGE = 30
# 仓库信息（星标数）缓存 TTL（秒）
_REPO_INFO_TTL = 600
# Issue/PR 拉取条数：issue/pr 动态远多于提交，10 条足够且省 API 配额
_ISSUE_PER_PAGE = 10
# Star 趋势历史保留天数（自动截断更早的数据）
_TREND_KEEP_DAYS = 30
# 每日更新统计保留天数
_DAILY_KEEP_DAYS = 30
# Star 趋势块字符图：由低到高 8 级
_TREND_CHART_BARS = "▁▂▃▄▅▆▇█"


@register(PLUGIN_NAME, PLUGIN_AUTHOR, PLUGIN_DESC, PLUGIN_VERSION)
class GitHubMonitorPlugin(Star):
    """GitHub 仓库监控插件"""

    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config or {}

        # 数据目录
        self.data_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "plugin_data",
            PLUGIN_NAME,
        )
        os.makedirs(self.data_dir, exist_ok=True)

        # 状态文件（持久化：token、监控仓库、订阅会话、上次已知更新位置）
        self._state_file = os.path.join(self.data_dir, "state.json")
        self._state = self._load_state()

        # Star 趋势历史文件（trends.json：各仓库按日期记录的 Star 数 + 提醒状态）
        self._trends_file = os.path.join(self.data_dir, "trends.json")
        self._trends = self._load_trends()

        # 每日更新统计文件（daily_stats.json：各仓库按日期记录的各类更新计数）
        self._daily_stats_file = os.path.join(self.data_dir, "daily_stats.json")
        self._daily_stats = self._load_daily_stats()
        # 当日日报是否已发送（持久化到 daily_stats.json 的 _last_sent 元字段）

        # 将配置中的默认仓库合并进监控列表（已存在的则不覆盖）
        self._merge_default_repos()

        # 后台轮询任务
        self._monitor_task: asyncio.Task | None = None
        self._session: aiohttp.ClientSession | None = None
        self._running = False
        # 检查互斥锁：防止定时轮询与手动 /gh check 并发检查导致重复推送
        self._check_lock = asyncio.Lock()
        # 仓库信息缓存（星标数）：{repo: (stargazers_count, 获取时间戳)}
        self._repo_info_cache: dict[str, tuple[int, float]] = {}

        logger.info(f"【{PLUGIN_NAME}】插件初始化完成")

    def _merge_default_repos(self):
        """将配置中的 default_repos 合并进监控列表"""
        cfg = str(self.config.get("default_repos", "") or "").strip()
        if not cfg:
            return
        repos = self._state.setdefault("repos", {})
        changed = False
        for item in cfg.split(","):
            item = item.strip()
            if not item:
                continue
            repo = self._parse_repo(item)
            if repo and repo not in repos:
                repos[repo] = {"last_commit": "", "last_release": "", "last_tag": ""}
                changed = True
        if changed:
            self._save_state()

    # ========== 状态持久化 ==========

    def _load_state(self) -> dict:
        """从磁盘加载状态"""
        default_state = {
            "token": "",           # GitHub Token（/gh settoken 写入，覆盖配置项）
            "repos": {},           # {owner/repo: {"last_commit": sha, "last_release": tag_name, "last_tag": tag_name}}
            "subscribers": [],     # 订阅推送的会话 unified_msg_origin 列表
            "initialized": set(),  # 已初始化基线（首次添加只设基线不通知）的仓库
        }
        try:
            if os.path.exists(self._state_file):
                with open(self._state_file, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    default_state.update(loaded)
        except Exception as e:
            logger.warning(f"加载状态文件失败: {e}")
        # 兼容旧数据：initialized 存成 list 时转 set
        if isinstance(default_state.get("initialized"), list):
            default_state["initialized"] = set(default_state["initialized"])
        # 类型防御：确保各字段类型正确，避免异常数据导致崩溃
        if not isinstance(default_state.get("initialized"), set):
            default_state["initialized"] = set()
        if not isinstance(default_state.get("subscribers"), list):
            default_state["subscribers"] = []
        if not isinstance(default_state.get("repos"), dict):
            default_state["repos"] = {}
        # 仓库值为 dict 的类型防御：非法条目直接丢弃，避免检查阶段 AttributeError
        default_state["repos"] = {
            k: v for k, v in default_state["repos"].items()
            if isinstance(v, dict)
        }
        if not isinstance(default_state.get("token"), str):
            default_state["token"] = ""
        return default_state

    def _save_state(self):
        """保存状态到磁盘"""
        try:
            data = dict(self._state)
            data["initialized"] = sorted(self._state.get("initialized", set()))
            os.makedirs(os.path.dirname(self._state_file), exist_ok=True)
            tmp = self._state_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self._state_file)
        except Exception as e:
            logger.warning(f"保存状态文件失败: {e}")

    # ---------- Star 趋势历史（trends.json） ----------
    # 结构：{repo: {日期(YYYY-MM-DD): star 数}}，另以 "_alerts" 键记录
    # 各仓库当日是否已推送过 24h 增长提醒（避免同一天重复打扰）

    def _load_trends(self) -> dict:
        """加载 Star 历史；提醒状态存入 self._trend_alerts，返回 {repo: {日期: star}}"""
        self._trend_alerts: dict[str, str] = {}
        data: dict = {}
        try:
            if os.path.exists(self._trends_file):
                with open(self._trends_file, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    data = loaded
        except Exception as e:
            logger.warning(f"加载 Star 历史失败: {e}")
        # 剥离提醒状态（键以 _ 开头且不含 /，不会与仓库名冲突）
        alerts = data.pop("_alerts", {})
        if isinstance(alerts, dict):
            self._trend_alerts = {
                str(k): str(v)
                for k, v in alerts.items()
                if isinstance(k, str) and isinstance(v, str)
            }
        # 类型防御：非法条目（键非日期、值非 int 或负数）直接丢弃
        cleaned: dict = {}
        for repo, hist in data.items():
            if not isinstance(hist, dict):
                continue
            day_map = {}
            for day, val in hist.items():
                if not isinstance(day, str) or not isinstance(val, int) or val < 0:
                    continue
                try:
                    datetime.strptime(day, "%Y-%m-%d")
                except ValueError:
                    continue  # 非日期键直接丢弃
                day_map[day] = val
            if day_map:
                cleaned[repo] = day_map
        self._trim_trends(cleaned)  # 加载即截断，防止文件长期膨胀
        return cleaned

    def _save_trends(self):
        """保存 Star 历史到磁盘（原子写，写前自动 30 天截断）"""
        try:
            self._trim_trends(self._trends)
            data = dict(self._trends)
            data["_alerts"] = self._trend_alerts
            os.makedirs(os.path.dirname(self._trends_file), exist_ok=True)
            tmp = self._trends_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self._trends_file)
        except Exception as e:
            logger.warning(f"保存 Star 历史失败: {e}")

    @staticmethod
    def _trim_trends(trends: dict, keep_days: int = _TREND_KEEP_DAYS) -> None:
        """就地截断 Star 历史：仅保留最近 keep_days 天（YYYY-MM-DD 字符串按字典序比较）"""
        cutoff = (
            datetime.now(TIMEZONE_BJ) - timedelta(days=keep_days - 1)
        ).strftime("%Y-%m-%d")
        for repo in list(trends.keys()):
            kept = {d: v for d, v in trends[repo].items() if d >= cutoff}
            if kept:
                trends[repo] = kept
            else:
                trends.pop(repo, None)

    # ---------- 每日更新统计（daily_stats.json） ----------
    # 结构：{repo: {日期: {"commits": n, "releases": n, "tags": n, "issues": n, "prs": n}}}
    # 另以 "_last_sent" 键记录最近一次日报发送日期（防止定时重复发送）

    def _load_daily_stats(self) -> dict:
        """加载每日更新统计；最近日报发送日期存入 self._daily_last_sent"""
        self._daily_last_sent = ""
        data: dict = {}
        try:
            if os.path.exists(self._daily_stats_file):
                with open(self._daily_stats_file, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    data = loaded
        except Exception as e:
            logger.warning(f"加载每日统计失败: {e}")
        self._daily_last_sent = str(data.pop("_last_sent", "") or "")
        # 类型防御：非法条目直接丢弃
        cleaned: dict = {}
        for repo, days in data.items():
            if not isinstance(days, dict):
                continue
            day_map = {}
            for day, vals in days.items():
                if not isinstance(day, str) or not isinstance(vals, dict):
                    continue
                day_map[day] = {
                    k: int(v)
                    for k, v in vals.items()
                    if k in ("commits", "releases", "tags", "issues", "prs")
                    and isinstance(v, int)
                }
            if day_map:
                cleaned[repo] = day_map
        return cleaned

    def _save_daily_stats(self):
        """保存每日更新统计（原子写，写前自动 30 天截断）"""
        try:
            cutoff = (
                datetime.now(TIMEZONE_BJ) - timedelta(days=_DAILY_KEEP_DAYS - 1)
            ).strftime("%Y-%m-%d")
            for repo in list(self._daily_stats.keys()):
                self._daily_stats[repo] = {
                    d: v
                    for d, v in self._daily_stats[repo].items()
                    if d >= cutoff
                }
                if not self._daily_stats[repo]:
                    self._daily_stats.pop(repo, None)
            data = dict(self._daily_stats)
            data["_last_sent"] = self._daily_last_sent
            os.makedirs(os.path.dirname(self._daily_stats_file), exist_ok=True)
            tmp = self._daily_stats_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self._daily_stats_file)
        except Exception as e:
            logger.warning(f"保存每日统计失败: {e}")

    # ========== 工具方法 ==========

    @staticmethod
    def _parse_repo(text: str) -> str | None:
        """解析 owner/repo，兼容 url 与带 .git 后缀的写法"""
        if not text:
            return None
        t = text.strip().rstrip("/")
        t = re.sub(r"^https?://[^/]+/", "", t)  # 去掉仓库地址前缀
        t = t.removesuffix(".git")
        parts = t.split("/")
        if len(parts) == 2 and parts[0] and parts[1]:
            # 只允许仓库名常用字符，防止注入
            if re.fullmatch(r"[\w.\-]+", parts[0]) and re.fullmatch(r"[\w.\-]+", parts[1]):
                return f"{parts[0]}/{parts[1]}"
        return None

    @staticmethod
    def _format_time(iso_str: str | None) -> str:
        """将 GitHub API 的 ISO8601 UTC 时间转换为北京时间字符串"""
        if not iso_str:
            return "未知时间"
        try:
            dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
            return dt.astimezone(TIMEZONE_BJ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(iso_str)

    def _get_token(self) -> str:
        """获取 Token：优先 /gh settoken 写入的状态（DPAPI 加密存储），其次配置项"""
        stored = self._state.get("token") or ""
        if stored.startswith("dpapi:"):
            try:
                from .secret import dpapi_decrypt

                stored = dpapi_decrypt(stored[6:])
            except Exception:  # noqa: BLE001
                stored = ""
        return stored.strip() or str(
            self.config.get("github_token", "") or ""
        ).strip()

    def _subscribers(self) -> list[str]:
        """获取订阅会话列表（去重）"""
        subs = list(dict.fromkeys(self._state.get("subscribers", [])))
        # 合并配置中的默认订阅
        cfg_subs = str(self.config.get("default_subscribers", "") or "").strip()
        if cfg_subs:
            for s in cfg_subs.split(","):
                s = s.strip()
                if s and s not in subs:
                    subs.append(s)
        return subs

    # ========== GitHub API 封装 ==========

    @staticmethod
    def _safe_int(key_value, default: int) -> int:
        """安全整数转换：非法值回退默认，避免 WebUI 脏值导致监控任务崩溃"""
        try:
            return int(key_value)
        except (TypeError, ValueError):
            return int(default)

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取（或创建）复用的 aiohttp 会话"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT),
                headers={
                    "User-Agent": GITHUB_USER_AGENT,
                    "Accept": "application/vnd.github+json",
                },
            )
        return self._session

    async def _gh_request(
        self,
        path: str,
        params: dict | None = None,
    ) -> tuple[int, dict | list | None]:
        """发起 GitHub API 请求，返回 (状态码, JSON 数据)；网络错误返回 (0, None)"""
        token = self._get_token()
        session = await self._get_session()
        url = f"{GITHUB_API}{path}"
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        try:
            async with session.get(url, params=params, headers=headers) as resp:
                status = resp.status
                if status == 204:
                    return status, None
                try:
                    data = await resp.json()
                except Exception:
                    data = None
                return status, data
        except asyncio.TimeoutError:
            logger.warning(f"GitHub API 请求超时: {path}")
            return 0, None
        except aiohttp.ClientError as e:
            logger.warning(f"GitHub API 请求失败: {path} - {e}")
            return 0, None

    async def _get_commits(self, repo: str, per_page: int = 10) -> list[dict]:
        """获取仓库最近提交"""
        status, data = await self._gh_request(
            f"/repos/{repo}/commits", params={"per_page": per_page}
        )
        if status == 200 and isinstance(data, list):
            return data
        return []

    async def _get_releases(self, repo: str, per_page: int = 10) -> list[dict]:
        """获取仓库最近 release"""
        status, data = await self._gh_request(
            f"/repos/{repo}/releases", params={"per_page": per_page}
        )
        if status == 200 and isinstance(data, list):
            return data
        return []

    async def _get_tags(self, repo: str, per_page: int = 10) -> list[dict]:
        """获取仓库最近 tag"""
        status, data = await self._gh_request(
            f"/repos/{repo}/tags", params={"per_page": per_page}
        )
        if status == 200 and isinstance(data, list):
            return data
        return []

    async def _get_issues(self, repo: str, per_page: int = 10) -> list[dict]:
        """获取仓库最近 issue（按创建时间倒序，不含 PR）"""
        status, data = await self._gh_request(
            f"/repos/{repo}/issues",
            params={"state": "all", "per_page": per_page},
        )
        if status == 200 and isinstance(data, list):
            # issues 端点会混入 PR，pull_request 字段存在即为 PR
            return [i for i in data if isinstance(i, dict) and not i.get("pull_request")]
        return []

    async def _get_pulls(self, repo: str, per_page: int = 10) -> list[dict]:
        """获取仓库最近 PR（按创建时间倒序）"""
        status, data = await self._gh_request(
            f"/repos/{repo}/pulls",
            params={"state": "all", "per_page": per_page},
        )
        if status == 200 and isinstance(data, list):
            return data
        return []

    async def _get_commit_stats(self, repo: str, sha: str) -> tuple[int, int]:
        """获取单个提交的变更统计，返回 (新增行数, 删除行数)"""
        status, data = await self._gh_request(f"/repos/{repo}/commits/{sha}")
        if status == 200 and isinstance(data, dict):
            files = data.get("files") or []
            additions = sum(int(f.get("additions", 0)) for f in files)
            deletions = sum(int(f.get("deletions", 0)) for f in files)
            return additions, deletions
        return 0, 0

    async def _get_user_repos(self) -> tuple[int, list[dict]]:
        """获取 Token 账号名下的仓库；未配置 Token 时尝试查询当前账号主页指定用户名（yunxiao258）的公开仓库"""
        token = self._get_token()
        if token:
            status, data = await self._gh_request(
                "/user/repos", params={"per_page": 100, "sort": "updated"}
            )
        else:
            # 未配置 Token：按账号名查询公开仓库（配置文件默认账号或指定用户名）
            owner = str(self.config.get("default_owner", "") or "").strip() or "yunxiao258"
            status, data = await self._gh_request(
                f"/users/{owner}/repos", params={"per_page": 100, "sort": "updated"}
            )
        return status, data if isinstance(data, list) else []

    async def _get_repo_stars(self, repo: str) -> int:
        """获取仓库星标数（带 TTL 缓存，失败返回 0 且不缓存，避免限流盲区）"""
        now = time.monotonic()
        cached = self._repo_info_cache.get(repo)
        if cached and now - cached[1] < _REPO_INFO_TTL:
            return cached[0]
        status, data = await self._gh_request(f"/repos/{repo}")
        if status == 200 and isinstance(data, dict):
            try:
                stars = int(data.get("stargazers_count") or 0)
            except (TypeError, ValueError):
                stars = 0
            self._repo_info_cache[repo] = (stars, now)
            return stars
        return 0

    def _keywords(self) -> list[str]:
        """解析关键词过滤配置（逗号/顿号分隔，去空）"""
        raw = str(self.config.get("keyword_filters", "") or "").strip()
        return [k.strip().lower() for k in re.split(r"[,，、]", raw) if k.strip()]

    @staticmethod
    def _matches_keywords(text: str, keywords: list[str]) -> bool:
        """判断文本是否命中任一关键词（大小写不敏感）"""
        if not keywords:
            return True
        t = (text or "").lower()
        return any(k in t for k in keywords)

    def _filter_updates(self, updates: list[dict]) -> list[dict]:
        """按关键词过滤更新条目（返回过滤后的列表）"""
        keywords = self._keywords()
        if not keywords:
            return updates
        filtered = []
        for group in updates:
            kind = group["type"]
            kept = []
            for item in group["items"]:
                if kind == "commit":
                    text = (item.get("commit") or {}).get("message", "")
                elif kind == "release":
                    r = item
                    text = f"{r.get('name') or ''} {r.get('tag_name') or ''} {r.get('body') or ''}"
                elif kind == "issue":
                    i = item
                    text = f"{i.get('title') or ''} {i.get('body') or ''}"
                elif kind == "pr":
                    p = item
                    text = f"{p.get('title') or ''} {p.get('body') or ''}"
                else:  # tag
                    text = item.get("name", "")
                if self._matches_keywords(text, keywords):
                    kept.append(item)
            if kept:
                filtered.append({"type": kind, "items": kept})
        return filtered

    def _tag_list(self, raw: str) -> list[str]:
        """解析标签过滤配置（逗号/顿号分隔，去空，小写）"""
        return [t.strip().lower() for t in re.split(r"[,，、]", raw or "") if t.strip()]

    @staticmethod
    def _matches_tags(item: dict, tags: list[str]) -> bool:
        """判断 issue/pr 是否命中任一标签（大小写不敏感）；tags 为空则全部通过"""
        tags = [str(t).lower() for t in tags if t]  # 统一小写，调用方大小写随意
        if not tags:
            return True
        labels = item.get("labels") or []
        names = [
            (l.get("name") or "") if isinstance(l, dict) else ""
            for l in labels
        ]
        text = " ".join(names).lower()
        return any(tag in text for tag in tags)

    async def _summarize(self, text: str, context_hint: str) -> str | None:
        """用 LLM 生成更新摘要，失败或无 provider 时返回 None"""
        if not self.config.get("enable_ai_summary", False):
            return None
        try:
            provider = self.context.get_using_provider(None)
            if provider is None:
                return None
            async with _SUMMARY_SEM:
                # 加超时：LLM 卡住时避免长期占用信号量阻塞其他仓库
                resp = await asyncio.wait_for(
                    provider.text_chat(
                        prompt=(
                            f"请用简体中文为下面的 GitHub {context_hint} 写一句不超过 80 字的摘要"
                            f"（只输出摘要本身，不要任何前缀）。内容如下：\n{text[:2000]}"
                        ),
                        system_prompt="你是一个 Git 更新摘要助手，输出简洁客观。",
                    ),
                    timeout=30,
                )
                summary = (getattr(resp, "completion_text", "") or "").strip()
            return summary[:200] or None
        except asyncio.TimeoutError:
            logger.warning("AI 摘要生成超时")
            return None
        except Exception as e:
            logger.warning(f"AI 摘要生成失败: {e}")
            return None

    async def _attach_summaries(self, kind: str, items: list[dict]):
        """为更新条目生成摘要并挂到 _summary 字段（逐个失败降级）"""
        if not self.config.get("enable_ai_summary", False):
            return
        for item in items:
            if kind == "commit":
                commit = item.get("commit", {})
                text = (commit.get("message") or "").strip()
                hint = "提交"
            elif kind == "release":
                text = f"{item.get('name') or ''}\n{item.get('body') or ''}".strip()
                hint = "版本发布"
            elif kind == "issue":
                text = f"{item.get('title') or ''}\n{item.get('body') or ''}".strip()
                hint = "Issue"
            elif kind == "pr":
                text = f"{item.get('title') or ''}\n{item.get('body') or ''}".strip()
                hint = "Pull Request"
            else:
                continue
            if text:
                try:
                    summary = await self._summarize(text, hint)
                except Exception:  # noqa: BLE001
                    summary = None
                if summary:
                    item["_summary"] = summary
                else:
                    item.pop("_summary", None)

    # ========== 更新检测与去重 ==========

    async def _check_repo_updates(self, repo: str) -> list[dict]:
        """检查单个仓库的更新，返回需要通知的更新列表；首次添加只设基线不通知"""
        async with self._check_lock:  # 互斥：防止定时与手动并发重复推送
            return await self._check_repo_updates_locked(repo, save=True)

    async def _check_repo_updates_locked(self, repo: str, save: bool = True) -> list[dict]:
        """持锁状态下的检查实现（勿直接调用）"""
        # 仓库状态不存在则创建（/_gh add 之外的首次检查）；删除操作与检查互斥由 _check_lock 保证
        repo_state = self._state.setdefault("repos", {}).setdefault(
            repo,
            {
                "last_commit": "",
                "last_release": "",
                "last_tag": "",
                "last_issue": 0,
                "last_pr": 0,
            },
        )
        initialized = self._state.setdefault("initialized", set())
        max_events = max(1, self._safe_int(self.config.get("max_events_per_check", 5), 5))
        # 星标数下限过滤：低于阈值直接跳过（基线不推进，后续涨星后仍会正常推送）
        min_stars = self._safe_int(self.config.get("min_stars", 0), 0)
        if min_stars > 0 and await self._get_repo_stars(repo) < min_stars:
            return []
        updates: list[dict] = []
        # 本次检查各类型新增计数（记录真实新增量，供每日趋势日报使用）
        stats_delta = {"commits": 0, "releases": 0, "tags": 0, "issues": 0, "prs": 0}

        # 1. 新提交
        commits = await self._get_commits(repo, per_page=_FETCH_PER_PAGE)
        if commits:
            latest_sha = commits[0].get("sha", "")
            new_commits = []
            if repo_state.get("last_commit"):
                for c in commits:
                    sha = c.get("sha", "")
                    if sha == repo_state["last_commit"]:
                        break
                    new_commits.append(c)
            if repo_state.get("last_commit"):
                repo_state["last_commit"] = latest_sha
                # 翻转：API 返回最新在前
                new_commits.reverse()
                stats_delta["commits"] = len(new_commits)  # 真实新增数（截断前）
                new_commits = new_commits[:max_events]  # 通知条数上限
                if new_commits:
                    # 预取变更统计（异步），供消息构建使用
                    if self.config.get("show_commit_stats", True):
                        for c in new_commits:
                            sha = c.get("sha", "")
                            if sha:
                                try:
                                    add, dele = await self._get_commit_stats(repo, sha)
                                    c["_stats"] = (add, dele)
                                except Exception:
                                    c["_stats"] = None
                    await self._attach_summaries("commit", new_commits)
                    updates.append({"type": "commit", "items": new_commits})
            else:
                repo_state["last_commit"] = latest_sha

        # 2. 新 release
        if self.config.get("notify_release", True):
            releases = await self._get_releases(repo, per_page=_FETCH_PER_PAGE)
            if releases:
                last_release = repo_state.get("last_release", "")
                if last_release:
                    new_releases = []
                    for r in releases:
                        tag = r.get("tag_name", "")
                        if tag == last_release:
                            break
                        new_releases.append(r)
                    new_releases.reverse()
                    stats_delta["releases"] = len(new_releases)  # 真实新增数（截断前）
                    new_releases = new_releases[:max_events]  # 通知条数上限
                    if new_releases:
                        repo_state["last_release"] = releases[0].get("tag_name", "")
                        await self._attach_summaries("release", new_releases)
                        updates.append({"type": "release", "items": new_releases})
                else:
                    repo_state["last_release"] = releases[0].get("tag_name", "")

        # 3. 新 tag
        if self.config.get("notify_tag", True):
            tags = await self._get_tags(repo, per_page=_FETCH_PER_PAGE)
            if tags:
                last_tag = repo_state.get("last_tag", "")
                if last_tag:
                    new_tags = []
                    for t in tags:
                        name = t.get("name", "")
                        if name == last_tag:
                            break
                        new_tags.append(t)
                    new_tags.reverse()
                    stats_delta["tags"] = len(new_tags)  # 真实新增数（截断前）
                    new_tags = new_tags[:max_events]  # 通知条数上限
                    if new_tags:
                        repo_state["last_tag"] = tags[0].get("name", "")
                        updates.append({"type": "tag", "items": new_tags})
                else:
                    repo_state["last_tag"] = tags[0].get("name", "")

        # 4. 新 issue
        if self.config.get("notify_issue", True):
            issues = await self._get_issues(repo, per_page=_ISSUE_PER_PAGE)
            if issues:
                last_issue = self._safe_int(repo_state.get("last_issue", 0), 0)
                if last_issue:
                    new_issues = []
                    for i in issues:
                        num = self._safe_int(i.get("number", 0), 0)
                        if num <= last_issue:
                            break
                        new_issues.append(i)
                    if new_issues:
                        new_issues.reverse()
                        repo_state["last_issue"] = max(
                            self._safe_int(issues[0].get("number", 0), 0), last_issue
                        )
                        stats_delta["issues"] = len(new_issues)
                        # 标签过滤：issue_tags 为空则全部通过（纯内存操作，不额外请求 API）
                        issue_tags = self._tag_list(self.config.get("issue_tags", ""))
                        kept = [i for i in new_issues if self._matches_tags(i, issue_tags)]
                        if kept:
                            await self._attach_summaries("issue", kept)
                            updates.append({"type": "issue", "items": kept})
                else:
                    repo_state["last_issue"] = self._safe_int(
                        issues[0].get("number", 0), 0
                    )

        # 5. 新 PR
        if self.config.get("notify_pr", True):
            pulls = await self._get_pulls(repo, per_page=_ISSUE_PER_PAGE)
            if pulls:
                last_pr = self._safe_int(repo_state.get("last_pr", 0), 0)
                if last_pr:
                    new_prs = []
                    for p in pulls:
                        num = self._safe_int(p.get("number", 0), 0)
                        if num <= last_pr:
                            break
                        new_prs.append(p)
                    if new_prs:
                        new_prs.reverse()
                        repo_state["last_pr"] = max(
                            self._safe_int(pulls[0].get("number", 0), 0), last_pr
                        )
                        stats_delta["prs"] = len(new_prs)
                        # 标签过滤：pull_tags 为空则全部通过（纯内存操作，不额外请求 API）
                        pull_tags = self._tag_list(self.config.get("pull_tags", ""))
                        kept = [p for p in new_prs if self._matches_tags(p, pull_tags)]
                        if kept:
                            await self._attach_summaries("pr", kept)
                            updates.append({"type": "pr", "items": kept})
                else:
                    repo_state["last_pr"] = self._safe_int(pulls[0].get("number", 0), 0)

        # 累计每日更新统计（含被关键词/标签过滤掉的条目，反映真实新增量）
        if any(stats_delta.values()):
            self._bump_daily_stats(repo, stats_delta)

        # 关键词过滤 + 标记已初始化基线
        updates = self._filter_updates(updates)
        if repo not in initialized:
            initialized.add(repo)

        if save:
            self._save_state()
            self._save_daily_stats()
        return updates

    # ========== 消息构建与推送 ==========

    def _build_update_message(self, repo: str, updates: list[dict]) -> str:
        """构建富信息更新消息"""
        lines = [f"📦 仓库更新提醒 [{repo}]"]
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")

        for group in updates:
            kind = group["type"]
            items = group["items"]
            if kind == "commit":
                for idx, c in enumerate(items, 1):
                    commit = c.get("commit", {})
                    sha_short = (c.get("sha") or "")[:7]
                    message = (commit.get("message") or "").split("\n")[0].strip() or "(无提交说明)"
                    author = (commit.get("author") or {}).get("name", "未知")
                    author_email = (commit.get("author") or {}).get("email", "")
                    date_str = self._format_time((commit.get("author") or {}).get("date"))
                    lines.append(f"🆕 新提交 #{idx}")
                    lines.append(f"🔖 SHA: {sha_short}")
                    lines.append(f"💬 信息: {message}")
                    # AI 摘要（检查阶段已生成到 _summary）
                    if c.get("_summary"):
                        lines.append(f"🤖 摘要: {c['_summary']}")
                    lines.append(f"👤 作者: {author}" + (f" <{author_email}>" if author_email else ""))
                    lines.append(f"🕐 时间: {date_str}")
                    # 变更统计在检查阶段已预取到 _stats 字段
                    stats = c.get("_stats")
                    if stats:
                        lines.append(f"📈 变更: +{stats[0]} / -{stats[1]} 行")
                    lines.append(f"🔗 {c.get('html_url') or f'https://github.com/{repo}/commit/{sha_short}'}")
                    if idx < len(items):
                        lines.append("")
            elif kind == "release":
                for idx, r in enumerate(items, 1):
                    tag = r.get("tag_name", "")
                    name = r.get("name") or tag
                    published = self._format_time(r.get("published_at"))
                    author = (r.get("author") or {}).get("login", "未知")
                    body = (r.get("body") or "").strip()
                    lines.append(f"🏷️ 新版本发布 #{idx}")
                    lines.append(f"📌 版本: {tag}")
                    lines.append(f"📝 名称: {name}")
                    lines.append(f"🕐 发布时间: {published}")
                    lines.append(f"👤 发布者: {author}")
                    if r.get("_summary"):
                        lines.append(f"🤖 摘要: {r['_summary']}")
                    elif body:
                        body_short = body[:200] + ("…" if len(body) > 200 else "")
                        lines.append(f"📄 说明: {body_short}")
                    lines.append(f"🔗 {r.get('html_url') or f'https://github.com/{repo}/releases/tag/{tag}'}")
                    if idx < len(items):
                        lines.append("")
            elif kind == "tag":
                for idx, t in enumerate(items, 1):
                    name = t.get("name", "")
                    sha_short = ((t.get("commit") or {}).get("sha") or "")[:7]
                    lines.append(f"🔖 新标签 #{idx}")
                    lines.append(f"🏷️ 标签: {name}")
                    lines.append(f"🎯 指向提交: {sha_short or '未知'}")
                    lines.append(f"🔗 https://github.com/{repo}/tree/{name}")
                    if idx < len(items):
                        lines.append("")
            elif kind == "issue":
                for idx, i in enumerate(items, 1):
                    num = i.get("number", "")
                    title = (i.get("title") or "").strip() or "(无标题)"
                    state = "✅ 已关闭" if i.get("state") == "closed" else "🟢 进行中"
                    author = (i.get("user") or {}).get("login", "未知")
                    created = self._format_time(i.get("created_at"))
                    lines.append(f"🐛 新 Issue #{num} {state}")
                    lines.append(f"📝 标题: {title}")
                    if i.get("_summary"):
                        lines.append(f"🤖 摘要: {i['_summary']}")
                    lines.append(f"👤 提出者: {author}")
                    lines.append(f"🕐 时间: {created}")
                    lines.append(
                        f"🔗 {i.get('html_url') or f'https://github.com/{repo}/issues/{num}'}"
                    )
                    if idx < len(items):
                        lines.append("")
            elif kind == "pr":
                for idx, p in enumerate(items, 1):
                    num = p.get("number", "")
                    title = (p.get("title") or "").strip() or "(无标题)"
                    state = "✅ 已合并" if p.get("merged") else (
                        "🔀 待合并" if p.get("state") == "open" else "❌ 已关闭"
                    )
                    author = (p.get("user") or {}).get("login", "未知")
                    created = self._format_time(p.get("created_at"))
                    lines.append(f"🛠️ 新 PR #{num} {state}")
                    lines.append(f"📝 标题: {title}")
                    if p.get("_summary"):
                        lines.append(f"🤖 摘要: {p['_summary']}")
                    lines.append(f"👤 作者: {author}")
                    lines.append(f"🕐 时间: {created}")
                    lines.append(
                        f"🔗 {p.get('html_url') or f'https://github.com/{repo}/pull/{num}'}"
                    )
                    if idx < len(items):
                        lines.append("")

        # 长度保护：超出限制时折叠，避免消息过长被平台拒绝
        MAX_LINES = 60
        if len(lines) > MAX_LINES:
            lines = lines[:MAX_LINES]
            lines.append("…")
            lines.append("💡 更新较多，已折叠显示。可到 GitHub 查看完整记录。")

        return "\n".join(lines)

    async def _send_to_subscribers(self, message: str):
        """向所有订阅会话推送消息"""
        if not message:
            return
        chain = MessageChain([Plain(message)])
        for session in self._subscribers():
            try:
                await self.context.send_message(session, chain)
            except Exception as e:
                logger.warning(f"向会话 {session} 推送失败: {e}")

    async def _broadcast_updates(self):
        """并发检查所有仓库并将更新推送到订阅会话"""
        repos = list(self._state.get("repos", {}).keys())
        if not repos:
            return
        # 持锁并发检查：与手动检查互斥，同时避免仓库多时串行拉取过慢
        async with self._check_lock:
            results = await asyncio.gather(
                *(self._check_repo_updates_locked(r, save=False) for r in repos),
                return_exceptions=True,
            )
            self._save_state()
            self._save_daily_stats()
        for repo, result in zip(repos, results):
            try:
                if isinstance(result, Exception):
                    raise result
                if result:
                    msg = self._build_update_message(repo, result)
                    await self._send_to_subscribers(msg)
                    await asyncio.sleep(0.5)  # 避免连续推送过快
            except Exception as e:
                logger.error(f"检查仓库 {repo} 更新失败: {e}")

    # ========== 后台轮询 ==========

    async def initialize(self) -> None:
        """插件加载/重载时启动后台监控任务（幂等）"""
        await self._start_monitor()

    @filter.on_astrbot_loaded()
    async def _on_astrbot_loaded(self) -> None:
        """AstrBot 启动时启动后台监控任务（初始化兜底，幂等）"""
        await self._start_monitor()

    async def _start_monitor(self) -> None:
        """启动后台监控任务；已在运行时跳过"""
        if not self.config.get("enable_auto_check", True):
            logger.info("【github_monitor】自动检查已禁用，仅手动检查")
            return
        if self._running:
            return
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())

        def _on_done(t: asyncio.Task):
            # 任务退出（含异常/被取消）时复位标志，允许后续重新启动
            self._running = False
            if not t.cancelled() and t.exception():
                logger.error(f"监控任务异常退出: {t.exception()}")

        self._monitor_task.add_done_callback(_on_done)
        logger.info("【github_monitor】后台监控任务已启动")

    async def _monitor_loop(self):
        """后台轮询循环（配置非法时回退默认间隔，任务不会死亡）
        每轮依次：检查更新推送 → 记录 Star 趋势 → 判断是否到点发送每日日报"""
        interval = max(1, self._safe_int(self.config.get("check_interval_minutes", 5), 5)) * 60
        while self._running:
            try:
                await self._broadcast_updates()
                # Star 趋势记录（同天去重；24h 增长达阈值推送提醒）
                for alert in await self._record_stars_today():
                    await self._send_to_subscribers(alert)
                # 每日趋势日报（到配置时间且当日未发送过）
                await self._maybe_send_daily_report()
            except Exception as e:
                logger.error(f"定时检查仓库更新失败: {e}")
            await asyncio.sleep(interval)

    # ========== Star 趋势追踪 ==========

    def _record_star(self, repo: str, stars: int, date: str | None = None) -> str | None:
        """记录单仓库当日 Star 数（同天只保留最新值）并检测 24h 增长提醒；
        返回提醒文本（无需提醒或数据不足返回 None）"""
        date = date or datetime.now(TIMEZONE_BJ).strftime("%Y-%m-%d")
        hist = self._trends.setdefault(repo, {})
        if hist.get(date) == stars:
            return None  # 同值不重复写
        hist[date] = stars
        self._trim_trends(self._trends)  # 写后即时截断，防止文件膨胀
        # 24h 增长提醒：需有昨日记录可对比，且当日未提醒过
        threshold = max(1, self._safe_int(self.config.get("star_alert_threshold", 10), 10))
        try:
            yesterday = (
                datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)
            ).strftime("%Y-%m-%d")
        except ValueError:
            return None
        prev = hist.get(yesterday)
        if prev is None:
            return None
        delta = stars - prev
        if delta >= threshold and self._trend_alerts.get(repo) != date:
            self._trend_alerts[repo] = date  # 同一天只提醒一次
            return self._build_star_alert(repo, stars, delta, prev)
        return None

    async def _record_stars_today(self, save: bool = True) -> list[str]:
        """随监控循环记录各仓库今日 Star 数（_get_repo_stars 带 TTL 缓存，不额外耗配额）；
        返回需要推送的 24h 增长提醒文本列表；API 失败/无缓存的仓库静默跳过"""
        alerts: list[str] = []
        repos = list(self._state.get("repos", {}).keys())
        if not repos:
            return alerts
        for repo in repos:
            try:
                stars = await self._get_repo_stars(repo)
                if repo not in self._repo_info_cache:
                    continue  # 请求失败（未缓存），跳过避免写入脏数据 0
                alert = self._record_star(repo, stars)
                if alert:
                    alerts.append(alert)
            except Exception as e:
                logger.warning(f"记录 {repo} Star 趋势失败: {e}")
        if save:
            self._save_trends()
        return alerts

    def _star_change(self, repo: str, days: int) -> tuple[int, float, int, int] | None:
        """计算仓库距今天 days 天的 Star 变化；
        返回 (变化量, 变化率%, 当前值, 对比值)；历史不足两条或对比日无数据返回 None"""
        hist = self._trends.get(repo) or {}
        if len(hist) < 2:
            return None
        sorted_items = sorted(hist.items())  # 按日期升序（YYYY-MM-DD 字典序即时间序）
        cur_val = sorted_items[-1][1]  # 最近一次记录（今天或最近运行日）
        today = datetime.now(TIMEZONE_BJ).date()
        target = today - timedelta(days=days)
        prev_val: int | None = None
        for d, v in sorted_items:
            try:
                d_obj = datetime.strptime(d, "%Y-%m-%d").date()
            except ValueError:
                continue
            if d_obj <= target:
                prev_val = v
            else:
                break
        if prev_val is None:
            return None
        delta = cur_val - prev_val
        rate = (delta / prev_val * 100.0) if prev_val else 0.0
        return delta, rate, cur_val, prev_val

    def _build_star_chart(self, repo: str, days: int = 7) -> list[str]:
        """生成近 days 天 Star 趋势块字符文本图（每行: 日期 块符 数值）；
        无数据返回空列表"""
        hist = self._trends.get(repo) or {}
        if not hist:
            return []
        today = datetime.now(TIMEZONE_BJ).date()
        start = today - timedelta(days=days - 1)
        points = []
        for i in range(days):
            d = (start + timedelta(days=i)).strftime("%Y-%m-%d")
            if d in hist:
                points.append((d, hist[d]))
        if not points:
            return []
        lo = min(v for _, v in points)
        hi = max(v for _, v in points)
        span = (hi - lo) or 1  # 全相等时避免除零
        bars = _TREND_CHART_BARS
        lines = []
        for d, v in points:
            level = int((v - lo) / span * (len(bars) - 1) + 0.5)
            lines.append(f"{d[5:]} {bars[level]} {v}")
        return lines

    def _build_star_alert(self, repo: str, stars: int, delta: int, prev: int) -> str:
        """构建 Star 24h 增长提醒文本"""
        rate = (delta / prev * 100.0) if prev else 0.0
        return (
            f"⭐ Star 增长提醒 [{repo}]\n"
            f"当前 {stars} 颗，24h 内 +{delta}（{rate:+.1f}%）"
        )

    # ========== 每日趋势日报 ==========

    def _bump_daily_stats(self, repo: str, delta: dict) -> None:
        """将本次检查发现的新增条目数累加到当日统计（纯内存操作，保存由调用方负责）"""
        date = datetime.now(TIMEZONE_BJ).strftime("%Y-%m-%d")
        day = self._daily_stats.setdefault(repo, {}).setdefault(
            date, {"commits": 0, "releases": 0, "tags": 0, "issues": 0, "prs": 0}
        )
        for key, val in delta.items():
            if val:
                day[key] = int(day.get(key, 0) or 0) + int(val)

    def _build_daily_report(self, date_str: str | None = None) -> str:
        """生成指定日期（默认昨日）的趋势日报纯文本；
        数据来源：daily_stats.json（更新计数）+ trends.json（Star 变化），不请求 API"""
        date = date_str or (
            datetime.now(TIMEZONE_BJ) - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        try:
            prev_date = (
                datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)
            ).strftime("%Y-%m-%d")
        except ValueError:
            prev_date = ""
        repos = list(self._state.get("repos", {}).keys())
        if not repos:
            return ""
        lines = [f"📊 GitHub 每日趋势日报（{date}）", "━━━━━━━━━━━━━━━━━━━━━━━━"]
        any_data = False
        for repo in repos:
            day = (self._daily_stats.get(repo) or {}).get(date)
            has_stats = day is not None and any(day.get(k, 0) for k in
                                                ("commits", "releases", "tags", "issues", "prs"))
            # Star 变化：date vs 前一天（来自 trends.json）
            hist = self._trends.get(repo) or {}
            cur, prev = hist.get(date), hist.get(prev_date) if prev_date else None
            star_txt = "—"
            if cur is not None and prev is not None:
                delta = cur - prev
                rate = (delta / prev * 100.0) if prev else 0.0
                star_txt = f"{prev} → {cur}（{delta:+d}，{rate:+.1f}%）"
            elif cur is not None:
                star_txt = f"{cur}（无前日对比）"
            if not has_stats and cur is None:
                continue  # 该仓库昨日完全无数据（未运行或未检查），跳过
            any_data = True
            lines.append(f"📍 {repo}")
            if has_stats:
                lines.append(
                    f"  🆕 提交 {day.get('commits', 0)} | 🏷️ 发布 {day.get('releases', 0)}"
                    f" | 🔖 标签 {day.get('tags', 0)}"
                    f" | 🐛 Issue {day.get('issues', 0)} | 🛠️ PR {day.get('prs', 0)}"
                )
            else:
                lines.append("  🆕 昨日无新增提交/发布/标签/Issue/PR")
            lines.append(f"  ⭐ Star: {star_txt}")
        if not any_data:
            return (
                f"📊 GitHub 每日趋势日报（{date}）\n"
                "昨日无任何数据。请确认插件在昨日处于运行状态。"
            )
        return "\n".join(lines)

    async def _maybe_send_daily_report(self) -> None:
        """每日定时发送趋势日报：到达配置时间（默认 09:00）且当日未发送过；
        定时任务并入 _monitor_loop，由 initialize()/terminate() 生命周期统一管理"""
        if not self.config.get("daily_report_enabled", True):
            return
        now = datetime.now(TIMEZONE_BJ)
        today = now.strftime("%Y-%m-%d")
        if self._daily_last_sent == today:
            return
        # 解析配置时间（如 "09:00"/"9:00"），非法回退默认 09:00
        time_str = str(self.config.get("daily_report_time", "09:00") or "09:00").strip()
        target = None
        try:
            target = datetime.strptime(f"{today} {time_str}", "%Y-%m-%d %H:%M").replace(
                tzinfo=TIMEZONE_BJ
            )
        except ValueError:
            try:
                target = datetime.strptime(f"{today} 09:00", "%Y-%m-%d %H:%M").replace(
                    tzinfo=TIMEZONE_BJ
                )
            except ValueError:
                return
        if now < target:
            return  # 未到点
        report = self._build_daily_report()
        if report:
            await self._send_to_subscribers(report)
        self._daily_last_sent = today
        self._save_daily_stats()

    async def _manual_check(self, repo: str | None = None):
        """手动检查：指定仓库或全部，返回结果文本"""
        repos = [repo] if repo else list(self._state.get("repos", {}).keys())
        if not repos:
            return "📭 当前没有监控任何仓库。使用 /gh add owner/repo 添加。"
        reports = []
        for r in repos:
            if r not in self._state.get("repos", {}):
                reports.append(f"❌ 仓库 {r} 不在监控列表中")
                continue
            try:
                updates = await self._check_repo_updates(r)
                if updates:
                    reports.append(self._build_update_message(r, updates))
                else:
                    reports.append(f"✅ 仓库 {r} 暂无更新")
            except Exception as e:
                reports.append(f"❌ 检查 {r} 失败: {e}")
        return "\n\n".join(reports)

    # ========== 指令处理 ==========

    @filter.command("gh", priority=100)
    async def gh_command(self, event: AstrMessageEvent) -> MessageEventResult | None:
        """GitHub 监控指令入口"""
        # 隐式订阅：使用过指令的会话自动加入订阅
        self._ensure_subscribed(event.unified_msg_origin)

        text = event.message_str.strip()
        parts = [p for p in text.split(" ") if p]
        sub = parts[1].lower() if len(parts) > 1 else "help"
        args = parts[2:]

        handlers = {
            "add": self._cmd_add,
            "remove": self._cmd_remove,
            "del": self._cmd_remove,
            "list": self._cmd_list,
            "repos": self._cmd_repos,
            "check": self._cmd_check,
            "star": self._cmd_star,
            "daily": self._cmd_daily,
            "settoken": self._cmd_settoken,
            "token": self._cmd_settoken,
            "sub": self._cmd_sub,
            "subscribe": self._cmd_sub,
            "订阅": self._cmd_sub,
            "unsub": self._cmd_unsub,
            "unsubscribe": self._cmd_unsub,
            "退订": self._cmd_unsub,
            "help": self._cmd_help,
            "": self._cmd_help,
        }
        handler = handlers.get(sub)
        if not handler:
            return self._send_text(event, f"❌ 未知子指令: {sub}\n发送 /gh help 查看帮助")
        try:
            return await handler(event, args)
        except Exception as e:
            logger.error(f"/gh {sub} 执行失败: {e}")
            return self._send_text(event, f"❌ 执行失败: {e}")

    def _ensure_subscribed(self, session: str):
        """将会话加入订阅列表（隐式订阅）"""
        if not session:
            return
        subs = self._state.setdefault("subscribers", [])
        if session not in subs:
            subs.append(session)
            self._save_state()

    async def _cmd_add(self, event: AstrMessageEvent, args: list[str]) -> MessageEventResult | None:
        if not args:
            return self._send_text(event, "❌ 用法: /gh add owner/repo")
        repo = self._parse_repo(args[0])
        if not repo:
            return self._send_text(event, "❌ 仓库格式不正确，应为 owner/repo，例如: /gh add yunxiao258/astrbot_plugin_context_analyzer")
        repos = self._state.setdefault("repos", {})
        if repo in repos:
            return self._send_text(event, f"ℹ️ 仓库 {repo} 已在监控列表中")
        # 验证仓库是否存在（404 不存在；401/403 为 Token 无效或限流，此时仍允许添加）
        status, _ = await self._gh_request(f"/repos/{repo}")
        if status == 404:
            return self._send_text(event, f"❌ 仓库 {repo} 不存在（或为私有仓库且未配置 Token）")
        repos[repo] = {"last_commit": "", "last_release": "", "last_tag": ""}
        self._save_state()
        if status in (401, 403):
            return self._send_text(event, f"✅ 已添加监控仓库: {repo}\n⚠️ 但当前 Token 无效或已限流，可能无法获取更新。请先 /gh settoken 设置有效 Token。")
        if status == 0:
            return self._send_text(event, f"✅ 已添加监控仓库: {repo}\n⚠️ 但当前网络无法访问 GitHub，请稍后使用 /gh check 验证。")
        return self._send_text(event, f"✅ 已添加监控仓库: {repo}\n首次将建立基线，之后的更新会自动推送。")

    async def _cmd_remove(self, event: AstrMessageEvent, args: list[str]) -> MessageEventResult | None:
        if not args:
            return self._send_text(event, "❌ 用法: /gh remove owner/repo")
        repo = self._parse_repo(args[0])
        if not repo:
            return self._send_text(event, "❌ 仓库格式不正确")
        # 持锁删除：防止与定时/手动检查并发时仓库被检查逻辑"复活"
        async with self._check_lock:
            repos = self._state.setdefault("repos", {})
            if repo not in repos:
                return self._send_text(event, f"ℹ️ 仓库 {repo} 不在监控列表中")
            repos.pop(repo, None)
            self._state.get("initialized", set()).discard(repo)
            self._save_state()
        return self._send_text(event, f"🗑️ 已移除监控仓库: {repo}")

    async def _cmd_list(self, event: AstrMessageEvent, args: list[str]) -> MessageEventResult | None:
        repos = self._state.get("repos", {})
        if not repos:
            return self._send_text(event, "📭 当前没有监控任何仓库。\n使用 /gh add owner/repo 添加，或 /gh repos 查看你的仓库。")
        lines = ["📋 当前监控的仓库:", "━━━━━━━━━━━━━━━━━━━━━━"]
        for repo in repos:
            last_commit = (repos[repo].get("last_commit") or "")[:7]
            last_release = repos[repo].get("last_release") or "-"
            lines.append(f"📍 {repo}")
            lines.append(f"   最新提交: {last_commit or '未同步'} | 最新版本: {last_release}")
        subs = self._subscribers()
        lines.append("")
        lines.append(f"👥 订阅推送的会话: {len(subs)} 个")
        return self._send_text(event, "\n".join(lines))

    async def _cmd_repos(self, event: AstrMessageEvent, args: list[str]) -> MessageEventResult | None:
        status, repos = await self._get_user_repos()
        if status == 401:
            return self._send_text(event, "❌ Token 无效或已过期。使用 /gh settoken <token> 重新设置。")
        if status == 403:
            return self._send_text(event, "⚠️ GitHub API 已限流（Rate Limit）。请稍后再试，或设置 Token 提升限额。")
        if status != 200:
            return self._send_text(event, "❌ 获取仓库列表失败，请稍后再试。")
        if not repos:
            return self._send_text(event, "📭 未查询到任何仓库。")
        token = self._get_token()
        owner = "Token 账号" if token else self.config.get("default_owner", "yunxiao258")
        lines = [f"📚 {owner} 的仓库列表（共 {len(repos)} 个）:", "━━━━━━━━━━━━━━━━━━━━━━"]
        for r in repos:
            name = r.get("full_name", r.get("name", "?"))
            desc = (r.get("description") or "")[:40]
            updated = self._format_time(r.get("updated_at"))
            lines.append(f"📍 {name}" + (f" - {desc}" if desc else ""))
            lines.append(f"   ⏱ 更新于 {updated}")
        lines.append("")
        lines.append("添加监控: /gh add <仓库名>")
        return self._send_text(event, "\n".join(lines))

    async def _cmd_check(self, event: AstrMessageEvent, args: list[str]) -> MessageEventResult | None:
        if args:
            repo = self._parse_repo(args[0])
            if not repo:
                return self._send_text(event, "❌ 仓库格式不正确，应为 owner/repo，例如: /gh check yunxiao258/astrbot_plugin_context_analyzer")
        else:
            repo = None
        result = await self._manual_check(repo)
        return self._send_text(event, result)

    async def _cmd_star(self, event: AstrMessageEvent, args: list[str]) -> MessageEventResult | None:
        """查看仓库近 7 天 Star 趋势（块字符文本图 + 24h/7d 变化数据）"""
        if not args:
            return self._send_text(event, "❌ 用法: /gh star owner/repo")
        repo = self._parse_repo(args[0])
        if not repo:
            return self._send_text(event, "❌ 仓库格式不正确，应为 owner/repo，例如: /gh star yunxiao258/astrbot_plugin_context_analyzer")
        if repo not in self._state.get("repos", {}):
            return self._send_text(event, f"ℹ️ 仓库 {repo} 不在监控列表中，先 /gh add {repo}")
        # 先刷新今日 Star（命中 TTL 缓存则不额外请求 API），并顺手落盘记录
        try:
            stars = await self._get_repo_stars(repo)
        except Exception as e:
            logger.warning(f"获取 {repo} Star 失败: {e}")
            return self._send_text(event, f"❌ 获取 {repo} Star 数失败，请稍后再试")
        if repo not in self._repo_info_cache:
            return self._send_text(event, f"❌ 获取 {repo} Star 数失败，请稍后再试")
        self._record_star(repo, stars)
        self._save_trends()
        lines = [f"⭐ Star 趋势 [{repo}]（近 7 天）", "━━━━━━━━━━━━━━━━━━━━━━━━"]
        chart = self._build_star_chart(repo, 7)
        if not chart:
            return self._send_text(event, f"📭 仓库 {repo} 暂无 Star 历史数据，稍后重试")
        lines.extend(chart)
        c24 = self._star_change(repo, 1)
        c7 = self._star_change(repo, 7)
        lines.append("")
        if c24:
            d, r, cur, prev = c24
            lines.append(f"24h: {prev} → {cur}（{d:+d}，{r:+.1f}%）")
        else:
            lines.append("24h: 数据不足")
        if c7:
            d, r, cur, prev = c7
            lines.append(f"7d:  {prev} → {cur}（{d:+d}，{r:+.1f}%）")
        else:
            lines.append("7d:  数据不足")
        return self._send_text(event, "\n".join(lines))

    async def _cmd_daily(self, event: AstrMessageEvent, args: list[str]) -> MessageEventResult | None:
        """手动触发每日趋势日报（汇总各订阅仓库昨日数据）"""
        report = self._build_daily_report()
        if not report:
            return self._send_text(event, "📭 当前没有监控任何仓库，无法生成日报。")
        # 手动触发后标记当日已发送，避免定时任务重复推送
        self._daily_last_sent = datetime.now(TIMEZONE_BJ).strftime("%Y-%m-%d")
        self._save_daily_stats()
        return self._send_text(event, report)

    async def _cmd_settoken(self, event: AstrMessageEvent, args: list[str]) -> MessageEventResult | None:
        if not args:
            return self._send_text(event, "❌ 用法: /gh settoken <token>\nToken 只保存在本机插件数据目录（Windows 上加密存储），不会上传。")
        token = args[0].strip()
        if not re.fullmatch(r"[A-Za-z0-9_\-]+", token) or len(token) < 20:
            return self._send_text(event, "❌ Token 格式不正确（应为 GitHub Personal Access Token）")
        # 先验证 Token 有效性（验证失败一律不保存，避免固化坏 Token）
        old_token = self._state.get("token", "")
        self._state["token"] = token
        status, _ = await self._gh_request("/user")
        if status == 200:
            try:
                from .secret import secure_store
            except ImportError:
                from secret import secure_store  # 测试/独立运行环境

            self._state["token"] = secure_store(token)
            self._save_state()
            return self._send_text(event, "✅ Token 已验证有效，已保存（Windows 加密存储，仅本机可读）。")
        elif status == 401:
            self._state["token"] = old_token  # 还原旧 Token
            return self._send_text(event, "❌ Token 无效（401 Unauthorized），未保存。")
        elif status == 403:
            self._state["token"] = old_token
            return self._send_text(event, "❌ Token 验证被拒绝（403），可能无权限访问 /user，未保存。")
        else:
            self._state["token"] = old_token
            return self._send_text(event, f"⚠️ 无法验证 Token（状态码 {status}，可能是网络错误），未保存，请重试。")

    async def _cmd_sub(self, event: AstrMessageEvent, args: list[str]) -> MessageEventResult | None:
        session = event.unified_msg_origin
        subs = self._state.setdefault("subscribers", [])
        if session not in subs:
            subs.append(session)
            self._save_state()
        return self._send_text(event, f"✅ 本会话已订阅仓库更新推送。\n退订请发送 /gh 退订")

    async def _cmd_unsub(self, event: AstrMessageEvent, args: list[str]) -> MessageEventResult | None:
        session = event.unified_msg_origin
        subs = self._state.get("subscribers", [])
        if session in subs:
            subs.remove(session)
            self._save_state()
        return self._send_text(event, "🗑️ 本会话已退订仓库更新推送。")

    async def _cmd_help(self, event: AstrMessageEvent, args: list[str]) -> MessageEventResult | None:
        help_text = (
            "🔧 GitHub 仓库监控插件 v" + PLUGIN_VERSION + "\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "/gh add owner/repo   添加监控仓库\n"
            "/gh remove owner/repo 移除监控仓库\n"
            "/gh list             查看监控列表\n"
            "/gh repos            查看个人仓库列表\n"
            "/gh check [repo]     立即检查更新（可指定仓库）\n"
            "/gh star owner/repo  查看近 7 天 Star 趋势图\n"
            "/gh daily            手动发送每日趋势日报\n"
            "/gh settoken <token> 设置 GitHub Token\n"
            "/gh 订阅 / 退订      订阅或退订本会话推送\n"
            "/gh help             查看帮助\n"
            "\n"
            "💡 定时自动检查默认每 5 分钟一次；使用过指令的会话会自动订阅推送。\n"
            "💡 过滤配置：keyword_filters 命中关键词才推送，issue_tags/pull_tags 按标签过滤，min_stars 按星标数过滤，enable_ai_summary 启用 AI 摘要。\n"
            "💡 Star 趋势与每日日报：star_alert_threshold 设置 24h 增长提醒阈值，daily_report_time 设置日报发送时间。"
        )
        return self._send_text(event, help_text)

    # ========== 工具 ==========

    def _send_text(self, event, text: str) -> MessageEventResult:
        """构造纯文本回复结果"""
        return event.chain_result([Plain(text)])

    async def terminate(self):
        """插件卸载时清理"""
        self._running = False
        if self._monitor_task:
            try:
                self._monitor_task.cancel()
                await asyncio.gather(self._monitor_task, return_exceptions=True)
            except Exception:
                pass
        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception:
                pass
        self._save_state()
