"""AstrBot GitHub 仓库监控插件：监控仓库更新（提交/发布/标签），自动推送到群聊与私聊"""

import asyncio
import json
import os
import re
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
PLUGIN_VERSION = "1.0.0"

# GitHub API 基础地址与请求头
GITHUB_API = "https://api.github.com"
GITHUB_USER_AGENT = "astrbot-plugin-github-monitor"
TIMEZONE_BJ = timezone(timedelta(hours=8))  # 北京时间 UTC+8
DEFAULT_TIMEOUT = 15  # HTTP 请求超时（秒）


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

        # 将配置中的默认仓库合并进监控列表（已存在的则不覆盖）
        self._merge_default_repos()

        # 后台轮询任务
        self._monitor_task: asyncio.Task | None = None
        self._session: aiohttp.ClientSession | None = None
        self._running = False
        # 检查互斥锁：防止定时轮询与手动 /gh check 并发检查导致重复推送
        self._check_lock = asyncio.Lock()

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
        if not isinstance(default_state.get("token"), str):
            default_state["token"] = ""
        return default_state

    def _save_state(self):
        """保存状态到磁盘"""
        try:
            data = dict(self._state)
            data["initialized"] = sorted(self._state.get("initialized", set()))
            with open(self._state_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存状态文件失败: {e}")

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
        """获取 Token：优先 /gh settoken 写入的状态，其次配置项"""
        return (self._state.get("token") or "").strip() or str(
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

    # ========== 更新检测与去重 ==========

    async def _check_repo_updates(self, repo: str) -> list[dict]:
        """检查单个仓库的更新，返回需要通知的更新列表；首次添加只设基线不通知"""
        async with self._check_lock:  # 互斥：防止定时与手动并发重复推送
            return await self._check_repo_updates_locked(repo)

    async def _check_repo_updates_locked(self, repo: str) -> list[dict]:
        """持锁状态下的检查实现（勿直接调用）"""
        repo_state = self._state.setdefault(
            "repos", {}
        ).setdefault(repo, {"last_commit": "", "last_release": "", "last_tag": ""})
        initialized = self._state.setdefault("initialized", set())
        max_events = max(1, int(self.config.get("max_events_per_check", 5) or 5))
        updates: list[dict] = []

        # 1. 新提交
        commits = await self._get_commits(repo, per_page=max_events)
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
                    updates.append({"type": "commit", "items": new_commits})
            else:
                repo_state["last_commit"] = latest_sha

        # 2. 新 release
        if self.config.get("notify_release", True):
            releases = await self._get_releases(repo, per_page=max_events)
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
                    if new_releases:
                        repo_state["last_release"] = releases[0].get("tag_name", "")
                        updates.append({"type": "release", "items": new_releases})
                else:
                    repo_state["last_release"] = releases[0].get("tag_name", "")

        # 3. 新 tag
        if self.config.get("notify_tag", True):
            tags = await self._get_tags(repo, per_page=max_events)
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
                    if new_tags:
                        repo_state["last_tag"] = tags[0].get("name", "")
                        updates.append({"type": "tag", "items": new_tags})
                else:
                    repo_state["last_tag"] = tags[0].get("name", "")

        # 标记已初始化基线
        if repo not in initialized:
            initialized.add(repo)

        self._save_state()
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
                    if body:
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
        """检查所有仓库并将更新推送到订阅会话"""
        repos = list(self._state.get("repos", {}).keys())
        if not repos:
            return
        for repo in repos:
            try:
                updates = await self._check_repo_updates(repo)
                if updates:
                    msg = self._build_update_message(repo, updates)
                    await self._send_to_subscribers(msg)
                    await asyncio.sleep(0.5)  # 避免连续推送过快
            except Exception as e:
                logger.error(f"检查仓库 {repo} 更新失败: {e}")

    # ========== 后台轮询 ==========

    @filter.on_astrbot_loaded()
    async def _on_astrbot_loaded(self) -> None:
        """AstrBot 加载完成后启动后台监控任务"""
        if not self.config.get("enable_auto_check", True):
            logger.info("【github_monitor】自动检查已禁用，仅手动检查")
            return
        if self._running:
            return
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("【github_monitor】后台监控任务已启动")

    async def _monitor_loop(self):
        """后台轮询循环"""
        interval = max(1, int(self.config.get("check_interval_minutes", 5) or 5)) * 60
        while self._running:
            try:
                await self._broadcast_updates()
            except Exception as e:
                logger.error(f"定时检查仓库更新失败: {e}")
            await asyncio.sleep(interval)

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
        repo = self._parse_repo(args[0]) if args else None
        result = await self._manual_check(repo)
        return self._send_text(event, result)

    async def _cmd_settoken(self, event: AstrMessageEvent, args: list[str]) -> MessageEventResult | None:
        if not args:
            return self._send_text(event, "❌ 用法: /gh settoken <token>\nToken 只保存在本机插件数据目录，不会上传。")
        token = args[0].strip()
        if not re.fullmatch(r"[A-Za-z0-9_\-]+", token) or len(token) < 20:
            return self._send_text(event, "❌ Token 格式不正确（应为 GitHub Personal Access Token）")
        # 先验证 Token 有效性
        old_token = self._state.get("token", "")
        self._state["token"] = token
        status, _ = await self._gh_request("/user")
        if status == 200:
            self._save_state()
            return self._send_text(event, "✅ Token 已验证有效，已保存（仅本机存储）。")
        elif status == 401:
            self._state["token"] = old_token  # 还原旧 Token
            return self._send_text(event, "❌ Token 无效（401 Unauthorized），未保存。")
        else:
            self._save_state()
            return self._send_text(event, f"⚠️ 无法验证 Token（状态码 {status}），但已保存，请确认网络与 Token 权限。")

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
            "/gh settoken <token> 设置 GitHub Token\n"
            "/gh 订阅 / 退订      订阅或退订本会话推送\n"
            "/gh help             查看帮助\n"
            "\n"
            "💡 定时自动检查默认每 5 分钟一次；使用过指令的会话会自动订阅推送。"
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
