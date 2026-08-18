# -*- coding: utf-8 -*-
"""github_monitor 插件单元测试：仓库解析、时间格式化、更新检测去重、并发广播、Star 趋势、标签过滤、每日日报"""
import asyncio
import copy
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, r"D:\astrbot\data\plugins\astrbot_plugin_github_monitor")
sys.path.insert(0, r"D:\astrbot\data\plugins")

from main import GitHubMonitorPlugin, TIMEZONE_BJ  # noqa: E402


class FakeEvent:
    """模拟 AstrMessageEvent（支持 chain_result 与 message 属性）"""

    def __init__(self, text="", origin="onebot:123:gid:10001"):
        self.message = {"text": text}
        self.message_str = text
        self.unified_msg_origin = origin

    def chain_result(self, chain):
        return chain


def make_plugin(**overrides):
    cfg = {
        "default_repos": "",
        "default_subscribers": "",
        "max_events_per_check": 5,
        "show_commit_stats": True,
        "notify_release": True,
        "notify_tag": True,
    }
    cfg.update(overrides)
    p = GitHubMonitorPlugin(context=None, config=cfg)
    # 隔离持久化：重定向到临时目录，避免污染真实 state/trends/daily_stats 文件
    tmp = tempfile.mkdtemp(prefix="gh_monitor_test_")
    p._state_file = os.path.join(tmp, "state.json")
    p._trends_file = os.path.join(tmp, "trends.json")
    p._daily_stats_file = os.path.join(tmp, "daily_stats.json")
    p._state = {
        "token": "",
        "repos": {},
        "subscribers": [],
        "initialized": set(),
    }
    p._trends = {}
    p._trend_alerts = {}
    p._daily_stats = {}
    p._daily_last_sent = ""
    return p


def dstr(offset: int = 0) -> str:
    """返回相对今天偏移 offset 天的 YYYY-MM-DD 日期字符串"""
    return (datetime.now(TIMEZONE_BJ) + timedelta(days=offset)).strftime("%Y-%m-%d")


class FakeResponse:
    """模拟 aiohttp 响应对象"""

    def __init__(self, status, data):
        self.status = status
        self._data = data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self):
        return self._data


class FakeSession:
    """可编程的假 aiohttp 会话：按请求路径返回预设数据"""

    def __init__(self, routes: dict):
        self.routes = routes
        self.closed = False

    def get(self, url, params=None, headers=None):
        for key, (status, data) in self.routes.items():
            if url.endswith(key):
                return FakeResponse(status, data)
        return FakeResponse(404, None)


COMMIT1 = {"sha": "abc123", "html_url": "https://github.com/o/r/commit/abc123",
           "commit": {"message": "fix bug\n\nlong body", "author": {"name": "Alice", "email": "a@x.com", "date": "2026-08-01T00:00:00Z"}}}
COMMIT2 = {"sha": "def456", "html_url": "https://github.com/o/r/commit/def456",
           "commit": {"message": "add feature", "author": {"name": "Bob", "date": "2026-08-02T00:00:00Z"}}}
RELEASE = {"tag_name": "v1.2.0", "name": "v1.2.0", "body": "发布说明", "published_at": "2026-08-03T00:00:00Z", "html_url": "https://github.com/o/r/releases/tag/v1.2.0", "author": {"login": "bob"}}
TAG = {"name": "v1.1.0", "commit": {"sha": "abc123"}, "html_url": "https://github.com/o/r/tree/v1.1.0"}
ISSUE1 = {"number": 1, "title": "登录接口超时", "state": "open", "user": {"login": "alice"},
          "created_at": "2026-08-01T00:00:00Z", "html_url": "https://github.com/o/r/issues/1", "body": "复现步骤…"}
ISSUE2 = {"number": 2, "title": "支持深色模式", "state": "open", "user": {"login": "bob"},
          "created_at": "2026-08-02T00:00:00Z", "html_url": "https://github.com/o/r/issues/2", "body": "建议新增…"}
PR1 = {"number": 11, "title": "修复登录问题", "state": "open", "merged": False, "user": {"login": "carol"},
       "created_at": "2026-08-02T00:00:00Z", "html_url": "https://github.com/o/r/pull/11", "body": "close #1"}
PR2 = {"number": 12, "title": "合并深色模式", "state": "closed", "merged": True, "user": {"login": "dave"},
       "created_at": "2026-08-03T00:00:00Z", "html_url": "https://github.com/o/r/pull/12", "body": "close #2"}


def C(d: dict) -> dict:
    """返回深拷贝，避免插件就地修改 _stats 污染模块级常量"""
    return copy.deepcopy(d)


class TestParse(unittest.TestCase):
    def test_parse_repo_plain(self):
        self.assertEqual(GitHubMonitorPlugin._parse_repo("owner/repo"), "owner/repo")

    def test_parse_repo_url(self):
        self.assertEqual(
            GitHubMonitorPlugin._parse_repo("https://github.com/owner/repo/"),
            "owner/repo",
        )
        self.assertEqual(
            GitHubMonitorPlugin._parse_repo("https://github.com/owner/repo.git"),
            "owner/repo",
        )

    def test_parse_repo_invalid(self):
        for bad in ("", "single", "a/b/c", "a b/c", "a//b", "x/\t"):
            self.assertIsNone(GitHubMonitorPlugin._parse_repo(bad), bad)

    def test_parse_repo_injection(self):
        self.assertIsNone(GitHubMonitorPlugin._parse_repo("a/b.json/x"))
        self.assertEqual(GitHubMonitorPlugin._parse_repo("a/b.c-d_1"), "a/b.c-d_1")

    def test_format_time_utc_to_bj(self):
        s = GitHubMonitorPlugin._format_time("2026-08-01T00:00:00Z")
        self.assertTrue(s.startswith("2026-08-01 08:00:00"), s)

    def test_format_time_invalid(self):
        self.assertEqual(GitHubMonitorPlugin._format_time(""), "未知时间")
        self.assertEqual(GitHubMonitorPlugin._format_time("乱码"), "乱码")

    def test_load_state_type_defense(self):
        p = make_plugin()
        with open(p._state_file, "w", encoding="utf-8") as f:
            f.write('{"repos": {}, "subscribers": "oops", "initialized": ["a"], "token": 3}')
        st = p._load_state()
        self.assertEqual(st["subscribers"], [])
        self.assertEqual(st["initialized"], {"a"})  # list 自动转 set
        self.assertEqual(st["token"], "")

    def test_subscribers_merge_config(self):
        p = make_plugin(default_subscribers="onebot:1, onebot:1, onebot:2")
        p._state["subscribers"] = ["onebot:0"]
        self.assertEqual(p._subscribers(), ["onebot:0", "onebot:1", "onebot:2"])

    def test_token_prefers_state(self):
        p = make_plugin(github_token="cfg_token")
        p._state["token"] = "state_token"
        self.assertEqual(p._get_token(), "state_token")
        p._state["token"] = ""
        self.assertEqual(p._get_token(), "cfg_token")


class TestUpdateDetection(unittest.TestCase):
    def _routes(self, commits=None, releases=None, tags=None, stats=None):
        routes = {}
        if commits is not None:
            routes["/commits"] = (200, commits)
        if releases is not None:
            routes["/releases"] = (200, releases)
        if tags is not None:
            routes["/tags"] = (200, tags)
        if stats:
            for sha, (add, dele) in stats.items():
                routes[f"/commits/{sha}"] = (200, {"files": [{"additions": add, "deletions": dele}]})
        return routes

    def test_first_check_sets_baseline_no_notify(self):
        p = make_plugin()
        p._session = FakeSession(self._routes(commits=[C(COMMIT1)], releases=[RELEASE], tags=[TAG]))
        updates = asyncio.run(p._check_repo_updates("o/r"))
        self.assertEqual(updates, [])
        self.assertEqual(p._state["repos"]["o/r"]["last_commit"], "abc123")
        self.assertIn("o/r", p._state["initialized"])

    def test_new_commits_detected(self):
        p = make_plugin()
        p._state["repos"]["o/r"] = {"last_commit": "abc123", "last_release": "", "last_tag": ""}
        p._state["initialized"].add("o/r")
        p._session = FakeSession(self._routes(
            commits=[C(COMMIT2), C(COMMIT1)],
            stats={"def456": (10, 2)},
        ))
        updates = asyncio.run(p._check_repo_updates("o/r"))
        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]["type"], "commit")
        self.assertEqual(len(updates[0]["items"]), 1)
        self.assertEqual(updates[0]["items"][0]["_stats"], (10, 2))
        self.assertEqual(p._state["repos"]["o/r"]["last_commit"], "def456")

    def test_release_and_tag_detected(self):
        p = make_plugin()
        p._state["repos"]["o/r"] = {"last_commit": "abc123", "last_release": "v1.0.0", "last_tag": "v1.0.0"}
        p._state["initialized"].add("o/r")
        p._session = FakeSession(self._routes(
            commits=[C(COMMIT1)],
            releases=[C(RELEASE)],
            tags=[C(TAG)],
        ))
        updates = asyncio.run(p._check_repo_updates("o/r"))
        kinds = {u["type"] for u in updates}
        self.assertEqual(kinds, {"release", "tag"})

    def test_api_error_no_crash(self):
        p = make_plugin()
        p._state["repos"]["o/r"] = {"last_commit": "abc123", "last_release": "", "last_tag": ""}
        p._state["initialized"].add("o/r")
        p._session = FakeSession({})  # 全部 404
        updates = asyncio.run(p._check_repo_updates("o/r"))
        self.assertEqual(updates, [])
        self.assertEqual(p._state["repos"]["o/r"]["last_commit"], "abc123")

    def test_show_commit_stats_off(self):
        p = make_plugin(show_commit_stats=False)
        p._state["repos"]["o/r"] = {"last_commit": "abc123", "last_release": "", "last_tag": ""}
        p._state["initialized"].add("o/r")
        p._session = FakeSession(self._routes(commits=[C(COMMIT2), C(COMMIT1)]))
        updates = asyncio.run(p._check_repo_updates("o/r"))
        self.assertNotIn("_stats", updates[0]["items"][0])

    def test_issue_pr_first_check_sets_baseline(self):
        p = make_plugin()
        p._session = FakeSession({
            "/issues": (200, [C(ISSUE2), C(ISSUE1)]),
            "/pulls": (200, [C(PR2), C(PR1)]),
        })
        updates = asyncio.run(p._check_repo_updates("o/r"))
        self.assertEqual(updates, [])
        self.assertEqual(p._state["repos"]["o/r"]["last_issue"], 2)
        self.assertEqual(p._state["repos"]["o/r"]["last_pr"], 12)

    def test_new_issue_and_pr_detected(self):
        p = make_plugin()
        p._state["repos"]["o/r"] = {
            "last_commit": "", "last_release": "", "last_tag": "",
            "last_issue": 1, "last_pr": 11,
        }
        p._state["initialized"].add("o/r")
        p._session = FakeSession({
            "/issues": (200, [C(ISSUE2), C(ISSUE1)]),
            "/pulls": (200, [C(PR2), C(PR1)]),
        })
        updates = asyncio.run(p._check_repo_updates("o/r"))
        kinds = {u["type"] for u in updates}
        self.assertEqual(kinds, {"issue", "pr"})
        issue_group = next(u for u in updates if u["type"] == "issue")
        self.assertEqual(len(issue_group["items"]), 1)
        self.assertEqual(issue_group["items"][0]["number"], 2)
        self.assertEqual(p._state["repos"]["o/r"]["last_issue"], 2)
        self.assertEqual(p._state["repos"]["o/r"]["last_pr"], 12)

    def test_issue_pr_disabled_skips(self):
        p = make_plugin(notify_issue=False, notify_pr=False)
        p._state["repos"]["o/r"] = {
            "last_commit": "", "last_release": "", "last_tag": "",
            "last_issue": 1, "last_pr": 11,
        }
        p._state["initialized"].add("o/r")
        p._session = FakeSession({
            "/issues": (200, [C(ISSUE2), C(ISSUE1)]),
            "/pulls": (200, [C(PR2), C(PR1)]),
        })
        updates = asyncio.run(p._check_repo_updates("o/r"))
        self.assertEqual(updates, [])
        # 禁用时不推进基线
        self.assertEqual(p._state["repos"]["o/r"]["last_issue"], 1)

    def test_issues_endpoint_excludes_pulls(self):
        p = make_plugin()
        # /issues 端点混入 PR（带 pull_request 字段）时应剔除
        p._session = FakeSession({
            "/issues": (200, [C(ISSUE1), {**C(PR1), "pull_request": {"url": "x"}}]),
        })
        issues = asyncio.run(p._get_issues("o/r", per_page=10))
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["number"], 1)


class TestBuildMessage(unittest.TestCase):
    def test_build_commit_message(self):
        p = make_plugin()
        msg = p._build_update_message("o/r", [{"type": "commit", "items": [{**C(COMMIT2), "_stats": (10, 2)}]}])
        self.assertIn("仓库更新提醒 [o/r]", msg)
        self.assertIn("新提交 #1", msg)
        self.assertIn("add feature", msg)
        self.assertIn("+10 / -2", msg)
        self.assertIn("https://github.com/o/r/commit/def456", msg)

    def test_build_release_message(self):
        p = make_plugin()
        msg = p._build_update_message("o/r", [{"type": "release", "items": [RELEASE]}])
        self.assertIn("新版本发布 #1", msg)
        self.assertIn("v1.2.0", msg)
        self.assertIn("发布说明", msg)

    def test_build_tag_message(self):
        p = make_plugin()
        msg = p._build_update_message("o/r", [{"type": "tag", "items": [TAG]}])
        self.assertIn("新标签 #1", msg)
        self.assertIn("v1.1.0", msg)

    def test_build_issue_message(self):
        p = make_plugin()
        msg = p._build_update_message("o/r", [{"type": "issue", "items": [{**C(ISSUE1), "_summary": "认证超时"}]}])
        self.assertIn("新 Issue #1", msg)
        self.assertIn("登录接口超时", msg)
        self.assertIn("🤖 摘要: 认证超时", msg)
        self.assertIn("https://github.com/o/r/issues/1", msg)

    def test_build_pr_message(self):
        p = make_plugin()
        msg = p._build_update_message("o/r", [{"type": "pr", "items": [C(PR1)], }])
        self.assertIn("新 PR #11", msg)
        self.assertIn("修复登录问题", msg)
        self.assertIn("待合并", msg)
        msg2 = p._build_update_message("o/r", [{"type": "pr", "items": [C(PR2)]}])
        self.assertIn("已合并", msg2)

    def test_long_message_truncated(self):
        p = make_plugin()
        items = [{"type": "commit", "items": [{**C(COMMIT1)} for _ in range(30)]}]
        msg = p._build_update_message("o/r", items)
        self.assertIn("已折叠显示", msg)


class TestFilterAndSummary(unittest.TestCase):
    def test_keywords_parsing(self):
        p = make_plugin(keyword_filters="修复, v1.2，优化")
        self.assertEqual(p._keywords(), ["修复", "v1.2", "优化"])

    def test_keywords_empty_means_no_filter(self):
        p = make_plugin()
        self.assertEqual(p._keywords(), [])

    def test_matches_keywords_case_insensitive(self):
        self.assertTrue(GitHubMonitorPlugin._matches_keywords("Fix bug", ["fix"]))
        self.assertFalse(GitHubMonitorPlugin._matches_keywords("Fix bug", ["release"]))
        self.assertTrue(GitHubMonitorPlugin._matches_keywords("anything", []))

    def test_filter_updates_by_type(self):
        p = make_plugin(keyword_filters="feature")
        updates = [
            {"type": "commit", "items": [C(COMMIT1), C(COMMIT2)]},  # fix bug / add feature
            {"type": "release", "items": [{**C(RELEASE), "body": "修复若干问题"}]},  # 不命中
            {"type": "tag", "items": [{**C(TAG), "name": "v1.2.0-feature"}]},  # 命中
            {"type": "issue", "items": [{**C(ISSUE1), "title": "feature 请求"}]},  # 命中
            {"type": "pr", "items": [C(PR1)]},  # 不命中
        ]
        filtered = p._filter_updates(updates)
        self.assertEqual(len(filtered), 3)
        self.assertEqual(filtered[0]["type"], "commit")
        self.assertEqual(len(filtered[0]["items"]), 1)
        self.assertEqual(filtered[0]["items"][0]["commit"]["message"], "add feature")
        self.assertEqual(filtered[1]["type"], "tag")
        self.assertEqual(filtered[2]["type"], "issue")

    def test_issue_pr_summary_attached(self):
        p = make_plugin(enable_ai_summary=True)
        items = [C(ISSUE1), C(PR1)]
        hints = []

        async def fake_summarize(text, hint):
            hints.append(hint)
            return f"摘要：{hint}"

        p._summarize = fake_summarize
        asyncio.run(p._attach_summaries("issue", items[:1]))
        asyncio.run(p._attach_summaries("pr", items[1:]))
        self.assertEqual(items[0]["_summary"], "摘要：Issue")
        self.assertEqual(items[1]["_summary"], "摘要：Pull Request")
        self.assertEqual(hints, ["Issue", "Pull Request"])

    def test_check_respects_keyword_filter(self):
        p = make_plugin(keyword_filters="nonsense-keyword")
        p._state["repos"]["o/r"] = {"last_commit": "abc123", "last_release": "v1.0.0", "last_tag": "v1.0.0"}
        p._state["initialized"].add("o/r")
        p._session = FakeSession({
            "/commits": (200, [C(COMMIT2), C(COMMIT1)]),
            "/releases": (200, [C(RELEASE)]),
            "/tags": (200, [C(TAG)]),
        })
        updates = asyncio.run(p._check_repo_updates("o/r"))
        self.assertEqual(updates, [])
        # 基线仍推进：被过滤的更新不会重复推送
        self.assertEqual(p._state["repos"]["o/r"]["last_commit"], "def456")

    def test_min_stars_filters_repo(self):
        p = make_plugin(min_stars=100)
        p._state["repos"]["o/r"] = {"last_commit": "abc123", "last_release": "", "last_tag": ""}
        p._state["initialized"].add("o/r")
        session = FakeSession({
            "/repos/o/r": (200, {"stargazers_count": 42}),
            "/commits": (200, [C(COMMIT2), C(COMMIT1)]),
        })
        p._session = session
        updates = asyncio.run(p._check_repo_updates("o/r"))
        self.assertEqual(updates, [])
        # 星标不足时基线不推进（涨星后仍会推送）
        self.assertEqual(p._state["repos"]["o/r"]["last_commit"], "abc123")

    def test_repo_stars_cached(self):
        p = make_plugin(min_stars=0)
        calls = {"n": 0}

        class CountingSession(FakeSession):
            def get(self, url, params=None, headers=None):
                if url.endswith("/repos/o/r"):
                    calls["n"] += 1
                    return FakeResponse(200, {"stargazers_count": 123})
                return FakeResponse(404, None)

        p._session = CountingSession({})
        stars = asyncio.run(p._get_repo_stars("o/r"))
        self.assertEqual(stars, 123)
        stars = asyncio.run(p._get_repo_stars("o/r"))
        self.assertEqual(stars, 123)
        self.assertEqual(calls["n"], 1, "第二次应命中 TTL 缓存")

    def test_ai_summary_attached_when_enabled(self):
        p = make_plugin(enable_ai_summary=True)
        items = [C(COMMIT2)]

        async def fake_summarize(text, hint):
            return f"摘要：{hint}"

        p._summarize = fake_summarize
        asyncio.run(p._attach_summaries("commit", items))
        self.assertEqual(items[0]["_summary"], "摘要：提交")

    def test_ai_summary_skipped_when_disabled(self):
        p = make_plugin(enable_ai_summary=False)
        items = [C(COMMIT2)]
        asyncio.run(p._attach_summaries("commit", items))
        self.assertNotIn("_summary", items[0])

    def test_summary_failure_degrades(self):
        p = make_plugin(enable_ai_summary=True)
        items = [C(RELEASE)]

        async def fail(text, hint):
            raise RuntimeError("provider down")

        p._summarize = fail
        asyncio.run(p._attach_summaries("release", items))
        self.assertNotIn("_summary", items[0])

    def test_build_message_shows_summary(self):
        p = make_plugin()
        msg = p._build_update_message(
            "o/r",
            [{"type": "commit", "items": [{**C(COMMIT2), "_summary": "修复了登录问题"}]}],
        )
        self.assertIn("🤖 摘要: 修复了登录问题", msg)
        msg2 = p._build_update_message(
            "o/r",
            [{"type": "release", "items": [{**C(RELEASE), "_summary": "本次更新优化了性能"}]}],
        )
        self.assertIn("🤖 摘要: 本次更新优化了性能", msg2)


class TestConcurrentBroadcast(unittest.TestCase):
    def test_broadcast_runs_concurrently(self):
        p = make_plugin()
        p._state["repos"] = {"r1": {}, "r2": {}, "r3": {}, "r4": {}}
        p._state["initialized"] = {"r1", "r2", "r3", "r4"}
        max_active = {"v": 0}
        active = {"v": 0}
        sent = []

        async def fake_check(repo, save=True):
            active["v"] += 1
            max_active["v"] = max(max_active["v"], active["v"])
            await asyncio.sleep(0.05)
            active["v"] -= 1
            return [{"type": "commit", "items": [C(COMMIT1)]}]

        async def fake_send(msg):
            sent.append(1)
            await asyncio.sleep(0.01)

        p._check_repo_updates_locked = fake_check
        p._send_to_subscribers = fake_send
        p._save_state = lambda: None
        asyncio.run(p._broadcast_updates())
        # 4 个仓库并发（并发度 > 1 即证明非串行）
        self.assertGreater(max_active["v"], 1, "仓库检查应并发执行")
        self.assertEqual(len(sent), 4)

    def test_broadcast_error_isolation(self):
        p = make_plugin()
        p._state["repos"] = {"good": {}, "bad": {}}
        p._state["initialized"] = {"good", "bad"}

        async def fake_check(repo, save=True):
            if repo == "bad":
                raise RuntimeError("boom")
            return [{"type": "tag", "items": [TAG]}]

        sent = []

        async def fake_send(msg):
            sent.append(1)

        p._check_repo_updates_locked = fake_check
        p._send_to_subscribers = fake_send
        p._save_state = lambda: None
        asyncio.run(p._broadcast_updates())
        # 单仓异常不影响其他仓库推送
        self.assertEqual(len(sent), 1)


class TestFixes(unittest.TestCase):
    def test_safe_int_falls_back_on_dirty_config(self):
        p = make_plugin(check_interval_minutes="abc", max_events_per_check=None, min_stars="")
        self.assertEqual(p._safe_int(p.config.get("check_interval_minutes", 5), 5), 5)
        self.assertEqual(p._safe_int(p.config.get("max_events_per_check", 5), 5), 5)
        self.assertEqual(p._safe_int(p.config.get("min_stars", 0), 0), 0)
        p2 = make_plugin(check_interval_minutes=7)
        self.assertEqual(p2._safe_int(p2.config.get("check_interval_minutes", 5), 5), 7)

    def test_fetch_per_page_beyond_max_events(self):
        # 提交数量超过 max_events_per_check=2 时仍能全部检测（per_page=30），通知截断为 2 条
        p = make_plugin(max_events_per_check=2)
        commits = [
            {"sha": f"sha{i:02d}", "html_url": f"https://github.com/o/r/commit/sha{i:02d}",
             "commit": {"message": f"c{i}", "author": {"name": "A", "date": "2026-08-02T00:00:00Z"}}}
            for i in range(5, 0, -1)  # sha05 ... sha01（最新在前）
        ]
        p._state["repos"]["o/r"] = {"last_commit": "sha01", "last_release": "", "last_tag": ""}
        p._state["initialized"].add("o/r")
        p.config["show_commit_stats"] = False
        p._session = FakeSession({
            "/commits": (200, commits),
            "/releases": (200, []),
            "/tags": (200, []),
        })
        updates = asyncio.run(p._check_repo_updates("o/r"))
        self.assertEqual(len(updates), 1)
        self.assertEqual(len(updates[0]["items"]), 2)  # 通知条数上限 2
        # 但基线已推进到最新（sha05 之后全部检测到，不丢失）
        self.assertEqual(p._state["repos"]["o/r"]["last_commit"], "sha05")

    def test_remove_under_lock_does_not_resurrect(self):
        p = make_plugin()
        p._state["repos"]["o/r"] = {"last_commit": "abc", "last_release": "", "last_tag": ""}
        p._state["initialized"].add("o/r")
        p._session = FakeSession({})
        # 先 remove（持锁），再并发检查已删仓库 → 检查会重建吗？remove 持锁后检查列表不含它
        async def scenario():
            await p._cmd_remove(FakeEvent(), ["o/r"])
            return "o/r" in p._state["repos"]

        self.assertFalse(asyncio.run(scenario()))

    def test_stars_failure_not_cached(self):
        p = make_plugin()
        p._session = FakeSession({})  # 请求全部失败
        # 第一次失败返回 0 且不缓存
        self.assertEqual(asyncio.run(p._get_repo_stars("o/r")), 0)
        self.assertNotIn("o/r", p._repo_info_cache)

    def test_check_bad_repo_returns_format_error(self):
        p = make_plugin()
        p._session = FakeSession({})
        result = asyncio.run(p._cmd_check(FakeEvent(), ["not-a-repo"]))
        self.assertIn("格式不正确", result[0].text)

    def test_state_load_drops_bad_repo_entries(self):
        p = make_plugin()
        tmp = tempfile.mkdtemp(prefix="gh_state_test_")
        state_file = os.path.join(tmp, "state.json")
        with open(state_file, "w", encoding="utf-8") as f:
            import json
            json.dump({"repos": {"good": {"last_commit": "x"}, "bad": "not-a-dict"}}, f)
        p._state_file = state_file
        p._state = p._load_state()
        self.assertIn("good", p._state["repos"])
        self.assertNotIn("bad", p._state["repos"])


class TestCommands(unittest.TestCase):
    """命令分发层：隐式订阅、订阅/退订、settoken 验证与还原"""

    def test_ensure_subscribed_on_command(self):
        p = make_plugin()
        ev = FakeEvent("/gh list", origin="onebot:g:10001")
        p._session = FakeSession({})
        asyncio.run(p.gh_command(ev))
        self.assertIn("onebot:g:10001", p._state["subscribers"])

    def test_sub_and_unsub(self):
        p = make_plugin()
        ev = FakeEvent("/gh 订阅", origin="onebot:g:10001")
        p._session = FakeSession({})
        asyncio.run(p._cmd_sub(ev, []))
        self.assertIn("onebot:g:10001", p._state["subscribers"])
        # 重复订阅不重复添加
        asyncio.run(p._cmd_sub(ev, []))
        self.assertEqual(p._state["subscribers"].count("onebot:g:10001"), 1)
        asyncio.run(p._cmd_unsub(ev, []))
        self.assertNotIn("onebot:g:10001", p._state["subscribers"])

    def test_unknown_subcommand(self):
        p = make_plugin()
        ev = FakeEvent("/gh 不存在", origin="onebot:g:10001")
        result = asyncio.run(p.gh_command(ev))
        self.assertIn("未知子指令", result[0].text)

    def test_settoken_short_token_rejected(self):
        p = make_plugin()
        p._session = FakeSession({})
        ev = FakeEvent()
        result = asyncio.run(p._cmd_settoken(ev, ["short"]))
        self.assertIn("格式不正确", result[0].text)
        self.assertEqual(p._state["token"], "")

    def test_settoken_invalid_restores_old(self):
        p = make_plugin()
        p._state["token"] = "old_token"
        p._session = FakeSession({"/user": (401, {"message": "Bad credentials"})})
        ev = FakeEvent()
        result = asyncio.run(p._cmd_settoken(ev, ["ghp_" + "x" * 30]))
        self.assertIn("无效", result[0].text)
        self.assertEqual(p._state["token"], "old_token")

    def test_settoken_valid_saves_encrypted(self):
        p = make_plugin()
        p._session = FakeSession({"/user": (200, {"login": "yunxiao258"})})
        ev = FakeEvent()
        import secret
        original = secret.secure_store

        def fake_store(token):
            return f"enc:{token}"

        try:
            secret.secure_store = fake_store
            result = asyncio.run(p._cmd_settoken(ev, ["ghp_" + "y" * 30]))
        finally:
            secret.secure_store = original
        self.assertIn("已验证有效", result[0].text)
        self.assertEqual(p._state["token"], "enc:ghp_" + "y" * 30)

    def test_settoken_network_error_restores_old(self):
        p = make_plugin()
        p._state["token"] = "old_token"
        p._session = FakeSession({})  # 404 → 视为网络/异常
        ev = FakeEvent()
        result = asyncio.run(p._cmd_settoken(ev, ["ghp_" + "z" * 30]))
        self.assertIn("无法验证", result[0].text)
        self.assertEqual(p._state["token"], "old_token")


class TestStarTrends(unittest.TestCase):
    """Star 趋势追踪：历史记录、30 天截断、24h/7d 变化率、阈值提醒、块字符图、命令"""

    def test_record_star_same_day_dedupe(self):
        p = make_plugin()
        # 同一天两次记录只保留最新值
        p._record_star("o/r", 100, date=dstr())
        p._record_star("o/r", 120, date=dstr())
        self.assertEqual(p._trends["o/r"][dstr()], 120)
        self.assertEqual(len(p._trends["o/r"]), 1)

    def test_record_star_no_yesterday_no_alert(self):
        p = make_plugin(star_alert_threshold=1)
        # 无昨日记录无法对比 → 不提醒，但记录仍写入
        alert = p._record_star("o/r", 500, date=dstr())
        self.assertIsNone(alert)
        self.assertEqual(p._trends["o/r"][dstr()], 500)

    def test_trim_trends_keeps_30_days(self):
        p = make_plugin()
        p._trends = {"o/r": {dstr(-(34 - i)): 100 + i for i in range(35)}}
        GitHubMonitorPlugin._trim_trends(p._trends)
        self.assertEqual(len(p._trends["o/r"]), 30)
        self.assertIn(dstr(-29), p._trends["o/r"])
        self.assertNotIn(dstr(-30), p._trends["o/r"])
        # 全部过期的仓库被整体清除
        p._trends["old"] = {dstr(-40): 1}
        GitHubMonitorPlugin._trim_trends(p._trends)
        self.assertNotIn("old", p._trends)

    def test_star_change_24h_and_7d(self):
        p = make_plugin()
        p._trends = {"o/r": {dstr(-7): 90, dstr(-1): 100, dstr(): 130}}
        c24 = p._star_change("o/r", 1)
        self.assertEqual(c24[0], 30)  # 130 - 100
        self.assertAlmostEqual(c24[1], 30.0)
        c7 = p._star_change("o/r", 7)
        self.assertEqual(c7[0], 40)  # 130 - 90
        self.assertAlmostEqual(c7[1], 40.0 / 90 * 100)

    def test_star_change_insufficient_data(self):
        p = make_plugin()
        self.assertIsNone(p._star_change("o/r", 1))  # 无数据
        p._trends = {"o/r": {dstr(): 100}}
        self.assertIsNone(p._star_change("o/r", 1))  # 仅一条记录
        p._trends = {"o/r": {dstr(): 100, dstr(-1): 99}}
        self.assertIsNone(p._star_change("o/r", 7))  # 对比日无记录

    def test_star_change_fallback_to_earlier_record(self):
        p = make_plugin()
        # 对比日（7 天前）当天无记录时，回退到该日期之前最近的一条
        p._trends = {"o/r": {dstr(-9): 80, dstr(-2): 95, dstr(): 105}}
        c7 = p._star_change("o/r", 7)
        self.assertEqual(c7[0], 25)

    def test_star_alert_threshold_triggered(self):
        p = make_plugin(star_alert_threshold=10)
        p._trends = {"o/r": {dstr(-1): 100}}
        alert = p._record_star("o/r", 120, date=dstr())
        self.assertIsNotNone(alert)
        self.assertIn("o/r", alert)
        self.assertIn("+20", alert)
        self.assertEqual(p._trend_alerts["o/r"], dstr())
        # 同一天再次记录不重复提醒
        self.assertIsNone(p._record_star("o/r", 125, date=dstr()))

    def test_star_alert_below_threshold(self):
        p = make_plugin(star_alert_threshold=10)
        p._trends = {"o/r": {dstr(-1): 100}}
        self.assertIsNone(p._record_star("o/r", 105, date=dstr()))

    def test_star_alert_threshold_invalid_falls_back(self):
        p = make_plugin(star_alert_threshold="abc")
        p._trends = {"o/r": {dstr(-1): 100}}
        # 非法阈值回退默认 10 → 增量 5 不触发
        self.assertIsNone(p._record_star("o/r", 105, date=dstr()))
        self.assertIsNotNone(p._record_star("o/r", 120, date=dstr()))

    def test_build_star_chart(self):
        p = make_plugin()
        p._trends = {"o/r": {dstr(-i): 100 + i for i in range(6, -1, -1)}}
        lines = p._build_star_chart("o/r", 7)
        self.assertEqual(len(lines), 7)
        self.assertIn(dstr(-6)[5:], lines[0])
        self.assertTrue(any("█" in l for l in lines))  # 最高值应为满格

    def test_build_star_chart_empty(self):
        p = make_plugin()
        self.assertEqual(p._build_star_chart("o/r", 7), [])
        p._trends = {"o/r": {dstr(-20): 1}}
        self.assertEqual(p._build_star_chart("o/r", 7), [])  # 7 天内无记录

    def test_cmd_star_output(self):
        p = make_plugin()
        p._state["repos"] = {"o/r": {}}
        p._trends = {"o/r": {dstr(-1): 100, dstr(): 110}}
        p._session = FakeSession({"/repos/o/r": (200, {"stargazers_count": 110})})
        result = asyncio.run(p._cmd_star(FakeEvent(), ["o/r"]))
        text = result[0].text
        self.assertIn("Star 趋势 [o/r]", text)
        self.assertIn("24h:", text)
        self.assertIn("+10", text)
        self.assertIn("7d:", text)

    def test_cmd_star_unknown_repo(self):
        p = make_plugin()
        result = asyncio.run(p._cmd_star(FakeEvent(), ["o/r"]))
        self.assertIn("不在监控列表中", result[0].text)

    def test_cmd_star_invalid_format(self):
        p = make_plugin()
        result = asyncio.run(p._cmd_star(FakeEvent(), ["bad"]))
        self.assertIn("格式不正确", result[0].text)

    def test_cmd_star_api_failure(self):
        p = make_plugin()
        p._state["repos"] = {"o/r": {}}
        p._session = FakeSession({})  # 404 → 未缓存 → 视为失败
        result = asyncio.run(p._cmd_star(FakeEvent(), ["o/r"]))
        self.assertIn("失败", result[0].text)

    def test_record_stars_today_skips_failed_api(self):
        p = make_plugin()
        p._state["repos"] = {"o/r": {}}
        p._session = FakeSession({})  # 全部 404 → 不缓存 → 跳过
        alerts = asyncio.run(p._record_stars_today(save=False))
        self.assertEqual(alerts, [])
        self.assertEqual(p._trends, {})

    def test_record_stars_today_alerts(self):
        p = make_plugin(star_alert_threshold=10)
        p._state["repos"] = {"o/r": {}}
        p._trends = {"o/r": {dstr(-1): 100}}
        p._session = FakeSession({"/repos/o/r": (200, {"stargazers_count": 130})})
        alerts = asyncio.run(p._record_stars_today(save=False))
        self.assertEqual(len(alerts), 1)
        self.assertIn("+30", alerts[0])
        # 已落盘记录
        self.assertEqual(p._trends["o/r"][dstr()], 130)

    def test_load_trends_dirty_data(self):
        p = make_plugin()
        with open(p._trends_file, "w", encoding="utf-8") as f:
            json.dump({
                "_alerts": {"o/r": dstr(), "bad": 3},
                "o/r": {dstr(): 100, "坏键": 1, dstr(-1): "x", dstr(-2): -5},
                "bad": "not-a-dict",
            }, f, ensure_ascii=False)
        p._trends = p._load_trends()
        self.assertEqual(p._trends["o/r"], {dstr(): 100})
        self.assertEqual(p._trend_alerts, {"o/r": dstr()})
        self.assertNotIn("bad", p._trends)

    def test_save_trends_roundtrip(self):
        p = make_plugin()
        p._trends = {"o/r": {dstr(): 100}}
        p._trend_alerts = {"o/r": dstr()}
        p._save_trends()
        # 重新加载验证持久化成功
        p2 = make_plugin()
        p2._trends_file = p._trends_file
        p2._trends = p2._load_trends()
        self.assertEqual(p2._trends["o/r"][dstr()], 100)
        self.assertEqual(p2._trend_alerts["o/r"], dstr())

    def test_save_trends_trims_on_write(self):
        p = make_plugin()
        p._trends = {"o/r": {dstr(-40): 1, dstr(): 100}}
        p._save_trends()
        self.assertNotIn(dstr(-40), p._trends["o/r"])


class TestIssuePrTags(unittest.TestCase):
    """Issue/PR 标签过滤：配置为空全通过、命中保留、未命中剔除且基线照常推进"""

    ISSUE_BUG = {**ISSUE2, "labels": [{"name": "bug"}]}                  # number 2
    ISSUE_FEATURE = {**ISSUE1, "labels": [{"name": "enhancement"}]}      # number 1
    PR_BUG = {**PR2, "labels": [{"name": "bug"}]}                        # number 12
    PR_NO_LABEL = {**PR1, "labels": []}                                  # number 11

    def _repo_state(self):
        return {
            "last_commit": "", "last_release": "", "last_tag": "",
            "last_issue": 1, "last_pr": 11,
        }

    def test_tags_empty_no_filter(self):
        p = make_plugin()
        self.assertTrue(p._matches_tags(self.ISSUE_BUG, []))
        self.assertTrue(p._matches_tags({"labels": None}, []))
        self.assertTrue(p._matches_tags({"labels": []}, []))
        self.assertEqual(p._tag_list(""), [])
        self.assertEqual(p._tag_list("   "), [])

    def test_tag_parse(self):
        p = make_plugin()
        self.assertEqual(
            p._tag_list("bug, enhancement，优化"), ["bug", "enhancement", "优化"]
        )

    def test_matches_tags_case_insensitive(self):
        p = make_plugin()
        self.assertTrue(p._matches_tags(self.ISSUE_BUG, ["BUG"]))
        self.assertFalse(p._matches_tags(self.ISSUE_FEATURE, ["bug"]))

    def test_issue_tags_filter_in_check(self):
        p = make_plugin(issue_tags="bug")
        p._state["repos"]["o/r"] = self._repo_state()
        p._state["initialized"].add("o/r")
        p._session = FakeSession({
            "/issues": (200, [C(self.ISSUE_BUG), C(self.ISSUE_FEATURE)]),
            "/pulls": (200, [C(self.PR_BUG), C(self.PR_NO_LABEL)]),
        })
        updates = asyncio.run(p._check_repo_updates("o/r"))
        kinds = {u["type"] for u in updates}
        # issue_tags=bug：仅带 bug 标签的 Issue 保留；pull_tags 为空 → PR 全通过
        self.assertEqual(kinds, {"issue", "pr"})
        issue_group = next(u for u in updates if u["type"] == "issue")
        self.assertEqual(len(issue_group["items"]), 1)
        self.assertEqual(issue_group["items"][0]["number"], 2)
        # 基线照常推进（被标签过滤的不重复推送）
        self.assertEqual(p._state["repos"]["o/r"]["last_issue"], 2)
        self.assertEqual(p._state["repos"]["o/r"]["last_pr"], 12)

    def test_pull_tags_filter_in_check(self):
        p = make_plugin(pull_tags="bug")
        p._state["repos"]["o/r"] = self._repo_state()
        p._state["initialized"].add("o/r")
        p._session = FakeSession({
            "/issues": (200, [C(self.ISSUE_BUG), C(self.ISSUE_FEATURE)]),
            "/pulls": (200, [C(self.PR_BUG), C(self.PR_NO_LABEL)]),
        })
        updates = asyncio.run(p._check_repo_updates("o/r"))
        kinds = {u["type"] for u in updates}
        self.assertEqual(kinds, {"issue", "pr"})
        pr_group = next(u for u in updates if u["type"] == "pr")
        self.assertEqual(len(pr_group["items"]), 1)
        self.assertEqual(pr_group["items"][0]["number"], 12)

    def test_tags_no_match_still_advances_baseline(self):
        p = make_plugin(issue_tags="nonsense", pull_tags="nonsense")
        p._state["repos"]["o/r"] = self._repo_state()
        p._state["initialized"].add("o/r")
        p._session = FakeSession({
            "/issues": (200, [C(self.ISSUE_BUG), C(self.ISSUE_FEATURE)]),
            "/pulls": (200, [C(self.PR_BUG), C(self.PR_NO_LABEL)]),
        })
        updates = asyncio.run(p._check_repo_updates("o/r"))
        self.assertEqual(updates, [])
        # 基线推进：被过滤条目不会重复推送
        self.assertEqual(p._state["repos"]["o/r"]["last_issue"], 2)
        self.assertEqual(p._state["repos"]["o/r"]["last_pr"], 12)


class TestDailyReport(unittest.TestCase):
    """每日趋势日报：统计累加、文本生成、定时发送、手动触发、30 天截断"""

    def test_bump_daily_stats_accumulates(self):
        p = make_plugin()
        p._bump_daily_stats("o/r", {"commits": 2, "issues": 1})
        p._bump_daily_stats("o/r", {"commits": 1, "prs": 3})
        day = p._daily_stats["o/r"][dstr()]
        self.assertEqual(day["commits"], 3)
        self.assertEqual(day["issues"], 1)
        self.assertEqual(day["prs"], 3)
        self.assertEqual(day["releases"], 0)

    def test_build_daily_report_content(self):
        p = make_plugin()
        p._state["repos"] = {"o/r": {}, "o/r2": {}}
        p._daily_stats = {
            "o/r": {dstr(-1): {"commits": 3, "releases": 1, "tags": 0, "issues": 2, "prs": 1}},
            "o/r2": {dstr(-1): {"commits": 0, "releases": 0, "tags": 0, "issues": 0, "prs": 0}},
        }
        p._trends = {
            "o/r": {dstr(-2): 100, dstr(-1): 110},
            "o/r2": {dstr(-1): 120},  # 无更新统计但有 Star 记录 → 也应展示
        }
        report = p._build_daily_report()
        self.assertIn("每日趋势日报", report)
        self.assertIn(dstr(-1), report)
        self.assertIn("o/r", report)
        self.assertIn("提交 3", report)
        self.assertIn("Issue 2", report)
        self.assertIn("PR 1", report)
        self.assertIn("100 → 110", report)
        # 无统计但有 Star 记录的仓库也展示
        self.assertIn("o/r2", report)

    def test_build_daily_report_no_data(self):
        p = make_plugin()
        p._state["repos"] = {"o/r": {}}
        report = p._build_daily_report()
        self.assertIn("无任何数据", report)

    def test_build_daily_report_no_repos(self):
        p = make_plugin()
        self.assertEqual(p._build_daily_report(), "")

    def test_build_daily_report_star_no_prev(self):
        p = make_plugin()
        p._state["repos"] = {"o/r": {}}
        p._trends = {"o/r": {dstr(-1): 110}}
        report = p._build_daily_report()
        self.assertIn("无前日对比", report)

    def test_cmd_daily_manual(self):
        p = make_plugin()
        p._state["repos"] = {"o/r": {}}
        p._daily_stats = {
            "o/r": {dstr(-1): {"commits": 5, "releases": 0, "tags": 0, "issues": 0, "prs": 0}}
        }
        result = asyncio.run(p._cmd_daily(FakeEvent(), []))
        self.assertIn("提交 5", result[0].text)
        # 手动触发后标记当日已发送，定时任务不再重复
        self.assertEqual(p._daily_last_sent, dstr())

    def test_cmd_daily_no_repos(self):
        p = make_plugin()
        result = asyncio.run(p._cmd_daily(FakeEvent(), []))
        self.assertIn("没有监控任何仓库", result[0].text)

    def test_maybe_send_daily_after_time(self):
        p = make_plugin(daily_report_time="00:00")  # 必然已过
        p._state["repos"] = {"o/r": {}}
        p._daily_stats = {
            "o/r": {dstr(-1): {"commits": 2, "releases": 0, "tags": 0, "issues": 0, "prs": 0}}
        }
        sent = []

        async def fake_send(msg):
            sent.append(msg)

        p._send_to_subscribers = fake_send
        asyncio.run(p._maybe_send_daily_report())
        self.assertEqual(len(sent), 1)
        self.assertIn("提交 2", sent[0])
        self.assertEqual(p._daily_last_sent, dstr())
        # 同日再次调用不重复发送
        asyncio.run(p._maybe_send_daily_report())
        self.assertEqual(len(sent), 1)

    def test_maybe_send_disabled(self):
        p = make_plugin(daily_report_enabled=False, daily_report_time="00:00")
        p._state["repos"] = {"o/r": {}}
        p._daily_stats = {
            "o/r": {dstr(-1): {"commits": 1, "releases": 0, "tags": 0, "issues": 0, "prs": 0}}
        }
        sent = []

        async def fake_send(msg):
            sent.append(msg)

        p._send_to_subscribers = fake_send
        asyncio.run(p._maybe_send_daily_report())
        self.assertEqual(sent, [])
        self.assertEqual(p._daily_last_sent, "")

    def test_maybe_send_invalid_time_falls_back(self):
        p = make_plugin(daily_report_time="abc")
        p._state["repos"] = {"o/r": {}}
        p._daily_stats = {
            "o/r": {dstr(-1): {"commits": 1, "releases": 0, "tags": 0, "issues": 0, "prs": 0}}
        }
        sent = []

        async def fake_send(msg):
            sent.append(msg)

        p._send_to_subscribers = fake_send
        try:
            asyncio.run(p._maybe_send_daily_report())
        except Exception as e:
            self.fail(f"非法时间应回退默认值而非抛异常: {e}")
        # 非法时间回退 09:00：当前时间未到 09:00 则不发送，已到则发送（两者皆合理）
        if datetime.now(TIMEZONE_BJ).hour < 9:
            self.assertEqual(sent, [])
        else:
            self.assertEqual(p._daily_last_sent, dstr())

    def test_daily_stats_save_trim_and_reload(self):
        p = make_plugin()
        p._daily_stats = {
            "o/r": {
                dstr(-40): {"commits": 1, "releases": 0, "tags": 0, "issues": 0, "prs": 0},
                dstr(-1): {"commits": 2, "releases": 0, "tags": 0, "issues": 0, "prs": 0},
            },
        }
        p._daily_last_sent = dstr()
        p._save_daily_stats()
        self.assertNotIn(dstr(-40), p._daily_stats["o/r"])
        self.assertIn(dstr(-1), p._daily_stats["o/r"])
        # 重新加载：元字段与截断生效
        p2 = make_plugin()
        p2._daily_stats_file = p._daily_stats_file
        p2._daily_stats = p2._load_daily_stats()
        self.assertEqual(p2._daily_last_sent, dstr())
        self.assertNotIn(dstr(-40), p2._daily_stats["o/r"])
        self.assertIn(dstr(-1), p2._daily_stats["o/r"])

    def test_daily_stats_bump_through_check(self):
        # 端到端：检查更新后 daily_stats 记录真实新增数（即使被关键词过滤仍计数）
        p = make_plugin(keyword_filters="nonsense")
        p._state["repos"]["o/r"] = {
            "last_commit": "abc123", "last_release": "v1.0.0", "last_tag": "v1.0.0",
        }
        p._state["initialized"].add("o/r")
        p._session = FakeSession({
            "/commits": (200, [C(COMMIT2), C(COMMIT1)]),
            "/releases": (200, [C(RELEASE)]),
            "/tags": (200, [C(TAG)]),
        })
        asyncio.run(p._check_repo_updates("o/r"))
        day = p._daily_stats["o/r"][dstr()]
        self.assertEqual(day["commits"], 1)
        self.assertEqual(day["releases"], 1)
        self.assertEqual(day["tags"], 1)


if __name__ == "__main__":
    unittest.main()
