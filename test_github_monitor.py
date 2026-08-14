# -*- coding: utf-8 -*-
"""github_monitor 插件单元测试：仓库解析、时间格式化、更新检测去重、并发广播"""
import asyncio
import copy
import os
import sys
import tempfile
import unittest

sys.path.insert(0, r"D:\astrbot\data\plugins\astrbot_plugin_github_monitor")
sys.path.insert(0, r"D:\astrbot\data\plugins")

from main import GitHubMonitorPlugin, TIMEZONE_BJ  # noqa: E402


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
    # 隔离持久化：重定向到临时目录，避免污染真实 state.json
    tmp = tempfile.mkdtemp(prefix="gh_monitor_test_")
    p._state_file = os.path.join(tmp, "state.json")
    p._state = {
        "token": "",
        "repos": {},
        "subscribers": [],
        "initialized": set(),
    }
    return p


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

    def test_long_message_truncated(self):
        p = make_plugin()
        items = [{"type": "commit", "items": [{**C(COMMIT1)} for _ in range(30)]}]
        msg = p._build_update_message("o/r", items)
        self.assertIn("已折叠显示", msg)


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
        # 单仓库异常不影响其他仓库推送
        self.assertEqual(len(sent), 1)


if __name__ == "__main__":
    unittest.main()
