from __future__ import annotations

from contextlib import contextmanager

from fastapi import FastAPI
from fastapi.testclient import TestClient


class ScriptedCursor:
    def __init__(self, steps: list[dict] | None = None):
        self.steps = list(steps or [])
        self.executed: list[dict] = []
        self._active_step: dict = {}
        self.rowcount = 0
        self.lastrowid = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql: str, params=None):
        self._run("execute", sql, params)

    def executemany(self, sql: str, params=None):
        self._run("executemany", sql, params)

    def fetchone(self):
        return self._active_step.get("fetchone")

    def fetchall(self):
        return self._active_step.get("fetchall", [])

    def _run(self, method: str, sql: str, params):
        normalized_sql = " ".join(sql.split())
        self.executed.append({"method": method, "sql": normalized_sql, "params": params})
        if not self.steps:
            raise AssertionError(f"Unexpected {method}: {normalized_sql}")

        step = self.steps.pop(0)
        expected_method = step.get("method", "execute")
        if expected_method != method:
            raise AssertionError(f"Expected {expected_method}, got {method}: {normalized_sql}")

        match = step.get("match")
        if match and match not in normalized_sql:
            raise AssertionError(f"Expected SQL containing {match!r}, got: {normalized_sql}")

        self.rowcount = step.get("rowcount", 0)
        self.lastrowid = step.get("lastrowid", self.lastrowid)
        self._active_step = step

        if step.get("raise") is not None:
            raise step["raise"]

    def assert_finished(self):
        assert not self.steps, f"Unconsumed SQL steps: {self.steps!r}"


class FakeConnection:
    def __init__(self, cursor: ScriptedCursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True

    def commit(self):
        return None

    def rollback(self):
        return None


def build_get_db(*connections: FakeConnection):
    queue = list(connections)

    @contextmanager
    def _get_db(*, autocommit: bool = True):
        if not queue:
            raise AssertionError("No fake database connections left")
        conn = queue.pop(0)
        yield conn

    return _get_db


def make_test_client(*routers) -> TestClient:
    app = FastAPI()
    for router in routers:
        app.include_router(router)
    return TestClient(app)
