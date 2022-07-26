import functools
import unittest
import uuid


class MockRemoteSession:
    def __init__(self):
        self.hooks = {}

    def set_hook(self, name, hook):
        self.hooks[name] = hook

    def __getattr__(self, item):
        if not item in self.hooks:
            raise LookupError(
                f"{item} is not defined as a method hook in MockRemoteSession"
            )
        return functools.partial(self.hooks[item])


class PlanOnlyTestFixture(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.connect = MockRemoteSession()
        cls.tbl_name = f"tbl{uuid.uuid4()}".replace("-", "")
