import asyncio
import unittest
from datetime import datetime, timezone
import uploader

# Switch to IsolatedAsyncioTestCase in python >= 3.8
def async_test(coro):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()

        try:
            res = loop.run_until_complete(coro(*args, **kwargs))
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
        return res
    return wrapper


class MergeEquencesTest(unittest.TestCase):
    @async_test
    async def test_merge_sequences(self):
        async def gen1():
            yield (datetime(2020, 1, 1, tzinfo=timezone.utc), 0)
            yield (datetime(2020, 1, 4, tzinfo=timezone.utc), 0)
            yield (datetime(2020, 1, 6, tzinfo=timezone.utc), 0)

        async def gen2():
            yield (datetime(2020, 1, 2, tzinfo=timezone.utc), 1)
            yield (datetime(2020, 1, 3, tzinfo=timezone.utc), 1)
            yield (datetime(2020, 1, 5, tzinfo=timezone.utc), 1)

        seq = uploader.merge_sequences([gen1(), gen2()])

        self.assertEqual(await seq.__anext__(), (datetime(2020, 1, 1, tzinfo=timezone.utc), 0))
        self.assertEqual(await seq.__anext__(), (datetime(2020, 1, 2, tzinfo=timezone.utc), 1))
        self.assertEqual(await seq.__anext__(), (datetime(2020, 1, 3, tzinfo=timezone.utc), 1))
        self.assertEqual(await seq.__anext__(), (datetime(2020, 1, 4, tzinfo=timezone.utc), 0))
        self.assertEqual(await seq.__anext__(), (datetime(2020, 1, 5, tzinfo=timezone.utc), 1))
        self.assertEqual(await seq.__anext__(), (datetime(2020, 1, 6, tzinfo=timezone.utc), 0))

        with self.assertRaises(StopAsyncIteration):
            await seq.__anext__()

class DictGetTest(unittest.TestCase):
    def test_dict_get_shallow(self):
        test_dict = {
            'a': 10
        }
        self.assertEqual(uploader.dict_get(test_dict, 'a'), 10)

    def test_dict_get_deep(self):
        test_dict = {
            'a': {
                'b': {
                    'c': 10
                }
            }
        }
        self.assertEqual(uploader.dict_get(test_dict, 'a.b.c'), 10)

    def test_dict_get_none(self):
        test_dict = {
            'a': {
                'b': {
                    'c': 10
                }
            }
        }
        self.assertIsNone(uploader.dict_get(test_dict, 'a.b.d'))


if __name__ == '__main__':
    unittest.main()
