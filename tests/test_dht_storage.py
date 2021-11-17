""" Test our custom dht storage """
from unittest import IsolatedAsyncioTestCase, mock
import time

from ipo.daemon.routing.storage import (
    unpack_metrics,
    pack_metrics,
    RouteStorage,
    DistanceMetric,
)

time_patch = mock.patch('time.time_ns')  # We will simulate time moving forward one week..
REFTIME = time.time_ns()
HOUR = 60 * 60 * 10 ** 9
WEEK = 7 * 24 * HOUR


def make_ip(a = 10, b = 1, c = 2, d = 1) -> bytes:  # pylint: disable: invalid-name
    """ Create a packed IP address """
    return bytes([a, b, c, d])


def sort_metrics(metrics: dict[bytes, list[DistanceMetric]]) -> dict[bytes, list[DistanceMetric]]:
    """ Sort the metrics list """
    new_dict: dict[bytes, list[DistanceMetric]] = {}
    for k, v in metrics.items():
        new_dict[k] = list(sorted(v, key = lambda metric: metric.rtt))
    return new_dict


class TestStorage(IsolatedAsyncioTestCase):
    """ Test the custom kademlia storage """
    # Test metrics in unsorted order to simulate real inserts
    TEST_METRICS = {
        make_ip(b = 2): [
            DistanceMetric(20, 3, make_ip(c = 3), 1337, REFTIME - 2 * HOUR),
            DistanceMetric(6,  2, make_ip(c = 1), 1337, REFTIME - HOUR),     # noqa: E241
            DistanceMetric(10, 3, make_ip(c = 2), 1337, REFTIME - HOUR),
        ],
        make_ip(b = 4) : [
            DistanceMetric(15, 3, make_ip(c = 5), 1337, REFTIME - HOUR),
            DistanceMetric(25, 3, make_ip(c = 6), 1337, REFTIME - 2 * HOUR),
            DistanceMetric(5,  2, make_ip(c = 4), 1337, REFTIME - HOUR),     # noqa: E241
        ]
    }

    SORTED_METRICS = sort_metrics(TEST_METRICS)

    async def asyncSetUp(self):
        """ storage need running loop """
        self.storage = RouteStorage()  # pylint: disable=attribute-defined-outside-init
        # Set some default values
        for k, metrics in self.TEST_METRICS.items():
            for metric in metrics:
                self.storage[k] = pack_metrics([metric])

    def validate_metrics(self, key, comparison: list[DistanceMetric]):
        """ compare storage metrics to supplied comparison """
        store_metrics = unpack_metrics(self.storage[key])
        self.assertEqual(len(comparison), len(store_metrics), f'Metric count for {key} differs')
        for a, b in zip(comparison, store_metrics):
            self.assertEqual(a, b, 'Metrics differ')

    def test_contents(self):
        """ Test that the set-up contents are stored correctly """
        self.storage.cull()  # Might as well test it here
        for k, v in self.SORTED_METRICS.items():
            self.validate_metrics(k, v)
            self.validate_metrics(k, unpack_metrics(self.storage.get(k)))

    def test_append_first(self):
        """ Test appending first in list """
        KEY = next(self.SORTED_METRICS.keys().__iter__())
        metrics = self.SORTED_METRICS[KEY].copy()
        new = DistanceMetric(5, 1, make_ip(d = 10), 8888, REFTIME - HOUR)
        metrics.insert(0, new)
        self.storage[KEY] = pack_metrics([new])
        self.validate_metrics(KEY, metrics)

    def test_append_last(self):
        """ Test append last in list """
        KEY = next(self.SORTED_METRICS.keys().__iter__())
        metrics = self.SORTED_METRICS[KEY].copy()
        new = DistanceMetric(40, 1, make_ip(d = 10), 8888, REFTIME - HOUR)
        metrics.append(new)
        self.storage[KEY] = pack_metrics([new])
        self.validate_metrics(KEY, metrics)

    def test_append(self):
        """ Test appending a metric to an existing collection """
        it = self.SORTED_METRICS.keys().__iter__()
        _KEY1 = next(it)
        KEY2 = next(it)
        metrics = list(self.SORTED_METRICS[KEY2])
        new_metric = DistanceMetric(10, 3, make_ip(d = 10), 1337, REFTIME)
        metrics.insert(1, new_metric)  # As second item
        # And append to store
        self.storage[KEY2] = pack_metrics([new_metric])
        # And validate
        self.validate_metrics(KEY2, metrics)

    def test_cull(self):
        """ Test if cull purges the old entries as it should """
        with time_patch as tp:
            # Let one week pass
            tp.return_value = REFTIME + WEEK + HOUR
            new_metrics = pack_metrics([DistanceMetric(40, 3, make_ip(d = 10), 1337, REFTIME + WEEK)])
            KEY = make_ip(c = 100)
            self.storage[KEY] = new_metrics
            self.storage.cull()
            tp.assert_called()
            # The "old" values should be deprecated
            for k in self.SORTED_METRICS:
                self.assertIsNone(self.storage.get(k), f'Key {k} is still in store??')

            metrics = self.storage[KEY]
            self.assertEqual(new_metrics, metrics, 'New metrics don\'t match')
