import unittest

from xcube_geodb.core.geodb import GeoDBClient


class TestIterator(unittest.TestCase):
    def setUp(self) -> None:
        self._client = GeoDBClient(dotenv_file='tests/envs/.env')

    def test_iterator_get_collections(self):
        it = self._client.get_collection('land_use', it_page_size=50, query='order=id', iterate=True)
        page = 0
        for v in it:
            lower_bound = v["id"].min()
            upper_bound = v["id"].max()
            self.assertEqual(page * 50 + 1, lower_bound)
            self.assertEqual((page + 1) * 50, upper_bound)
            print(v)
            page += 1

        self.assertEqual(2, page)

    def test_iterator_get_collections2(self):
        it = self._client.get_collection('land_use', it_page_size=40, query='order=id', iterate=True)

        page = 0
        for v in it:
            page += 1

        self.assertEqual(3, page)
        self.assertEqual(20, len(v))

    def test_iterator_get_collections_stop(self):
        it = self._client.get_collection('land_use', it_page_size=10, it_stop_page=2, query='order=id', iterate=True)
        page = 0
        for v in it:
            lower_bound = v["id"].min()
            upper_bound = v["id"].max()
            self.assertEqual(page * 10 + 1, lower_bound)
            self.assertEqual((page + 1) * 10, upper_bound)
            page += 1

        self.assertEqual(3, page)

    def test_iterator_get_collections_start(self):
        it = self._client.get_collection('land_use', it_page_size=10, it_start_page=1, it_stop_page=2,
                                         query='order=id', iterate=True)
        page = 1
        for v in it:
            lower_bound = v["id"].min()
            upper_bound = v["id"].max()
            self.assertEqual(page * 10 + 1, lower_bound)
            self.assertEqual((page + 1) * 10, upper_bound)
            page += 1

        self.assertEqual(3, page)


if __name__ == '__main__':
    unittest.main()
