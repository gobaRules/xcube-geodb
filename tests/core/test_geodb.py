import json
import unittest
import requests_mock
from geopandas import GeoDataFrame

from dcfs_geodb.core.geo_db import GeoDB


class GeoDBTest(unittest.TestCase):
    def setUp(self) -> None:
        self._server_test_url = "http://test"
        self._server_test_port = 3000

        self._api = GeoDB(server_url=self._server_test_url, server_port=self._server_test_port)

    def tearDown(self) -> None:
        pass

    @requests_mock.mock()
    def test_create_dataset(self, m):
        expected_response = 'Success'
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_create_dataset"
        m.post(url, text=json.dumps(expected_response))

        res = self._api.create_dataset('test', {'columns': [{'name': 'test_col', 'type': 'inger'}]})
        self.assertTrue(res)

    @requests_mock.mock()
    def test_drop_dataset(self, m):
        expected_response = 'Success'
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_drop_dataset"
        m.post(url, text=json.dumps(expected_response))

        res = self._api.drop_dataset('test', {'columns': [{'name': 'test_col', 'type': 'integer'}]})
        self.assertTrue(res)

    @requests_mock.mock()
    def test_add_properties(self, m):
        expected_response = 'Success'
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_add_properties"
        m.post(url, text=json.dumps(expected_response))

        res = self._api.add_properties('test', {'columns': [{'name': 'test_col', 'type': 'integer'}]})
        self.assertTrue(res)

    @requests_mock.mock()
    def test_query_by_bbox(self, m):
        expected_response = [{'src': [{'id': 'dd', 'geometry': "0103000020D20E000001000000110000007593188402B51B41B6F3FDD4423FF6405839B4C802B51B412B8716D9EC3EF6406F1283C0EBB41B41A8C64B37C53EF640B6F3FDD4E4B41B419A999999A33EF6400E2DB29DCFB41B41EE7C3F35B63EF6407F6ABC74C0B41B41EE7C3F35B63EF6407B14AE47BDB41B41AAF1D24D043FF6408B6CE77B64B41B413F355EBA8F3FF6402B8716D970B41B41986E1283EC3FF640A4703D0A76B41B4179E92631AE3FF6404260E5D08AB41B4123DBF97E923FF6409EEFA7C69CB41B4100000000AC3FF6405839B448B3B41B411D5A643B973FF6408195438BC6B41B41666666666C3FF640D122DBF9E3B41B4139B4C876383FF640E9263188F8B41B41333333333D3FF6407593188402B51B41B6F3FDD4423FF640"}]}]

        url = "http://test:3000/rpc/get_by_bbox"
        m.post(url, text=json.dumps(expected_response))

        gdf = self._api.get_by_bbox(dataset='dataset', minx=452750.0, miny=88909.549, maxx=464000.0, maxy=102486.299)

        res = gdf.to_dict()
        id = res['id'][0]
        geo = res['geometry'][0]
        exp_geo = "POLYGON ((453952.629 91124.177, 453952.696 91118.803, 453946.938 91116.326, 453945.208 91114.22500000001, 453939.904 91115.38800000001, 453936.114 91115.38800000001, 453935.32 91120.269, 453913.121 91128.98299999999, 453916.212 91134.78200000001, 453917.51 91130.887, 453922.704 91129.156, 453927.194 91130.75, 453932.821 91129.452, 453937.636 91126.77499999999, 453944.994 91123.52899999999, 453950.133 91123.825, 453952.629 91124.177))"
        self.assertIsInstance(gdf, GeoDataFrame)
        self.assertEqual(id, 'dd')
        self.assertEqual(str(geo), exp_geo)










