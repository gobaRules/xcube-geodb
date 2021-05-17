import json
import os
import unittest
from io import StringIO

import pandas
import pandas as pd
import requests_mock
from geopandas import GeoDataFrame
from pandas import DataFrame
from shapely import wkt

from xcube_geodb.core.collections import Collections
from xcube_geodb.core.geodb import GeoDBClient, GeoDBError

TEST_GEOM = "0103000020D20E000001000000110000007593188402B51B4" \
            "1B6F3FDD4423FF6405839B4C802B51B412B8716D9EC3EF6406" \
            "F1283C0EBB41B41A8C64B37C53EF640B6F3FDD4E4B41B419A999" \
            "999A33EF6400E2DB29DCFB41B41EE7C3F35B63EF6407F6ABC" \
            "74C0B41B41EE7C3F35B63EF6407B14AE47BDB41B41AAF1D24D" \
            "043FF6408B6CE77B64B41B413F355EBA8F3FF6402B8716D970" \
            "B41B41986E1283EC3FF640A4703D0A76B41B4179E92631AE3F" \
            "F6404260E5D08AB41B4123DBF97E923FF6409EEFA7C69CB41" \
            "B4100000000AC3FF6405839B448B3B41B411D5A643B973FF6" \
            "408195438BC6B41B41666666666C3FF640D122DBF9E3B41B4" \
            "139B4C876383FF640E9263188F8B41B41333333333D3FF64075" \
            "93188402B51B41B6F3FDD4423FF640"


@requests_mock.mock(real_http=False)
class GeoDBClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self._api = GeoDBClient(dotenv_file="tests/envs/.env_ci", config_file="tests/.geodb")

        self._server_test_url = self._api._server_url
        self._server_test_port = self._api._server_port
        self._server_full_address = self._server_test_url
        if self._server_test_port:
            self._server_full_address += ':' + str(self._server_test_port)

        self._server_test_auth_domain = "https://xcube-users.brockmann-consult.de/api/v2"

        os.environ['GEODB_AUTH0_CONFIG_FILE'] = 'ipyauth-auth0-demo_test.env'
        os.environ['GEODB_AUTH0_CONFIG_FOLDER'] = 'tests/envs/'

    def tearDown(self) -> None:
        pass

    def set_global_mocks(self, m):
        m.post(self._server_test_auth_domain + "/oauth/token", json={
            "access_token": "A long lived token",
            "expires_in": 12345
        })

        url = f"{self._server_full_address}/rpc/geodb_whoami"
        m.get(url, text=json.dumps("helge"))

        url = f"{self._server_full_address}/rpc/geodb_get_collection_srid"
        m.post(url, text=json.dumps([{"src": [{"srid": 4326}]}]))

    def set_auth_change_mocks(self, m):
        m.post(self._server_test_auth_domain + "/oauth/token", json={
            "access_token": "A long lived token but a different user",
            "expires_in": 12345
        })

        url = f"{self._server_full_address}/rpc/geodb_whoami"
        m.get(url, text=json.dumps("pope"))

        self._api._auth_client_id = "fsvsdv"

    def test_my_usage(self, m):
        # self.set_global_mocks(m)

        m.post(self._server_test_auth_domain + "/oauth/token", json={
            "access_token": "A long lived token",
            "expires_in": 12345
        })

        expected_response = {'usage': "10MB"}
        server_response = [{'src': [expected_response]}]
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_get_my_usage"
        m.post(url, text=json.dumps(server_response))

        res = self._api.get_my_usage()
        self.assertDictEqual(expected_response, res)

        expected_response = {'usage': "10000"}
        server_response = [{'src': [expected_response]}]
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_get_my_usage"
        m.post(url, text=json.dumps(server_response))

        res = self._api.get_my_usage(pretty=False)
        self.assertDictEqual(expected_response, res)

    def test_get_my_collections(self, m):
        self.set_global_mocks(m)

        server_response = [
            {
                "table_name": "geodb_admin_land_use",
                "grantee": "geodb_admin"
            },
            {
                "table_name": "geodb_admin_land_use",
                "grantee": "PUBLIC"
            },
        ]

        server_response = [{"src": server_response}]

        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_get_my_collections"
        m.post(url, text=json.dumps(server_response))

        res = self._api.get_my_collections()
        self.assertIsInstance(res, pandas.DataFrame)
        res = res.to_dict()
        expected_response = {'table_name': {0: 'geodb_admin_land_use', 1: 'geodb_admin_land_use'},
                             'grantee': {0: 'geodb_admin', 1: 'PUBLIC'}}
        self.assertDictEqual(expected_response, res)

    def test_get_collection(self, m):
        self.set_global_mocks(m)
        global TEST_GEOM
        expected_response = [
            {"id": 1, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 2, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 3, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 4, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 5, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 6, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 7, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 8, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 9, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
            {"id": 10, "created_at": "2020-04-08T13:08:06.733626+00:00", "modified_at": None,
             "geometry": TEST_GEOM,
             "d_od": "2019-03-26"},
        ]
        url = f"{self._server_test_url}:{self._server_test_port}/helge_test"
        m.get(url, text=json.dumps(expected_response))

        r = self._api.get_collection('test')
        self.assertIsInstance(r, GeoDataFrame)
        self.assertTrue('geometry' in r)

        r = self._api.head_collection('test')
        self.assertIsInstance(r, GeoDataFrame)
        self.assertTrue('geometry' in r)
        self.assertEqual(10, r.shape[0])

    def test_auth(self, m):
        self.set_global_mocks(m)

        cfg_file = "tests/.geodb"
        expected_response = False
        auth_access_token = self._api.auth_access_token
        self.assertEqual("A long lived token", auth_access_token)

        self.set_auth_change_mocks(m)
        auth_access_token = self._api.auth_access_token
        self.assertEqual('A long lived token but a different user', auth_access_token)

    def test_create_database(self, m):
        expected_response = True
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_create_database"
        m.post(url, text=json.dumps(expected_response))
        self.set_global_mocks(m)

        res = self._api.create_database(database='test')
        self.assertTrue(res)

    def test_truncate_database(self, m):
        expected_response = True
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_truncate_database"
        m.post(url, text=json.dumps(expected_response))
        self.set_global_mocks(m)

        res = self._api.truncate_database(database='test')
        self.assertEqual('<h1>Database test truncated</h1>', str(res))

    def test_create_collection(self, m):
        expected_response = 'Success'
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_create_database"
        m.post(url, text=json.dumps(expected_response))
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_create_collections"
        m.post(url, text=json.dumps(expected_response))
        url = f"{self._server_test_url}:{self._server_test_port}/geodb_user_databases?name=eq.helge"
        m.get(url, text=json.dumps('helge'))
        self.set_global_mocks(m)

        res = self._api.create_collection(collection='test', properties={'test_col': 'inger'})
        self.assertTrue(res)

    def test_create_collection2(self, m):
        expected_response = 'Success'
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_create_database"
        m.post(url, text=json.dumps(expected_response))
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_create_collections"
        m.post(url, text=json.dumps(expected_response))
        url = f"{self._server_test_url}:{self._server_test_port}/geodb_user_databases?name=eq.helge"
        m.get(url, text=json.dumps('helge'))
        self.set_global_mocks(m)

        res = self._api.create_collection(collection='test', properties={'test_col': 'inger'})
        self.assertTrue(res)

    def test_create_collections(self, m):
        expected_response = {'collections': {'helge_land_use3': {'crs': 3794,
                                                                 'properties': {'D_OD': 'date',
                                                                                'RABA_ID': 'float',
                                                                                'RABA_PID': 'float'}}}}
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_create_database"
        m.post(url, text=json.dumps(expected_response))
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_create_collections"
        m.post(url, text=json.dumps(expected_response))
        url = f"{self._server_test_url}:{self._server_test_port}/geodb_user_databases?name=eq.helge"
        m.get(url, text=json.dumps('helge'))
        self.set_global_mocks(m)

        collections = {
            "land_use3":
                {
                    "crs": 3794,
                    "properties":
                        {
                            "RABA_PID": "float",
                            "RABA_ID": "float",
                            "D_OD": "date"
                        }
                }
        }

        res = self._api.create_collections(collections=collections)
        self.assertIsInstance(res, Collections)
        self.assertDictEqual(expected_response, res.config)

        with self.assertWarns(DeprecationWarning) as e:
            self._api.create_collections(collections=collections, namespace='helge')
        msg = "Call to deprecated parameter 'namespace' in function 'create_collections'. Use 'database' instead. "
        self.assertEqual(msg, str(e.warning))

    def test_drop_collection(self, m):
        expected_response = 'Success'
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_drop_collections"
        m.post(url, text=json.dumps(expected_response))
        self.set_global_mocks(m)

        res = self._api.drop_collection('test')
        self.assertTrue(res)

    def test_add_properties(self, m):
        self.set_global_mocks(m)

        expected_response = 'Success'
        url = f"{self._server_test_url}:{self._server_test_port}/rpc/geodb_add_properties"
        m.post(url, text=json.dumps(expected_response))

        # noinspection PyTypeChecker
        res = self._api.add_properties('test', [{'name': 'test_col', 'type': 'insssssteger'}])
        self.assertTrue(res)

    def test_filter_by_bbox(self, m):
        expected_response = {'src': [{'id': 'dd', 'geometry': "0103000020D20E000001000000110000007593188402B51B4"
                                                              "1B6F3FDD4423FF6405839B4C802B51B412B8716D9EC3EF6406"
                                                              "F1283C0EBB41B41A8C64B37C53EF640B6F3FDD4E4B41B419A999"
                                                              "999A33EF6400E2DB29DCFB41B41EE7C3F35B63EF6407F6ABC"
                                                              "74C0B41B41EE7C3F35B63EF6407B14AE47BDB41B41AAF1D24D"
                                                              "043FF6408B6CE77B64B41B413F355EBA8F3FF6402B8716D970"
                                                              "B41B41986E1283EC3FF640A4703D0A76B41B4179E92631AE3F"
                                                              "F6404260E5D08AB41B4123DBF97E923FF6409EEFA7C69CB41"
                                                              "B4100000000AC3FF6405839B448B3B41B411D5A643B973FF6"
                                                              "408195438BC6B41B41666666666C3FF640D122DBF9E3B41B4"
                                                              "139B4C876383FF640E9263188F8B41B41333333333D3FF64075"
                                                              "93188402B51B41B6F3FDD4423FF640"}]}

        url = f"{self._server_test_url}:{self._server_test_port}"
        path = "/rpc/geodb_get_by_bbox"

        self.set_global_mocks(m)

        m.get(url=url, text=json.dumps({'paths': ['/rpc/geodb_get_by_bbox'],
                                        'definitions': ['helge_collection']}))

        m.get(url=url + '/helge_collection?limit=10', json={'test': 1})

        m.post(url + path, text=json.dumps(expected_response))

        gdf = self._api.get_collection_by_bbox(collection='collection',
                                               bbox=(452750.0, 88909.549, 464000.0, 102486.299))

        res = gdf.to_dict()
        ident = res['id'][0]
        geo = res['geometry'][0]
        exp_geo = "POLYGON ((453952.629 91124.177, 453952.696 91118.803, 453946.938 91116.326, " \
                  "453945.208 91114.22500000001, 453939.904 91115.38800000001, 453936.114 91115.38800000001, " \
                  "453935.32 91120.269, 453913.121 91128.98299999999, 453916.212 91134.78200000001, " \
                  "453917.51 91130.887, 453922.704 91129.156, 453927.194 91130.75, 453932.821 91129.452, " \
                  "453937.636 91126.77499999999, 453944.994 91123.52899999999, 453950.133 91123.825, " \
                  "453952.629 91124.177))"
        self.assertIsInstance(gdf, GeoDataFrame)
        self.assertEqual(ident, 'dd')
        self.assertEqual(str(geo), exp_geo)

    def test_reproject_bbox(self, m):
        bbox_4326 = (9.8, 53.51, 10.0, 53.57)
        crs_4326 = 4326

        bbox_3857 = (1090931.0097740812, 7077896.970141199, 1113194.9079327357, 7089136.418602032)
        crs_3857 = 3857

        bbox = GeoDBClient.transform_bbox_crs(bbox=bbox_4326,
                                              from_crs=crs_4326, to_crs=crs_3857, wsg84_order="lat_lon")

        for i in range(4):
            self.assertAlmostEquals(bbox_3857[i], bbox[i])

        bbox = GeoDBClient.transform_bbox_crs(bbox=bbox_3857,
                                              from_crs=crs_3857, to_crs=crs_4326, wsg84_order="lat_lon")

        for i in range(4):
            self.assertAlmostEquals(bbox_4326[i], bbox[i])

    def test_reproject_bbox_lon_lat(self, m):
        bbox_4326 = (53.51, 9.8, 53.57, 10.0)
        crs_4326 = 4326

        bbox_3857 = (1090931.0097740812, 7077896.970141199, 1113194.9079327357, 7089136.418602032)
        crs_3857 = 3857

        bbox = GeoDBClient.transform_bbox_crs(bbox=bbox_4326,
                                              from_crs=crs_4326, to_crs=crs_3857, wsg84_order="lon_lat")

        for i in range(4):
            self.assertAlmostEquals(bbox_3857[i], bbox[i])

        bbox = GeoDBClient.transform_bbox_crs(bbox=bbox_3857,
                                              from_crs=crs_3857, to_crs=crs_4326, wsg84_order="lon_lat")

        for i in range(4):
            self.assertAlmostEquals(bbox_4326[i], bbox[i])

    def test_delete_from_collection(self, m):
        path = '/helge_tt?id=eq.1'
        expected_response = 'success'

        self.set_global_mocks(m)

        url = self._server_full_address + path
        m.delete(url, text=expected_response)

        r = self._api.delete_from_collection('tt', 'id=eq.1')

        self.assertTrue(r)

    def test_update_collection(self, m):
        m.get(url=self._server_full_address + '/helge_tt?limit=10', json={'test': 1})
        path = '/helge_tt?id=eq.1'
        expected_response = 'success'

        m.get(url=self._server_full_address + "/", text=json.dumps({'definitions': ['helge_tt']}))
        m.patch(self._server_full_address + path, text=expected_response)
        self.set_global_mocks(m)

        values = {'column': 200}
        r = self._api.update_collection('tt', values, 'id=eq.1')

        self.assertTrue(r)

        with self.assertRaises(ValueError) as e:
            # noinspection PyTypeChecker
            self._api.update_collection('tt', [1, 2, 3], 'id=eq.1')

        self.assertEqual(str(e.exception), "Format <class 'list'> not supported.")

    # noinspection PyMethodMayBeStatic
    def make_test_df(self):
        csv = ("\n"
               "column1,column2,geometry\n\n"
               "1,ì,POINT(10 10)\n\n"
               "3,b,POINT(10 10)\n\n")

        for i in range(11000):
            csv += f"{i},b,POINT(10 10)\n\n"
        file = StringIO(csv)

        df = pd.read_csv(file)
        df['geometry'] = df['geometry'].apply(wkt.loads)
        return df
        # return GeoDataFrame(df, crs={'init': 'epsg:4326'}, geometry=df['geometry'])

    def test_insert_into_collection(self, m):
        path = '/helge_tt'
        expected_response = 'success'

        m.get(url=self._server_full_address, text=json.dumps({'definitions': ['tt']}))
        m.post(self._server_full_address + path, text=expected_response)
        self.set_global_mocks(m)

        df = self.make_test_df()
        values = GeoDataFrame(df, crs={'init': 'epsg:4326'}, geometry=df['geometry'])

        r = self._api.insert_into_collection('tt', values)

        self.assertEqual('<h1>11002 rows inserted into tt</h1>', str(r))

        with self.assertRaises(ValueError) as e:
            # noinspection PyTypeChecker
            self._api.insert_into_collection('tt', [1, 2, 3])

        self.assertEqual("Error: Format <class 'list'> not supported.", str(e.exception))

    def test_list_grants(self, m):
        path = '/rpc/geodb_list_grants'
        expected_response = [{'src': [{'table_name': 'test', 'grantee': 'ernie'}]}]

        m.post(self._server_full_address + path, json=expected_response)
        self.set_global_mocks(m)

        r = self._api.list_my_grants()

        self.assertEqual('test', r.table_name[0])
        self.assertEqual('ernie', r.grantee[0])
        self.assertIsInstance(r, DataFrame)

        expected_response = []

        m.post(self._server_full_address + path, json=expected_response)
        self.set_global_mocks(m)

        r = self._api.list_my_grants()

        self.assertEqual('No Grants', r.Grants[0])
        self.assertIsInstance(r, DataFrame)

        expected_response = 'vijdasovjidasjo'

        m.post(self._server_full_address + path, text=expected_response)
        self.set_global_mocks(m)

        with self.assertRaises(GeoDBError) as e:
            self._api.list_my_grants()

        self.assertEqual("Could not read response from GeoDB. Expecting value: line 1 column 1 (char 0)",
                         str(e.exception))

    @unittest.skip("Not yet implemented")
    def test_register_user_to_geoserver(self, m):
        m.post(self._server_full_address + '/rpc/geodb_register_user', text="success")
        self.set_global_mocks(m)

        # self._api.register_user_to_geoserver('mama', 'mamaspassword')

    def test_filter_raw(self, m):
        m.get(url=self._server_full_address + '/helge_test?limit=10', json={'test': 1})
        m.get(url=self._server_full_address + '/helge_tesdsct?limit=10', json={}, status_code=404)
        m.get(url=self._server_full_address + "/", text=json.dumps({'definitions': ['helge_test'],
                                                                    'paths': ['/rpc/geodb_get_pg']}))

        expected_result = {'src': []}
        m.post(self._server_full_address + '/rpc/geodb_get_pg', json=expected_result)

        self.set_global_mocks(m)

        with self.assertRaises(GeoDBError) as e:
            self._api.get_collection_pg('tesdsct', select='min(tt)', group='tt', limit=1, offset=2)

        self.assertEqual("Collection tesdsct does not exist", str(e.exception))

        expected_result = {'src': [{'count': 142, 'D_OD': '2019-03-21'}, {'count': 114, 'D_OD': '2019-02-20'}]}
        m.post(self._server_full_address + '/rpc/geodb_get_pg', json=expected_result)

        r = self._api.get_collection_pg('test', select='count(D_OD)', group='D_OD', limit=1, offset=2)
        self.assertIsInstance(r, DataFrame)
        self.assertEqual((2, 2), r.shape)

        expected_result = {'src': [{
            'id': 11,
            'created_at': '2020-01-20T14:45:30.763162+00:00',
            'modified_at': None,
            'geometry': '0103000020D20E0000010000001100000046B6F3FDA7151C417D3F355ECE58F740DD2406013C151C410E2DB29DC'
                        '35BF740C74B3709E6141C41F6285C8F1C5EF740BE9F1A2F40141C417F6ABC748562F740894160E583141C417B14A'
                        'E472363F740EC51B81EB0141C415EBA490CE061F7405EBA498CCE141C41E5D022DB1961F7404E621058EA141C41AA'
                        'F1D24D6860F7402FDD248612151C41FED478E9585FF7404A0C022B1E151C4114AE47E1045FF7405839B4C860151C4'
                        '1DBF97E6A2A5DF74021B072E881151C41D122DBF9425CF74093180456A2151C41FED478E9845BF74075931884C3151'
                        'C415839B4C8B45AF7405EBA498CF3151C4191ED7C3FA159F740C3F528DCF1151C41F6285C8F7659F74046B6F3FDA71'
                        '51C417D3F355ECE58F740',
            'RABA_PID': 5983161,
            'RABA_ID': 1100,
            'D_OD': '2019-03-11'}]}
        m.post(self._server_full_address + '/rpc/geodb_get_pg', json=expected_result)

        r = self._api.get_collection_pg('test', limit=1, offset=2)
        self.assertIsInstance(r, GeoDataFrame)
        self.assertEqual((1, 7), r.shape)
        self.assertIs(True, 'geometry' in r)
        self.assertIs(True, 'id' in r)
        self.assertIs(True, 'created_at' in r)
        self.assertIs(True, 'modified_at' in r)

    def test_init(self, m):
        with self.assertRaises(NotImplementedError) as e:
            GeoDBClient(auth_mode='interactive')

        self.assertEqual("The interactive mode has not been implemented.", str(e.exception))

        # Interactive has been deactivated at this stage due to deployment struggles and it not used in any deployment
        # with self.assertRaises(FileExistsError) as e:
        #     os.environ['GEODB_AUTH0_CONFIG_FILE'] = 'bla.env'
        #     GeoDBClient(auth_mode='interactive')
        #
        # self.assertEqual("Mandatory auth configuration file ipyauth-auth0-demo.env must exist", str(e.exception))

        with self.assertRaises(ValueError) as e:
            GeoDBClient(auth_mode='interacti')

        self.assertEqual("auth_mode can only be 'interactive', 'password', or 'client-credentials'!", str(e.exception))

    def test_auth_token(self, m):
        m.post(self._server_test_auth_domain + "/oauth/token", json={"broken_access_token": "A long lived token"})

        with self.assertRaises(ValueError) as e:
            access_token = self._api.auth_access_token

        self.assertEqual("The authorization request did not return an access token.",
                         str(e.exception))

    def test_get_collection_info(self, m):
        self.set_global_mocks(m)

        expected_result = {'required': ['id', 'geometry'],
                           'properties': {'id': {'format': 'integer',
                                                 'type': 'integer',
                                                 'description': 'Note:\nThis is a Primary Key.<pk/>'},
                                          'created_at': {'format': 'timestamp with time zone', 'type': 'string'},
                                          'modified_at': {'format': 'timestamp with time zone', 'type': 'string'},
                                          'geometry': {'format': 'public.geometry(Geometry,3794)', 'type': 'string'},
                                          'raba_pid': {'format': 'double precision', 'type': 'number'}},
                           'type': 'object'}
        m.post(self._server_full_address + '/rpc/geodb_get_raw', json=expected_result)

        expected_result = {'id': 'integer'}
        m.get(url=self._server_full_address + "/", text=json.dumps({'definitions': {'helge_test': expected_result},
                                                                    'paths': ['/']}))
        res = self._api.get_collection_info('test')
        self.assertDictEqual(expected_result, res)

        with self.assertRaises(ValueError) as e:
            self._api.get_collection_info('test_not_exist')

        self.assertEqual("Table helge_test_not_exist does not exist.", str(e.exception))

    def test_namespace(self, m):
        self.set_global_mocks(m)

        geodb = GeoDBClient()
        self.assertEqual('helge', geodb.database)

        geodb = GeoDBClient(database='test')
        self.assertEqual('test', geodb.database)

    def test_auth_token_propery(self, m):
        self.set_global_mocks(m)

        geodb = GeoDBClient()
        geodb._auth_access_token = "testölasdjdkas"

        self.assertEqual("testölasdjdkas", geodb.auth_access_token)

        geodb._auth_access_token = None
        self.assertEqual("A long lived token", geodb.auth_access_token)

        geodb._auth_mode = "a mode"
        with self.assertRaises(GeoDBError) as e:
            token = geodb.auth_access_token

        self.assertEqual("System Error: auth mode unknown.", str(e.exception))
