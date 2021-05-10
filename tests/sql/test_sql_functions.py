import os
import unittest
import json

import psycopg2


GEODB_EXT_INSTALLED = False


def get_app_dir():
    import inspect
    import xcube_geodb.version as version
    # noinspection PyTypeChecker
    version_path = inspect.getfile(version)
    return os.path.dirname(version_path)


def make_install_geodb():
    global GEODB_EXT_INSTALLED
    if not GEODB_EXT_INSTALLED:
        GEODB_EXT_INSTALLED = True
        from subprocess import call
        import os
        cwd = os.getcwd()

        app_path = get_app_dir()
        os.chdir(os.path.join(app_path, 'sql'))
        call(["make", "install"])

        os.chdir(cwd)


# noinspection SqlNoDataSourceInspection
# @unittest.skipIf(os.environ.get('SKIP_PSQL_TESTS', '1') == '1', 'DB Tests skipped')
# noinspection SqlInjection,SqlResolve
class GeoDBSqlTest(unittest.TestCase):

    @classmethod
    def setUp(cls) -> None:
        make_install_geodb()

        import psycopg2
        import testing.postgresql
        postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=False)

        cls._postgresql = postgresql()
        cls._conn = psycopg2.connect(**cls._postgresql.dsn())
        cls._cursor = cls._conn.cursor()
        app_path = get_app_dir()
        fn = os.path.join(app_path, '..', 'tests', 'sql', 'setup.sql')
        with open(fn) as sql_file:
            cls._cursor.execute(sql_file.read())

    def tearDown(self) -> None:
        self._postgresql.stop()

    def tearDownModule(self):
        # clear cached database at end of tests
        self._postgresql.clear_cache()

    def test_query_by_bbox(self):
        sql_filter = "SELECT geodb_get_by_bbox('postgres_land_use', 452750.0, 88909.549, 464000.0, " \
                     "102486.299, 'contains', 3794)"
        self._cursor.execute(sql_filter)

        res = self._cursor.fetchone()

        exp_geo = {'type': 'Polygon', 'coordinates': [
            [[453952.629, 91124.177], [453952.696, 91118.803], [453946.938, 91116.326], [453945.208, 91114.225],
             [453939.904, 91115.388], [453936.114, 91115.388], [453935.32, 91120.269], [453913.121, 91128.983],
             [453916.212, 91134.782], [453917.51, 91130.887], [453922.704, 91129.156], [453927.194, 91130.75],
             [453932.821, 91129.452], [453937.636, 91126.775], [453944.994, 91123.529], [453950.133, 91123.825],
             [453952.629, 91124.177]]]}
        self.assertEqual(len(res), 1)

        self.assertEqual(res[0][0]['id'], 1)
        self.assertDictEqual(res[0][0]['geometry'], exp_geo)

    def column_exists(self, table: str, column: str, data_type: str) -> bool:
        sql = (f'\n'
               f'                    SELECT EXISTS\n'
               f'                    (\n'
               f'                        SELECT 1\n'
               f'                        FROM "information_schema".columns\n'
               f'                        WHERE "table_schema" = \'public\'\n'
               f'                          AND "table_name"   = \'{table}\'\n'
               f'                          AND "column_name" = \'{column}\'\n'
               f'                          AND "data_type" = \'{data_type}\'\n'
               f'                    )\n'
               f'                         ;\n'
               f'            ')
        self._cursor.execute(sql)
        return self._cursor.fetchone()[0]

    def table_exists(self, table: str) -> bool:
        # noinspection SqlInjection
        sql = f"""SELECT EXISTS 
                        (
                            SELECT 1 
                            FROM pg_tables
                            WHERE schemaname = 'public'
                            AND tablename = '{table}'
                        );"""

        self._cursor.execute(sql)
        return self._cursor.fetchone()[0]

    def _set_role(self, user_name: str):
        sql = f"SET LOCAL ROLE \"{user_name}\""
        self._cursor.execute(sql)

    def test_manage_table(self):
        user_name = "geodb_9bfgsdfg-453f-445b-a459"
        user_table = f"{user_name}_test"
        self._set_role(user_name)

        props = {'tt': 'integer'}
        sql = f"SELECT geodb_create_collection('{user_table}', '{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)

        self.assertTrue(self.table_exists(user_table))

        self.assertTrue(self.column_exists(user_table, 'id', 'integer'))
        self.assertTrue(self.column_exists(user_table, 'geometry', 'USER-DEFINED'))

        datasets = {f'{user_name}_tt1': {'crs': '4326', 'properties': {'tt': 'integer'}},
                    f'{user_name}_tt2': {'crs': '4326', 'properties': {'tt': 'integer'}}}

        sql = f"SELECT geodb_create_collections('{json.dumps(datasets)}')"
        self._cursor.execute(sql)

        self.assertTrue(self.table_exists(user_table))

        self.assertTrue(self.column_exists(user_table, 'id', 'integer'))
        self.assertTrue(self.column_exists(user_table, 'geometry', 'USER-DEFINED'))

        datasets = [user_table, f'{user_name}_tt1', f'{user_name}_tt2']
        sql = f"SELECT geodb_drop_collections('{json.dumps(datasets)}')"
        self._cursor.execute(sql)
        self.assertFalse(self.table_exists(user_table))
        self.assertFalse(self.table_exists(f'{user_name}_tt1'))
        self.assertFalse(self.table_exists(f'{user_name}_tt2'))

    def test_manage_properties(self):
        user_name = "geodb_9bfgsdfg-453f-445b-a459"
        table = f"{user_name}_dummest"
        self._set_role(user_name)

        props = {'tt': 'integer'}
        sql = f"SELECT geodb_create_collection('{table}', '{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)

        cols = {'test_col1': 'integer', 'test_col2': 'integer'}

        sql = f"SELECT public.geodb_add_properties('{table}', '{json.dumps(cols)}'::json)"
        self._cursor.execute(sql)

        self.assertTrue(self.column_exists(table, 'test_col1', 'integer'))

        cols = ['test_col1', 'test_col2']

        sql = f"SELECT public.geodb_drop_properties('{table}', '{json.dumps(cols)}')"
        self._cursor.execute(sql)
        self.assertFalse(self.column_exists(table, 'test_col', 'integer'))

    def test_get_my_usage(self):
        user_name = "geodb_9bfgsdfg-453f-445b-a459"
        self._set_role(user_name)

        qry = "SELECT geodb_user_allowed('geodb_9bfgsdfg-453f-445b-a459_test_usage', 'geodb_9bfgsdfg-453f-445b-a459');"
        self._cursor.execute(qry)
        res = self._cursor.fetchone()

        props = {'tt': 'integer'}
        sql = f"SELECT geodb_create_collection('geodb_9bfgsdfg-453f-445b-a459_test_usage', '{json.dumps(props)}', '4326')"
        self._cursor.execute(sql)

        sql = f"SELECT public.geodb_get_my_usage()"
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertEqual(([{'usage': None}],), res)

    def test_create_database(self):
        user_name = "geodb_9bfgsdfg-453f-445b-a459"
        self._set_role(user_name)

        sql = f"SELECT geodb_create_database('test')"
        self._cursor.execute(sql)

        sql = f"SELECT * FROM geodb_user_databases WHERE name='test' AND owner = '{user_name}'"
        self._cursor.execute(sql)
        res = self._cursor.fetchall()

        self.assertEqual(1, len(res))
        res = res[0]
        self.assertEqual('test', res[1])
        self.assertEqual(user_name, res[2])

    def test_truncate_database(self):
        user_name = "geodb_9bfgsdfg-453f-445b-a459"
        self._set_role(user_name)

        sql = f"INSERT INTO geodb_user_databases(name, owner) VALUES('test_truncate', '{user_name}')"
        self._cursor.execute(sql)

        sql = f"SELECT geodb_truncate_database('test_truncate')"
        self._cursor.execute(sql)

        sql = f"SELECT * FROM geodb_user_databases WHERE name='test_truncate' AND owner = '{user_name}'"
        self._cursor.execute(sql)
        res = self._cursor.fetchall()

        self.assertEqual(0, len(res))

    def test_grant_access_refused(self):
        self._set_role('geodb_not_allowed')
        sql = 'SELECT * FROM "geodb_9bfgsdfg-453f-445b-a459_land_use"'

        # noinspection PyUnresolvedReferences
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege) as e:
            self._cursor.execute(sql)

        self.assertIn('permission denied', e.exception.pgerror)
        self.assertEqual('42501', e.exception.pgcode)

    def test_grant_access_success(self):
        user_name = "geodb_9bfgsdfg-453f-445b-a459"
        self._set_role(user_name)

        sql = "SELECT geodb_grant_access_to_collection('geodb_9bfgsdfg-453f-445b-a459_land_use', 'public')"
        self._cursor.execute(sql)

        self._set_role('geodb_not_allowed')
        sql = 'SELECT * FROM "geodb_9bfgsdfg-453f-445b-a459_land_use"'

        self._cursor.execute(sql)
        res = self._cursor.fetchall()

        self.assertEqual(2, len(res))

    def test_publish_collection(self):
        user_name = "geodb_9bfgsdfg-453f-445b-a459"
        self._set_role(user_name)

        sql = f"SELECT geodb_publish_collection('{user_name}_land_use')"

        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertTrue(res)

        self._set_role("geodb_not_allowed")

        sql = f"SELECT geodb_publish_collection('{user_name}_land_use')"

        # noinspection PyUnresolvedReferences
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege) as e:
            self._cursor.execute(sql)

        self.assertTrue("permission denied for table geodb_user_databases" in e.exception.pgerror)
        self.assertEqual('42501', e.exception.pgcode)

    def test_unpublish_collection(self):
        user_name = "geodb_9bfgsdfg-453f-445b-a459"
        self._set_role(user_name)

        sql = f"SELECT geodb_unpublish_collection('{user_name}_land_use')"

        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.assertTrue(res)

        self._set_role("geodb_not_allowed")

        sql = f"SELECT geodb_unpublish_collection('{user_name}_land_use')"

        # noinspection PyUnresolvedReferences
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege) as e:
            self._cursor.execute(sql)

        self.assertTrue("permission denied for table geodb_user_databases" in e.exception.pgerror)
        self.assertEqual('42501', e.exception.pgcode)


