"""
Integration tests on API v2 methods.

Version 2 adds Jinja2 templating support on stored queries.
"""
import unittest
import requests
import json
import os

from src.test.utils import save_test_docs

# Use the mock auth tokens
_ADMIN_TOKEN = 'admin_token'
_INVALID_TOKEN = 'invalid_token'

# Use the docker-compose url of the running flask server
_URL = os.environ.get('TEST_URL', 'http://web:5000')
_VERSION = 'v2'
_API_URL = '/'.join([_URL, 'api', _VERSION])

_HEADERS_ADMIN = {'Authorization': 'Bearer ' + _ADMIN_TOKEN, 'Content-Type': 'application/json'}


class TestApi(unittest.TestCase):
    """
    Test all query functionality for API v2.
    Make sure existing functionality still works, while jinja templating also works.
    """

    @classmethod
    def setUpClass(cls):
        # Initialize collections before running any tests
        resp = requests.put(
            _API_URL + '/specs',
            headers=_HEADERS_ADMIN,
            params={'init_collections': '1'}
        )
        print('update_specs response', resp.text)

    def test_query_with_cursor(self):
        """Test getting more data via a query cursor and setting batch size."""
        save_test_docs(_API_URL, count=20)
        resp = requests.post(
            _API_URL + '/query_results',
            params={'view': 'list_test_vertices', 'batch_size': 10}
        ).json()
        self.assertTrue(resp['cursor_id'])
        self.assertEqual(resp['has_more'], True)
        self.assertEqual(resp['count'], 20)
        self.assertTrue(len(resp['results']), 10)
        cursor_id = resp['cursor_id']
        resp = requests.post(
            _API_URL + '/query_results',
            params={'cursor_id': cursor_id}
        ).json()
        self.assertEqual(resp['count'], 20)
        self.assertEqual(resp['has_more'], False)
        self.assertEqual(resp['cursor_id'], None)
        self.assertTrue(len(resp['results']), 10)
        # Try to get the same cursor again
        resp = requests.post(
            _API_URL + '/query_results',
            params={'cursor_id': cursor_id}
        ).json()
        self.assertTrue(resp['error'])
        self.assertEqual(resp['arango_message'], 'cursor not found')

    def test_query_no_name(self):
        """Test a query error with a view name that does not exist."""
        resp = requests.post(
            _API_URL + '/query_results',
            params={'view': 'nonexistent'}
        ).json()
        self.assertEqual(resp['error'], 'View does not exist.')
        self.assertEqual(resp['name'], 'nonexistent')

    def test_query_missing_bind_var(self):
        """Test a query error with a missing bind variable."""
        resp = requests.post(
            _API_URL + '/query_results',
            params={'view': 'list_test_vertices'},
            data=json.dumps({'bind_vars': {'xyz': 'test_vertex'}})
        ).json()
        self.assertEqual(resp['error'], 'ArangoDB server error.')
        self.assertTrue(resp['arango_message'])

    def test_auth_query_with_access(self):
        """Test the case where we query a collection with specific workspace access."""
        ws_id = 3
        # Remove all test vertices and create one with a ws_id
        requests.put(
            _API_URL + '/documents',
            params={'overwrite': True, 'collection': 'test_vertex'},
            data=json.dumps({
                'name': 'requires_auth',
                '_key': '123',
                'ws_id': ws_id
            }),
            headers=_HEADERS_ADMIN
        )
        resp = requests.post(
            _API_URL + '/query_results',
            params={'view': 'list_test_vertices'},
            headers={'Authorization': 'valid_token'}  # see ./mock_workspace/endpoints.json
        ).json()
        self.assertEqual(resp['count'], 1)
        self.assertEqual(resp['results'][0]['ws_id'], ws_id)

    def test_auth_query_no_access(self):
        """Test the case where we try to query a collection without the right workspace access."""
        # Remove all test vertices and create one with a ws_id
        requests.put(
            _API_URL + '/documents',
            params={'overwrite': True, 'collection': 'test_vertex'},
            data='{"name": "requires_auth", "_key": "1", "ws_id": 9999}',
            headers=_HEADERS_ADMIN
        )
        resp = requests.post(
            _API_URL + '/query_results',
            params={'view': 'list_test_vertices'},
            headers={'Authorization': 'valid_token'}  # see ./mock_workspace/endpoints.json
        ).json()
        self.assertEqual(resp['count'], 0)

    def test_query_cannot_pass_ws_ids(self):
        """Test that users cannot set the ws_ids param."""
        ws_id = 99
        requests.put(
            _API_URL + '/documents',
            params={'overwrite': True, 'collection': 'test_vertex'},
            data='{"name": "requires_auth", "_key": "1", "ws_id": 99}',
            headers=_HEADERS_ADMIN
        )
        resp = requests.post(
            _API_URL + '/query_results',
            params={'view': 'list_test_vertices'},
            data=json.dumps({'bind_vars': {'ws_ids': [ws_id]}}),
            headers={'Authorization': 'valid_token'}
        ).json()
        self.assertEqual(resp['count'], 0)

    def test_auth_query_invalid_token(self):
        """Test the case where we try to authorize a query using an invalid auth token."""
        requests.put(
            _API_URL + '/documents',
            params={'overwrite': True, 'collection': 'test_vertex'},
            data='{"name": "requires_auth", "_key": "1", "ws_id": 99}',
            headers=_HEADERS_ADMIN
        )
        resp = requests.post(
            _API_URL + '/query_results',
            params={'view': 'list_test_vertices'},
            headers={'Authorization': _INVALID_TOKEN}
        )
        self.assertEqual(resp.status_code, 403)

    def test_auth_adhoc_query(self):
        """Test that the 'ws_ids' bind-var is set for RE_ADMINs."""
        ws_id = 99
        res = requests.put(
            _API_URL + '/documents',
            params={'overwrite': True, 'collection': 'test_vertex'},
            data=json.dumps({'name': 'requires_auth', '_key': '1', 'ws_id': ws_id}),
            headers={'Authorization': _ADMIN_TOKEN}
        )
        self.assertTrue(res.ok)
        # This is the same query as list_test_vertices.aql in the spec
        query = 'for o in test_vertex filter o.is_public || o.ws_id IN @ws_ids return o'
        resp = requests.post(
            _API_URL + '/query_results',
            data=json.dumps({'query': query}),
            headers={'Authorization': _ADMIN_TOKEN}  # see ./mock_workspace/endpoints.json
        ).json()
        self.assertEqual(resp['count'], 1)
