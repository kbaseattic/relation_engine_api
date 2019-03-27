import requests


def create_test_docs(count):
    """Produce some test documents."""
    def doc(i):
        return '{"name": "name", "_key": "%s", "is_public": true}' % i
    return '\n'.join(doc(i) for i in range(0, count))


def create_test_edges(count):
    """Produce some test edges."""
    def doc(i):
        return '{"_from": "test_vertex/%s", "_to": "test_vertex/%s"}' % (i, i)
    return '\n'.join(doc(i) for i in range(0, count))


def save_test_docs(url, count, edges=False):
    headers = {'Authorization': 'Bearer admin_token', 'Content-Type': 'application/json'}
    if edges:
        docs = create_test_edges(count)
        collection = 'test_edge'
    else:
        docs = create_test_docs(count)
        collection = 'test_vertex'
    return requests.put(
        url + '/documents',
        params={'overwrite': True, 'collection': collection},
        data=docs,
        headers=headers
    ).json()
