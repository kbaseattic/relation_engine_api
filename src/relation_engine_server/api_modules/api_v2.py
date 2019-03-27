import flask
import jinja2

from . import api_v1

from ..utils import arango_client, spec_loader, auth, parse_json
from ..exceptions import InvalidParameters


def run_query():
    """
    Run a stored query from the spec repo.
    Auth:
     - only kbase re admins for ad-hoc queries
     - public for views (views will have access controls within them based on params)
    """
    json_body = parse_json.get_json_body() or {}
    if 'bind_vars' not in json_body:
        json_body['bind_vars'] = {}
    # Don't allow the user to set the special 'ws_ids' field
    json_body['bind_vars']['ws_ids'] = []
    auth_token = auth.get_auth_header()
    # Fetch any authorized workspace IDs using a KBase auth token, if present
    json_body['bind_vars']['ws_ids'] = auth.get_workspace_ids(auth_token)
    # fetch number of documents to return
    batch_size = int(flask.request.args.get('batch_size', 100))
    if 'query' in json_body:
        # Run an adhoc query for a sysadmin
        auth.require_auth_token(roles=['RE_ADMIN'])
        query_text = json_body['query']
        resp_body = arango_client.run_query(query_text=query_text,
                                            bind_vars=json_body['bind_vars'],
                                            batch_size=batch_size)
        return resp_body
    if 'view' in flask.request.args:
        # Run a query from a view name
        view_name = flask.request.args['view']
        view_str = spec_loader.get_view(view_name)
        templ = jinja2.Environment(loader=jinja2.BaseLoader(), autoescape=True).from_string(view_str)
        compiled_query = templ.render(**json_body.get('template_vars', {}))
        resp_body = arango_client.run_query(query_text=compiled_query,
                                            bind_vars=json_body['bind_vars'],
                                            batch_size=batch_size)
        return resp_body
    if 'cursor_id' in flask.request.args:
        # Run a query from a cursor ID
        cursor_id = flask.request.args['cursor_id']
        resp_body = arango_client.run_query(cursor_id=cursor_id)
        return resp_body
    # No valid options were passed
    raise InvalidParameters('Pass in a view or a cursor_id')


endpoints = {
    'query_results': {'handler': run_query, 'methods': {'POST'}},
    'specs/schemas': {'handler': api_v1.show_schemas},
    'specs/views': {'handler': api_v1.show_views},
    'config': {'handler': api_v1.show_config},
    'specs': {'handler': api_v1.update_specs, 'methods': {'PUT'}},
    'documents': {'handler': api_v1.save_documents, 'methods': {'PUT'}}
}
