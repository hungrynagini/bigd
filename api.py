from collections import Counter

from flask import request, jsonify, Flask, abort, Blueprint
# from flask_rest_api import Api, Blueprint, abort

from cass_cluster import cluster

app = Flask('API')

app.config['OPENAPI_VERSION'] = '3.0.2'
app.config['OPENAPI_URL_PREFIX'] = '/docs'
app.config['OPENAPI_SWAGGER_UI_VERSION'] = '3.3.0'
app.config['OPENAPI_SWAGGER_UI_PATH'] = '/swagger_ui'
app.config['OPENAPI_REDOC_PATH'] = '/redoc_ui'

# api = Api(app)
blp = Blueprint('api', 'api', url_prefix='/api/v1')

session = cluster.connect('meetups', wait_for_all_pools=True)
session.set_keyspace('meetups')

cluster.connect()

"""""""""""""""""""""""""""""""""""
"          AD HOC QUERIES         "
"""""""""""""""""""""""""""""""""""


@blp.route('/countries', methods=["GET"], strict_slashes=False)
def list_countries():
    results = session.execute("select country from countries group by country")
    if not results:
        abort(404)
    results_ = [{name: (getattr(row, name)) for name in row._fields} for row in results]
    return jsonify(results_)

@blp.route('/cities', methods=["GET"], strict_slashes=False)
def list_cities():
    country = request.args.get('country')
    results = session.execute("select city from countries where country = \'{}\'".format(country))
    if not results:
        abort(404)
    results_ = [{name: (getattr(row, name)) for name in row._fields} for row in results]
    return jsonify(results_)

@blp.route('/event', methods=["GET"], strict_slashes=False)
def get_event():
    event_id = request.args.get('event_id')
    results = session.execute("select event_name, event_time, topics, group_name, city, country from events where event_id = \'{}\'".format(event_id))
    if not results:
        abort(404)
    results_ = [{name: (getattr(row, name)) for name in row._fields} for row in results]
    return jsonify(results_)

@blp.route('/groups', methods=["GET"], strict_slashes=False)
def list_groups():
    city = request.args.get('city')
    results = session.execute("select group_name, group_id from city_groups where city_name = \'{}\'".format(city))
    if not results:
        abort(404)
    results_ = [{name: (getattr(row, name)) for name in row._fields} for row in results]
    return jsonify(results_)

@blp.route('/events', methods=["GET"], strict_slashes=False)
def list_events():
    group_id = request.args.get('group_id')
    results = session.execute("select event_name, event_time, topics, group_name, city, country from groups where group_id= {}".format(group_id))
    if not results:
        abort(404)
    results_ = [{name: (getattr(row, name)) for name in row._fields} for row in results]
    return jsonify(results_)

app.register_blueprint(blp)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=4321, debug=True)
    cluster.shutdown()