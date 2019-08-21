import MySQLdb.cursors
import flask
import functools
import os
import pathlib
import copy
import json
import subprocess
from io import StringIO
import csv
import operator as op


class CustomFlask(flask.Flask):
    jinja_options = flask.Flask.jinja_options.copy()
    jinja_options.update(dict(
        block_start_string='(%',
        block_end_string='%)',
        variable_start_string='((',
        variable_end_string='))',
        comment_start_string='(#',
        comment_end_string='#)',
    ))


app = CustomFlask(__name__, static_folder=None)
app.config['SECRET_KEY'] = 'tagomoris'


def make_base_url(request):
    return request.url_root[:-1]


@app.template_filter('tojsonsafe')
def tojsonsafe(target):
    return json.dumps(target).translate({
        "+": "\\u002b",
        "<": "\\u003c",
        ">": "\\u003e",
    })


def jsonify(target):
    return json.dumps(target)


def res_error(error='unknown', status=500):
    return (jsonify({'error': error}), status)


def login_required(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not get_login_user():
            return res_error('login_required', 401)
        return f(*args, **kwargs)
    return wrapper


def admin_login_required(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not get_login_administrator():
            return res_error('admin_login_required', 401)
        return f(*args, **kwargs)
    return wrapper


def dbh(
    host=os.environ['DB_HOST'],
    port=int(os.environ['DB_PORT']),
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASS'],
    database=os.environ['DB_DATABASE'],
    charset='utf8mb4',
    cursorclass=MySQLdb.cursors.DictCursor,
    autocommit=True,
    **kwargs,
):
    if hasattr(flask.g, 'db'):
        return flask.g.db
    flask.g.db = MySQLdb.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset,
        cursorclass=cursorclass,
        autocommit=autocommit,
        **kwargs
    )
    return flask.g.db


@app.teardown_appcontext
def teardown(error):
    if hasattr(flask.g, 'db'):
        flask.g.db.close()


def get_events(public=False):
    cur = dbh().cursor()
    cur.execute("""
SELECT id, title, price, public_fg AS public, closed_fg AS closed,
  GROUP_CONCAT(total ORDER BY sheet_rank SEPARATOR ",") AS total,
  GROUP_CONCAT(remains ORDER BY sheet_rank SEPARATOR ",") AS remains,
  GROUP_CONCAT((price + sheet_price) ORDER BY sheet_rank SEPARATOR ",") AS prices
FROM (
  SELECT e.id, e.title, e.price, e.public_fg, e.closed_fg,
    s.rank AS sheet_rank, s.price AS sheet_price,
    COUNT(*) AS total, COUNT(r.reserved_at IS NULL OR NULL) AS remains
  FROM events e
    CROSS JOIN sheets s {}
    LEFT JOIN reservations r
      ON r.canceled_at IS NULL
        AND e.id = r.event_id
        AND s.id = r.sheet_id
  GROUP BY e.id, s.rank
) e
GROUP BY id
ORDER BY id
""".format('ON e.public_fg = 1' if public else ''))
    events = []
    for event in cur.fetchall():
        total = list(map(int, event['total'].split(',')))
        remains = list(map(int, event['remains'].split(',')))
        prices = list(map(int, event.pop('prices').split(',')))
        event['total'] = sum(total)
        event['remains'] = sum(remains)
        event['sheets'] = {
            rank: {'total': t, 'remains': r, 'price': p}
            for rank, t, r, p in zip(['A', 'B', 'C', 'S'], total, remains, prices)
        }
        event['public'] = bool(event['public'])
        event['closed'] = bool(event['closed'])
        events.append(event)
    return events


def get_event(event_id, login_user_id=None):
    cur = dbh().cursor()
    cur.execute("""
SELECT id, title, public_fg AS public, closed_fg AS closed, price
FROM events
WHERE id = %s
""", [event_id])
    event = cur.fetchone()
    if not event: return None

    event['public'] = bool(event['public'])
    event['closed'] = bool(event['closed'])

    event['total'] = 0
    event['remains'] = 0
    event['sheets'] = {}
    for rank in ['S', 'A', 'B', 'C']:
        event['sheets'][rank] = {'total': 0, 'remains': 0, 'detail': []}

    cur.execute("""
SELECT s.rank, s.num, s.price, r.user_id,
  CAST(UNIX_TIMESTAMP(r.reserved_at) AS INT) AS reserved_at
FROM sheets s
  LEFT JOIN reservations r
    ON r.canceled_at IS NULL
      AND s.id = sheet_id
      AND r.event_id = %s
ORDER BY rank, num
""", [event['id']])

    sheets = cur.fetchall()
    for sheet in sheets:
        if not event['sheets'][sheet['rank']].get('price'):
            event['sheets'][sheet['rank']]['price'] = event['price'] + sheet['price']
        event['total'] += 1
        event['sheets'][sheet['rank']]['total'] += 1

        if sheet['reserved_at']:
            if login_user_id and sheet['user_id'] == login_user_id:
                sheet['mine'] = True
            sheet['reserved'] = True
        else:
            event['remains'] += 1
            event['sheets'][sheet['rank']]['remains'] += 1
            del sheet['reserved_at']

        event['sheets'][sheet['rank']]['detail'].append(sheet)

        del sheet['price']
        del sheet['rank']
        del sheet['user_id']

    return event


def sanitize_event(event):
    del event['price']
    del event['public']
    del event['closed']
    return event


def get_login_user():
    return flask.session.get('user', None)


def get_login_administrator():
    return flask.session.get('administrator', None)


def validate_rank(rank):
    return rank in ['A', 'B', 'C', 'S']
    cur = dbh().cursor()
    cur.execute("""
SELECT COUNT(*) AS total_sheets
FROM sheets
WHERE rank = %s
""", [rank])
    ret = cur.fetchone()
    return int(ret['total_sheets']) > 0


def render_report_csv(reports):

    def CSV(rows):
        keys = ['reservation_id', 'event_id', 'rank', 'num', 'price', 'user_id', 'sold_at', 'canceled_at']
        getter = op.itemgetter(*keys)
        yield ','.join(keys) + '\r\n'
        yield from (row + '\r\n' for row in rows)

    return flask.Response(
        CSV(map(op.itemgetter('row'), reports)),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=report.csv'},
    )


@app.route('/')
def get_index():
    user = get_login_user()
    events = list(map(sanitize_event, get_events(public=True)))
    return flask.render_template('index.html', user=user, events=events, base_url=make_base_url(flask.request))


@app.route('/initialize')
def get_initialize():
    subprocess.call(['../../db/webapp1/init.sh'])
    subprocess.call(['../../db/webapp2/init.sh'])
    #subprocess.call(['../../db/webapp3/init.sh'])
    return ('', 204)


@app.route('/api/users', methods=['POST'])
def post_users():
    nickname = flask.request.json['nickname']
    login_name = flask.request.json['login_name']
    password = flask.request.json['password']

    cur = dbh(host='localhost').cursor()
    try:
        cur.execute("""
INSERT INTO users (login_name, pass_hash, nickname)
VALUES (%s, SHA2(%s, 256), %s)
""", [login_name, password, nickname])
        user_id = cur.lastrowid
    except MySQLdb.Error as e:
        print(e)
        return res_error('duplicated', 409)
    user = {'id': user_id, 'nickname': nickname}
    return (jsonify(user), 201)


@app.route('/api/users/<int:user_id>')
@login_required
def get_users(user_id):
    user = get_login_user()

    if user_id != user['id']:
        return ('', 403)

    cur = dbh().cursor()
    cur.execute("""
SELECT
  r.id,
  CAST(UNIX_TIMESTAMP(r.reserved_at) AS INT) as reserved_at,
  CAST(UNIX_TIMESTAMP(r.canceled_at) AS INT) as canceled_at,
  s.price + e.price AS price,
  s.rank AS sheet_rank,
  s.num AS sheet_num,
  e.id AS event_id,
  e.title AS event_title,
  e.public_fg AS event_public,
  e.closed_fg AS event_closed,
  e.price AS event_price
FROM reservations r
  STRAIGHT_JOIN events e
    ON e.id = r.event_id
  STRAIGHT_JOIN sheets s
    ON s.id = r.sheet_id
WHERE r.user_id = %s
ORDER BY IFNULL(r.canceled_at, r.reserved_at) DESC LIMIT 5
""", [user['id']])
    recent_reservations = []
    for row in cur.fetchall():
        recent_reservations.append({
            'id': row['id'],
            'event': {
                'id': row['event_id'],
                'title': row['event_title'],
                'public': bool(row['event_public']),
                'closed': bool(row['event_closed']),
                'price': row['event_price'],
            },
            'sheet_rank': row['sheet_rank'],
            'sheet_num': row['sheet_num'],
            'price': row['price'],
            'reserved_at': row['reserved_at'],
            'canceled_at': row['canceled_at'],
        })
    user['recent_reservations'] = recent_reservations

    cur.execute("""
SELECT IFNULL(SUM(e.price + s.price), 0) AS total_price
FROM reservations r
  STRAIGHT_JOIN events e
    ON r.event_id = e.id
  STRAIGHT_JOIN sheets s
    ON r.sheet_id = s.id
WHERE r.user_id = %s
   AND r.canceled_at IS NULL
""", [user['id']])
    row = cur.fetchone()
    user['total_price'] = int(row['total_price'])

    cur.execute("""
SELECT id, title, price, public_fg AS public, closed_fg AS closed,
  GROUP_CONCAT(total ORDER BY sheet_rank SEPARATOR ",") AS total,
  GROUP_CONCAT(remains ORDER BY sheet_rank SEPARATOR ",") AS remains,
  GROUP_CONCAT((price + sheet_price) ORDER BY sheet_rank SEPARATOR ",") AS prices
FROM (
  SELECT e.id, e.title, e.price, e.public_fg, e.closed_fg,
    s.rank AS sheet_rank, s.price AS sheet_price,
    COUNT(*) AS total, COUNT(r.reserved_at IS NULL OR NULL) AS remains,
    tmp.update_at
  FROM (
    SELECT event_id, MAX(IFNULL(canceled_at, reserved_at)) AS update_at
    FROM reservations
    WHERE user_id = %s
    GROUP BY event_id
    ORDER BY MAX(IFNULL(canceled_at, reserved_at)) DESC LIMIT 5
  ) tmp
    INNER JOIN events e
      ON tmp.event_id = e.id
    CROSS JOIN sheets s
    LEFT JOIN reservations r
      ON r.canceled_at IS NULL
        AND e.id = r.event_id
        AND s.id = r.sheet_id
  GROUP BY e.id, s.rank
) e
GROUP BY id
ORDER BY MAX(update_at) DESC LIMIT 5
""", [user['id']])
    events = []
    for event in cur.fetchall():
        total = list(map(int, event['total'].split(',')))
        remains = list(map(int, event['remains'].split(',')))
        prices = list(map(int, event.pop('prices').split(',')))
        event['total'] = sum(total)
        event['remains'] = sum(remains)
        event['sheets'] = {
            rank: {'total': t, 'remains': r, 'price': p}
            for rank, t, r, p in zip(['A', 'B', 'C', 'S'], total, remains, prices)
        }
        event['public'] = bool(event['public'])
        event['closed'] = bool(event['closed'])
        events.append(event)
    user['recent_events'] = events

    return jsonify(user)


@app.route('/api/actions/login', methods=['POST'])
def post_login():
    login_name = flask.request.json['login_name']
    password = flask.request.json['password']

    cur = dbh(host='localhost').cursor()
    cur.execute("""
SELECT id, nickname
FROM users
WHERE login_name = %s
  AND pass_hash = SHA2(%s, 256)
""", [login_name, password])
    user = cur.fetchone()
    if not user:
        return res_error('authentication_failed', 401)

    flask.session['user'] = user
    return flask.jsonify(user)


@app.route('/api/actions/logout', methods=['POST'])
@login_required
def post_logout():
    flask.session.pop('user', None)
    return ('', 204)


@app.route('/api/events')
def get_events_api():
    events = list(map(sanitize_event, get_events(public=True)))
    return jsonify(events)


@app.route('/api/events/<int:event_id>')
def get_events_by_id(event_id):
    user = get_login_user()
    event = get_event(event_id, user['id'] if user else None)

    if not event or not event['public']:
        return res_error('not_found', 404)

    event = sanitize_event(event)
    return jsonify(event)


@app.route('/api/events/<int:event_id>/actions/reserve', methods=['POST'])
@login_required
def post_reserve(event_id):
    rank = flask.request.json['sheet_rank']

    user = get_login_user()

    conn =  dbh()
    cur = conn.cursor()
    cur.execute("""
SELECT id
FROM events
WHERE id = %s
  AND public_fg = 1
""", [event_id])
    event = cur.fetchone()

    if not event:
        return res_error('invalid_event', 404)
    if not validate_rank(rank):
        return res_error('invalid_rank', 400)

    try:
      cur.execute("""
INSERT INTO reservations (event_id, sheet_id, user_id, reserved_at)
SELECT
  %s AS event_id,
  s.id AS sheet_id,
  %s AS user_id,
  NOW() AS reserved_at
FROM (
  SELECT id
  FROM sheets
  WHERE rank = %s
) s
  LEFT JOIN reservations r
    ON r.canceled_at IS NULL
      AND r.event_id = %s
      AND r.sheet_id = s.id
WHERE r.reserved_at IS NULL
ORDER BY RAND() LIMIT 1
FOR UPDATE
""", [event_id, user['id'], rank, event_id])
      reservation_id = cur.lastrowid
    except MySQLdb.Error as e:
        print(e)
        return res_error('sold_out', 409)

    cur.execute("""
SELECT s.num
FROM sheets s
  INNER JOIN reservations r
    ON r.id = %s
      AND r.sheet_id = s.id
""", [reservation_id])
    sheet = cur.fetchone()

    content = jsonify({
        'id': reservation_id,
        'sheet_rank': rank,
        'sheet_num': sheet['num']})
    return flask.Response(content, status=202, mimetype='application/json')


@app.route('/api/events/<int:event_id>/sheets/<rank>/<int:num>/reservation', methods=['DELETE'])
@login_required
def delete_reserve(event_id, rank, num):
    user = get_login_user()

    cur = dbh().cursor()
    cur.execute("""
SELECT
  (
    SELECT id
    FROM events
    WHERE id = %s
      AND public_fg = 1
  ) AS event_id,
  (
    SELECT id
    FROM sheets
    WHERE rank = %s
      AND num = %s
  ) AS sheet_id
""", [event_id, rank, num])
    valid = cur.fetchone()
    if not valid['event_id']:
        return res_error('invalid_event', 404)
    if not validate_rank(rank):
        return res_error('invalid_rank', 404)
    if not valid['sheet_id']:
        return res_error('invalid_sheet', 404)
    sheet_id = valid['sheet_id']

    try:
        conn = dbh()
        conn.autocommit(False)
        cur = conn.cursor()

        cur.execute("""
SELECT id, user_id
FROM reservations
WHERE event_id = %s
  AND sheet_id = %s
  AND canceled_at IS NULL
FOR UPDATE
""", [event_id, sheet_id])
        reservation = cur.fetchone()

        if not reservation:
            conn.rollback()
            return res_error('not_reserved', 400)
        if reservation['user_id'] != user['id']:
            conn.rollback()
            return res_error('not_permitted', 403)

        cur.execute("""
UPDATE reservations
SET canceled_at = NOW()
WHERE id = %s
""", [reservation['id']])
        conn.commit()
    except MySQLdb.Error as e:
        conn.rollback()
        print(e)
        return res_error()

    return flask.Response(status=204)


@app.route('/admin/')
def get_admin():
    administrator = get_login_administrator()
    events = get_events() if administrator else {}
    return flask.render_template('admin.html', administrator=administrator, events=events, base_url=make_base_url(flask.request))


@app.route('/admin/api/actions/login', methods=['POST'])
def post_admin_login():
    login_name = flask.request.json['login_name']
    password = flask.request.json['password']

    cur = dbh(host='localhost').cursor()

    cur.execute("""
SELECT id, nickname
FROM administrators
WHERE login_name = %s
  AND pass_hash = SHA2(%s, 256)
""", [login_name, password])
    administrator = cur.fetchone()

    if not administrator:
        return res_error('authentication_failed', 401)

    flask.session['administrator'] = administrator
    return jsonify(administrator)


@app.route('/admin/api/actions/logout', methods=['POST'])
@admin_login_required
def get_admin_logout():
    flask.session.pop('administrator', None)
    return ('', 204)


@app.route('/admin/api/events')
@admin_login_required
def get_admin_events_api():
    return jsonify(get_events())


@app.route('/admin/api/events', methods=['POST'])
@admin_login_required
def post_admin_events_api():
    title = flask.request.json['title']
    public = flask.request.json['public']
    price = flask.request.json['price']

    cur = dbh().cursor()
    cur.execute("""
INSERT INTO events (title, public_fg, closed_fg, price)
VALUES (%s, %s, 0, %s)
""", [title, public, price])
    event_id = cur.lastrowid
    event = get_event(event_id)

    return jsonify(event)


@app.route('/admin/api/events/<int:event_id>')
@admin_login_required
def get_admin_events_by_id(event_id):
    event = get_event(event_id)
    if not event:
        return res_error('not_found', 404)
    return jsonify(event)


@app.route('/admin/api/events/<int:event_id>/actions/edit', methods=['POST'])
@admin_login_required
def post_event_edit(event_id):
    public = flask.request.json.get('public', False)
    closed = flask.request.json.get('closed', False)
    if closed:
        public = False

    event = get_event(event_id)
    if not event:
        return res_error('not_found', 404)
    if event['closed']:
        return res_error('cannot_edit_closed_event', 400)
    if event['public'] and closed:
        return res_error('cannot_close_public_event', 400)

    cur = dbh().cursor()
    cur.execute("""
UPDATE events
SET public_fg = %s, closed_fg = %s
WHERE id = %s
""", [public, closed, event_id])

    event.update(public=public, closed=closed)
    return jsonify(event)


@app.route('/admin/api/reports/events/<int:event_id>/sales')
@admin_login_required
def get_admin_event_sales(event_id):
    cur = dbh(compress=True).cursor()
    reservations = cur.execute("""
SELECT
  CONCAT_WS(',',
    r.id, r.event_id, s.rank, s.num, s.price + e.price, r.user_id,
    DATE_FORMAT(r.reserved_at, '%%Y-%%m-%%dT%%TZ'),
    IFNULL(DATE_FORMAT(r.canceled_at, '%%Y-%%m-%%dT%%TZ'), '')
  ) AS row
FROM reservations r
  LEFT JOIN sheets s
    ON r.sheet_id = s.id
  LEFT JOIN events e
    ON r.event_id = e.id
WHERE r.event_id = %s
ORDER BY r.id
""", [event_id])

    reports = cur.fetchall()
    return render_report_csv(reports)


@app.route('/admin/api/reports/sales')
@admin_login_required
def get_admin_sales():
    cur = dbh(compress=True).cursor()
    cur.execute("""
SELECT
  CONCAT_WS(',',
    r.id, r.event_id, s.rank, s.num, s.price + e.price, r.user_id,
    DATE_FORMAT(r.reserved_at, '%Y-%m-%dT%TZ'),
    IFNULL(DATE_FORMAT(r.canceled_at, '%Y-%m-%dT%TZ'), '')
  ) AS row
FROM reservations r
  LEFT JOIN sheets s
    ON r.sheet_id = s.id
  LEFT JOIN events e
    ON r.event_id = e.id
ORDER BY r.id
""")

    reports = cur.fetchall()
    return render_report_csv(reports)


PROFILE = False
if PROFILE:
    from wsgi_lineprof.middleware import LineProfilerMiddleware
    from wsgi_lineprof.filters import FilenameFilter, TotalTimeSorter
    f = open('lineprof.log', 'a')
    filters = [
        FilenameFilter(__file__),
        TotalTimeSorter(),
    ]
    app.wsgi_app = LineProfilerMiddleware(
        app.wsgi_app,
        filters=filters,
        stream=f,
        async_stream=True,
    )


if __name__ == '__main__':
    app.run(port=8080, debug=True, threaded=True)
