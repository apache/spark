import copy
from datetime import datetime, timedelta
import dateutil.parser
import json
import logging
import os
import re
import socket
import sys
import urllib2

from flask import Flask, url_for, Markup, Blueprint, redirect, flash, Response
from flask.ext.admin import Admin, BaseView, expose, AdminIndexView
from flask.ext.admin.form import DateTimePickerWidget
from flask.ext.admin import base
from flask.ext.admin.contrib.sqla import ModelView
from flask.ext.cache import Cache
from flask import request
import sqlalchemy as sqla
from wtforms import Form, DateTimeField, SelectField, TextAreaField

from pygments import highlight
from pygments.lexers import PythonLexer, SqlLexer, BashLexer, IniLexer
from pygments.formatters import HtmlFormatter

import jinja2
import markdown
import chartkick

import airflow
from airflow.settings import Session
from airflow import jobs
from airflow import models
from airflow.models import State
from airflow import settings
from airflow.configuration import conf
from airflow import utils
from airflow.www import utils as wwwutils

from airflow.www.login import login_manager
import flask_login
from flask_login import login_required

QUERY_LIMIT = 100000
CHART_LIMIT = 200000

special_attrs = {
    'sql': SqlLexer,
    'hql': SqlLexer,
    'bash_command': BashLexer,
}

AUTHENTICATE = conf.getboolean('master', 'AUTHENTICATE')
if AUTHENTICATE is False:
    login_required = lambda x: x

dagbag = models.DagBag(os.path.expanduser(conf.get('core', 'DAGS_FOLDER')))
utils.pessimistic_connection_handling()

app = Flask(__name__)
app.config['SQLALCHEMY_POOL_RECYCLE'] = 3600

login_manager.init_app(app)
app.secret_key = 'airflowified'

cache = Cache(
    app=app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'})

# Init for chartkick, the python wrapper for highcharts
ck = Blueprint(
    'ck_page', __name__,
    static_folder=chartkick.js(), static_url_path='/static')
app.register_blueprint(ck, url_prefix='/ck')
app.jinja_env.add_extension("chartkick.ext.charts")


class DateTimeForm(Form):
    # Date filter form needed for gantt and graph view
    execution_date = DateTimeField(
        "Execution date", widget=DateTimePickerWidget())


class GraphForm(Form):
    execution_date = DateTimeField(
        "Execution date", widget=DateTimePickerWidget())
    arrange = SelectField("Layout", choices=(
        ('LR', "Left->Right"),
        ('RL', "Right->Left"),
        ('TB', "Top->Bottom"),
        ('BT', "Bottom->Top"),
    ))


@app.route('/')
def index():
    return redirect(url_for('admin.index'))


@app.route('/health')
def health():
    """ We can add an array of tests here to check the server's health """
    content = Markup(markdown.markdown("The server is healthy!"))
    return content


@app.teardown_appcontext
def shutdown_session(exception=None):
    settings.Session.remove()


class HomeView(AdminIndexView):
    """
    Basic home view, just showing the README.md file
    """
    @expose("/")
    def index(self):
        dags = sorted(dagbag.dags.values(), key=lambda dag: dag.dag_id)
        return self.render('airflow/dags.html', dags=dags)

admin = Admin(
    app,
    name="Airflow",
    index_view=HomeView(name='DAGs'),
    template_mode='bootstrap3')

admin.add_link(
    base.MenuLink(
        category='Tools',
        name='Ad Hoc Query',
        url='/admin/airflow/query'))


class Airflow(BaseView):

    def is_visible(self):
        return False

    @expose('/')
    def index(self):
        return self.render('airflow/dags.html')

    @expose('/query')
    @login_required
    @wwwutils.gzipped
    def query(self):
        session = settings.Session()
        dbs = session.query(models.Connection).order_by(
            models.Connection.conn_id)
        db_choices = [(db.conn_id, db.conn_id) for db in dbs]
        conn_id_str = request.args.get('conn_id')
        csv = request.args.get('csv') == "true"
        sql = request.args.get('sql')

        class QueryForm(Form):
            conn_id = SelectField("Layout", choices=db_choices)
            sql = TextAreaField("SQL", widget=wwwutils.AceEditorWidget())
        data = {
            'conn_id': conn_id_str,
            'sql': sql,
        }
        results = None
        has_data = False
        error = False
        if conn_id_str:
            db = [db for db in dbs if db.conn_id == conn_id_str][0]
            hook = db.get_hook()
            try:
                df = hook.get_pandas_df(wwwutils.limit_sql(sql, QUERY_LIMIT))
                # df = hook.get_pandas_df(sql)
                has_data = len(df) > 0
                df = df.fillna('')
                results = df.to_html(
                    classes="table table-bordered table-striped no-wrap",
                    index=False,
                    na_rep='',
                ) if has_data else ''
            except Exception as e:
                flash(str(e), 'error')
                error = True

        if has_data and len(df) == QUERY_LIMIT:
            flash(
                "Query output truncated at " + str(QUERY_LIMIT) +
                " rows", 'info')

        if not has_data and error:
            flash('No data', 'error')

        if csv:
            return Response(
                response=df.to_csv(index=False),
                status=200,
                mimetype="application/text")

        form = QueryForm(request.form, data=data)
        session.commit()
        session.close()
        return self.render(
            'airflow/query.html', form=form,
            title="Ad Hoc Query",
            results=results or '',
            has_data=has_data)

    @expose('/chart_data')
    @login_required
    @wwwutils.gzipped
    @cache.cached(timeout=3600, key_prefix=wwwutils.make_cache_key)
    def chart_data(self):
        session = settings.Session()
        chart_id = request.args.get('chart_id')
        csv = request.args.get('csv') == "true"
        chart = session.query(models.Chart).filter_by(id=chart_id).all()[0]
        db = session.query(
            models.Connection).filter_by(conn_id=chart.conn_id).all()[0]
        session.expunge_all()
        session.commit()
        session.close()

        payload = {}
        payload['state'] = 'ERROR'
        payload['error'] = ''

        # Processing templated fields
        try:
            args = eval(chart.default_params)
            if type(args) is not type(dict()):
                raise Exception('Not a dict')
        except:
            args = {}
            payload['error'] += (
                "Default params is not valid, string has to evaluate as "
                "a Python dictionary. ")

        request_dict = {k: request.args.get(k) for k in request.args}
        from airflow import macros
        args.update(request_dict)
        args['macros'] = macros
        sql = jinja2.Template(chart.sql).render(**args)
        label = jinja2.Template(chart.label).render(**args)
        payload['sql_html'] = Markup(highlight(
            sql,
            SqlLexer(),  # Lexer call
            HtmlFormatter(noclasses=True))
        )
        payload['label'] = label

        import pandas as pd
        pd.set_option('display.max_colwidth', 100)
        hook = db.get_hook()
        try:
            df = hook.get_pandas_df(wwwutils.limit_sql(sql, CHART_LIMIT))
        except Exception as e:
            payload['error'] += "SQL execution failed. Details: " + str(e)

        if csv:
            return Response(
                response=df.to_csv(index=False),
                status=200,
                mimetype="application/text")

        if not payload['error'] and len(df) == CHART_LIMIT:
            payload['warning'] = (
                "Data has been truncated to {0}"
                " rows. Expect incomplete results.").format(CHART_LIMIT)

        if not payload['error'] and len(df) == 0:
            payload['error'] += "Empty result set. "
        elif (
                not payload['error'] and
                chart.sql_layout == 'series' and
                len(df.columns) < 3):
            payload['error'] += "SQL needs to return at least 3 columns. "
        elif (
                not payload['error'] and
                chart.sql_layout == 'columns'and
                len(df.columns) < 2):
            payload['error'] += "SQL needs to return at least 2 columns. "
        elif not payload['error']:
            import numpy as np

            data = None
            if chart.chart_type == "datatable":
                chart.show_datatable = True
            if chart.show_datatable:
                data = df.to_dict(orient="split")
                data['columns'] = [{'title': c} for c in data['columns']]

            # Trying to convert time to something Highcharts likes
            x_col = 1 if chart.sql_layout == 'series' else 0
            if chart.x_is_date:
                try:
                    # From string to datetime
                    df[df.columns[x_col]] = pd.to_datetime(
                        df[df.columns[x_col]])
                except Exception as e:
                    raise Exception(str(e))
                df[df.columns[x_col]] = df[df.columns[x_col]].apply(
                    lambda x: int(x.strftime("%s")) * 1000)

            series = []
            colorAxis = None
            if chart.chart_type in ('para',):
                df.rename(columns={
                    df.columns[0]: 'name',
                    df.columns[1]: 'group',
                }, inplace=True)
                return Response(
                    response=df.to_csv(index=False),
                    status=200,
                    mimetype="application/text")

            elif chart.chart_type == 'heatmap':
                color_perc_lbound = float(
                    request.args.get('color_perc_lbound', 0))
                color_perc_rbound = float(
                    request.args.get('color_perc_rbound', 1))
                color_scheme = request.args.get('color_scheme', 'blue_red')

                if color_scheme == 'blue_red':
                    stops = [
                        [color_perc_lbound, '#00D1C1'],
                        [
                            color_perc_lbound +
                            ((color_perc_rbound - color_perc_lbound)/2),
                            '#FFFFCC'
                        ],
                        [color_perc_rbound, '#FF5A5F']
                    ]
                elif color_scheme == 'blue_scale':
                    stops = [
                        [color_perc_lbound, '#FFFFFF'],
                        [color_perc_rbound, '#2222FF']
                    ]
                elif color_scheme == 'fire':
                    diff = float(color_perc_rbound - color_perc_lbound)
                    stops = [
                        [color_perc_lbound, '#FFFFFF'],
                        [color_perc_lbound + 0.33*diff, '#FFFF00'],
                        [color_perc_lbound + 0.66*diff, '#FF0000'],
                        [color_perc_rbound, '#000000']
                    ]
                else:
                    stops = [
                        [color_perc_lbound, '#FFFFFF'],
                        [
                            color_perc_lbound +
                            ((color_perc_rbound - color_perc_lbound)/2),
                            '#888888'
                        ],
                        [color_perc_rbound, '#000000'],
                    ]

                xaxis_label = df.columns[1]
                yaxis_label = df.columns[2]
                data = []
                for row in df.itertuples():
                    data.append({
                        'x': row[2],
                        'y': row[3],
                        'value': row[4],
                    })
                x_format = '{point.x:%Y-%m-%d}' \
                    if chart.x_is_date else '{point.x}'
                series.append({
                    'data': data,
                    'borderWidth': 0,
                    'colsize': 24 * 36e5,
                    'turboThreshold': sys.float_info.max,
                    'tooltip': {
                        'headerFormat': '',
                        'pointFormat': (
                            df.columns[1] + ': ' + x_format + '<br/>' +
                            df.columns[2] + ': {point.y}<br/>' +
                            df.columns[3] + ': <b>{point.value}</b>'
                        ),
                    },
                })
                colorAxis = {
                    'stops': stops,
                    'minColor': '#FFFFFF',
                    'maxColor': '#000000',
                    'min': 50,
                    'max': 2200,
                }
            else:
                if chart.sql_layout == 'series':
                    # User provides columns (series, x, y)
                    xaxis_label = df.columns[1]
                    yaxis_label = df.columns[2]
                    df[df.columns[2]] = df[df.columns[2]].astype(np.float)
                    df = df.pivot_table(
                        index=df.columns[1],
                        columns=df.columns[0],
                        values=df.columns[2], aggfunc=np.sum)
                else:
                    # User provides columns (x, y, metric1, metric2, ...)
                    xaxis_label = df.columns[0]
                    yaxis_label = 'y'
                    df.index = df[df.columns[0]]
                    df = df.sort(df.columns[0])
                    del df[df.columns[0]]
                    for col in df.columns:
                        df[col] = df[col].astype(np.float)

                for col in df.columns:
                    series.append({
                        'name': col,
                        'data': [
                            (i, v)
                            for i, v in df[col].iteritems() if not np.isnan(v)]
                    })
                series = [serie for serie in sorted(
                    series, key=lambda s: s['data'][0][1], reverse=True)]

            chart_type = chart.chart_type
            if chart.chart_type == "stacked_area":
                stacking = "normal"
                chart_type = 'area'
            elif chart.chart_type == "percent_area":
                stacking = "percent"
                chart_type = 'area'
            else:
                stacking = None
            hc = {
                'chart': {
                    'type': chart_type
                },
                'plotOptions': {
                    'series': {
                        'marker': {
                            'enabled': False
                        }
                    },
                    'area': {'stacking': stacking},
                },
                'title': {'text': ''},
                'xAxis': {
                    'title': {'text': xaxis_label},
                    'type': 'datetime' if chart.x_is_date else None,
                },
                'yAxis': {
                    'min': 0,
                    'title': {'text': yaxis_label},
                },
                'colorAxis': colorAxis,
                'tooltip': {
                    'useHTML': True,
                    'backgroundColor': None,
                    'borderWidth': 0,
                },
                'series': series,
            }

            if chart.y_log_scale:
                hc['yAxis']['type'] = 'logarithmic'
                hc['yAxis']['minorTickInterval'] = 0.1
                del hc['yAxis']['min']

            payload['state'] = 'SUCCESS'
            payload['hc'] = hc
            payload['data'] = data
            payload['request_dict'] = request_dict

        def date_handler(obj):
            return obj.isoformat() if hasattr(obj, 'isoformat') else obj

        return Response(
            response=json.dumps(payload, indent=4, default=date_handler),
            status=200,
            mimetype="application/json")


    @expose('/chart')
    @login_required
    def chart(self):
        session = settings.Session()
        chart_id = request.args.get('chart_id')
        chart = session.query(models.Chart).filter_by(id=chart_id).all()[0]
        session.expunge_all()
        session.commit()
        session.close()
        if chart.chart_type == 'para':
            return self.render('airflow/para/para.html', chart=chart)

        if chart.show_sql:
            sql = Markup(highlight(
                chart.sql,
                SqlLexer(),  # Lexer call
                HtmlFormatter(noclasses=True))
            )
        return self.render(
            'airflow/highchart.html',
            chart=chart,
            title="Airflow - Chart",
            sql=sql,
            label=chart.label)

    @expose('/dag_stats')
    def dag_stats(self):
        states = [State.SUCCESS, State.RUNNING, State.FAILED]
        task_ids = []
        for dag in dagbag.dags.values():
            task_ids += dag.task_ids
        data = {}
        TI = models.TaskInstance
        session = Session()
        qry = session.query(
            TI.dag_id,
            TI.state,
            sqla.func.count(TI.task_id)
        ).filter(TI.task_id.in_(task_ids)).group_by(TI.dag_id, TI.state)
        for dag_id, state, count in qry:
            if dag_id not in data:
                data[dag_id] = {}
            data[dag_id][state] = count
        session.commit()
        session.close()

        payload = {}
        for dag in dagbag.dags.values():
            payload[dag.dag_id] = []
            for state in states:
                try:
                    count = data[dag.dag_id][state]
                except:
                    count = 0
                d = {
                    'state': state,
                    'count': count,
                    'dag_id': dag.dag_id,
                    'color': State.color(state)
                }
                payload[dag.dag_id].append(d)
        return Response(
            response=json.dumps(payload, indent=4),
            status=200, mimetype="application/json")

    @expose('/code')
    def code(self):
        dag_id = request.args.get('dag_id')
        dag = dagbag.dags[dag_id]
        code = "".join(open(dag.full_filepath, 'r').readlines())
        title = dag.filepath
        html_code = highlight(
            code, PythonLexer(), HtmlFormatter(linenos=True))
        return self.render(
            'airflow/dag_code.html', html_code=html_code, dag=dag, title=title)

    @expose('/circles')
    def circles(self):
        return self.render(
            'airflow/circles.html')

    @expose('/sandbox')
    @login_required
    def sandbox(self):
        from airflow import configuration
        title = "Sandbox Suggested Configuration"
        cfg_loc = configuration.AIRFLOW_CONFIG + '.sandbox'
        f = open(cfg_loc, 'r')
        config = f.read()
        f.close()
        code_html = Markup(highlight(
            config,
            IniLexer(),  # Lexer call
            HtmlFormatter(noclasses=True))
        )
        return self.render(
            'airflow/code.html',
            code_html=code_html, title=title, subtitle=cfg_loc)

    @expose('/conf')
    @login_required
    def conf(self):
        from airflow import configuration
        raw = request.args.get('raw') == "true"
        title = "Airflow Configuration"
        subtitle = configuration.AIRFLOW_CONFIG
        f = open(configuration.AIRFLOW_CONFIG, 'r')
        config = f.read()

        f.close()
        if raw:
            return Response(
                response=config,
                status=200,
                mimetype="application/text")
        else:
            code_html = Markup(highlight(
                config,
                IniLexer(),  # Lexer call
                HtmlFormatter(noclasses=True))
            )
            return self.render(
                'airflow/code.html',
                pre_subtitle=settings.HEADER + "  v" + airflow.__version__,
                code_html=code_html, title=title, subtitle=subtitle)

    @expose('/noaccess')
    def noaccess(self):
        return self.render('airflow/noaccess.html')

    @expose('/health_checks')
    def health_checks(self):
        J = jobs.BaseJob

        session = settings.Session()
        latest_heartbeat = session.query(
            sqla.func.max(J.latest_heartbeat)).filter(
                J.job_type=='MasterJob').first()[0]
        session.commit()
        session.close()
        ago = (datetime.now() - latest_heartbeat).total_seconds()
        fail = False
        if (
                ago > (2.5 * conf.getint('master', "MASTER_HEARTBEAT_SEC"))
        ):
            fail = True
        d = {
            'master_latest_heartbeat': latest_heartbeat.isoformat(),
            'master_latest_heartbeat_sec_ago': ago,
            'fail': fail,
        }

        return Response(
            response=json.dumps(d, indent=4),
            status=200, mimetype="application/json")

    @expose('/headers')
    def headers(self):
        d = {k: v for k, v in request.headers}
        return Response(
            response=json.dumps(d, indent=4),
            status=200, mimetype="application/json")

    @expose('/login')
    def login(self):
        session = settings.Session()
        roles = [
            'airpal_topsecret.engineering.airbnb.com',
            'hadoop_user.engineering.airbnb.com',
            'analytics.engineering.airbnb.com',
            'nerds.engineering.airbnb.com',
        ]
        if 'X-Internalauth-Username' not in request.headers:
            return redirect(url_for('airflow.noaccess'))
        username = request.headers.get('X-Internalauth-Username')
        groups = request.headers.get(
            'X-Internalauth-Groups').lower().split(',')
        has_access = any([g in roles for g in groups])

        d = {k: v for k, v in request.headers}
        cookie = urllib2.unquote(d.get('Cookie', ''))
        mailsrch = re.compile(
            r'[\w\-][\w\-\.]+@[\w\-][\w\-\.]+[a-zA-Z]{1,4}')
        res = mailsrch.findall(cookie)
        email = res[0] if res else ''

        if has_access:
            user = session.query(models.User).filter(
                models.User.username == username).first()
            if not user:
                user = models.User(username=username)
            user.email = email
            session.merge(user)
            session.commit()
            flask_login.login_user(user)
            session.commit()
            session.close()
            return redirect(request.args.get("next") or url_for("index"))
        return redirect('/')

    @expose('/logout')
    def logout(self):
        flask_login.logout_user()
        return redirect('/admin')

    @expose('/rendered')
    def rendered(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = dateutil.parser.parse(execution_date)
        dag = dagbag.dags[dag_id]
        task = copy.copy(dag.get_task(task_id))
        ti = models.TaskInstance(task=task, execution_date=dttm)
        try:
            ti.render_templates()
        except Exception as e:
            flash("Error rendering template: " + str(e), "error")
        title = "{dag_id}.{task_id} [{execution_date}] rendered"
        html_dict = {}
        for template_field in task.__class__.template_fields:
            content = getattr(task, template_field)
            if template_field in special_attrs:
                html_dict[template_field] = highlight(
                    content,
                    special_attrs[template_field](),  # Lexer call
                    HtmlFormatter(linenos=True),
                )
            else:
                html_dict[template_field] = (
                    "<pre><code>" + content + "</pre></code>")

        return self.render(
            'airflow/dag_code.html',
            html_dict=html_dict,
            dag=dag,
            title=title.format(**locals()))

    @expose('/log')
    def log(self):
        BASE_LOG_FOLDER = os.path.expanduser(
            conf.get('core', 'BASE_LOG_FOLDER'))
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dag = dagbag.dags[dag_id]
        log_relative = "/{dag_id}/{task_id}/{execution_date}".format(
            **locals())
        loc = BASE_LOG_FOLDER + log_relative
        loc = loc.format(**locals())
        log = ""
        TI = models.TaskInstance
        session = Session()
        dttm = dateutil.parser.parse(execution_date)
        ti = session.query(TI).filter(
            TI.dag_id == dag_id, TI.task_id == task_id,
            TI.execution_date == dttm).first()
        if ti:
            host = ti.hostname
            if socket.gethostname() == host:
                try:
                    f = open(loc)
                    log += "".join(f.readlines())
                    f.close()
                except:
                    log = "Log file isn't where expected.\n".format(loc)
            else:
                WORKER_LOG_SERVER_PORT = \
                    conf.get('celery', 'WORKER_LOG_SERVER_PORT')
                url = (
                    "http://{host}:{WORKER_LOG_SERVER_PORT}/log"
                    "{log_relative}").format(**locals())
                log += "Log file isn't local.\n"
                log += "Fetching here: {url}\n".format(**locals())
                try:
                    import requests
                    log += requests.get(url).text
                except:
                    log += "Failed to fetch log file.".format(**locals())
            session.commit()
            session.close()

        title = "Logs for {task_id} on {execution_date}".format(**locals())

        return self.render(
            'airflow/dag_code.html', code=log, dag=dag, title=title)

    @expose('/task')
    def task(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        dag = dagbag.dags[dag_id]
        task = dag.get_task(task_id)
        task = copy.copy(task)
        task.resolve_template_files()

        attributes = []
        for attr_name in dir(task):
            if not attr_name.startswith('_'):
                attr = getattr(task, attr_name)
                if type(attr) != type(self.task) and \
                        attr_name not in special_attrs:
                    attributes.append((attr_name, str(attr)))

        title = "Task Details for {task_id}".format(**locals())

        # Color coding the special attributes that are code
        special_attrs_rendered = {}
        for attr_name in special_attrs:
            if hasattr(task, attr_name):
                source = getattr(task, attr_name)
                special_attrs_rendered[attr_name] = highlight(
                    source,
                    special_attrs[attr_name](),  # Lexer call
                    HtmlFormatter(linenos=True),
                )

        return self.render(
            'airflow/task.html',
            attributes=attributes,
            special_attrs_rendered=special_attrs_rendered,
            dag=dag, title=title)

    @expose('/action')
    def action(self):
        session = settings.Session()
        action = request.args.get('action')
        dag_id = request.args.get('dag_id')
        origin = request.args.get('origin')
        dag = dagbag.dags[dag_id]

        if action == 'clear':
            task_id = request.args.get('task_id')
            task = dag.get_task(task_id)
            execution_date = request.args.get('execution_date')
            execution_date = dateutil.parser.parse(execution_date)
            future = request.args.get('future') == "true"
            past = request.args.get('past') == "true"
            upstream = request.args.get('upstream') == "true"
            downstream = request.args.get('downstream') == "true"
            confirmed = request.args.get('confirmed') == "true"

            if confirmed:
                end_date = execution_date if not future else None
                start_date = execution_date if not past else None

                count = task.clear(
                    start_date=start_date,
                    end_date=end_date,
                    upstream=upstream,
                    downstream=downstream)

                flash("{0} task instances have been cleared".format(count))
                return redirect(origin)
            else:
                TI = models.TaskInstance
                qry = session.query(TI).filter(TI.dag_id == dag_id)

                if not future:
                    qry = qry.filter(TI.execution_date <= execution_date)
                if not past:
                    qry = qry.filter(TI.execution_date >= execution_date)

                tasks = [task_id]

                if upstream:
                    tasks += [
                        t.task_id
                        for t in task.get_flat_relatives(upstream=True)]
                if downstream:
                    tasks += [
                        t.task_id
                        for t in task.get_flat_relatives(upstream=False)]

                qry = qry.filter(TI.task_id.in_(tasks))
                if not qry.count():
                    flash("No task instances to clear", 'error')
                    response = redirect(origin)
                else:
                    details = "\n".join([str(t) for t in qry])

                    response = self.render(
                        'airflow/confirm.html',
                        message=(
                            "Here's the list of task instances you are about "
                            "to clear:"),
                        details=details,)

                session.commit()
                session.close()
                return response

    @expose('/tree')
    @wwwutils.gzipped
    def tree(self):
        dag_id = request.args.get('dag_id')
        dag = dagbag.dags[dag_id]
        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_downstream=False,
                include_upstream=True)

        session = settings.Session()

        base_date = request.args.get('base_date')
        if not base_date:
            base_date = datetime.now()
        else:
            base_date = dateutil.parser.parse(base_date)

        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else 25
        from_date = (base_date-(num_runs * dag.schedule_interval)).date()
        from_date = datetime.combine(from_date, datetime.min.time())

        dates = utils.date_range(
            from_date, base_date, dag.schedule_interval)
        task_instances = {}
        for ti in dag.get_task_instances(session, from_date):
            task_instances[(ti.task_id, ti.execution_date)] = ti

        expanded = []

        def recurse_nodes(task):
            children = [recurse_nodes(t) for t in task.upstream_list]

            # D3 tree uses children vs _children to define what is
            # expanded or not. The following block makes it such that
            # repeated nodes are collapsed by default.
            children_key = 'children'
            if task.task_id not in expanded:
                expanded.append(task.task_id)
            elif children:
                children_key = "_children"

            return {
                'name': task.task_id,
                'instances': [
                    utils.alchemy_to_dict(
                        task_instances.get((task.task_id, d))) or {
                            'execution_date': d.isoformat(),
                            'task_id': task.task_id
                        }
                    for d in dates],
                children_key: children,
                'num_dep': len(task.upstream_list),
                'operator': task.task_type,
                'retries': task.retries,
                'owner': task.owner,
                'start_date': task.start_date,
                'end_date': task.end_date,
                'depends_on_past': task.depends_on_past,
            }
        if len(dag.roots) > 1:
            # d3 likes a single root
            data = {
                'name': 'root',
                'instances': [],
                'children': [recurse_nodes(t) for t in dag.roots]
            }
        else:
            data = recurse_nodes(dag.roots[0])

        data = json.dumps(data, indent=4, default=utils.json_ser)
        session.commit()
        session.close()

        return self.render(
            'airflow/tree.html',
            dag=dag, data=data)

    @expose('/graph')
    def graph(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        arrange = request.args.get('arrange', "LR")
        if dag_id not in dagbag.dags:
            flash('DAG "{0}" seems to be missing.'.format(dag_id), "error")
            return redirect(url_for('index'))

        dag = dagbag.dags[dag_id]
        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_downstream=False,
                include_upstream=True)

        nodes = []
        edges = []
        for task in dag.tasks:
            nodes.append({
                'id': task.task_id,
                'value': {
                    'label': task.task_id,
                    'style': "fill:{0};".format(task.ui_color),
                }
            })

        def get_upstream(task):
            for t in task.upstream_list:
                edge = {
                    'u': t.task_id,
                    'v': task.task_id,
                }
                if edge not in edges:
                    edges.append(edge)
                    get_upstream(t)

        for t in dag.roots:
            get_upstream(t)

        dttm = request.args.get('execution_date')
        if dttm:
            dttm = dateutil.parser.parse(dttm)
        else:
            dttm = dag.latest_execution_date or datetime.now().date()

        form = GraphForm(data={'execution_date': dttm, 'arrange': arrange})

        task_instances = {
            ti.task_id: utils.alchemy_to_dict(ti)
            for ti in dag.get_task_instances(session, dttm, dttm)
        }
        tasks = {
            t.task_id: utils.alchemy_to_dict(t)
            for t in dag.tasks
        }
        session.commit()
        session.close()

        return self.render(
            'airflow/graph.html',
            dag=dag,
            form=form,
            execution_date=dttm.isoformat(),
            arrange=arrange,
            task_instances=json.dumps(task_instances, indent=2),
            tasks=json.dumps(tasks, indent=2),
            nodes=json.dumps(nodes, indent=2),
            edges=json.dumps(edges, indent=2),)

    @expose('/duration')
    def duration(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        days = int(request.args.get('days', 30))
        dag = dagbag.dags[dag_id]
        from_date = (datetime.today()-timedelta(days)).date()
        from_date = datetime.combine(from_date, datetime.min.time())

        all_data = []
        for task in dag.tasks:
            data = []
            for ti in task.get_task_instances(session, from_date):
                if ti.end_date:
                    data.append([
                        ti.execution_date.isoformat(),
                        float(ti.duration) / (60*60)
                    ])
            if data:
                all_data.append({'data': data, 'name': task.task_id})

        session.commit()
        session.close()

        return self.render(
            'airflow/chart.html',
            dag=dag,
            data=all_data,
            height="500px",
        )

    @expose('/landing_times')
    def landing_times(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        days = int(request.args.get('days', 30))
        dag = dagbag.dags[dag_id]
        from_date = (datetime.today()-timedelta(days)).date()
        from_date = datetime.combine(from_date, datetime.min.time())

        all_data = []
        for task in dag.tasks:
            data = []
            for ti in task.get_task_instances(session, from_date):
                if ti.end_date:
                    data.append([
                        ti.execution_date.isoformat(), (
                            ti.end_date - (
                                ti.execution_date + task.schedule_interval)
                        ).total_seconds()/(60*60)
                    ])
            all_data.append({'data': data, 'name': task.task_id})

        session.commit()
        session.close()

        return self.render(
            'airflow/chart.html',
            dag=dag,
            data=all_data,
            height="500px",
        )

    @expose('/gantt')
    def gantt(self):

        session = settings.Session()
        dag_id = request.args.get('dag_id')
        dag = dagbag.dags[dag_id]

        dttm = request.args.get('execution_date')
        if dttm:
            dttm = dateutil.parser.parse(dttm)
        else:
            dttm = dag.latest_execution_date or datetime.now().date()

        form = DateTimeForm(data={'execution_date': dttm})

        tis = dag.get_task_instances(session, dttm, dttm)
        tis = sorted(tis, key=lambda ti: ti.start_date)
        tasks = []
        data = []
        for i, ti in enumerate(tis):
            end_date = ti.end_date or datetime.now()
            tasks += [ti.task_id]
            color = State.color(ti.state)
            data.append({
                'x': i,
                'low': int(ti.start_date.strftime('%s')) * 1000,
                'high': int(end_date.strftime('%s')) * 1000,
                'color': color,
            })
        height = (len(tis) * 25) + 50
        session.commit()
        session.close()

        hc = {
            'chart': {
                'type': 'columnrange',
                'inverted': True,
                'height': height,
            },
            'xAxis': {'categories': tasks},
            'yAxis': {'type': 'datetime'},
            'title': {
                'text': None
            },
            'plotOptions': {
                'series': {
                    'cursor': 'pointer',
                    'minPointLength': 4,
                },
            },
            'legend': {
                'enabled': False
            },
            'series': [{
                'data': data
            }]
        }
        return self.render(
            'airflow/gantt.html',
            dag=dag,
            execution_date=dttm.isoformat(),
            form=form,
            hc=json.dumps(hc, indent=4),
            height=height,
        )


admin.add_view(Airflow(name='DAGs'))

# ------------------------------------------------
# Leveraging the admin for CRUD and browse on models
# ------------------------------------------------


class LoginMixin(object):
    def is_accessible(self):
        return AUTHENTICATE is False or \
            flask_login.current_user.is_authenticated()


class ModelViewOnly(ModelView):
    """
    Modifying the base ModelView class for non edit, browse only operations
    """
    named_filter_urls = True
    can_create = False
    can_edit = False
    can_delete = False
    column_display_pk = True


def log_link(v, c, m, p):
    url = url_for(
        'airflow.log',
        dag_id=m.dag_id,
        task_id=m.task_id,
        execution_date=m.execution_date.isoformat())
    return Markup(
        '<a href="{url}">'
        '    <span class="glyphicon glyphicon-book" aria-hidden="true">'
        '</span></a>').format(**locals())


def task_link(v, c, m, p):
    url = url_for(
        'airflow.task',
        dag_id=m.dag_id,
        task_id=m.task_id)
    return Markup(
        '<a href="{url}">{m.task_id}</a>'.format(**locals()))


def dag_link(v, c, m, p):
    url = url_for(
        'airflow.graph',
        dag_id=m.dag_id)
    return Markup(
        '<a href="{url}">{m.dag_id}</a>'.format(**locals()))


def duration_f(v, c, m, p):
    if m.end_date:
        return timedelta(seconds=m.duration)


class JobModelView(ModelViewOnly):
    column_default_sort = ('start_date', True)
    column_filters = (
        'job_type', 'dag_id', 'state',
        'unixname', 'hostname', 'start_date', 'end_date', 'latest_heartbeat')
mv = JobModelView(jobs.BaseJob, Session, name="Jobs", category="Browse")

admin.add_view(mv)


class LogModelView(ModelViewOnly):
    column_default_sort = ('dttm', True)
    column_filters = ('dag_id', 'task_id', 'execution_date')

mv = LogModelView(
    models.Log, Session, name="Logs", category="Browse")
admin.add_view(mv)


class TaskInstanceModelView(ModelViewOnly):
    column_filters = ('dag_id', 'task_id', 'state', 'execution_date')
    named_filter_urls = True
    column_formatters = dict(
        log=log_link, task_id=task_link, dag_id=dag_link, duration=duration_f)
    column_searchable_list = ('dag_id', 'task_id', 'state')
    column_list = (
        'dag_id', 'task_id', 'execution_date',
        'start_date', 'end_date', 'duration', 'state', 'job_id', 'log')
    can_delete = True
mv = TaskInstanceModelView(
    models.TaskInstance, Session, name="Task Instances", category="Browse")
admin.add_view(mv)

admin.add_link(
    base.MenuLink(
        category='Admin',
        name='Configuration',
        url='/admin/airflow/conf'))


class ConnectionModelView(LoginMixin, ModelView):
    column_list = ('conn_id', 'conn_type', 'host', 'port')
    form_choices = {
        'conn_type': [
            ('ftp', 'FTP',),
            ('hdfs', 'HDFS',),
            ('hive_cli', 'Hive Client Wrapper',),
            ('hive_metastore', 'Hive Metastore Thrift',),
            ('hiveserver2', 'Hive Server 2 Thrift',),
            ('mysql', 'MySQL',),
            ('oracle', 'Oracle',),
            ('presto', 'Presto',),
            ('s3', 'S3',),
            ('samba', 'Samba',),
        ]
    }
mv = ConnectionModelView(
    models.Connection, Session,
    name="Connections", category="Admin")
admin.add_view(mv)


class UserModelView(LoginMixin, ModelView):
    column_default_sort = 'username'
mv = UserModelView(models.User, Session, name="Users", category="Admin")
admin.add_view(mv)


class DagModelView(ModelView):
    column_list = ('dag_id', 'is_paused')
    column_editable_list = ('is_paused',)
mv = DagModelView(
    models.DAG, Session, name="Pause DAGs", category="Admin")
admin.add_view(mv)


class ReloadTaskView(BaseView):
    @expose('/')
    def index(self):
        logging.info("Reloading the dags")
        dagbag.collect_dags()
        dagbag.merge_dags()
        return redirect(url_for('index'))
admin.add_view(ReloadTaskView(name='Reload DAGs', category="Admin"))


def label_link(v, c, m, p):
    try:
        default_params = eval(m.default_params)
    except:
        default_params = {}
    url = url_for(
        'airflow.chart', chart_id=m.id, iteration_no=m.iteration_no,
        **default_params)
    return Markup("<a href='{url}'>{m.label}</a>".format(**locals()))


class ChartModelView(LoginMixin, ModelView):
    form_columns = (
        'label',
        'owner',
        'db',
        'chart_type',
        'show_datatable',
        'x_is_date',
        'y_log_scale',
        'show_sql',
        'height',
        'sql_layout',
        'sql',
        'default_params',)
    column_list = (
        'label', 'conn_id', 'chart_type', 'owner',
        'show_datatable', 'show_sql',)
    column_formatters = dict(label=label_link)
    create_template = 'airflow/chart/create.html'
    edit_template = 'airflow/chart/edit.html'
    column_filters = ('label', 'owner.username', 'conn_id')
    column_searchable_list = ('owner.username', 'label', 'sql')
    column_descriptions = {
        'label': "Can include {{ templated_fields }} and {{ macros }}",
        'chart_type': "The type of chart to be displayed",
        'sql': "Can include {{ templated_fields }} and {{ macros }}.",
        'height': "Height of the chart, in pixels.",
        'x_is_date': (
            "Whether the X axis should be casted as a date field. Expect most "
            "intelligible date formats to get casted properly."
        ),
        'owner': (
            "The chart's owner, mostly used for reference and filtering in "
            "the list view."
        ),
        'show_datatable':
            "Whether to display an interactive data table under the chart.",
        'default_params': (
            'A dictionary of {"key": "values",} that define what the '
            'templated fields (parameters) values should be by default. '
            'To be valid, it needs to "eval" as a Python dict. '
            'The key values will show up in the url\'s querystring '
            'and can be altered there.'
        ),
        'show_sql': "Whether to display the SQL statement as a collapsible "
            "section in the chart page.",
        'y_log_scale': "Whether to use a log scale for the Y axis.",
        'sql_layout': (
            "Defines the layout of the SQL that the application should "
            "expect. Depending on the tables you are sourcing from, it may "
            "make more sense to pivot / unpivot the metrics."
        ),
    }
    column_labels = {
        'db': "Source Database",
        'sql': "SQL",
        'height': "Chart Height",
        'sql_layout': "SQL Layout",
        'show_sql': "Display the SQL Statement",
        'default_params': "Default Parameters",
    }
    form_choices = {
        'chart_type': [
            ('line', 'Line Chart'),
            ('spline', 'Spline Chart'),
            ('bar', 'Bar Chart'),
            ('para', 'Parallel Coordinates'),
            ('column', 'Column Chart'),
            ('area', 'Overlapping Area Chart'),
            ('stacked_area', 'Stacked Area Chart'),
            ('percent_area', 'Percent Area Chart'),
            ('heatmap', 'Heatmap'),
            ('datatable', 'No chart, data table only'),
        ],
        'sql_layout': [
            ('series', 'SELECT series, x, y FROM ...'),
            ('columns', 'SELECT x, y (series 1), y (series 2), ... FROM ...'),
        ],
    }

    def on_model_change(self, form, model, is_created=True):
        if model.iteration_no is None:
            model.iteration_no = 0
        else:
            model.iteration_no += 1
        if AUTHENTICATE and not model.user_id and flask_login.current_user:
            model.user_id = flask_login.current_user.id

mv = ChartModelView(
    models.Chart, Session,
    name="Charts", category="Tools")
admin.add_view(mv)

admin.add_link(
    base.MenuLink(
        category='Docs',
        name='Documentation',
        url='http://pythonhosted.org/airflow/'))
admin.add_link(
    base.MenuLink(
        category='Docs',
        name='Github',
        url='https://github.com/mistercrunch/Airflow'))
