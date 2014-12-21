from datetime import datetime, timedelta, date
import dateutil.parser
import json
import logging

from flask import Flask, url_for, Markup, Blueprint, redirect, flash
from flask.ext.admin import Admin, BaseView, expose, AdminIndexView
from flask.ext.admin.form import DateTimePickerWidget
from flask.ext.admin import base
from flask.ext.admin.contrib.sqla import ModelView
from flask import request
from wtforms import Form, DateTimeField, SelectField, TextAreaField
from cgi import escape
from wtforms.compat import text_type
import wtforms

from pygments import highlight
from pygments.lexers import PythonLexer, SqlLexer, BashLexer
from pygments.formatters import HtmlFormatter

import jinja2


import markdown
import chartkick

from airflow.settings import Session
from airflow import jobs
from airflow import models
from airflow.models import State
from airflow import settings
from airflow.configuration import getconf
from airflow import utils

dagbag = models.DagBag(getconf().get('core', 'DAGS_FOLDER'))
session = Session()

app = Flask(__name__)
app.secret_key = 'airflowified'

# Init for chartkick, the python wrapper for highcharts
ck = Blueprint(
    'ck_page', __name__,
    static_folder=chartkick.js(), static_url_path='/static')
app.register_blueprint(ck, url_prefix='/ck')
app.jinja_env.add_extension("chartkick.ext.charts")


class AceEditorWidget(wtforms.widgets.TextArea):
    """
    Renders an ACE code editor.
    """
    def __call__(self, field, **kwargs):
        kwargs.setdefault('id', field.id)
        html = '''
        <div id="{el_id}" style="height:100px;">{contents}</div>
        <textarea id="{el_id}_ace" name="{form_name}" style="display:none;visibility:hidden;">
        </textarea>
        '''.format(
            el_id=kwargs.get('id', field.id),
            contents=escape(text_type(field._value())),
            form_name=field.id,
        )
        return wtforms.widgets.core.HTMLString(html)

# Date filter form needed for gantt and graph view
class DateTimeForm(Form):
    execution_date = DateTimeField("Execution date", widget=DateTimePickerWidget())


class GraphForm(Form):
    execution_date = DateTimeField("Execution date", widget=DateTimePickerWidget())
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
    return content;

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
admin = Admin(app, name="Airflow", index_view=HomeView(name='DAGs'))
admin.add_link(
    base.MenuLink(
        category='Tools',
        name='Query',
        url='/admin/airflow/query'))
admin.add_link(
    base.MenuLink(
        category='Docs',
        name='@readthedocs.org',
        url='http://airflow.readthedocs.org/en/latest/'))
admin.add_link(
    base.MenuLink(
        category='Docs',
        name='Github',
        url='https://github.com/mistercrunch/Airflow'))


class Airflow(BaseView):

    def is_visible(self):
        return False

    @expose('/')
    def index(self):
        return self.render('airflow/dags.html')

    @expose('/query')
    def query(self):
        session = settings.Session()
        dbs = session.query(models.DatabaseConnection).order_by(
            models.DatabaseConnection.db_id)
        db_choices = [(db.db_id, db.db_id) for db in dbs]
        db_id_str = request.args.get('db_id')
        sql = request.args.get('sql')
        class QueryForm(Form):
            db_id = SelectField("Layout", choices=db_choices)
            sql = TextAreaField("SQL", widget=AceEditorWidget())
        data = {
            'db_id': db_id_str,
            'sql': sql,
        }
        results = None
        has_data = False
        error = False
        if db_id_str:
            db = [db for db in dbs if db.db_id == db_id_str][0]
            hook = db.get_hook()
            try:
                df = hook.get_pandas_df(sql)
                has_data = len(df) > 0
                df = df.fillna('')
                results = df.to_html(
                    classes=(
                        "table initialism table-striped "
                        "table-bordered table-condensed"),
                    index=False,
                    na_rep='',

                ) if has_data else ''
            except Exception as e:
                flash(str(e), 'error')
                error = True

        if not has_data and error:
            flash('No data', 'error')

        form = QueryForm(request.form, data=data)
        session.commit()
        session.close()
        return self.render(
            'airflow/query.html', form=form,
            title="Query",
            results=results or '',
            has_data=has_data)

    @expose('/chart')
    def chart(self):
        from pandas.tslib import Timestamp
        session = settings.Session()
        chart_id = request.args.get('chart_id')
        chart = session.query(models.Chart).filter_by(id=chart_id).all()[0]
        show_sql = chart.show_sql
        db = session.query(
            models.DatabaseConnection).filter_by(db_id=chart.db_id).all()[0]
        session.expunge_all()

        all_data = {}
        hc = None
        hook = db.get_hook()
        try:
            args = eval(chart.default_params)
            if type(args) is not type(dict()):
                raise Exception('Not a dict')
        except:
            args = {}
            flash(
                "Default params is not valid, string has to evaluate as "
                "a Python dictionary", 'error')

        request_dict = {k:request.args.get(k) for k in request.args}
        args.update(request_dict)
        from airflow import macros
        args['macros'] = macros
        sql = jinja2.Template(chart.sql).render(**args)
        label = jinja2.Template(chart.label).render(**args)
        has_data = False
        table = None
        failed = False
        import pandas as pd
        pd.set_option('display.max_colwidth', 100)
        try:
            df = hook.get_pandas_df(sql)
            has_data = len(df)
        except Exception as e:
            flash(str(e), 'error')
            has_data = False
            failed = True

        if show_sql:
            sql = Markup(highlight(
                sql,
                SqlLexer(), # Lexer call
                HtmlFormatter(noclasses=True))
            )
        show_chart = True
        if failed:
            show_sql = True
            show_chart = False
        elif not has_data:
            show_sql = True
            show_chart = False
            flash('No data was returned', 'error')
        elif len(df.columns) < 3:
            show_sql = True
            show_chart = False
            flash(
                "SQL needs to return at least 3 columns (series, x, y)",
                'error')
        else:
            # Preparing the data in a format that chartkick likes
            for i, t in df.iterrows():
                series, x, y = t[:3]
                series = str(series)
                if series not in all_data:
                    all_data[series] = []
                if type(x) in (datetime, Timestamp, date) :
                    x = int(x.strftime("%s")) * 1000
                else:
                    x = int(dateutil.parser.parse(x).strftime("%s")) * 1000
                all_data[series].append([x, float(y)])
            all_data = [{
                    'name': series,
                    'data': sorted(all_data[series], key=lambda r: r[0])
                }
                for series in sorted(
                    all_data, key=lambda s: all_data[s][0][1], reverse=True)
            ]
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
                'chart':{
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
                    'title': {'text': df.columns[1]},
                    'type': 'datetime',
                },
                'yAxis': {
                    'min': 0,
                    'title': {'text': df.columns[2]},
                },
                'series': all_data,
            }

            if chart.show_datatable:
                table = df.to_html(
                    classes='table table-striped table-bordered')

        response = self.render(
            'airflow/highchart.html',
            chart=chart,
            title="Chart",
            table=Markup(table),
            hc=json.dumps(hc) if hc else None,
            show_chart=show_chart,
            show_sql=show_sql,
            sql=sql, label=label)
        session.commit()
        session.close()
        return response

    @expose('/code')
    def code(self):
        dag_id = request.args.get('dag_id')
        dag = dagbag.dags[dag_id]
        code = "".join(open(dag.full_filepath, 'r').readlines())
        title = dag.filepath.replace(getconf().get('core', 'BASE_FOLDER') + '/dags/', '')
        html_code = highlight(
            code, PythonLexer(), HtmlFormatter(noclasses=True))
        return self.render(
            'airflow/code.html', html_code=html_code, dag=dag, title=title)

    @expose('/log')
    def log(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dag = dagbag.dags[dag_id]
        loc = getconf().get('core', 'BASE_LOG_FOLDER') + "/{dag_id}/{task_id}/{execution_date}"
        loc = loc.format(**locals())
        try:
            f = open(loc)
            log = "".join(f.readlines())
        except:
            log = "The log file '{loc}' is missing.".format(**locals())
            TI = models.TaskInstance
            session = Session()
            ti = session.query(TI).filter(
                TI.dag_id==dag_id, TI.task_id==task_id,
                TI.execution_date==execution_date).first()
            if ti:
                host = ti.hostname
                log += "\n\nIt should be on host [{host}]".format(**locals())
            session.commit()
            session.close()


        log = "<pre><code>" + log + "</code></pre>"
        title = "Logs for {task_id} on {execution_date}".format(**locals())
        html_code = log

        return self.render(
            'airflow/code.html', html_code=html_code, dag=dag, title=title)

    @expose('/task')
    def task(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        dag = dagbag.dags[dag_id]
        task = dag.get_task(task_id)

        special_attrs = {
            'sql': SqlLexer,
            'hql': SqlLexer,
            'bash_command': BashLexer,
        }
        special_exts = ['.hql', '.sql', '.sh', '.bash']
        attributes = []
        for attr_name in dir(task):
            if not attr_name.startswith('_'):
                attr = getattr(task, attr_name)
                if type(attr) != type(self.task) and attr_name not in special_attrs:
                    attributes.append((attr_name, str(attr)))

        title = "Task Details for {task_id}".format(**locals())

        # Color coding the special attributes that are code
        special_attrs_rendered = {}
        for attr_name in special_attrs:
            if hasattr(task, attr_name):
                source = getattr(task, attr_name)
                if any([source.endswith(ext) for ext in special_exts]):
                    filepath = dag.folder + '/' + source
                    try:
                        f = open(filepath, 'r')
                        source = f.read()
                        f.close()
                    except Exception as e:
                        logging.error(e)
                        source = getattr(task, attr_name)
                special_attrs_rendered[attr_name] = highlight(
                    source,
                    special_attrs[attr_name](), # Lexer call
                    HtmlFormatter(noclasses=True)
                )

        return self.render(
            'airflow/task.html',
            attributes=attributes,
            special_attrs_rendered=special_attrs_rendered,
            dag=dag, title=title)

    @expose('/action')
    def action(self):
        action = request.args.get('action')
        dag_id = request.args.get('dag_id')
        origin = request.args.get('origin')
        dag = dagbag.dags[dag_id]

        if action == 'clear':
            task_id = request.args.get('task_id')
            task = dag.get_task(task_id)
            execution_date = request.args.get('execution_date')
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
                    tasks += \
                        [t.task_id for t in task.get_flat_relatives(upstream=True)]
                if downstream:
                    tasks += \
                        [t.task_id for t in task.get_flat_relatives(upstream=False)]

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
    def tree(self):
        dag_id = request.args.get('dag_id')
        dag = dagbag.dags[dag_id]
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
        dag = dagbag.dags[dag_id]

        nodes = []
        edges = []
        for task in dag.tasks:
            nodes.append({
                'id': task.task_id,
                'value': {'label': task.task_id}
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
        dag = dagbag.dags[dag_id]
        from_date = (datetime.today()-timedelta(30)).date()
        from_date = datetime.combine(from_date, datetime.min.time())

        all_data = []
        for task in dag.tasks:
            data = []
            for ti in task.get_task_instances(session, from_date):
                if ti.end_date:
                    data.append([
                        ti.execution_date.isoformat(),
                        int(ti.duration)
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
        dag = dagbag.dags[dag_id]
        from_date = (datetime.today()-timedelta(30)).date()
        from_date = datetime.combine(from_date, datetime.min.time())

        all_data = []
        for task in dag.tasks:
            data = []
            for ti in task.get_task_instances(session, from_date):
                if ti.end_date:
                    data.append([
                        ti.execution_date.isoformat(), (
                            ti.end_date - ti.execution_date
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
        execution_date=m.execution_date)
    return Markup(
        '<a href="{url}"><i class="icon-book"></i></a>'.format(**locals()))


def task_link(v, c, m, p):
    url = url_for(
        'airflow.task',
        dag_id=m.dag_id,
        task_id=m.task_id)
    return Markup(
        '<a href="{url}">{m.task_id}</a>'.format(**locals()))

def dag_link(v, c, m, p):
    url = url_for(
        'airflow.tree',
        dag_id=m.dag_id)
    return Markup(
        '<a href="{url}">{m.dag_id}</a>'.format(**locals()))

def duration_f(v, c, m, p):
    if m.end_date:
        return timedelta(seconds=m.duration)

class TaskInstanceModelView(ModelView):
    column_filters = ('dag_id', 'task_id', 'state', 'execution_date')
    column_formatters = dict(
        log=log_link, task_id=task_link, dag_id=dag_link, duration=duration_f)
    column_searchable_list = ('dag_id', 'task_id', 'state')
    column_list = (
        'dag_id', 'task_id', 'execution_date',
        'start_date', 'end_date', 'duration', 'state', 'log')
    can_delete = True
mv = TaskInstanceModelView(
    models.TaskInstance, session, name="Task Instances", category="Admin")
admin.add_view(mv)


class JobModelView(ModelViewOnly):
    column_default_sort = ('start_date', True)
mv = JobModelView(jobs.BaseJob, session, name="Jobs", category="Admin")
admin.add_view(mv)


mv = ModelView(models.User, session, name="Users", category="Admin")
admin.add_view(mv)


class DatabaseConnectionModelView(ModelView):
    column_list = ('db_id', 'db_type', 'host', 'port')
    form_choices = {
        'db_type': [
            ('hive', 'Hive',),
            ('presto', 'Presto',),
            ('mysql', 'MySQL',),
            ('oracle', 'Oracle',),
        ]
    }
mv = DatabaseConnectionModelView(
    models.DatabaseConnection, session,
    name="Database Connections", category="Admin")
admin.add_view(mv)


class LogModelView(ModelViewOnly):
    column_default_sort = ('dttm', True)
    column_filters = ('dag_id', 'task_id', 'execution_date')

mv = LogModelView(
    models.Log, session, name="Logs", category="Admin")
admin.add_view(mv)

class ReloadTaskView(BaseView):
    @expose('/')
    def index(self):
        logging.info("Reloading the dags")
        dagbag.collect_dags()
        return redirect(url_for('index'))
admin.add_view(ReloadTaskView(name='Reload DAGs', category="Admin"))



def label_link(v, c, m, p):
    try:
        default_params = eval(m.default_params)
    except:
        default_params = {}
    url = url_for('airflow.chart', chart_id=m.id, **default_params)
    return Markup("<a href='{url}'>{m.label}</a>".format(**locals()))


class ChartModelView(ModelView):
    column_list = ('label', 'id', 'db_id', 'chart_type', 'show_datatable', )
    column_formatters = dict(label=label_link)
    form_choices = {
        'chart_type': [
            ('line', 'Line Chart'),
            ('spline', 'Spline Chart'),
            ('bar', 'Bar Chart'),
            ('column', 'Column Chart'),
            ('area', 'Overlapping Area Chart'),
            ('stacked_area', 'Stacked Area Chart'),
            ('percent_area', 'Percent Area Chart'),
        ]
    }
mv = ChartModelView(
    models.Chart, session,
    name="Charts", category="Tools")
admin.add_view(mv)
