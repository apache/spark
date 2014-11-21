from datetime import datetime, timedelta
import dateutil.parser
import json
import logging

from flask import Flask, url_for, Markup, Blueprint, redirect, flash
from flask.ext.admin import Admin, BaseView, expose, AdminIndexView
from flask.ext.admin.contrib.sqla import ModelView
from flask import request
from wtforms import Form, DateTimeField, SelectField, TextField, TextAreaField

from pygments import highlight
from pygments.lexers import PythonLexer, SqlLexer, BashLexer
from pygments.formatters import HtmlFormatter


import markdown
import chartkick

from airflow.settings import Session
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


# Date filter form needed for gantt and graph view
class DateTimeForm(Form):
    execution_date = DateTimeField("Execution date")


class GraphForm(Form):
    execution_date = DateTimeField("Execution date")
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

class HomeView(AdminIndexView):
    """
    Basic home view, just showing the README.md file
    """
    @expose("/")
    def index(self):
        md = "".join(
            open(getconf().get('core', 'AIRFLOW_HOME') + '/README.md', 'r').readlines())
        content = Markup(markdown.markdown(md))
        return self.render('admin/index.html', content=content)
admin = Admin(app, name="Airflow", index_view=HomeView(name='Home'))


class Airflow(BaseView):

    def is_visible(self):
        return False

    @expose('/')
    def index(self):
        return self.render('admin/dags.html')

    @expose('/query')
    def query(self):
        session = settings.Session()
        dbs = session.query(models.DatabaseConnection)
        db_choices = [(db.db_id, db.db_id) for db in dbs]
        db_id_str = request.args.get('db_id')
        sql = request.args.get('sql')
        class QueryForm(Form):
            db_id = SelectField("Layout", choices=db_choices)
            sql = TextAreaField("Execution date")
        data = {
            'db_id': db_id_str,
            'sql': sql,
        }
        results = None
        if db_id_str:
            db = [db for db in dbs if db.db_id == db_id_str][0]
            hook = db.get_hook()
            results = hook.get_pandas_df(sql).to_html(
                classes="table table-striped table-bordered model-list")

        form = QueryForm(request.form, data=data)
        session.commit()
        session.close()
        return self.render('admin/query.html', form=form, results=results)

    @expose('/code')
    def code(self):
        dag_id = request.args.get('dag_id')
        dag = dagbag.dags[dag_id]
        code = "".join(open(dag.filepath, 'r').readlines())
        title = dag.filepath.replace(getconf().get('core', 'BASE_FOLDER') + '/dags/', '')
        html_code = highlight(
            code, PythonLexer(), HtmlFormatter(noclasses=True))
        return self.render(
            'admin/code.html', html_code=html_code, dag=dag, title=title)

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
            log = "Log file is missing"

        log = "<pre><code>" + log + "</code></pre>"
        title = "Logs for {task_id} on {execution_date}".format(**locals())
        html_code = log

        return self.render(
            'admin/code.html', html_code=html_code, dag=dag, title=title)

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
            'admin/task.html',
            attributes=attributes,
            special_attrs_rendered=special_attrs_rendered,
            dag=dag, title=title)

    @expose('/tree')
    def tree(self):
        dag_id = request.args.get('dag_id')
        action = request.args.get('action')
        dag = dagbag.dags[dag_id]

        base_date = request.args.get('base_date')
        if not base_date:
            base_date = datetime.now()
        else:
            base_date = dateutil.parser.parse(base_date)

        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else 25
        from_date = (base_date-(num_runs * dag.schedule_interval)).date()
        from_date = datetime.combine(from_date, datetime.min.time())

        if action == 'clear':
            task_id = request.args.get('task_id')
            task = dag.get_task(task_id)
            execution_date = request.args.get('execution_date')
            future = request.args.get('future') == "true"
            past = request.args.get('past') == "true"
            upstream = request.args.get('upstream') == "true"
            downstream = request.args.get('downstream') == "true"

            start_date = execution_date
            end_date = execution_date

            if future:
                end_date = None
            if past:
                start_date = None

            count = task.clear(
                start_date=start_date,
                end_date=end_date,
                upstream=upstream,
                downstream=downstream)

            flash("{0} task instances have been cleared".format(count))
            return redirect(url_for('airflow.tree', dag_id=dag_id))

        dates = utils.date_range(
            from_date, base_date, dag.schedule_interval)
        task_instances = {}
        for ti in dag.get_task_instances(from_date):
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

        return self.render(
            'admin/tree.html',
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
            for ti in dag.get_task_instances(dttm, dttm)
        }
        tasks = {
            t.task_id: utils.alchemy_to_dict(t)
            for t in dag.tasks
        }
        session.commit()
        session.close()

        return self.render(
            'admin/graph.html',
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
            for ti in task.get_task_instances(from_date):
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
            'admin/chart.html',
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
            for ti in task.get_task_instances(from_date):
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
            'admin/chart.html',
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
            dttm = dag.latest_execution_date

        form = DateTimeForm(data={'execution_date': dttm})

        tis = dag.get_task_instances(dttm, dttm)
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
                    # 'borderColor': 'black',
                    'minPointLength': 5,
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
            'admin/gantt.html',
            dag=dag,
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


def filepath_formatter(view, context, model, name):
    url = url_for('airflow.code', dag_id=model.dag_id)
    short_fp = model.filepath.replace(getconf().get('core', 'BASE_FOLDER') + '/dags/', '')
    link = Markup('<a href="{url}">{short_fp}</a>'.format(**locals()))
    return link


def dag_formatter(view, context, model, name):
    url = url_for('airflow.tree', dag_id=model.dag_id, num_runs=25)
    link = Markup('<a href="{url}">{model.dag_id}</a>'.format(**locals()))
    return link


class DagModelView(ModelViewOnly):
    column_formatters = {
        'dag_id': dag_formatter,
        'filepath': filepath_formatter,
    }
    column_list = ('dag_id', 'task_count', 'filepath')
mv = DagModelView(models.DAG, session, name="DAGs", endpoint="dags")
admin.add_view(mv)


def dag_link(view, context, model, name):
    return model


class DagModelView(ModelViewOnly):
    column_formatters = {
        'dag_id': dag_link,
    }


class TaskModelView(ModelViewOnly):
    column_filters = ('dag_id', 'owner', 'start_date', 'end_date')
    column_formatters = {
        'dag': dag_formatter,
    }
mv = TaskModelView(
    models.BaseOperator, session, name="Tasks", category="Objects")
admin.add_view(mv)


class TaskInstanceModelView(ModelViewOnly):
    column_filters = ('dag_id', 'task_id', 'state', 'execution_date')
    column_list = (
        'state', 'dag_id', 'task_id', 'execution_date',
        'start_date', 'end_date', 'duration')
    can_delete = True
mv = TaskInstanceModelView(
    models.TaskInstance, session, name="Task Instances", category="Objects")
admin.add_view(mv)


class JobModelView(ModelViewOnly):
    column_default_sort = ('start_date', True)
mv = JobModelView(models.BaseJob, session, name="Jobs", category="Objects")
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
mv = LogModelView(models.Log, session, name="Logs", category="Admin")
admin.add_view(mv)

class ReloadTaskView(BaseView):
    @expose('/')
    def index(self):
        logging.info("Reloading the dags")
        bag = models.DagBag();
        return redirect(url_for('dags.index_view'))
admin.add_view(ReloadTaskView(name='Reload DAGs', category="Admin"))

if __name__ == "__main__":
    logging.info("Starting the web server.")
    app.run(debug=True)
