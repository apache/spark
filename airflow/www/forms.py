from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from builtins import str
from past.builtins import basestring
from past.utils import old_div
import copy
from datetime import datetime, timedelta
import dateutil.parser
from functools import wraps
import inspect
from itertools import chain, product
import json
import logging
import os
import socket
import sys
import time
import traceback

from flask._compat import PY2
from flask import (
    Flask, url_for, Markup, Blueprint, redirect,
    flash, Response, render_template)
from flask.ext.admin import Admin, BaseView, expose, AdminIndexView
from flask.ext.admin.form import DateTimePickerWidget
from flask.ext.admin import base
from flask.ext.admin.contrib.sqla import ModelView
from flask.ext.cache import Cache
from flask import request, current_app
import sqlalchemy as sqla
from wtforms import (
    widgets,
    Form, DateTimeField, SelectField, TextAreaField, PasswordField, StringField)

from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter

import chartkick
import jinja2
import markdown
from sqlalchemy import or_

import airflow
from airflow import jobs, login, models, settings, utils
from airflow.configuration import conf
from airflow.models import State
from airflow.settings import Session
from airflow.utils import AirflowException
from airflow.www import utils as wwwutils


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


class TreeForm(Form):
    base_date = DateTimeField(
        "Anchor date", widget=DateTimePickerWidget(), default=datetime.now())
    num_runs = SelectField("Number of runs", default=25, choices=(
        (5, "5"),
        (25, "25"),
        (50, "50"),
        (100, "100"),
        (365, "365"),
    ))

