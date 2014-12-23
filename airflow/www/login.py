import urllib2
import json

import flask_login
from flask import redirect

from airflow.models import User
from airflow import settings

login_manager = flask_login.LoginManager()

@login_manager.user_loader
def load_user(userid):
    return "max"


login_manager.login_view = 'airflow.login'
login_manager.login_message = None
