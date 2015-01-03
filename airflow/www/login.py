import urllib2
import json

import flask_login
from flask import redirect

from airflow.models import User
from airflow import settings
from airflow.configuration import conf

login_manager = flask_login.LoginManager()

@login_manager.user_loader
def load_user(userid):
    session = settings.Session()
    user = session.query(User).filter(User.id == userid).first()
    if not user:
        raise Exception(userid)
    session.expunge_all()
    session.commit()
    session.close()
    return user


login_manager.login_view = 'airflow.login'
login_manager.login_message = None
