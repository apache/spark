from datetime import datetime, timedelta
from functools import wraps
import inspect
import re


class State(object):
    """
    Static class with task instance states constants and color method to
    avoid hardcoding.
    """
    FAILED = "failed"
    RUNNING = "running"
    SUCCESS = "success"
    UP_FOR_RETRY = "up_for_retry"

    @classmethod
    def color(cls, state):
        if state == cls.FAILED:
            return "red"
        elif state == cls.RUNNING:
            return "lime"
        elif state == cls.SUCCESS:
            return "green"

    @classmethod
    def runnable(cls):
        return [None, cls.FAILED, cls.UP_FOR_RETRY]


def validate_key(k, max_length=250):
    if type(k) is not str:
        raise TypeError("The key has to be a string")
    elif len(k) > max_length:
        raise Exception("The key has to be less than {0} characters".format(
            max_length))
    elif not re.match(r'^[A-Za-z0-9_-]+$', k):
        raise Exception(
            "The key has to be made of alphanumeric characters, dashes "
            "and underscores exclusively")
    else:
        return True


def date_range(start_date, end_date=datetime.now(), delta=timedelta(1)):
    l = []
    if end_date >= start_date:
        while start_date <= end_date:
            l.append(start_date)
            start_date += delta
    else:
        raise Exception("start_date can't be after end_date")
    return l


def json_ser(obj):
    """
    json serializer that deals with dates
    usage: json.dumps(object, default=utils.json_ser)
    """
    if isinstance(obj, datetime):
        obj = obj.isoformat()
    return obj


def alchemy_to_dict(obj):
    """
    Transforms a SQLAlchemy model instance into a dictionary
    """
    if not obj:
        return None
    d = {}
    for c in obj.__table__.columns:
        value = getattr(obj, c.name)
        if type(value) == datetime:
            value = value.isoformat()
        d[c.name] = value
    return d

def readfile(filepath):
    f = open(filepath)
    content = f.read()
    f.close()
    return content


def apply_defaults(func):
    '''
    Function decorator that Looks for an argument named "default_args", and
    fills the unspecified arguments from it.

    Since python2.* isn't clear about which arguments are missing when
    calling a function, and that this can be quite confusing with multi-level
    inheritance and argument defaults, this decorator also alerts with
    specific information about the missing arguments.
    '''
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'default_args' in kwargs:
            default_args = kwargs['default_args']
            arg_spec = inspect.getargspec(func)
            num_defaults = len(arg_spec.defaults) if arg_spec.defaults else 0
            non_optional_args = arg_spec.args[:-num_defaults]
            if 'self' in non_optional_args:
                non_optional_args.remove('self')
            for arg in func.__code__.co_varnames:
                if arg in default_args and arg not in kwargs:
                    kwargs[arg] = default_args[arg]
            missing_args = list(set(non_optional_args) - set(kwargs))
            if missing_args:
                msg = "Argument {0} is required".format(missing_args)
                raise Exception(msg)
        result = func(*args, **kwargs)
        return result
    return wrapper
