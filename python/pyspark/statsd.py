"""
DogStatsd is a Python client for DogStatsd, a Statsd fork for Datadog.
"""

import logging
from random import random
from time import time
import socket

try:
    from itertools import imap
except ImportError:
    imap = map


log = logging.getLogger('dogstatsd')


class DogStatsd(object):

    def __init__(self, host='localhost', port=8125):
        """
        Initialize a DogStatsd object.

        >>> statsd = DogStatsd()

        :param host: the host of the DogStatsd server.
        :param port: the port of the DogStatsd server.
        """
        self._host = None
        self._port = None
        self.socket = None
        self.connect(host, port)

    def connect(self, host, port):
        """
        Connect to the statsd server on the given host and port.
        """
        self._host = host
        self._port = int(port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.connect((self._host, self._port))

    def gauge(self, metric, value, tags=None, sample_rate=1):
        """
        Record the value of a gauge, optionally setting a list of tags and a
        sample rate.

        >>> statsd.gauge('users.online', 123)
        >>> statsd.gauge('active.connections', 1001, tags=["protocol:http"])
        """
        return self._send(metric, 'g', value, tags, sample_rate)

    def increment(self, metric, value=1, tags=None, sample_rate=1):
        """
        Increment a counter, optionally setting a value, tags and a sample
        rate. Tags is an optional list of key value pairs separated by
        colons.

        >>> statsd.increment('page.views')
        >>> statsd.increment('files.transferred', 124)
        """
        self._send(metric, 'c', value, tags, sample_rate)

    def decrement(self, metric, value=1, tags=None, sample_rate=1):
        """
        Decrement a counter, optionally setting a value, tags and a sample
        rate.

        >>> statsd.decrement('files.remaining')
        >>> statsd.decrement('active.connections', 2)
        """
        self._send(metric, 'c', -value, tags, sample_rate)

    def histogram(self, metric, value, tags=None, sample_rate=1):
        """
        Sample a histogram value, optionally setting tags and a sample rate.

        >>> statsd.histogram('uploaded.file.size', 1445)
        >>> statsd.histogram('album.photo.count', 26, tags=["gender:female"])
        """
        self._send(metric, 'h', value, tags, sample_rate)

    def timing(self, metric, value, tags=None, sample_rate=1):
        """
        Record a timing, optionally setting tags and a sample rate.

        >>> statsd.timing("query.response.time", 1234)
        """
        self._send(metric, 'ms', value, tags, sample_rate)

    def timed(self, metric, tags=None, sample_rate=1):
        """
        A decorator that will mesaure the distribution of a function's run time.
        Optionally specify a list of tag or a sample rate.
        ::

            @statsd.timed('user.query.time', sample_rate=0.5)
            def get_user(user_id):
                # Do what you need to ...
                pass

            # Is equivalent to ...
            start = time.time()
            try:
                get_user(user_id)
            finally:
                statsd.timing('user.query.time', time.time() - start)
        """
        def wrapper(func):
            def wrapped(*args, **kwargs):
                start = time()
                result = func(*args, **kwargs)
                self.timing(metric, time() - start, tags=tags, sample_rate=sample_rate)
                return result
            wrapped.__name__ = func.__name__
            wrapped.__doc__  = func.__doc__
            wrapped.__dict__.update(func.__dict__)
            return wrapped
        return wrapper

    def set(self, metric, value, tags=None, sample_rate=1):
        """
        Sample a set value.

        >>> statsd.set('visitors.uniques', 999)
        """
        self._send(metric, 's', value, tags, sample_rate)

    def _send(self, metric, metric_type, value, tags, sample_rate):
        if sample_rate != 1 and random() > sample_rate:
            return

        payload = [metric, ":", value, "|", metric_type]
        if sample_rate != 1:
            payload.extend(["|@", sample_rate])
        if tags:
            if not type(tags) is list:
                tags = [tags]
            payload.extend(["|#", ",".join([str(tag) for tag in tags])])

        try:
            self.socket.send("".join(imap(str, payload)))
        except socket.error:
            log.exception("Error submitting metric %s" % metric)
