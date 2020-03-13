""" Simple and small Celery Monitor """
import time
import json
import logging
import random

from functools import wraps


import requests

try:
    from requests.compat import json as complexjson
except ImportError:
    import json as complexjson

from requests.sessions import Session
from requests.exceptions import Timeout, ConnectionError

from requests.auth import HTTPBasicAuth
from requests.auth import _basic_auth_str


from celery import current_app

LOGGER = logging.getLogger(__name__)

# ELASTIC_SEARCH_SERVER = 'http://127.0.0.1:9200'
ELASTIC_SEARCH_SERVER = 'http://10.129.65.10:9200'

ELASTIC_AUTH_API_ID = None
ELASTIC_AUTH_API_KEY = None
ELASTIC_AUTH_REQUIRED = False

PROXY = None
ELASTIC_PROXY_REQUIRED = False


def is_json_serializable(object_, request=False):
    """
    Util to verify if object is json serializable,
    support requests object json serializer verification for requests lib (doesn't use json lib).
    Args:
        object_: A python object to analyze.
        request: A boolean indicating if data will send on requests action [default: false].

    Returns:
        A boolean indicating if object is serializable or nor.
    """
    try:
        if request:
            complexjson.dumps(object_)
        else:
            json.dumps(object_)
        return True
    except (TypeError, ValueError):
        return False


class ElasticApiAuth(HTTPBasicAuth):
    """
    Class to define a new header head to manage elastic api_key authentication, inspired from HTTPBasicAuth
    """

    def __call__(self, r):
        header = _basic_auth_str(username=self.username, password=self.password)
        r.headers['Authorization'] = header.replace(u'Basic', u'ApiKey')
        return r


class RequestMonitor(type):
    """ Meta-Class to define a monitor over each request method """
    _service_url = None

    @classmethod
    def exponential_delay(mcs):
        """ Method to create a exponential value """
        minutes = getattr(mcs, '_delay_time', 60) / 60
        rand = random.uniform(minutes, minutes * 2)
        return int(rand ** getattr(mcs, '_retries', 0)) * 60

    @staticmethod
    def request_manager(class_=None):
        """
        Method decorator to analyze all Timeout exception on request methods
        and define a retry system using circuit break pattern logic
        """
        def manager(func_):
            """ Decorator function manager """
            @wraps(func_)
            def monitor(*args, **kwargs):
                """ Inner function to process call execution """
                try:
                    if not class_._close:
                        requests.head(class_._service_url, timeout=2)
                        LOGGER.info(
                            msg=u'Available Service: {0} was found.'.format(class_._service_url)
                        )
                        class_._close = True

                    return func_(*args, **kwargs)
                except (Timeout, ConnectionError) as e:
                    class_._close = False
                    class_._failures += 1
                    if class_._retries > class_._max_retries:
                        raise e
                    delay = class_.exponential_delay() if class_._retries > 0 else class_._delay_time
                    delay_seconds = class_._max_delay_time if delay > class_._max_delay_time else delay
                    LOGGER.critical(
                        msg=u'Unable to connect to Service: {0}, retrying on {1} secs'.format(
                            class_._service_url,
                            delay_seconds,
                        )
                    )
                    time.sleep(delay_seconds)
                    class_._retries += 1
                    return monitor(*args, **kwargs)

            monitor.__name__ = func_.__name__
            return monitor
        return manager

    def __new__(mcs, classname, base_classes, attributes):
        """
        Override __new__ method to assign monitor decorator on all callable attributes,
        all classes using this metaclass its method could be executing a request process.
        """
        if not attributes.get('_service_url', None):
            raise NotImplementedError(u'Must define a _service_url class attribute')
        attributes.update({
            '_close': True,
            '_failures': 0,
            '_retries': 0,
            '_delay_time': 60,  # 1 minute
            '_max_retries': 5,
            '_max_delay_time': 300,  # 5 minutes
        })
        class_ = type.__new__(mcs, classname, base_classes, attributes)
        for attr_name, attr in vars(class_).iteritems():
            if callable(attr) and not attr_name.startswith(u'__') and not attr_name.startswith(u'_%s' % classname):
                setattr(
                    class_,
                    attr_name,
                    mcs.request_manager(class_=class_)(getattr(class_, attr_name)),
                )
        return class_


class ElasticManager(object):
    """ Simple Elastic Connection Manager Class with Retry System {Singleton} """
    __metaclass__ = RequestMonitor
    __proxies = {
        'http': 'http://' + PROXY,
        'https': 'https://' + PROXY,
        'ftp': 'ftp://' + PROXY
    } if ELASTIC_PROXY_REQUIRED else {}
    # TODO Debate if change to token auth behavior
    __auth = ElasticApiAuth(
        username=ELASTIC_AUTH_API_ID,
        password=ELASTIC_AUTH_API_KEY,
    ) if ELASTIC_AUTH_REQUIRED else None
    _service_url = ELASTIC_SEARCH_SERVER

    def __init__(self, index):
        """ Initialize Class to create Session Object """
        self.__session = Session()
        self.__session.trust_env = False
        self.__session.auth = self.__auth
        self.__session.proxies = self.__proxies
        self.__session.timeout = 10
        self.__session.headers.update({'Content-Type': u'application/json'})
        self.elastic_index = index
        self.base_url = self.__build_url()

    def __build_url(self):
        """ Method to build elastic base url """
        return u'{base}{index}/'.format(
            base=u'{0}/'.format(self._service_url) if self._service_url[-1] != '/' else self._service_url,
            index=self.elastic_index,
        )

    def get_simple_doc(self, key, doc_type=u'_doc', **kwargs):
        """ Method to get simple elastic search document by key """
        url = u'{base}{doc_type}/{key}/'.format(
            base=self.base_url,
            doc_type=doc_type,
            key=key,
        )
        response = self.__session.get(
            url=url,
        )
        try:
            return response.json()
        except ValueError:
            LOGGER.exception(msg=u'Un-parsable response from elastic.')
            return {}

    def get_query_doc(self, query, **kwargs):
        """ Method to get documents from elastic by query """
        if type(query) != dict or not is_json_serializable(query, request=True):
            LOGGER.error(msg=u'`query` information is not json serializable no allowed to for perform elastic request.')
            return {}

        url = u'{base}_search/'.format(
            base=self.base_url,
        )
        response = self.__session.get(
            url=url,
            json=query,
        )
        try:
            return response.json()
        except ValueError:
            LOGGER.exception(msg=u'Un-parsable response from elastic.')
            return {}

    def create_doc(self, data, doc_type=u'_doc', **kwargs):
        """ Method to create doc on """
        if type(data) != dict or not is_json_serializable(data, request=True):
            LOGGER.error(msg=u'`data` information is not json serializable no allowed to for perform elastic request.')
            return {}

        url = u'{base}{doc_type}/{key}'.format(
            base=self.base_url,
            doc_type=doc_type,
            key=kwargs.get('key', ''),
        )
        response = self.__session.post(
            url=url,
            json=data,
        )
        try:
            return response.json()
        except ValueError:
            LOGGER.exception(msg=u'Un-parsable response from elastic.')
            return {}

    def simple_doc_update(self, key, data, **kwargs):
        """ Simple update method for update document on Elastic using _update doc_type """
        if type(data) != dict or not is_json_serializable(data, request=True):
            LOGGER.error(msg=u'`data` information is not json serializable no allowed to for perform elastic request.')
            return {}

        url = u'{base}_update/{key}'.format(
            base=self.base_url,
            key=key,
        )
        data_ = {
            'doc': data
        }
        response = self.__session.put(
            url=url,
            json=data_,
        )
        try:
            return response.json()
        except ValueError:
            LOGGER.exception(msg=u'Un-parsable response from elastic.')
            return {}

    def simple_doc_delete(self, key, doc_type=u'_doc', **kwargs):
        """ Method to delete elastic document """
        url = u'{base}{doc_type}/{key}'.format(
            base=self.base_url,
            doc_type=doc_type,
            key=key,
        )
        response = self.__session.delete(
            url=url,
        )
        try:
            return response.json()
        except ValueError:
            LOGGER.exception(msg=u'Un-parsable response from elastic.')
            return {}

    def __enter__(self):
        """ For Context Manager Compatibility """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Close session connection """
        self.__session.close()

    def __delete__(self, instance):
        """ Close session connection """
        self.__session.close()


class Monitor(object):
    app = current_app

    def __init__(self):
        self.state = self.app.events.State()
        self.app.control.enable_events()

    @staticmethod
    def serialize_task(task):
        """ Simple method to serialize Task object """
        from datetime import datetime
        return {
            'task_id': task.uuid,
            'task_name': task.name,
            'task_args': task.args,
            'task_kwargs': task.kwargs,
            'type': task.type,
            'ready': task.ready,
            'status': task.state,
            'result': task.result,

            'sent': task.sent,
            'failed': task.failed,
            'revoked': task.revoked,
            'runtime': task.runtime,
            'retried': task.retried,
            'retries': task.retries,
            'exception': task.exception,
            'traceback': task.traceback,

            'worker': task.worker.id,
            'hostname': task.hostname,
            'pid': task.pid,
            'eta': task.eta,
            'expires': task.expires,
            'exchange': task.exchange,
            'routing_key': task.routing_key,
            'clock': task.clock,
            'utcoffset': task.utcoffset,

            'received': datetime.fromtimestamp(task.received).isoformat(),
            'started': datetime.fromtimestamp(task.started).isoformat(),
            'succeeded': datetime.fromtimestamp(task.succeeded).isoformat(),
            'local_received': datetime.fromtimestamp(task.local_received).isoformat(),
            'timestamp': datetime.fromtimestamp(task.timestamp).isoformat(),
        }

    def announce_failed_tasks(self, event):
        self.state.event(event)
        # task name is sent only with -received event, and self.state
        # will keep track of this for us.
        task = self.state.tasks.get(event['uuid'])

        with ElasticManager(index='celery-events') as elastic:
            elastic.create_doc(
                key=task.uuid,
                data=self.serialize_task(task)
            )

        print('TASK FAILED: %s[%s] %s' % (
            task.name, task.uuid, task.info(),))

    def announce_received_tasks(self, event):
        self.state.event(event)
        # task name is sent only with -received event, and self.state
        # will keep track of this for us.
        task = self.state.tasks.get(event['uuid'])

        with ElasticManager(index='celery-events') as elastic:
            elastic.create_doc(
                key=task.uuid,
                data=self.serialize_task(task)
            )

        print('TASK RECEIVED: %s[%s] %s' % (
            task.name, task.uuid, task.info(),))

    def announce_sent_tasks(self, event):
        self.state.event(event)
        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task = self.state.tasks.get(event['uuid'])

        with ElasticManager(index='celery-events') as elastic:
            elastic.create_doc(
                key=task.uuid,
                data=self.serialize_task(task)
            )

        print('TASK SEND: %s[%s] %s' % (
            task.name, task.uuid, task.info(),))

    def announce_started_tasks(self, event):
        self.state.event(event)
        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task = self.state.tasks.get(event['uuid'])

        with ElasticManager(index='celery-events') as elastic:
            elastic.create_doc(
                key=task.uuid,
                data=self.serialize_task(task)
            )

        print('TASK STARTED: %s[%s] %s' % (
            task.name, task.uuid, task.info(),))

    def announce_succeded_tasks(self, event):
        self.state.event(event)
        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task = self.state.tasks.get(event['uuid'])

        with ElasticManager(index='celery-events') as elastic:
            elastic.create_doc(
                key=task.uuid,
                data=self.serialize_task(task)
            )

        print('TASK SUCCEDED: %s[%s] %s' % (
            task.name, task.uuid, task.info(),))

    def announce_rejected_tasks(self, event):
        self.state.event(event)
        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task = self.state.tasks.get(event['uuid'])

        with ElasticManager(index='celery-events') as elastic:
            elastic.create_doc(
                key=task.uuid,
                data=self.serialize_task(task)
            )

        print('TASK REJECTED: %s[%s] %s' % (
            task.name, task.uuid, task.info(),))

    def announce_revoked_tasks(self, event):
        self.state.event(event)
        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task = self.state.tasks.get(event['uuid'])

        with ElasticManager(index='celery-events') as elastic:
            elastic.create_doc(
                key=task.uuid,
                data=self.serialize_task(task)
            )

        print('TASK REVOKED: %s[%s] %s' % (
            task.name, task.uuid, task.info(),))

    def announce_retried_tasks(self, event):
        self.state.event(event)
        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task = self.state.tasks.get(event['uuid'])

        with ElasticManager(index='celery-events') as elastic:
            elastic.create_doc(
                key=task.uuid,
                data=self.serialize_task(task)
            )

        print('TASK RETRIED: %s[%s] %s' % (
            task.name, task.uuid, task.info(),))

    def worker_disconnect(self, event):
        print "WORKER DISCONNECT", event

    def woker_heartbeat(self, event):
        """ """
        # print "WORKER HEARTBEAT", event

    def worker_online(self, event):
        print "WORKER ONLINE", event

    def __start_monitoring(self):
        with self.app.connection() as connection:
            recv = self.app.events.Receiver(
                connection,
                handlers={
                    'task-failed': self.announce_failed_tasks,
                    'task-received': self.announce_received_tasks,
                    'task-sent': self.announce_sent_tasks,
                    'task-started': self.announce_started_tasks,
                    'task-succeeded': self.announce_succeded_tasks,
                    'task-rejected': self.announce_rejected_tasks,
                    'task-revoked': self.announce_revoked_tasks,
                    'task-retried': self.announce_retried_tasks,
                    'worker-offline': self.worker_disconnect,
                    'worker-online': self.worker_online,
                    'worker-heartbeat': self.woker_heartbeat,
                }
            )

            recv.capture(limit=None, timeout=None, wakeup=False)

    def __call__(self, *args, **kwargs):
        self.__start_monitoring()
