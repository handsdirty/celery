# -* coding: utf-8 -*-
"""Elasticsearch result store backend."""
from __future__ import absolute_import, unicode_literals
from datetime import datetime
import socket

from kombu.utils.encoding import bytes_to_str
from kombu.utils.url import _parse_url
from celery.exceptions import ImproperlyConfigured
from celery import states

from .base import KeyValueStoreBackend
try:
    import elasticsearch
except ImportError:
    elasticsearch = None  # noqa

__all__ = ['ElasticsearchBackend']

E_LIB_MISSING = """\
You need to install the elasticsearch library to use the Elasticsearch \
result backend.\
"""


class ElasticsearchBackend(KeyValueStoreBackend):
    """Elasticsearch Backend.

    Raises:
        celery.exceptions.ImproperlyConfigured:
            if module :pypi:`elasticsearch` is not available.
    """

    index = 'celery'
    doc_type = 'backend'
    scheme = 'http'
    host = 'localhost'
    port = 9200
    date_string = datetime.utcnow().strftime('%Y-%m-%d')

    def __init__(self, url=None, *args, **kwargs):
        super(ElasticsearchBackend, self).__init__(*args, **kwargs)
        self.url = url

        if elasticsearch is None:
            raise ImproperlyConfigured(E_LIB_MISSING)

        index = doc_type = scheme = host = port = None

        if url:
            scheme, host, port, _, _, path, _ = _parse_url(url)  # noqa
            if path:
                path = path.strip('/')
                index, _, doc_type = path.partition('/')

        self.index = index or self.index
        self.doc_type = doc_type or self.doc_type
        self.scheme = scheme or self.scheme
        self.host = host or self.host
        self.port = port or self.port

        self._server = None

    def _store_result(self, task_id, result, state,
                      traceback=None, request=None, **kwargs):
        # Record the start time in request object for duration calculation.
        if request and state == states.STARTED:
            request.start_time = datetime.utcnow()
            self.date_string = request.start_time.strftime('%Y-%m-%d')
        meta = {
            'status': state, 'result': result, 'traceback': traceback,
            'children': self.current_task_children(request),
            'task_id': bytes_to_str(task_id),
        }
        extra_meta = {
            'hostname': socket.gethostname(),
        }
        if request:
            extra_meta.update({
                'queue': request.delivery_info['routing_key'],
                'task_name': request.task,
                'args': self.encode(request.argsrepr),
                'start_time':
                    '{0}Z'.format(request.start_time.isoformat()[:-3]),
            })
        self.set(self.get_key_for_task(task_id), self.encode(meta),
                 extra_meta=extra_meta)
        return result

    def get(self, key, all_source=False):
        try:
            res = self.server.get(
                index=self.index,
                doc_type=self.doc_type,
                id=key,
            )
            try:
                if res['found']:
                    return res['_source']['old_value'] if not all_source else \
                        res['_source']
            except (TypeError, KeyError):
                pass
        except elasticsearch.exceptions.NotFoundError:
            pass

    def set(self, key, value, extra_meta={}):
        body_dict = self.decode(value)
        # Record important times.
        time_key = 'end_time'
        if body_dict['status'] == states.RECEIVED:
            time_key = 'receive_time'
        dt_now = datetime.utcnow()
        time_now = '{0}Z'.format(dt_now.isoformat()[:-3])

        body = {}
        for k in body_dict:
            body[k] = str(body_dict[k])
        body[time_key] = time_now
        body['old_value'] = value
        body['@timestamp'] = time_now
        body.update(extra_meta)
        if body_dict['status'] == states.STARTED:
            # Create.
            self._index(
                id=key,
                body=body,
                index='%s_%s' % (self.index, self.date_string),
            )
        else:
            # Update.
            start_time_str = extra_meta['start_time']
            start_datetime = datetime.strptime(
                start_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            duration = (dt_now - start_datetime).total_seconds() * 1000
            body['duration_ms'] = round(duration, 3)
            self._update(
                id=key,
                body=body,
                index='%s_%s' % (self.index, self.date_string),
            )

    def _index(self, id, body, index=None, **kwargs):
        return self.server.index(
            index=index or self.index,
            id=id,
            doc_type=self.doc_type,
            body=body,
            timeout='10s',
            **kwargs
        )

    def _update(self, id, body, index=None):
        return self.server.update(
            index=index or self.index,
            doc_type=self.doc_type,
            id=id,
            body={'doc': body},
            timeout='10s',
        )

    def mget(self, keys):
        return [self.get(key) for key in keys]

    def delete(self, key):
        self.server.delete(index=self.index, doc_type=self.doc_type, id=key)

    def _get_server(self):
        """Connect to the Elasticsearch server."""
        return elasticsearch.Elasticsearch(self.host)

    @property
    def server(self):
        if self._server is None:
            self._server = self._get_server()
        return self._server
