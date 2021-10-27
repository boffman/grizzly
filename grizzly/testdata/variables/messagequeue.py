# pylint: disable=line-too-long
'''This variable listens for messages on message queue and extracts a value based on a json/x-path expression.

Grizzly *must* have been installed with the extra `mq` package and native IBM MQ libraries must be installed for being able to use this variable:

```plain
pip3 install grizzly-loadtester[mq]
```

## Format

Initial value is the name of the queue on the MQ server specified in argument `url`.

## Arguments

* `repeat` _bool_ (optional) - if `True`, values read for the queue will be saved in a list and re-used if there are no new messages available
* `url` _str_ - see format of url below.
* `expression` _str_ - JSON path or XPath expression for finding _one_ specific value or object in the payload
* `content_type` _str_ - see [`step_response_content_type`](/grizzly/usage/steps/scenario/response/#step_response_content_type)
* `wait` _int_ - number of seconds to wait for a message on the queue

### URL format

```plain
mq[s]://[<username>:<password>@]<hostname>[:<port>]/?QueueManager=<queue manager>&Channel=<channel>[&KeyFile=<key repository path>[&SslCipher=<ssl cipher>][&CertLabel=<certificate label>]]
```

All variables in the URL have support for [templating](/grizzly/usage/variables/templating/).

* `mq[s]` _str_ - must be specified, `mqs` implies connecting with TLS, if `KeyFile` is not set in querystring, it will look for a key repository in `./<username>`
* `username` _str_ (optional) - username to authenticate with, default `None`
* `password` _str_ (optional) - password to authenticate with, default `None`
* `hostname` _str_ - hostname of MQ server
* `port` _int_ (optional) - port on MQ server, default `1414`
* `QueueManager` _str_ - name of queue manager
* `Channel` _str_ - name of channel to connect to
* `KeyFile` _str_ (optional) - path to key repository for certificates needed to connect over TLS
* `SslCipher` _str_ (optional) - SSL cipher to use for connection, default `ECDHE_RSA_AES_256_GCM_SHA384`
* `CertLabel` _str_ (optional) - label of certificate in key repository, default `username`

## Example

```gherkin
And value of variable "AtomicMessageQueue.document_id" is "IN.DOCUMENTS | wait=120, url='mqs://mq_subscription:$conf::mq.password@mq.example.com/?QueueManager=QM1&Channel=SRV.CONN', repeat=True, expression='$.document.id', content_type=json"
...
Given a user of type "RestApi" load testing "http://example.com"
...
Then get request "fetch-document" from "/api/v1/document/{{ AtomicMessageQueue.document_id }}"
```

When the scenario starts `grizzly` will wait up to 120 seconds until `AtomicMessageQueue.document_id` has been populated from a message on the queue `IN.DOCUMENTS`.

If there are no messages within 120 seconds, and it is the first iteration of the scenario, it will fail. If there has been at least one message on the queue since the scenario started, it will use
the oldest of those values, and then add it back in the end of the list again.
'''
from typing import Dict, Any, Type, Optional, List, cast
from urllib.parse import urlparse, parse_qs, unquote

import zmq

from gevent import sleep as gsleep
from grizzly_extras.messagequeue import MessageQueueContext, MessageQueueRequest, MessageQueueResponse

from ...types import bool_typed, str_response_content_type
from ...utils import resolve_variable
from ...context import GrizzlyContext
from ...transformer import transformer
from . import AtomicVariable, parse_arguments

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi


def atomicmessagequeue__base_type__(value: str) -> str:
    if '|' not in value:
        raise ValueError(f'AtomicMessageQueue: initial value must contain arguments')

    queue_name, queue_arguments = [v.strip() for v in value.split('|', 1)]

    arguments = parse_arguments(AtomicMessageQueue, queue_arguments)

    if queue_name is None or len(queue_name) < 1:
        raise ValueError(f'AtomicMessageQueue: queue name is not valid: {queue_name}')

    if 'url' not in arguments:
        raise ValueError('AtomicMessageQueue: url parameter must be specified')

    if 'expression' not in arguments:
        raise ValueError('AtomicMessageQueue: expression parameter must be specified')

    if 'content_type' not in arguments:
        raise ValueError('AtomicMessageQueue: content_type parameter must be specified')

    for argument_name, argument_value in arguments.items():
        if argument_name not in AtomicMessageQueue.arguments:
            raise ValueError(f'AtomicMessageQueue: argument {argument_name} is not allowed')
        else:
            AtomicMessageQueue.arguments[argument_name](argument_value)

    # validate url
    AtomicMessageQueue.create_context({
        'url': arguments['url'],
        'wait': arguments.get('wait', None),
    })

    content_type = AtomicMessageQueue.arguments['content_type'](arguments['content_type'])
    transform = transformer.available.get(content_type, None)

    if transform is None:
        raise ValueError(f'AtomicMessageQueue: could not find a transformer for {content_type.name}')

    if not transform.validate(arguments['expression']):
        raise ValueError(f'AtomicMessageQueue: expression "{arguments["expression"]}" is not a valid expression for {content_type.name}')

    return f'{queue_name} | {queue_arguments}'


class AtomicMessageQueue(AtomicVariable[str]):
    __base_type__ = atomicmessagequeue__base_type__
    __initialized: bool = False

    _settings: Dict[str, Dict[str, Any]]
    _queue_clients: Dict[str, zmq.Socket]
    _queue_values: Dict[str, List[str]]

    _zmq_url = 'tcp://127.0.0.1:5554'
    _zmq_context: zmq.Context

    arguments: Dict[str, Any] = {'repeat': bool_typed, 'url': str, 'expression': str, 'wait': int, 'content_type': str_response_content_type}

    def __init__(self, variable: str, value: str):
        if pymqi.__name__ == 'grizzly_extras.dummy_pymqi':
            raise NotImplementedError('AtomicMessageQueue could not import pymqi, have you installed IBM MQ dependencies?')

        safe_value = self.__class__.__base_type__(value)

        settings = {'repeat': False, 'wait': None, 'expression': None, 'url': None, 'worker': None, 'context': None}

        queue_name, queue_arguments = [v.strip() for v in safe_value.split('|', 1)]

        arguments = parse_arguments(self.__class__, queue_arguments)

        for argument, caster in self.__class__.arguments.items():
            if argument in arguments:
                settings[argument] = caster(arguments[argument])

        super().__init__(variable, queue_name)

        if self.__initialized:
            with self._semaphore:
                if variable not in self._queue_values:
                    self._queue_values[variable] = []

                if variable not in self._settings:
                    self._settings[variable] = settings

                if variable not in self._queue_clients:
                    self._queue_clients[variable] = self.create_client(variable, settings)

            return

        self._queue_values = {variable: []}
        self._settings = {variable: settings}
        self._zmq_context = zmq.Context()
        self._queue_clients = {variable: self.create_client(variable, settings)}
        self.__initialized = True

    @classmethod
    def create_context(cls, settings: Dict[str, Any]) -> MessageQueueContext:
        url = settings.get('url', None)
        parsed = urlparse(url)

        if parsed.scheme is None or parsed.scheme not in ['mq', 'mqs']:
            raise ValueError(f'{cls.__name__}: "{parsed.scheme}" is not a supported scheme for url')

        if parsed.hostname is None or len(parsed.hostname) < 1:
            raise ValueError(f'{cls.__name__}: hostname is not specified in "{url}"')

        if parsed.query == '':
            raise ValueError(f'{cls.__name__}: QueueManager and Channel must be specified in the query string of "{url}"')

        paths: List[str] = []

        grizzly = GrizzlyContext()

        for path in parsed.path.split('/'):
            resolved = cast(str, resolve_variable(grizzly, path))
            paths.append(resolved)

        parsed = parsed._replace(path='/'.join(paths))

        querystrings = parse_qs(parsed.query)

        parameters: List[str] = []

        for querystring in querystrings:
            try:
                resolved = cast(str, resolve_variable(grizzly, querystrings[querystring][0]))
            except:
                import json
                print(json.dumps(grizzly.state.configuration, indent=2))
                raise
            parameters.append(f'{querystring}={resolved}')

        parsed = parsed._replace(query='&'.join(parameters))

        if '@' in parsed.netloc:
            credentials, host = parsed.netloc.split('@')
            host = cast(str, resolve_variable(grizzly, host))
            credentials = credentials.replace('::', '%%')
            username, password = credentials.split(':', 1)
            username = cast(str, resolve_variable(grizzly, username.replace('%%', '::')))
            password = cast(str, resolve_variable(grizzly, password.replace('%%', '::')))
            host = f'{username}:{password}@{host}'
        else:
            host = cast(str, resolve_variable(grizzly, parsed.netloc))

        parsed = parsed._replace(netloc=host)

        port = parsed.port or 1414

        params = parse_qs(parsed.query)

        if 'QueueManager' not in params:
            raise ValueError(f'{cls.__name__}: QueueManager must be specified in the query string')

        if 'Channel' not in params:
            raise ValueError(f'{cls.__name__}: Channel must be specified in the query string')

        key_file: Optional[str] = None
        cert_label: Optional[str] = None
        ssl_cipher: Optional[str] = None

        if 'KeyFile' in params:
            key_file = params['KeyFile'][0]
        elif parsed.scheme == 'mqs' and username is not None:
            key_file = username

        if key_file is not None:
            cert_label = params.get('CertLabel', [parsed.username])[0]
            ssl_cipher = params.get('SslCipher', ['ECDHE_RSA_AES_256_GCM_SHA384'])[0]

        return {
            'connection': f'{parsed.hostname}({port})',
            'queue_manager': unquote(params['QueueManager'][0]),
            'channel': unquote(params['Channel'][0]),
            'username': parsed.username,
            'password': parsed.password,
            'key_file': key_file,
            'cert_label': cert_label,
            'ssl_cipher': ssl_cipher,
            'message_wait': settings.get('wait', None)
        }

    def create_client(self, variable: str, settings: Dict[str, Any]) -> zmq.Socket:
        self._settings[variable].update({'context': self.create_context(settings)})

        request: MessageQueueRequest = {
            'action': 'CONN',
            'context': self._settings[variable]['context'],
        }

        zmq_client = cast(zmq.Socket, self._zmq_context.socket(zmq.REQ))
        zmq_client.connect(self._zmq_url)
        zmq_client.send_json(request)

        response: Optional[MessageQueueResponse] = None

        while True:
            try:
                response = zmq_client.recv_json(flags=zmq.NOBLOCK)
                break
            except zmq.Again:
                gsleep(0.1)

        if response is None:
            raise RuntimeError(f'{self.__class__.__name__}.{variable}: no response when trying to connect')

        message = response.get('message', None)
        if not response['success']:
            raise RuntimeError(message)

        self._settings[variable]['worker'] = response['worker']

        return zmq_client

    @classmethod
    def destroy(cls: Type['AtomicMessageQueue']) -> None:
        try:
            instance = cast(AtomicMessageQueue, cls.get())
            queue_clients = getattr(instance, '_queue_clients', None)

            if queue_clients is not None:
                variables = list(queue_clients.keys())[:]
                for variable in variables:
                    try:
                        instance.__delitem__(variable)
                    except:
                        pass

            instance._zmq_context.destroy()
            del instance._zmq_context
        except:
            pass

        super().destroy()

    @classmethod
    def clear(cls: Type['AtomicMessageQueue']) -> None:
        super().clear()

        instance = cast(AtomicMessageQueue, cls.get())
        variables = list(instance._settings.keys())

        for variable in variables:
            instance.__delitem__(variable)

    def __getitem__(self, variable: str) -> Optional[str]:
        with self._semaphore:
            queue_name = cast(str, self._get_value(variable))

            request: MessageQueueRequest = {
                'action': 'GET',
                'worker': self._settings[variable]['worker'],
                'context': {
                    'queue': queue_name,
                },
                'payload': None
            }

            self._queue_clients[variable].send_json(request)

            response: Optional[MessageQueueResponse] = None

            while True:
                try:
                    response = cast(MessageQueueResponse, self._queue_clients[variable].recv_json(flags=zmq.NOBLOCK))
                    break
                except zmq.Again:
                    gsleep(0.1)

            if response is None:
                raise RuntimeError(f'{self.__class__.__name__}.{variable}: unknown error, no response')

            message = response.get('message', None)
            if not response['success']:
                if message is not None and 'MQRC_NO_MSG_AVAILABLE' in message and self._settings[variable].get('repeat', False) and len(self._queue_values[variable]) > 0:
                    value = self._queue_values[variable].pop(0)
                    self._queue_values[variable].append(value)

                    return value

                raise RuntimeError(f'{self.__class__.__name__}.{variable}: {message}')

            raw = response.get('payload', None)
            if raw is None or len(raw) < 1:
                raise RuntimeError(f'{self.__class__.__name__}.{variable}: payload in response was None')

            content_type = self._settings[variable]['content_type']
            expression = self._settings[variable]['expression']
            transform = transformer.available.get(content_type, None)

            if transform is None:
                raise TypeError(f'{self.__class__.__name__}.{variable}: could not find a transformer for {content_type.name}')

            get_values = transform.parser(expression)
            _, payload = transform.transform(content_type, raw)
            values = get_values(payload)

            number_of_values = len(values)

            if number_of_values != 1:
                if number_of_values < 1:
                    raise RuntimeError(f'{self.__class__.__name__}.{variable}: "{expression}" returned no values')
                elif number_of_values > 1:
                    raise RuntimeError(f'{self.__class__.__name__}.{variable}: "{expression}" returned more than one value')

            value = values[0]
            if self._settings[variable].get('repeat', False):
                self._queue_values[variable].append(value)

            return value

    def __setitem__(self, variable: str, value: Optional[str]) -> None:
        pass

    def __delitem__(self, variable: str) -> None:
        with self._semaphore:
            try:
                del self._settings[variable]
                del self._queue_values[variable]
                try:
                    self._queue_clients[variable].disconnect(self._zmq_url)
                except zmq.ZMQError:
                    pass
                del self._queue_clients[variable]
            except KeyError:
                pass

        super().__delitem__(variable)
