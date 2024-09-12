"""IBM MQ handler implementation for async-messaged."""
from __future__ import annotations

from time import perf_counter as time
from typing import Any, Optional

import requests

from grizzly_extras.arguments import get_unsupported_arguments, parse_arguments
from grizzly_extras.async_message import AsyncMessageError, AsyncMessageHandler, AsyncMessageRequest, AsyncMessageRequestHandler, AsyncMessageResponse, register

__all__ = [
    'AsyncMessageQueueHandler',
]

handlers: dict[str, AsyncMessageRequestHandler] = {}


class AsyncMessageQueueHandler(AsyncMessageHandler):

    def __init__(self, worker: str) -> None:
        super().__init__(worker)
        self.header_type: Optional[str] = None
        self.session: requests.Session|None = None

    def close(self) -> None:
        if self.session is not None:
            self.logger.debug('closing queue manager connection')
            self.session.close()
            self.session = None


    @register(handlers, 'DISC')
    def disconnect(self, _request: AsyncMessageRequest) -> AsyncMessageResponse:
        self.close()
        self.session = None

        return {
            'message': 'disconnected',
        }


    @register(handlers, 'CONN')
    def connect(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        context = request.get('context', None)
        if context is None:
            msg = 'no context in request'
            raise AsyncMessageError(msg)

        # queue_manager == url for now
        self.url = context['queue_manager']
        if self.session is not None:
            return {
                'message': 're-used connection',
            }

        self.session = requests.session()

        self.message_wait = context.get('message_wait', None) or 0
        self.header_type = context.get('header_type', None)

        return {
            'message': 'connected',
        }


    def _get_safe_message_descriptor(self, message: dict[str, str]) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            'PutDate': int(message.get('PutDate', 0)),
            'PutTime': int(message.get('PutTime', 0)),
            'MsgId': message.get('MessageId', '0'),
        }

        return metadata

    def _request(self, request: AsyncMessageRequest) -> AsyncMessageResponse:  # noqa: C901, PLR0915
        if self.session is None:
            msg = 'not connected'
            raise AsyncMessageError(msg)

        endpoint = request.get('context', {}).get('endpoint', None)
        if endpoint is None:
            msg = 'no endpoint specified'
            raise AsyncMessageError(msg)

        try:
            arguments = parse_arguments(endpoint, separator=':')
            unsupported_arguments = get_unsupported_arguments(['queue', 'expression', 'max_message_size'], arguments)
            if len(unsupported_arguments) > 0:
                msg = f'arguments {", ".join(unsupported_arguments)} is not supported'
                raise ValueError(msg)
        except ValueError as e:
            raise AsyncMessageError(str(e)) from e

        queue_name = arguments.get('queue', None)
        expression = arguments.get('expression', None)
        max_message_size: Optional[int] = int(arguments.get('max_message_size', '0'))

        if not max_message_size:
            max_message_size = None

        action = request['action']

        if action != 'GET' and expression is not None:
            msg = f'argument expression is not allowed for action {action}'
            raise AsyncMessageError(msg)

        message_wait = request.get('context', {}).get('message_wait', None) or self.message_wait
        message: dict[str, Any] = {}
        retries: int = 0

        self.logger.info('executing %s on %s', action, queue_name)
        start = time()

        if action == 'PUT':
            payload: str|None = request.get('payload', None)

            response_length = len(payload) if payload is not None else 0
            self.session.post(f'{self.url}/{queue_name}', data=(payload or '').encode())

        elif action == 'GET':
            payload = None

            message = self.session.get(f'{self.url}/{queue_name}', timeout=message_wait).json()
            payload = message['Body']
            response_length = len((payload or '').encode())

        delta = (time() - start) * 1000
        self.logger.info('%s on %s took %d ms, response_length=%d, retries=%d', action, queue_name, delta, response_length, 0)
        return {
            'payload': payload,
            'metadata': self._get_safe_message_descriptor(message),
            'response_length': response_length,
        }


    @register(handlers, 'PUT', 'SEND')
    def put(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        request['action'] = 'PUT'

        if request.get('payload', None) is None:
            msg = 'no payload'
            raise AsyncMessageError(msg)

        return self._request(request)

    @register(handlers, 'GET', 'RECEIVE')
    def get(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        request['action'] = 'GET'

        if request.get('payload', None) is not None:
            msg = 'payload not allowed'
            raise AsyncMessageError(msg)

        return self._request(request)

    def get_handler(self, action: str) -> Optional[AsyncMessageRequestHandler]:
        return handlers.get(action)
