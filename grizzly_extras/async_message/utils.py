"""Utilities used by grizzly_extras.async_message."""
from __future__ import annotations

import logging
from time import perf_counter, sleep
from typing import Any, Optional, Union, cast
import uuid

import zmq.green as zmq
from zmq.error import Again as ZMQAgain

from grizzly_extras.async_message import AsyncMessageAbort, AsyncMessageError, AsyncMessageRequest, AsyncMessageResponse

logger = logging.getLogger(__name__)


def tohex(value: Union[int, str, bytes, bytearray, Any]) -> str:
    if isinstance(value, str):
        return ''.join(f'{ord(c):02x}' for c in value)

    if isinstance(value, (bytes, bytearray)):
        return value.hex()

    if isinstance(value, int):
        return hex(value)[2:]

    message = f'{value} has an unsupported type {type(value)}'
    raise ValueError(message)


def async_message_request(client: zmq.Socket, request: AsyncMessageRequest) -> AsyncMessageResponse:
    try:
        request['request_id'] = uuid.uuid4().hex
        logger.info('DEBUG utils.async_message_request before send_json, request_id = %s, client = %r, action = %r, endpoint = %r', request['request_id'], request['client'], request['action'], request.get('context', {}).get('endpoint', None))
        client.send_json(request)
        logger.info('DEBUG utils.async_message_request after send_json, request_id = %s', request['request_id'])

        response: Optional[AsyncMessageResponse] = None

        while True:
            start = perf_counter()
            try:
                response = cast(Optional[AsyncMessageResponse], client.recv_json(flags=zmq.NOBLOCK))
                logger.info('DEBUG utils.async_message_request got response from request_id %s (==response request id %s)', request['request_id'], response.get('request_id', None))
                break
            except ZMQAgain:
                sleep(0.1)

            delta = perf_counter() - start
            if delta > 1.0:
                logger.debug('async_message_request::recv_json took %f seconds for request_id %s', delta, request['request_id'])

        if response is None:
            msg = 'no response'
            raise AsyncMessageError(msg)

        message = response.get('message', None)
        logger.info('DEBUG utils.async_message_request got message for request_id %s', request['request_id'])

        if not response['success']:
            if response['message'] == 'abort':
                logger.info('DEBUG utils.async_message_request Abort for request_id %s', request['request_id'])
                raise AsyncMessageAbort

            logger.info(f'DEBUG utils.async_message_request AsyncMessageError, message={message}, request_id={request['request_id']}')
            raise AsyncMessageError(message)

    except Exception as e:
        if not isinstance(e, (AsyncMessageError, AsyncMessageAbort)):
            logger.exception('failed to send request=%r', request)
        raise
    else:
        logger.info('DEBUG utils.async_message_request returning response for request_id %s', request['request_id'])
        return response
