from typing import Optional, cast
from json import dumps as jsondumps
from os import environ, path, listdir
from shutil import rmtree
from platform import node as hostname

import pytest

from pytest_mock import MockerFixture, mocker  # pylint: disable=unused-import
from _pytest.tmpdir import TempdirFactory
from _pytest.capture import CaptureFixture

from grizzly_extras.async_message import (
    AsyncMessageRequestHandler,
    JsonBytesEncoder,
    AsyncMessageResponse,
    AsyncMessageRequest,
    AsyncMessageHandler,
    register,
    configure_logger,
)


class TestJsonBytesEncoder:
    def test_default(self) -> None:
        encoder = JsonBytesEncoder()

        assert encoder.default(b'hello') == 'hello'
        assert encoder.default(b'invalid \xe9 char') == 'invalid \xe9 char'

        assert jsondumps({
            'hello': b'world',
            'invalid': b'\xe9 char',
            'value': 'something',
            'test': False,
            'int': 1,
            'empty': None,
        }, cls=JsonBytesEncoder) == '{"hello": "world", "invalid": "\\u00e9 char", "value": "something", "test": false, "int": 1, "empty": null}'

        with pytest.raises(TypeError):
            encoder.default(None)


class TestAsyncMessageHandler:
    def test_get_handler(self, mocker: MockerFixture) -> None:
        class AsyncMessageTest(AsyncMessageHandler):
            def get_handler(self, action: str) -> Optional[AsyncMessageRequestHandler]:
                return super().get_handler(action)

        handler = AsyncMessageTest('ID-12345')

        assert handler.worker == 'ID-12345'

        with pytest.raises(NotImplementedError):
            handler.get_handler('TEST')

    def test_handle(self, mocker: MockerFixture) -> None:
        class AsyncMessageTest(AsyncMessageHandler):
            def a_handler(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
                pass

            def get_handler(self, action: str) -> Optional[AsyncMessageRequestHandler]:
                if action == 'NONE':
                    return None
                else:
                    return cast(AsyncMessageRequestHandler, self.a_handler)

        handler = AsyncMessageTest(worker='asdf-asdf-asdf')

        request: AsyncMessageRequest = {
            'action': 'NONE',
        }

        response = handler.handle(request)

        assert response.get('success', True) == False
        assert response.get('worker', None) == 'asdf-asdf-asdf'
        assert response.get('message', None) == 'NONE: AsyncMessageError="no implementation for NONE"'
        assert response.get('response_time', None) is not None

        mocker.patch.object(handler, 'a_handler', side_effect=[{
            'payload': 'test payload',
            'metadata': {'value': 'hello world'},
            'response_length': len('test payload'),
        }])

        request.update({
            'action': 'GET',
            'context': {
                'endpoint': 'TEST.QUEUE',
            }
        })

        response = handler.handle(request)

        assert response.get('success', False) == True
        assert response.get('worker', None) == 'asdf-asdf-asdf'
        assert response.get('message', None) is None
        assert response.get('response_time', None) is not None
        assert response.get('response_length') == len('test payload')
        assert response.get('payload') == 'test payload'


def test_register() -> None:
    def handler_a(i: AsyncMessageHandler, request: AsyncMessageRequest) -> AsyncMessageResponse:
        pass

    def handler_b(i: AsyncMessageHandler, request: AsyncMessageRequest) -> AsyncMessageResponse:
        pass

    try:
        from grizzly_extras.async_message.mq import handlers

        actual = list(handlers.keys())
        actual.sort()

        expected = ['CONN', 'RECEIVE', 'SEND', 'PUT', 'GET']
        expected.sort()

        assert actual == expected

        register(handlers, 'TEST')(handler_a)
        register(handlers, 'TEST')(handler_b)

        from grizzly_extras.async_message.mq import handlers

        assert handlers['TEST'] is not handler_b
        assert handlers['TEST'] is handler_a
    finally:
        try:
            del handlers['TEST']
        except KeyError:
            pass


def test_configure_logger(mocker: MockerFixture, tmpdir_factory: TempdirFactory, capsys: CaptureFixture) -> None:
    test_context = tmpdir_factory.mktemp('test_context').mkdir('logs')
    test_context_root = path.dirname(str(test_context))

    try:
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        logger = configure_logger('test.logger')
        logger.error('hello world')
        logger.debug('no no')

        std = capsys.readouterr()
        assert '] INFO : test.logger: level=INFO\n' in std.err
        assert '] ERROR: test.logger: hello world\n' in std.err
        assert '] DEBUG: test.logger: no no\n' not in std.err

        log_files = listdir(str(test_context))
        assert len(log_files) == 0

        environ['GRIZZLY_EXTRAS_LOGLEVEL'] = 'DEBUG'

        logger = configure_logger('test.logger')
        logger.error('hello world')
        logger.debug('no no')

        std = capsys.readouterr()
        log_files = listdir(str(test_context))
        assert len(log_files) == 1
        log_file = log_files[0]
        assert log_file == f'async-messaged.{hostname()}.log'

        with open(path.join(str(test_context), log_file)) as fd:
            file = fd.read()

        for sink in [std.err, file]:
            assert '] INFO : test.logger: level=DEBUG\n' in sink
            assert '] ERROR: test.logger: hello world\n' in sink
            assert '] DEBUG: test.logger: no no\n' in sink
    finally:
        try:
            del environ['GRIZZLY_CONTEXT_ROOT']
        except:
            pass

        try:
            del environ['GRIZZLY_EXTRAS_LOGLEVEL']
        except:
            pass

        rmtree(test_context_root)
