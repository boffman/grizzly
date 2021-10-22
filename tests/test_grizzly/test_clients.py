
import gevent.monkey
gevent.monkey.patch_all()

from typing import Any, Dict, Optional, Callable, Union, Tuple
from json import dumps as jsondumps

import pytest

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from requests.models import Response
from locust.clients import ResponseContextManager
from locust.event import EventHook
from locust.exception import StopUser
from locust.user.users import User
from paramiko.transport import Transport
from paramiko.sftp_client import SFTPClient

from grizzly.clients import ResponseEventSession, SftpClientSession
from grizzly.types import RequestMethod
from grizzly.context import LocustContextScenario
from grizzly.task import RequestTask

from .fixtures import locust_environment, paramiko_mocker  # pylint: disable=unused-import
from .helpers import RequestEvent


class TestResponseEventSession:
    def test___init__(self) -> None:
        session = ResponseEventSession(base_url='', request_event=RequestEvent())

        assert isinstance(session.event_hook, EventHook)
        assert len(session.event_hook._handlers) == 0


    def test_request(self, mocker: MockerFixture) -> None:
        def mock_request(payload: Dict[str, Any], status_code: int = 200) -> None:
            def request(self: 'ResponseEventSession', method: str, url: str, data: Dict[str, Any], name: Optional[str] = None, **kwargs: Dict[str, Any]) -> ResponseContextManager:
                response = Response()
                response._content = jsondumps(payload).encode('utf-8')
                response.status_code = status_code
                return ResponseContextManager(response, RequestEvent(), None)

            mocker.patch(
                'locust.clients.HttpSession.request',
                request,
            )

        class HandlerCalled(StopUser):
            pass

        mock_request({}, 200)

        session = ResponseEventSession(base_url='', request_event=RequestEvent())
        request = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
        scenario = LocustContextScenario()
        scenario.name = 'TestScenario'
        scenario.context['host'] = 'test'
        request.scenario = scenario

        def handler(expected_request: Optional[RequestTask] = None) -> Callable[[str, Optional[RequestTask], User, Optional[ResponseContextManager]], None]:
            def wrapped(name: str, context: Union[ResponseContextManager, Tuple[Dict[str, Any], str]], request: Optional[RequestTask], user: User) -> None:
                if expected_request is request:
                    raise HandlerCalled()  # one of few exceptions which event handler lets through

            return wrapped

        assert len(session.event_hook._handlers) == 0

        session.event_hook.add_listener(handler())

        assert len(session.event_hook._handlers) == 1

        session.request('GET', 'http://example.org', 'test-name', catch_response=False, request=request)

        session.event_hook._handlers = []
        session.event_hook.add_listener(handler(request))

        # handler should be called, which raises StopUser
        with pytest.raises(HandlerCalled):
            session.request('GET', 'http://example.org', 'test-name', catch_response=False, request=request)

        second_request = RequestTask(RequestMethod.GET, name='test-request', endpoint='/api/test')
        second_request.scenario = scenario

        # handler is called, but request is not the same
        session.request('GET', 'http://example.org', 'test-name', catch_response=False, request=second_request)


class TestSftpClientSession:
    @pytest.mark.usefixtures('paramiko_mocker')
    def test(self, paramiko_mocker: Callable, mocker: MockerFixture) -> None:
        paramiko_mocker()

        context = SftpClientSession('example.org', 1337)

        assert context.host == 'example.org'
        assert context.port == 1337
        assert context.username == None
        assert context.key == None
        assert context.key_file == None
        assert context._client == None
        assert context._transport == None

        with pytest.raises(NotImplementedError):
            with context.session('username', 'password', '~/.ssh/id_rsa'):
                pass

        assert context.host == 'example.org'
        assert context.port == 1337
        assert context.username == None
        assert context.key == None
        assert context.key_file == None
        assert context._client == None
        assert context._transport == None

        username_transport: Optional[Transport] = None
        username_client: Optional[SFTPClient] = None

        # start session for user username
        with context.session('username', 'password') as session:
            assert isinstance(session, SFTPClient)
            assert context.username == None
            assert isinstance(context._transport, Transport)
            assert isinstance(context._client, SFTPClient)
            username_transport = context._transport
            username_client = context._client
        assert context.username == 'username'

        # change username, and hence start a new client
        with context.session('test-user', 'password') as session:
            assert isinstance(session, SFTPClient)
            assert context.username == None
            assert isinstance(context._transport, Transport)
            assert isinstance(context._client, SFTPClient)
            assert username_client is not context._client
            assert username_transport is not context._transport

        context.close()

        assert context._client == None
        assert context._transport == None
        assert context.username == None

        def _from_transport(transport: Transport, window_size: Optional[int] = None, max_packet_size: Optional[int] = None) -> Optional[SFTPClient]:
            return None

        mocker.patch(
            'paramiko.sftp_client.SFTPClient.from_transport',
            _from_transport,
        )

        with pytest.raises(RuntimeError) as e:
            with context.session('test-user', 'password') as session:
                pass
        assert 'there is no client' in str(e)
