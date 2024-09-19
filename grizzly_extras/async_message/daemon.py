"""Daemon implementation that handles are requests.
Based on ZMQ PUB/SUB, where each request type is handled to a worker which is for a specific client.
"""
from __future__ import annotations

import logging
from concurrent import futures
from contextlib import suppress
from json import dumps as jsondumps
from json import loads as jsonloads
from multiprocessing import Process
from signal import SIGINT, SIGTERM, Signals, signal
from threading import Event
from time import sleep
from typing import TYPE_CHECKING, Optional, Union, cast
from urllib.parse import urlparse
from uuid import uuid4

import setproctitle as proc
import zmq.green as zmq
from typing_extensions import Literal

from grizzly_extras.transformer import JsonBytesEncoder

from . import (
    LRU_READY,
    SPLITTER_FRAME,
    AsyncMessageHandler,
    AsyncMessageRequest,
    AsyncMessageResponse,
)

if TYPE_CHECKING:  # pragma: no cover
    from types import FrameType, TracebackType


class ThreadPoolExecutor(futures.ThreadPoolExecutor):
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        self.shutdown(wait=False, cancel_futures=True)
        return False


class Worker:
    logger: logging.Logger
    identity: str
    context: zmq.Context

    integration: Optional[AsyncMessageHandler]

    _event: Event

    def __init__(self, context: zmq.Context, identity: str, event: Optional[Event] = None) -> None:
        self.logger = logging.getLogger(f'worker::{identity}')
        self.identity = identity
        self.context = context
        self.integration = None
        self._event = Event() if event is None else event

    def run(self) -> None:
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt_string(zmq.IDENTITY, self.identity)
        self.socket.connect('inproc://workers')
        self.socket.send_string(LRU_READY)

        while not self._event.is_set():
            received = False
            try:
                request_proto = self.socket.recv_multipart(flags=zmq.NOBLOCK)
                received = True
            except zmq.Again:
                sleep(0.1)
                continue

            self.logger.debug("i'm alive! run_daemon=%r, received=%r", self._event.is_set(), received)
            self.logger.info('DEBUG Worker.run: got something from the socket')

            if not request_proto:
                self.logger.info('DEBUG Worker.run: not request_proto, continue')
                continue

            request = cast(
                AsyncMessageRequest,
                jsonloads(request_proto[-1].decode()),
            )

            request_request_id = request.get('request_id', None)

            self.logger.info('DEBUG Worker.run: got request_request_id: %r', request_request_id)
            response: Optional[AsyncMessageResponse] = None

            try:
                if request['worker'] != self.identity:
                    message = f'got {request["worker"]}, expected {self.identity}'
                    raise RuntimeError(message)

                if self.integration is None:
                    integration_url = request.get('context', {}).get('url', None)
                    if integration_url is None:
                        message = 'no url found in request context'
                        raise RuntimeError(message)

                    parsed = urlparse(integration_url)

                    if parsed.scheme in ['mq', 'mqs']:
                        from .mq import AsyncMessageQueueHandler
                        self.integration = AsyncMessageQueueHandler(self.identity)
                    elif parsed.scheme == 'sb':
                        from .sb import AsyncServiceBusHandler
                        self.integration = AsyncServiceBusHandler(self.identity)
                    else:
                        message = f'integration for {parsed.scheme}:// is not implemented'
                        raise RuntimeError(message)
            except Exception as e:
                response = {
                    'request_id': request_request_id,
                    'worker': self.identity,
                    'response_time': 0,
                    'success': False,
                    'message': str(e),
                }

            if response is None and self.integration is not None:
                self.logger.info('DEBUG Worker.run: handing over to integration, request_request_id = %r', request_request_id)
                response = self.integration.handle(request)
                self.logger.info('DEBUG Worker.run: got a response from integration, request_request_id = %r', request_request_id)

            response_proto = [
                request_proto[0],
                SPLITTER_FRAME,
                jsondumps(response, cls=JsonBytesEncoder).encode(),
            ]

            self.logger.info('DEBUG Worker.run: sending back response for request_request_id = %r', request_request_id)
            self.socket.send_multipart(response_proto)

        self.logger.info('stopping')
        if self.integration is not None:
            self.integration.close()

        self.socket.close()
        self.logger.info('stopped')


def create_router_socket(context: zmq.Context) -> zmq.Socket:
    socket = cast(zmq.Socket, context.socket(zmq.ROUTER))
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.ROUTER_HANDOVER, 1)

    return socket


def router(run_daemon: Event) -> None:  # noqa: C901, PLR0915
    logger = logging.getLogger('router')
    logger.debug('starting')

    context = zmq.Context()
    frontend = create_router_socket(context)
    backend = create_router_socket(context)

    frontend.bind('tcp://127.0.0.1:5554')
    backend.bind('inproc://workers')

    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    workers: dict[str, tuple[futures.Future, Worker]] = {}

    with ThreadPoolExecutor() as executor:
        def spawn_worker() -> None:
            identity = str(uuid4())

            worker = Worker(context, identity, run_daemon)

            future = executor.submit(worker.run)
            workers.update({identity: (future, worker)})
            logger.info('spawned worker %s', identity)

        workers_available: list[str] = []
        client_worker_map: dict[str, str] = {}
        worker_identifiers_map: dict[str, bytes] = {}

        spawn_worker()

        worker_id: str

        try:
            while not run_daemon.is_set():
                socks = dict(poller.poll(timeout=1000))

                if not socks:
                    continue

                logger.debug("i'm alive!")
                logger.info("DEUBG i'm alive!")

                if socks.get(backend) == zmq.POLLIN:
                    logger.debug('polling backend')
                    logger.info('DEBUG router: polling backend')
                    try:
                        logger.debug('waiting for backend')
                        backend_response = backend.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        sleep(0.1)
                        continue

                    logger.info('DEBUG router: got something from the backend')

                    if not backend_response:
                        logger.info('DEBUG router: ..but it was nothing, continue')
                        continue

                    logger.info('DEBUG router: yes we definitely got something from the backend')
                    logger.debug('backend_response: %r', backend_response)
                    reply = backend_response[2:]
                    worker_id = backend_response[0].decode()
                    async_response = jsonloads(backend_response[-1].decode())
                    response_request_id = async_response.get('request_id', None)
                    logger.info('DEBUG router: got response from request_id %s', response_request_id)

                    worker_identifiers_map.update({worker_id: reply[0]})

                    if reply[0] != LRU_READY.encode():
                        logger.debug('sending %r', reply)
                        logger.info('DEBUG router: before send_multipart reply for request_id %s', response_request_id)
                        frontend.send_multipart(reply)
                        logger.info('DEBUG router: after send_multipart reply for request_id %s', response_request_id)
                        logger.debug('forwarding backend response from %s', worker_id)
                    else:
                        logger.info('worker %s ready', worker_id)
                        workers_available.append(worker_id)

                if socks.get(frontend) == zmq.POLLIN:
                    logger.debug('polling frontend')
                    logger.info('DEBUG router: polling frontend')
                    try:
                        logger.debug('waiting for frontend')
                        msg = frontend.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        sleep(0.1)
                        continue


                    request_id = msg[0]
                    payload = cast(AsyncMessageRequest, jsonloads(msg[-1].decode()))

                    request_worker_id = payload.get('worker', None)
                    request_client_id = payload.get('client', None)
                    request_request_id = payload.get('request_id', None)
                    client_key: Optional[str] = None

                    logger.debug('request_worker_id=%r (%r), request_client_id=%r (%r)', request_worker_id, type(request_worker_id), request_client_id, type(request_client_id))
                    logger.info('DEBUG request_request_id=%r, request_worker_id=%r (%r), request_client_id=%r (%r)', request_request_id, request_worker_id, type(request_worker_id), request_client_id, type(request_client_id))

                    if request_client_id is not None:
                        integration_url = payload.get('context', {}).get('url', None)
                        parsed = urlparse(integration_url)
                        scheme = parsed.scheme
                        if isinstance(scheme, bytes):
                            scheme = scheme.decode()

                        client_key = f'{request_client_id}::{scheme}'

                    if request_worker_id is None and client_key is not None:
                        request_worker_id = client_worker_map.get(client_key)

                    if request_worker_id is None:
                        worker_id = workers_available.pop()

                        if client_key is not None:
                            client_worker_map.update({client_key: worker_id})

                        payload['worker'] = worker_id
                        logger.info('assigned worker %s to %s', worker_id, client_key)

                        if len(workers_available) == 0:
                            logger.debug('spawning an additional worker, for next client')
                            spawn_worker()
                    else:
                        logger.debug('%s is assigned %s', request_client_id, request_worker_id)
                        worker_id = request_worker_id

                        if payload.get('worker', None) is None:
                            payload['worker'] = worker_id

                    request = jsondumps(payload).encode()
                    backend_request = [worker_id.encode(), SPLITTER_FRAME, request_id, SPLITTER_FRAME, request]
                    logger.info(f'DEBUG router: before sending request to backend: {request_request_id}')
                    backend.send_multipart(backend_request)
                    logger.info(f'DEBUG router: after sending request to backend: {request_request_id}')

            logger.info('stopping')
            for identity, (future, worker) in workers.items():
                if not future.running():
                    continue

                # tell client that we aborted
                if worker.integration is not None:
                    response = {
                        'success': False,
                        'worker': identity,
                        'message': 'abort',
                    }

                    response_proto = [
                        worker_identifiers_map.get(identity),
                        SPLITTER_FRAME,
                        jsondumps(response, cls=JsonBytesEncoder).encode(),
                    ]

                    frontend.send_multipart(response_proto)

                    worker.logger.debug('sent abort to client')

                    worker.integration.close()
                    worker.socket.close()
                    worker.logger.info('socket closed')

                    # stop worker
                    cancelled = future.cancel()  # let's try at least...
                    logger.info('worker %s cancelled: %r', identity, cancelled)
                else:
                    # should complete when `worker.stop()` has had effect
                    futures.wait([future])

            try:
                logger.debug('destroy zmq context')
                context.destroy(linger=0)
            except:
                logger.exception('failed to destroy zmq context')
        except Exception as e:
            logger.exception('unhandled exception in router: %s', e)

    logger.info('stopped')

    if not run_daemon.is_set():
        run_daemon.set()

def main() -> int:
    logger = logging.getLogger('main')
    run_daemon = Event()
    process = Process(target=router, args=(run_daemon,))

    def signal_handler(signum: Union[int, Signals], _frame: Optional[FrameType]) -> None:
        if run_daemon.is_set():
            return

        logger.info('received signal %r', signum)
        run_daemon.set()

    signal(SIGTERM, signal_handler)
    signal(SIGINT, signal_handler)

    proc.setproctitle('grizzly-async-messaged')  # set appl name on ibm mq

    try:
        # start router I/O loop
        process.start()

        # wait for event to be set, which would be in `signal_handler` or when router I/O loop ends
        run_daemon.wait()

        # this should terminate the process, by sending SIGTERM to it
        process.terminate()

        # wait up to 3 seconds (socket poll timeout is 1 second) for the process to finish,
        # ignore any exception that might happen during that time
        with suppress(Exception):
            process.join(timeout=3.0)

        # if the process is still alive after timeout, forcefully kill it
        if process.is_alive():
            process.kill()

    except KeyboardInterrupt:
        return 1
    else:
        return process.exitcode or 0
