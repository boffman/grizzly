import logging
import resource

from os import environ
from typing import cast, Tuple, Any, Dict, Type, List

import pytest
import gevent

from _pytest.logging import LogCaptureFixture
from _pytest.capture import CaptureFixture
from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from behave.runner import Context
from locust.env import Environment
from locust.user.users import User
from jinja2 import Template, TemplateError

from grizzly.locust import greenlet_exception_logger, on_master, on_worker, on_local, run, setup_environment_listeners, setup_locust_scenarios, setup_resource_limits
from grizzly.types import RequestMethod
from grizzly.context import LocustContext, LocustContextScenario, RequestContext
from grizzly.users import RestApiUser
from grizzly.tasks import TrafficIteratorTasks
from grizzly.testdata.variables import AtomicInteger

from .fixtures import behave_context  # pylint: disable=unused-import


def test_greenlet_exception_logger(caplog: LogCaptureFixture) -> None:
    logger = logging.getLogger()
    exception_handler = greenlet_exception_logger(logger)

    assert callable(exception_handler)

    greenlet = gevent.Greenlet()

    from grizzly.locust import unhandled_greenlet_exception
    assert not unhandled_greenlet_exception

    with caplog.at_level(logging.CRITICAL):
        exception_handler(greenlet)
    assert 'unhandled exception in greenlet: ' in caplog.text

    # re-import to get updated value
    from grizzly.locust import unhandled_greenlet_exception
    assert unhandled_greenlet_exception

    caplog.clear()


@pytest.mark.usefixtures('behave_context')
def test_on_master(behave_context: Context) -> None:
    try:
        assert environ.get('LOCUST_IS_MASTER', None) is None
        assert not on_master(behave_context)
        assert environ.get('LOCUST_IS_MASTER', None) is None

        behave_context.config.userdata['master'] = 'TRUE'
        assert on_master(behave_context)
        assert environ.get('LOCUST_IS_MASTER', None) == 'true'
    finally:
        try:
            del environ['LOCUST_IS_MASTER']
        except KeyError:
            pass

        try:
            del behave_context.config.userdata['master']
        except KeyError:
            pass


@pytest.mark.usefixtures('behave_context')
def test_on_worker(behave_context: Context) -> None:
    try:
        assert environ.get('LOCUST_IS_WORKER', None) is None
        assert not on_worker(behave_context)
        assert environ.get('LOCUST_IS_WORKER', None) is None

        behave_context.config.userdata['worker'] = 'TRUE'
        assert on_worker(behave_context)
        assert environ.get('LOCUST_IS_WORKER', None) == 'true'
    finally:
        try:
            del environ['LOCUST_IS_WORKER']
        except KeyError:
            pass

        try:
            del behave_context.config.userdata['worker']
        except KeyError:
            pass


@pytest.mark.usefixtures('behave_context')
def test_on_local(behave_context: Context) -> None:
    try:
        assert environ.get('LOCUST_IS_LOCAL', None) is None
        behave_context.config.userdata['master'] = 'TrUe'
        assert not on_local(behave_context)
        assert environ.get('LOCUST_IS_LOCAL', None) is None
        del behave_context.config.userdata['master']

        assert environ.get('LOCUST_IS_LOCAL', None) is None
        behave_context.config.userdata['worker'] = 'TrUe'
        assert not on_local(behave_context)
        assert environ.get('LOCUST_IS_LOCAL', None) is None
        del behave_context.config.userdata['worker']

        assert on_local(behave_context)
        assert environ.get('LOCUST_IS_LOCAL', None) == 'true'
    finally:
        try:
            del environ['LOCUST_IS_LOCAL']
        except KeyError:
            pass

        try:
            del behave_context.config.userdata['master']
        except KeyError:
            pass

        try:
            del behave_context.config.userdata['worker']
        except KeyError:
            pass


@pytest.mark.usefixtures('behave_context')
def test_setup_locust_scenarios(behave_context: Context) -> None:
    context_locust = cast(LocustContext, behave_context.locust)

    with pytest.raises(AssertionError) as ae:
        setup_locust_scenarios(context_locust)
    assert 'no scenarios in feature' in str(ae)

    # scenario is missing host
    context_locust.add_scenario('test')

    with pytest.raises(AssertionError) as ae:
        setup_locust_scenarios(context_locust)
    assert 'variable "host" is not found in the context for' in str(ae)

    context_locust.scenario.context['host'] = 'https://test.example.org'

    # no tasks in scenario
    with pytest.raises(AssertionError) as ae:
        setup_locust_scenarios(context_locust)
    assert 'no tasks has been added to' in str(ae)

    task = RequestContext(RequestMethod.GET, 'test-1', '/api/v1/test/1')
    context_locust.scenario.add_task(task)
    context_locust.scenario.add_task(1.5)

    # incorrect user type
    context_locust.scenario.user_class_name = 'NonExistingUser'
    with pytest.raises(AttributeError):
        setup_locust_scenarios(context_locust)

    context_locust.scenario.user_class_name = 'RestApiUser'
    user_classes, request_tasks = setup_locust_scenarios(context_locust)

    assert len(user_classes) == 1
    assert len(request_tasks) == 1

    user_class = user_classes[-1]
    assert issubclass(user_class, (RestApiUser, ))
    assert len(user_class.tasks) == 1
    assert user_class.host == 'https://test.example.org'
    assert context_locust.scenario.name.startswith('TrafficIteratorTasks')

    user_tasks = user_class.tasks[-1]
    assert issubclass(user_tasks, (TrafficIteratorTasks, ))
    assert len(user_tasks.tasks) == 2 + 1  # TrafficIteratorTasks has an internal task other than what we've added


@pytest.mark.usefixtures('behave_context')
def test_setup_resource_limits(behave_context: Context, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
    def mock_on_master(is_master: bool) -> None:
        def mocked_on_master(context: Context) -> bool:
            return is_master

        mocker.patch(
            'grizzly.locust.on_master',
            mocked_on_master,
        )

    def mock_os_name(name: str) -> None:
        mocker.patch(
            'grizzly.locust.osname',
            name
        )

    def mock_getrlimit(limit: int) -> None:
        def mocked_getrlimit(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Tuple[int, int]:
            return limit, 0

        mocker.patch(
            'resource.getrlimit',
            mocked_getrlimit,
        )

    def mock_setrlimit(exception_type: Type[Exception]) -> None:
        def mocked_setrlimit(_resource: int, limits: Tuple[int, int]) -> Any:
            assert _resource == resource.RLIMIT_NOFILE
            assert limits == (10000, resource.RLIM_INFINITY, )

            raise exception_type()

        mocker.patch(
            'resource.setrlimit',
            mocked_setrlimit,
        )

    mock_on_master(False)
    mock_os_name('nt')
    setup_resource_limits(behave_context)

    mock_os_name('posix')
    setup_resource_limits(behave_context)

    # make sure setrlimit is called
    mock_on_master(True)
    mock_getrlimit(1024)
    mock_setrlimit(RuntimeError)
    with pytest.raises(RuntimeError):
        setup_resource_limits(behave_context)

    # failed to set resource limits
    mock_setrlimit(OSError)
    with caplog.at_level(logging.WARNING):
        setup_resource_limits(behave_context)
    assert "and the OS didn't allow locust to increase it by itself" in caplog.text
    caplog.clear()

    mock_setrlimit(ValueError)
    with caplog.at_level(logging.WARNING):
        setup_resource_limits(behave_context)
    assert "and the OS didn't allow locust to increase it by itself" in caplog.text
    caplog.clear()

    mock_getrlimit(10001)
    try:
        setup_resource_limits(behave_context)
    except RuntimeError:
        pytest.fail(f'setrlimit was unexpectedly called')


@pytest.mark.usefixtures('behave_context')
def test_setup_environment_listeners(behave_context: Context, mocker: MockerFixture) -> None:
    from locust import events

    def mock_on_worker(on_worker: bool) -> None:
        def mocked_on_worker(context: Context) -> bool:
            return on_worker

        mocker.patch(
            'grizzly.locust.on_worker',
            mocked_on_worker,
        )

    context_locust = cast(LocustContext, behave_context.locust)
    user_classes: List[User] = []
    environment = Environment(
        user_classes=user_classes,
        shape_class=None,
        events=events,
    )

    try:
        # event listeners for worker node
        mock_on_worker(True)
        setup_environment_listeners(behave_context, environment, [])

        assert len(environment.events.init._handlers) == 1
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 0

        context_locust.setup.statistics_url = 'influxdb://influx.example.com/testdb'

        setup_environment_listeners(behave_context, environment, [])
        assert len(environment.events.init._handlers) == 2
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 0

        context_locust.setup.statistics_url = None

        # event listeteners for master node, not validating results
        mock_on_worker(False)

        task = RequestContext(RequestMethod.POST, 'test-post-1', '/api/v3/test/post/1')
        task.source = '{{ AtomicInteger.value }}, {{ test_id }}'
        task.template = Template(task.source)
        task.scenario = LocustContextScenario()
        task.scenario.name = 'test-scenario-1'
        task.scenario.user_class_name = 'RestApiUser'
        request_tasks = [task]

        with pytest.raises(AssertionError) as ae:
            setup_environment_listeners(behave_context, environment, request_tasks)
        assert 'variable test_id has not been initialized' in str(ae)

        AtomicInteger.destroy()
        context_locust.state.variables['test_id'] = 'test-1'

        setup_environment_listeners(behave_context, environment, request_tasks)
        assert len(environment.events.init._handlers) == 1
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 1

        AtomicInteger.destroy()
        context_locust.setup.statistics_url = 'influxdb://influx.example.com/testdb'

        setup_environment_listeners(behave_context, environment, request_tasks)
        assert len(environment.events.init._handlers) == 2
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 1

        AtomicInteger.destroy()

        context_locust.scenario.validation.fail_ratio = 0.1

        setup_environment_listeners(behave_context, environment, request_tasks)
        assert len(environment.events.init._handlers) == 2
        assert len(environment.events.test_start._handlers) == 1
        assert len(environment.events.test_stop._handlers) == 1
        assert len(environment.events.spawning_complete._handlers) == 1
        assert len(environment.events.quitting._handlers) == 2

        AtomicInteger.destroy()

        context_locust.setup.statistics_url = None

        # problems initializing testdata
        def mocked_initialize_testdata(request_tasks: List[RequestContext]) -> Any:
            raise TemplateError('failed to initialize testdata')

        mocker.patch(
            'grizzly.locust.initialize_testdata',
            mocked_initialize_testdata,
        )

        with pytest.raises(AssertionError) as ae:
            setup_environment_listeners(behave_context, environment, request_tasks)
        assert 'error parsing request payload: ' in str(ae)
    finally:
        try:
            AtomicInteger.destroy()
        except:
            pass


@pytest.mark.usefixtures('behave_context')
def test_run_master(behave_context: Context, capsys: CaptureFixture, mocker: MockerFixture) -> None:
    def noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
        pass

    for method in [
        'locust.runners.MasterRunner.start',
        'locust.runners.MasterRunner.client_listener',
        'gevent.sleep',
        'locust.rpc.zmqrpc.Server.__init__',
        'grizzly.listeners._init_testdata_producer',
    ]:
        mocker.patch(
            method,
            noop,
        )

    for printer in ['print_error_report', 'print_percentile_stats', 'print_stats', 'stats_printer', 'stats_history']:
        mocker.patch(
            f'grizzly.locust.{printer}',
            noop,
        )

    def mock_on_node(master: bool, worker: bool) -> None:
        def mocked_on_worker(context: Context) -> bool:
            return worker

        mocker.patch(
            'grizzly.locust.on_worker',
            mocked_on_worker,
        )

        def mocked_on_master(context: Context) -> bool:
            return master

        mocker.patch(
            'grizzly.locust.on_master',
            mocked_on_master,
        )

    behave_context.config.userdata = {
        'master': 'true',
        'worker': 'true',
    }

    assert run(behave_context) == 254
    assert 'cannot be both master and worker' in capsys.readouterr().err

    behave_context.config.userdata = {}

    context_locust = cast(LocustContext, behave_context.locust)

    context_locust.setup.spawn_rate = 1

    assert run(behave_context) == 254
    assert 'step \'Given "user_count" users\' is not in the feature file' in capsys.readouterr().err

    context_locust.setup.user_count = 2
    context_locust.add_scenario('test')
    context_locust.scenario.user_class_name = 'RestApiUser'
    context_locust.scenario.context['host'] = 'https://test.example.org'
    context_locust.scenario.add_task(1.5)
    task = RequestContext(RequestMethod.GET, 'test-1', '/api/v1/test/1')
    context_locust.scenario.add_task(task)

    behave_context.config.userdata = {
        'expected-workers': 3,
    }

    mock_on_node(master=True, worker=False)

    with pytest.raises(AssertionError) as ae:
        run(behave_context)
    assert 'there are more workers (3) than users (2), which is not supported' in str(ae)

    del behave_context.config.userdata['expected-workers']

    # @TODO: this is where it gets hard(er)...

    '''
    def mock_clients_ready(count: int) -> None:
        def mocked_get_by_state(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> List[Any]:
            return [x for x in range(0, count)]

        mocker.patch(
            'locust.runners.WorkerNodes.get_by_state',
            mocked_get_by_state,
        )

    context_locust.state.spawning_complete = True  # fake that all worker nodes has fired spawning complete

    mocked_user_count = mocker.patch(
        'locust.runners.MasterRunner.user_count',
        new_callable=PropertyMock,
    )
    mocked_user_count.return_value = 0

    mock_clients_ready(2)  # same as context_locust.setup.user_count

    run(behave_context)
    '''
