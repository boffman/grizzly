from os import environ
from typing import Callable, Dict, Any, Optional

import pytest

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture

from locust.user.task import TaskSet
from locust.exception import StopUser

from grizzly.tasks.iterator import TrafficIteratorTasks
from grizzly.testdata.models import TemplateData
from grizzly.testdata.communication import TestdataConsumer
from grizzly.testdata.utils import transform

from ..fixtures import locust_context, request_context  # pylint: disable=unused-import
from ..helpers import RequestCalled


class TestTrafficIterationModel:
    @pytest.mark.usefixtures('locust_context')
    def test_initialize(self, locust_context: Callable) -> None:
        _, _, task, _ = locust_context()
        assert issubclass(task.__class__, TaskSet)

    @pytest.mark.usefixtures('locust_context')
    def test_add_scenario_task(self, locust_context: Callable, mocker: MockerFixture) -> None:
        _, _, task, [_, _, request] = locust_context(task_type=TrafficIteratorTasks)
        request.endpoint = '/api/v1/test'
        TrafficIteratorTasks.add_scenario_task(request)
        assert isinstance(task, TrafficIteratorTasks)
        assert len(task.tasks) == 2

        task.iteration_data = TemplateData()
        task_method = task.tasks[-1]

        assert callable(task_method)
        with pytest.raises(RequestCalled) as e:
            task_method(task)
        assert e.value.endpoint == '/api/v1/test' and e.value.request is request

        def generate_mocked_wait(sleep_time: float) -> None:
            def mocked_wait(time: float) -> None:
                assert sleep_time == time

            mocker.patch(
                'grizzly.tasks.iterator.gsleep',
                mocked_wait,
            )

        generate_mocked_wait(1.5)
        TrafficIteratorTasks.add_scenario_task(1.5)
        assert len(task.tasks) == 3

        task_method = task.tasks[-1]
        assert callable(task_method)
        task_method(task)

    @pytest.mark.usefixtures('locust_context')
    def test_on_event_handlers(self, locust_context: Callable, mocker: MockerFixture) -> None:
        try:
            _, _, task, _ = locust_context(task_type=TrafficIteratorTasks)

            def TestdataConsumer__init__(self: 'TestdataConsumer', address: str) -> None:
                pass

            mocker.patch(
                'grizzly.testdata.communication.TestdataConsumer.__init__',
                TestdataConsumer__init__,
            )

            def TestdataConsumer_on_stop(self: 'TestdataConsumer') -> None:
                raise StopUser()

            mocker.patch(
                'grizzly.testdata.communication.TestdataConsumer.stop',
                TestdataConsumer_on_stop,
            )

            assert task is not None

            with pytest.raises(StopUser):
                task.on_start()

            environ['TESTDATA_PRODUCER_ADDRESS'] = 'localhost:5555'

            task.on_start()

            with pytest.raises(StopUser):
                task.on_stop()
        finally:
            try:
                del environ['TESTDATA_PRODUCER_ADDRESS']
            except KeyError:
                pass

    @pytest.mark.usefixtures('locust_context')
    def test_iterator(self, locust_context: Callable, mocker: MockerFixture) -> None:
        _, user, task, _ = locust_context(task_type=TrafficIteratorTasks)

        assert task is not None

        task.consumer = TestdataConsumer()

        def mock_request(data: Optional[Dict[str, Any]]) -> None:
            def request(self: 'TestdataConsumer', scenario: str) -> Optional[Dict[str, Any]]:
                if data is None or data == {}:
                    return None

                if 'variables' in data:
                    data['variables'] = transform(data['variables'])

                return data

            mocker.patch(
                'grizzly.testdata.communication.TestdataConsumer.request',
                request,
            )

        mock_request(None)

        with pytest.raises(StopUser):
            task.iterator()

        assert user.context_variables == {}

        mock_request({})

        with pytest.raises(StopUser):
            task.iterator()

        assert user.context_variables == {}

        mock_request({
            'variables': {
                'AtomicIntegerIncrementer.messageID': 1337,
                'AtomicCsvRow.test': {
                    'header1': 'value1',
                    'header2': 'value2',
                },
            },
        })

        task.iterator()

        assert user.context_variables['AtomicIntegerIncrementer'].messageID == 1337
        assert user.context_variables['AtomicCsvRow'].test.header1 == 'value1'
        assert user.context_variables['AtomicCsvRow'].test.header2 == 'value2'