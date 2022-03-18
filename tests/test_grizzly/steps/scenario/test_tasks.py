from typing import cast

import pytest

from parse import compile
from json import dumps as jsondumps

from behave.model import Table, Row
from grizzly.context import GrizzlyContext
from grizzly.types import RequestMethod, RequestDirection
from grizzly.tasks import TransformerTask, PrintTask, WaitTask
from grizzly.tasks.getter import HttpGetTask
from grizzly.steps import *  # pylint: disable=unused-wildcard-import  # noqa: F403

from grizzly_extras.transformer import TransformerContentType

from ...fixtures import BehaveFixture


def test_parse_method() -> None:
    p = compile(
        'value {method:Method} world',
        extra_types=dict(
            Method=parse_method,
        ),
    )

    for method in RequestMethod:
        assert p.parse(f'value {method.name} world')['method'] == method

    with pytest.raises(ValueError):
        p.parse('value asdf world')


def test_parse_direction() -> None:
    p = compile(
        'value {direction:Direction} world',
        extra_types=dict(
            Direction=parse_direction,
        ),
    )

    for direction in RequestDirection:
        assert p.parse(f'value {direction.name} world')['direction'] == direction

    with pytest.raises(ValueError):
        p.parse('value asdf world')


def test_step_task_request_with_name_to_endpoint_until(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    assert len(grizzly.scenario.tasks) == 0

    with pytest.raises(AssertionError) as ae:
        step_task_request_with_name_to_endpoint_until(behave, RequestMethod.POST, 'test', '/api/test', '$.`this`[?status="ready"]')
    assert 'this step is only valid for request methods with direction FROM' in str(ae)

    behave.text = 'foo bar'
    with pytest.raises(AssertionError) as ae:
        step_task_request_with_name_to_endpoint_until(behave, RequestMethod.GET, 'test', '/api/test', '$.`this`[?status="ready"]')
    assert 'this step does not have support for step text' in str(ae)

    behave.text = None

    with pytest.raises(ValueError) as ve:
        step_task_request_with_name_to_endpoint_until(behave, RequestMethod.GET, 'test', '/api/test', '$.`this`[?status="ready"]')
    assert 'content type must be specified for request' in str(ve)

    step_task_request_with_name_to_endpoint_until(behave, RequestMethod.GET, 'test', '/api/test | content_type=json', '$.`this`[?status="ready"]')

    assert len(grizzly.scenario.tasks) == 1

    rows: List[Row] = []
    rows.append(Row(['endpoint'], ['{{ variable }}']))
    rows.append(Row(['endpoint'], ['foo']))
    rows.append(Row(['endpoint'], ['bar']))
    behave.table = Table(['endpoint'], rows=rows)

    step_task_request_with_name_to_endpoint_until(behave, RequestMethod.GET, 'test', '/api/{{ endpoint }} | content_type=json', '$.`this`[?status="{{ endpoint }}"]')

    assert len(grizzly.scenario.tasks) == 4
    tasks = cast(List[UntilRequestTask], grizzly.scenario.tasks)

    assert tasks[-1].request.endpoint == '/api/bar'
    assert tasks[-1].condition == '$.`this`[?status="bar"]'
    assert tasks[-2].request.endpoint == '/api/foo'
    assert tasks[-2].condition == '$.`this`[?status="foo"]'
    assert tasks[-3].request.endpoint == '/api/{{ variable }}'
    assert tasks[-3].condition == '$.`this`[?status="{{ variable }}"]'

    assert len(grizzly.scenario.orphan_templates) == 1
    assert grizzly.scenario.orphan_templates[-1] == '$.`this`[?status="{{ variable }}"]'


def test_step_task_request_file_with_name_endpoint(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    for method in RequestDirection.TO.methods:
        step_task_request_file_with_name_endpoint(behave, method, '{}', 'the_name', 'the_container')

    for method in RequestDirection.FROM.methods:
        with pytest.raises(AssertionError):
            step_task_request_file_with_name_endpoint(behave, method, '{}', 'the_name', 'the_container')


def test_step_task_request_file_with_name(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    for method in RequestDirection.TO.methods:
        with pytest.raises(ValueError):
            step_task_request_file_with_name(behave, method, '{}', f'{method.name}-test')

    for method in RequestDirection.TO.methods:
        step_task_request_file_with_name_endpoint(behave, method, '{}', f'{method.name}-test', f'/api/test/{method.name.lower()}')
        step_task_request_file_with_name(behave, method, '{}', f'{method.name}-test')

    for method in RequestDirection.FROM.methods:
        with pytest.raises(AssertionError):
            # step_request_to_payload_file_with_name_endpoint(behave, method, '{}', f'{method.name}-test', f'/api/test/{method.name.lower()}')
            step_task_request_file_with_name(behave, method, '{}', f'{method.name}-test')


def test_step_task_request_text_with_name_to_endpoint(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    behave.text = '{}'

    for method in RequestDirection.TO.methods:
        step_task_request_text_with_name_to_endpoint(behave, method, 'test-name', RequestDirection.TO, '/api/test')

        with pytest.raises(AssertionError):
            step_task_request_text_with_name_to_endpoint(behave, method, 'test-name', RequestDirection.FROM, '/api/test')

    for method in RequestDirection.FROM.methods:
        with pytest.raises(AssertionError):
            step_task_request_text_with_name_to_endpoint(behave, method, 'test-name', RequestDirection.TO, '/api/test')

        with pytest.raises(AssertionError):
            step_task_request_text_with_name_to_endpoint(behave, method, 'test-name', RequestDirection.FROM, '/api/test')

    behave.text = None

    for method in RequestDirection.FROM.methods:
        step_task_request_text_with_name_to_endpoint(behave, method, 'test-name', RequestDirection.FROM, '/api/test')

        with pytest.raises(AssertionError):
            step_task_request_text_with_name_to_endpoint(behave, method, 'test-name', RequestDirection.TO, '/api/test')

    with pytest.raises(AssertionError):
        step_task_request_text_with_name_to_endpoint(behave, 'GET', 'test-name', 'asdf', '/api/test')


def test_step_task_request_text_with_name(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    behave.text = '{}'

    with pytest.raises(ValueError):
        step_task_request_text_with_name(behave, RequestMethod.POST, 'test-name')

    step_task_request_text_with_name_to_endpoint(behave, RequestMethod.POST, 'test-name', RequestDirection.TO, '/api/test')

    behave.text = None
    with pytest.raises(ValueError):
        step_task_request_text_with_name(behave, RequestMethod.GET, 'test-name')

    with pytest.raises(AssertionError):
        step_task_request_text_with_name(behave, RequestMethod.POST, 'test-name')

    behave.text = '{}'
    step_task_request_text_with_name(behave, RequestMethod.POST, 'test-name')


def test_step_task_wait_seconds(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    with pytest.raises(AssertionError):
        step_task_wait_seconds(behave, -1.0)

    step_task_wait_seconds(behave, 1.337)

    assert isinstance(grizzly.scenario.tasks[-1], WaitTask)
    assert grizzly.scenario.tasks[-1].time == 1.337


def test_step_task_print_message(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    step_task_print_message(behave, 'hello {{ world }}')

    assert isinstance(grizzly.scenario.tasks[-1], PrintTask)
    assert grizzly.scenario.tasks[-1].message == 'hello {{ world }}'


def test_step_task_transform(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    with pytest.raises(ValueError) as ve:
        step_task_transform(
            behave,
            jsondumps({
                'document': {
                    'id': 'DOCUMENT_8483-1',
                    'title': 'TPM Report 2020',
                },
            }),
            TransformerContentType.JSON,
            '$.document.id',
            'document_id',
        )
    assert 'TransformerTask: document_id has not been initialized' in str(ve)

    grizzly.state.variables['document_id'] = 'None'
    step_task_transform(
        behave,
        jsondumps({
            'document': {
                'id': 'DOCUMENT_8483-1',
                'title': 'TPM Report 2020',
            },
        }),
        TransformerContentType.JSON,
        '$.document.id',
        'document_id',
    )

    task = grizzly.scenario.tasks[-1]
    assert isinstance(task, TransformerTask)
    assert task.content_type == TransformerContentType.JSON
    assert task.expression == '$.document.id'
    assert task.variable == 'document_id'

    assert len(grizzly.scenario.orphan_templates) == 0

    step_task_transform(
        behave,
        jsondumps({
            'document': {
                'id': 'DOCUMENT_8483-1',
                'title': 'TPM Report {{ year }}',
            },
        }),
        TransformerContentType.JSON,
        '$.document.id',
        'document_id',
    )

    assert len(grizzly.scenario.orphan_templates) == 1
    assert grizzly.scenario.orphan_templates[-1] == jsondumps({
        'document': {
            'id': 'DOCUMENT_8483-1',
            'title': 'TPM Report {{ year }}',
        },
    })


def test_step_task_get_endpoint(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    with pytest.raises(AssertionError) as ae:
        step_task_get_endpoint(behave, 'mq.example.com', 'test')
    assert 'could not find scheme in "mq.example.com"' in str(ae)

    with pytest.raises(AssertionError) as ae:
        step_task_get_endpoint(behave, 'mq://mq.example.com', 'test')
    assert 'no getter task registered for mq' in str(ae)

    with pytest.raises(ValueError) as ve:
        step_task_get_endpoint(behave, 'http://www.example.org', 'test')
    assert 'HttpGetTask: variable test has not been initialized' in str(ve)

    grizzly.state.variables['test'] = 'none'

    assert len(grizzly.scenario.tasks) == 0
    step_task_get_endpoint(behave, 'http://www.example.org', 'test')
    assert len(grizzly.scenario.tasks) == 1
    assert isinstance(grizzly.scenario.tasks[-1], HttpGetTask)

    step_task_get_endpoint(behave, 'https://{{ endpoint_url }}', 'test')

    task = grizzly.scenario.tasks[-1]
    assert task.endpoint == '{{ endpoint_url }}'


def test_step_task_date(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    with pytest.raises(AssertionError) as ae:
        step_task_date(behave, '{{ datetime.now() }} | offset=1D', 'date_variable')
    assert 'variable date_variable has not been initialized' in str(ae)

    grizzly.state.variables['date_variable'] = 'none'

    step_task_date(behave, '{{ datetime.now() }} | offset=1D', 'date_variable')

    assert len(grizzly.scenario.tasks) == 1
    assert len(grizzly.scenario.orphan_templates) == 1
    assert isinstance(grizzly.scenario.tasks[-1], DateTask)

    task = grizzly.scenario.tasks[-1]
    assert task.value == '{{ datetime.now() }}'
    assert task.variable == 'date_variable'
    assert task.arguments.get('offset') == '1D'
