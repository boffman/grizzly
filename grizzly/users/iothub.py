"""Put files to Azure IoT hub.

## Request methods

Supports the following request methods:

* send
* put
* get
* receive

## Metadata

The following properties is added to the metadata part of a message:

- `custom_properties` (dict)
- `message_id` (str)
- `expiry_time_utc` (str)
- `correlation_id` (str)
- `user_id` (str)
- `content_type` (str)
- `output_name` (str)
- `input_name` (str)
- `ack` (bool)
- `iothub_interface_id` (str)
- `size` (int)

## Sending

This user has support for sending IoT messages and uploading files. For the former the `endpoint` in the request must be `device-to-cloud`, otherwise
it will try to upload the request as a file. See [device-to-cloud communication guidance](https://learn.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-d2c-guidance)
for the difference.

## Receiving

Receiving cloud-to-device (C2D) messages is a little bit special, the `endpoint` in the in the request must be `cloud-to-device`. The first client to connect to a IoT device
will register a message handler, that will receive all the messages and store them in the global keystore. When a "receive" request is executing, it will wait for the keystore
to be populated with a message, and get (consume) it from the keystore.

There are 3 other context variables that will control wether a message will be pushed to the keystore for handling or not:

- `expression.unique`, this is a JSON-path expression will extract a value from the message payload, push the whole message to the keystore and save the value in a "volatile list",
  if a message with the same value is received within 5 seconds, it will not be handled. This is to avoid handling duplicates of messages, which can occur for different reasons.
  E.g. `$.body.event.id`.

- `expression.metadata` (bool), this a JSON-path expression that should validate to a boolean expression, messages for which this expression does not validate to `True` will be
  dropped and not pushed to the keystore. E.g. `$.size>=1024`.

- `expression.payload` (bool), same as `expression.metadata` except it will validate against the actual message payload. E.g. `$.body.timestamp>='2024-09-23 20:39:00`, which will
  drop all messages having a timestamp before the specified date.

## Pipe arguments

See {@pylink grizzly.tasks.request} endpoint format for standard pipe arguments, in additional to those the following are also supported:

- `wait` (int), number of seconds to wait for a available message before failing request. If not specified it will wait indefinitely.

## Format

Format of `host` is the following:

```plain
HostName=<hostname>;DeviceId=<device key>;SharedAccessKey=<access key>
```

The metadata values `content_type` and `content_encoding` can be set to gzip compress the payload before upload (see example below).

## Examples

Example of how to use it in a scenario:

```gherkin
Given a user of type "IotHub" load testing "HostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=="
Then send request "test/blob.file" to endpoint "uploaded_blob_filename"
```

The same example with gzip compression enabled:

```gherkin
Given a user of type "IotHub" load testing "HostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=="
And metadata "content_encoding" is "gzip"
Then send request "test/blob.file" to endpoint "uploaded_blob_filename | content_type=octet_stream_utf8"
```

Example of how to receive unique messages from a thermometer of type `temperature` (considered that the application sets a custom property `message_type`)
```gherkin
Given a user of type "IotHub" load testing "HostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=="
And set context variable "expression.unique" to "$.device.name=='thermometer01'"
And set context variable "expression.metadata" to "$.custom_properties.message_type=='temperature'"
And value for variable "temperature" is "none"

Then receive request with name "iot-get-temperature" from endpoint "cloud-to-device | content_type=json, wait=180"
Then save response payload "$.device.temperature" in variable "temperature"
```

"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from geventhttpclient import Session

from grizzly.types import GrizzlyResponse, RequestMethod, ScenarioState

from . import GrizzlyUser, grizzlycontext

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types.locust import Environment


@grizzlycontext(context={})
class IotHubUser(GrizzlyUser):

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        conn_str = self.host

        # Replace semicolon separators between parameters to ? and & and massage it to make it "urlparse-compliant"
        # for validation
        if not conn_str.startswith('http'):
            message = f'{self.__class__.__name__} host needs to start with "http": {self.host}'
            raise ValueError(message)
        self.session = Session()


    def on_start(self) -> None:
        super().on_start()
        self.stub_url = self.host

    def on_state(self, *, state: ScenarioState) -> None:
        super().on_state(state=state)


    def on_stop(self) -> None:
        super().on_stop()

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        if request.method not in [RequestMethod.SEND, RequestMethod.PUT]:
            message = f'{self.__class__.__name__} has not implemented {request.method.name}'
            raise NotImplementedError(message)

        try:
            if not request.source:
                message = f'Cannot upload empty payload to endpoint {request.endpoint} in IotHubUser'
                raise RuntimeError(message)

            filename = request.endpoint

            # TODO: gzip encoding?
            self.session.post(f'{self.stub_url}/iot', data = request.source.encode())
        except Exception:
            self.logger.exception('failed to upload file "%s" to IoT hub', filename)

            raise
        else:
            return {}, request.source
