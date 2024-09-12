"""Put files to Azure IoT hub.

## Request methods

Supports the following request methods:

* send
* put

## Format

Format of `host` is the following:

```plain
HostName=<hostname>;DeviceId=<device key>;SharedAccessKey=<access key>
```

`endpoint` in the request is the desired filename for the uploaded file.

The metadata values `content_type` and `content_encoding` can be set to
gzip compress the payload before upload (see example below).

## Examples

Example of how to use it in a scenario:

```gherkin
Given a user of type "IotHub" load testing "HostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=="
Then send request "test/blob.file" to endpoint "uploaded_blob_filename"
```

The same example with gzip compression enabled:

```gherkin
Given a user of type "IotHub" load testing "HostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=="
And metadata "content_type" is "application/octet-stream; charset=utf-8"
And metadata "content_encoding" is "gzip"
Then send request "test/blob.file" to endpoint "uploaded_blob_filename"
```

"""
from __future__ import annotations
import requests
import gzip
from typing import TYPE_CHECKING, Any, Optional, cast
from urllib.parse import parse_qs, urlparse

from azure.iot.device import IoTHubDeviceClient
from azure.storage.blob import BlobClient, ContentSettings

from grizzly.types import GrizzlyResponse, RequestMethod

from . import GrizzlyUser, grizzlycontext

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types.locust import Environment


@grizzlycontext(context={})
class IotHubUser(GrizzlyUser):
    iot_client: IoTHubDeviceClient

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        conn_str = self.host

        # Replace semicolon separators between parameters to ? and & and massage it to make it "urlparse-compliant"
        # for validation
        if not conn_str.startswith('http'):
            message = f'{self.__class__.__name__} host needs to start with "http": {self.host}'
            raise ValueError(message)
        self.session = requests.session()


    def on_start(self) -> None:
        super().on_start()
        self.stub_url = self.host

    def on_stop(self) -> None:
        self.iot_client.disconnect()

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
