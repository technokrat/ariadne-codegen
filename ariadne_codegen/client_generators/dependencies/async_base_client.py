import asyncio
import enum
import json
from collections.abc import AsyncIterator
from copy import deepcopy
from typing import IO, Any, Optional, TypeVar, cast
from uuid import uuid4

import httpx
from pydantic import BaseModel
from pydantic_core import to_jsonable_python

from .base_model import UNSET, Upload
from .exceptions import (
    GraphQLClientError,
    GraphQLClientGraphQLMultiError,
    GraphQLClientHttpError,
    GraphQLClientInvalidMessageFormat,
    GraphQLClientInvalidResponseError,
)

try:
    from websockets import (  # type: ignore[import-not-found,unused-ignore]
        ClientConnection,
    )
    from websockets import (  # type: ignore[import-not-found,unused-ignore]
        connect as ws_connect,
    )
    from websockets.typing import (  # type: ignore[import-not-found,unused-ignore]
        Data,
        Origin,
        Subprotocol,
    )
except ImportError:
    from contextlib import asynccontextmanager

    @asynccontextmanager  # type: ignore
    async def ws_connect(*args, **kwargs):
        raise NotImplementedError("Subscriptions require 'websockets' package.")
        yield

    ClientConnection = Any  # type: ignore[misc,assignment,unused-ignore]
    Data = Any  # type: ignore[misc,assignment,unused-ignore]
    Origin = Any  # type: ignore[misc,assignment,unused-ignore]

    def Subprotocol(*args, **kwargs):  # type: ignore # noqa: N802, N803
        raise NotImplementedError("Subscriptions require 'websockets' package.")


Self = TypeVar("Self", bound="AsyncBaseClient")

GRAPHQL_TRANSPORT_WS = "graphql-transport-ws"


class GraphQLTransportWSMessageType(str, enum.Enum):
    CONNECTION_INIT = "connection_init"
    CONNECTION_ACK = "connection_ack"
    PING = "ping"
    PONG = "pong"
    SUBSCRIBE = "subscribe"
    NEXT = "next"
    ERROR = "error"
    COMPLETE = "complete"


class AsyncBaseClient:
    def __init__(
        self,
        url: str = "",
        headers: Optional[dict[str, str]] = None,
        http_client: Optional[httpx.AsyncClient] = None,
        ws_url: str = "",
        ws_headers: Optional[dict[str, Any]] = None,
        ws_origin: Optional[str] = None,
        ws_connection_init_payload: Optional[dict[str, Any]] = None,
    ) -> None:
        self.url = url
        self.headers = headers
        self.http_client = (
            http_client if http_client else httpx.AsyncClient(headers=headers)
        )

        self.ws_url = ws_url
        self.ws_headers = ws_headers or {}
        self.ws_origin = Origin(ws_origin) if ws_origin else None
        self.ws_connection_init_payload = ws_connection_init_payload

    async def __aenter__(self: Self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> None:
        await self.http_client.aclose()

    async def execute(
        self,
        query: str,
        operation_name: Optional[str] = None,
        variables: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> httpx.Response:
        processed_variables, files, files_map = self._process_variables(variables)

        if files and files_map:
            return await self._execute_multipart(
                query=query,
                operation_name=operation_name,
                variables=processed_variables,
                files=files,
                files_map=files_map,
                **kwargs,
            )

        return await self._execute_json(
            query=query,
            operation_name=operation_name,
            variables=processed_variables,
            **kwargs,
        )

    async def execute_incremental(
        self,
        query: str,
        operation_name: Optional[str] = None,
        variables: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        processed_variables, files, files_map = self._process_variables(variables)

        if files and files_map:
            generator = self._execute_multipart_incremental(
                query=query,
                operation_name=operation_name,
                variables=processed_variables,
                files=files,
                files_map=files_map,
                **kwargs,
            )
        else:
            generator = self._execute_json_incremental(
                query=query,
                operation_name=operation_name,
                variables=processed_variables,
                **kwargs,
            )

        async for data in generator:
            yield data

    def get_data(self, response: httpx.Response) -> dict[str, Any]:
        if not response.is_success:
            raise GraphQLClientHttpError(
                status_code=response.status_code, response=response
            )

        response_json = self._get_response_json(response)
        return self._get_data_from_graphql_response(response_json, response)

    async def execute_ws(
        self,
        query: str,
        operation_name: Optional[str] = None,
        variables: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        headers = self.ws_headers.copy()
        headers.update(kwargs.pop("additional_headers", {}))

        merged_kwargs: dict[str, Any] = {"origin": self.ws_origin}
        merged_kwargs.update(kwargs)
        merged_kwargs["additional_headers"] = headers

        operation_id = str(uuid4())
        async with ws_connect(
            self.ws_url,
            subprotocols=[Subprotocol(GRAPHQL_TRANSPORT_WS)],
            **merged_kwargs,
        ) as websocket:
            await self._send_connection_init(websocket)
            # Wait for connection_ack; some servers (e.g. Hasura) send ping before
            # connection_ack, so we loop and handle pings until we get ack.
            try:
                await asyncio.wait_for(
                    self._wait_for_connection_ack(websocket),
                    timeout=5.0,
                )
            except asyncio.TimeoutError as exc:
                raise GraphQLClientError(
                    "Connection ack not received within 5 seconds"
                ) from exc
            await self._send_subscribe(
                websocket,
                operation_id=operation_id,
                query=query,
                operation_name=operation_name,
                variables=variables,
            )

            async for message in websocket:
                data = await self._handle_ws_message(message, websocket)
                if data and "connection_ack" not in data:
                    yield data

    def _process_variables(
        self, variables: Optional[dict[str, Any]]
    ) -> tuple[
        dict[str, Any], dict[str, tuple[str, IO[bytes], str]], dict[str, list[str]]
    ]:
        if not variables:
            return {}, {}, {}

        serializable_variables = self._convert_dict_to_json_serializable(variables)
        return self._get_files_from_variables(serializable_variables)

    def _convert_dict_to_json_serializable(
        self, dict_: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            key: self._convert_value(value)
            for key, value in dict_.items()
            if value is not UNSET
        }

    def _convert_value(self, value: Any) -> Any:
        if isinstance(value, BaseModel):
            return value.model_dump(by_alias=True, exclude_unset=True)
        if isinstance(value, list):
            return [self._convert_value(item) for item in value]
        return value

    def _get_files_from_variables(
        self, variables: dict[str, Any]
    ) -> tuple[
        dict[str, Any], dict[str, tuple[str, IO[bytes], str]], dict[str, list[str]]
    ]:
        files_map: dict[str, list[str]] = {}
        files_list: list[Upload] = []

        def separate_files(path: str, obj: Any) -> Any:
            if isinstance(obj, list):
                nulled_list = []
                for index, value in enumerate(obj):
                    value = separate_files(f"{path}.{index}", value)
                    nulled_list.append(value)
                return nulled_list

            if isinstance(obj, dict):
                nulled_dict = {}
                for key, value in obj.items():
                    value = separate_files(f"{path}.{key}", value)
                    nulled_dict[key] = value
                return nulled_dict

            if isinstance(obj, Upload):
                if obj in files_list:
                    file_index = files_list.index(obj)
                    files_map[str(file_index)].append(path)
                else:
                    file_index = len(files_list)
                    files_list.append(obj)
                    files_map[str(file_index)] = [path]
                return None

            return obj

        nulled_variables = separate_files("variables", variables)
        files: dict[str, tuple[str, IO[bytes], str]] = {
            str(i): (file_.filename, cast(IO[bytes], file_.content), file_.content_type)
            for i, file_ in enumerate(files_list)
        }
        return nulled_variables, files, files_map

    async def _execute_multipart(
        self,
        query: str,
        operation_name: Optional[str],
        variables: dict[str, Any],
        files: dict[str, tuple[str, IO[bytes], str]],
        files_map: dict[str, list[str]],
        **kwargs: Any,
    ) -> httpx.Response:
        data = {
            "operations": json.dumps(
                {
                    "query": query,
                    "operationName": operation_name,
                    "variables": variables,
                },
                default=to_jsonable_python,
            ),
            "map": json.dumps(files_map, default=to_jsonable_python),
        }

        return await self.http_client.post(
            url=self.url, data=data, files=files, **kwargs
        )

    async def _execute_multipart_incremental(
        self,
        query: str,
        operation_name: Optional[str],
        variables: dict[str, Any],
        files: dict[str, tuple[str, IO[bytes], str]],
        files_map: dict[str, list[str]],
        **kwargs: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        data = {
            "operations": json.dumps(
                {
                    "query": query,
                    "operationName": operation_name,
                    "variables": variables,
                },
                default=to_jsonable_python,
            ),
            "map": json.dumps(files_map, default=to_jsonable_python),
        }
        headers = self._get_incremental_headers(kwargs.get("headers", {}))

        merged_kwargs: dict[str, Any] = kwargs.copy()
        merged_kwargs["headers"] = headers

        async with self.http_client.stream(
            "POST",
            url=self.url,
            data=data,
            files=files,
            **merged_kwargs,
        ) as response:
            async for result in self._consume_incremental_response(response):
                yield result

    async def _execute_json(
        self,
        query: str,
        operation_name: Optional[str],
        variables: dict[str, Any],
        **kwargs: Any,
    ) -> httpx.Response:
        headers: dict[str, str] = {"Content-type": "application/json"}
        headers.update(kwargs.get("headers", {}))

        merged_kwargs: dict[str, Any] = kwargs.copy()
        merged_kwargs["headers"] = headers

        return await self.http_client.post(
            url=self.url,
            content=json.dumps(
                {
                    "query": query,
                    "operationName": operation_name,
                    "variables": variables,
                },
                default=to_jsonable_python,
            ),
            **merged_kwargs,
        )

    async def _execute_json_incremental(
        self,
        query: str,
        operation_name: Optional[str],
        variables: dict[str, Any],
        **kwargs: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        headers = self._get_incremental_headers(kwargs.get("headers", {}))
        headers["Content-type"] = "application/json"

        merged_kwargs: dict[str, Any] = kwargs.copy()
        merged_kwargs["headers"] = headers

        async with self.http_client.stream(
            "POST",
            url=self.url,
            content=json.dumps(
                {
                    "query": query,
                    "operationName": operation_name,
                    "variables": variables,
                },
                default=to_jsonable_python,
            ),
            **merged_kwargs,
        ) as response:
            async for result in self._consume_incremental_response(response):
                yield result

    def _get_incremental_headers(
        self, extra_headers: Optional[dict[str, str]] = None
    ) -> dict[str, str]:
        headers = {
            "Accept": "multipart/mixed; incrementalSpec=v0.2, application/json"
        }
        headers.update(extra_headers or {})
        return headers

    def _get_response_json(self, response: httpx.Response) -> dict[str, Any]:
        try:
            response_json = response.json()
        except ValueError as exc:
            raise GraphQLClientInvalidResponseError(response=response) from exc

        if not isinstance(response_json, dict):
            raise GraphQLClientInvalidResponseError(response=response)

        return cast(dict[str, Any], response_json)

    def _get_data_from_graphql_response(
        self, response_json: dict[str, Any], response: httpx.Response
    ) -> dict[str, Any]:
        if "data" not in response_json and "errors" not in response_json:
            raise GraphQLClientInvalidResponseError(response=response)

        data = response_json.get("data")
        errors = response_json.get("errors")

        if errors:
            raise GraphQLClientGraphQLMultiError.from_errors_dicts(
                errors_dicts=errors, data=data
            )

        if not isinstance(data, dict):
            raise GraphQLClientInvalidResponseError(response=response)

        return data

    async def _consume_incremental_response(
        self, response: httpx.Response
    ) -> AsyncIterator[dict[str, Any]]:
        if not response.is_success:
            raise GraphQLClientHttpError(
                status_code=response.status_code, response=response
            )

        content_type = response.headers.get("content-type", "")
        current_data: Optional[dict[str, Any]] = None

        if "multipart/mixed" in content_type.lower():
            boundary = self._get_multipart_boundary(content_type)
            if not boundary:
                raise GraphQLClientInvalidResponseError(response=response)

            async for payload in self._iter_multipart_payloads(response, boundary):
                current_data, snapshots = self._merge_incremental_payload(
                    current_data, payload, response
                )
                for snapshot in snapshots:
                    yield snapshot
            return

        body = await response.aread()
        try:
            response_json = json.loads(body)
        except ValueError as exc:
            raise GraphQLClientInvalidResponseError(response=response) from exc

        if not isinstance(response_json, dict):
            raise GraphQLClientInvalidResponseError(response=response)

        current_data, snapshots = self._merge_incremental_payload(
            current_data, response_json, response
        )
        for snapshot in snapshots:
            yield snapshot

    def _get_multipart_boundary(self, content_type: str) -> Optional[str]:
        for part in content_type.split(";"):
            part = part.strip()
            if part.startswith("boundary="):
                return part.split("=", 1)[1].strip('"')
        return None

    async def _iter_multipart_payloads(
        self, response: httpx.Response, boundary: str
    ) -> AsyncIterator[dict[str, Any]]:
        delimiter = f"--{boundary}".encode()
        closing_delimiter = f"--{boundary}--".encode()
        buffer = b""

        async for chunk in response.aiter_bytes():
            buffer += chunk
            async for payload in self._drain_multipart_buffer(
                response, buffer, delimiter, closing_delimiter
            ):
                buffer = payload[0]
                yield payload[1]

        async for payload in self._drain_multipart_buffer(
            response,
            buffer,
            delimiter,
            closing_delimiter,
            final=True,
        ):
            yield payload[1]

    async def _drain_multipart_buffer(
        self,
        response: httpx.Response,
        initial_buffer: bytes,
        delimiter: bytes,
        closing_delimiter: bytes,
        final: bool = False,
    ) -> AsyncIterator[tuple[bytes, dict[str, Any]]]:
        buffer = initial_buffer
        while True:
            start = buffer.find(delimiter)
            if start == -1:
                return

            if start > 0:
                buffer = buffer[start:]

            if buffer.startswith(closing_delimiter):
                return

            next_index = buffer.find(b"\r\n" + delimiter, len(delimiter))
            if next_index == -1:
                if not final:
                    return

                next_index = buffer.find(closing_delimiter, len(delimiter))
                if next_index == -1:
                    return

            part = buffer[len(delimiter) : next_index]
            parsed = self._parse_multipart_part(part, response)
            if parsed is not None:
                next_buffer = (
                    buffer[next_index + 2 :]
                    if buffer[next_index :].startswith(b"\r\n")
                    else buffer[next_index:]
                )
                yield next_buffer, parsed
                buffer = next_buffer
                continue

            buffer = (
                buffer[next_index + 2 :]
                if buffer[next_index :].startswith(b"\r\n")
                else buffer[next_index:]
            )

    def _parse_multipart_part(
        self, part: bytes, response: httpx.Response
    ) -> Optional[dict[str, Any]]:
        stripped_part = part.strip(b"\r\n")
        if not stripped_part:
            return None

        try:
            _headers, body = stripped_part.split(b"\r\n\r\n", 1)
        except ValueError as exc:
            raise GraphQLClientInvalidResponseError(response=response) from exc

        body = body.strip()
        if not body:
            return None

        try:
            payload = json.loads(body)
        except ValueError as exc:
            raise GraphQLClientInvalidResponseError(response=response) from exc

        if not isinstance(payload, dict):
            raise GraphQLClientInvalidResponseError(response=response)

        return cast(dict[str, Any], payload)

    def _merge_incremental_payload(
        self,
        current_data: Optional[dict[str, Any]],
        payload: dict[str, Any],
        response: httpx.Response,
    ) -> tuple[Optional[dict[str, Any]], list[dict[str, Any]]]:
        snapshots: list[dict[str, Any]] = []
        errors = payload.get("errors")
        if errors:
            raise GraphQLClientGraphQLMultiError.from_errors_dicts(
                errors_dicts=errors,
                data=current_data,
            )

        if "data" in payload:
            data = payload.get("data")
            if not isinstance(data, dict):
                raise GraphQLClientInvalidResponseError(response=response)
            current_data = deepcopy(data)
            snapshots.append(deepcopy(current_data))

        incrementals = payload.get("incremental")
        if incrementals is None:
            return current_data, snapshots

        if not isinstance(incrementals, list) or current_data is None:
            raise GraphQLClientInvalidResponseError(response=response)

        for item in incrementals:
            self._apply_incremental_item(current_data, item, response)
            snapshots.append(deepcopy(current_data))

        return current_data, snapshots

    def _apply_incremental_item(
        self,
        current_data: dict[str, Any],
        item: Any,
        response: httpx.Response,
    ) -> None:
        if not isinstance(item, dict):
            raise GraphQLClientInvalidResponseError(response=response)

        errors = item.get("errors")
        if errors:
            raise GraphQLClientGraphQLMultiError.from_errors_dicts(
                errors_dicts=errors,
                data=current_data,
            )

        path = item.get("path", [])
        if not isinstance(path, list):
            raise GraphQLClientInvalidResponseError(response=response)

        if "data" in item:
            self._apply_deferred_data_patch(
                current_data=current_data,
                path=path,
                patch=item.get("data"),
                response=response,
            )
            return

        if "items" in item:
            self._apply_stream_items_patch(
                current_data=current_data,
                path=path,
                items=item.get("items"),
                response=response,
            )

    def _apply_deferred_data_patch(
        self,
        current_data: dict[str, Any],
        path: list[Any],
        patch: Any,
        response: httpx.Response,
    ) -> None:
        if not isinstance(patch, dict):
            raise GraphQLClientInvalidResponseError(response=response)

        target: Any = self._resolve_incremental_path(current_data, path, response)
        if not isinstance(target, dict):
            raise GraphQLClientInvalidResponseError(response=response)

        target.update(patch)

    def _apply_stream_items_patch(
        self,
        current_data: dict[str, Any],
        path: list[Any],
        items: Any,
        response: httpx.Response,
    ) -> None:
        if not isinstance(items, list) or not path or not isinstance(path[-1], int):
            raise GraphQLClientInvalidResponseError(response=response)

        target = self._resolve_incremental_path(current_data, path[:-1], response)
        if not isinstance(target, list):
            raise GraphQLClientInvalidResponseError(response=response)

        start_index = path[-1]
        while len(target) < start_index:
            target.append(None)

        for offset, item in enumerate(items):
            index = start_index + offset
            if index < len(target):
                target[index] = item
            else:
                target.append(item)

    def _resolve_incremental_path(
        self,
        data: dict[str, Any],
        path: list[Any],
        response: httpx.Response,
    ) -> Any:
        target: Any = data
        for segment in path:
            if isinstance(segment, str):
                if not isinstance(target, dict) or segment not in target:
                    raise GraphQLClientInvalidResponseError(response=response)
                target = target[segment]
                continue

            if isinstance(segment, int):
                if (
                    not isinstance(target, list)
                    or segment < 0
                    or segment >= len(target)
                ):
                    raise GraphQLClientInvalidResponseError(response=response)
                target = target[segment]
                continue

            raise GraphQLClientInvalidResponseError(response=response)

        return target

    async def _send_connection_init(self, websocket: ClientConnection) -> None:
        payload: dict[str, Any] = {
            "type": GraphQLTransportWSMessageType.CONNECTION_INIT.value
        }
        if self.ws_connection_init_payload:
            payload["payload"] = self.ws_connection_init_payload
        await websocket.send(json.dumps(payload))

    async def _wait_for_connection_ack(self, websocket: ClientConnection) -> None:
        """Read messages until connection_ack; handle ping/pong in between."""
        async for message in websocket:
            data = await self._handle_ws_message(message, websocket)
            if data is not None and "connection_ack" in data:
                return

    async def _send_subscribe(
        self,
        websocket: ClientConnection,
        operation_id: str,
        query: str,
        operation_name: Optional[str] = None,
        variables: Optional[dict[str, Any]] = None,
    ) -> None:
        payload_inner: dict[str, Any] = {
            "query": query,
            "operationName": operation_name,
        }
        if variables:
            payload_inner["variables"] = self._convert_dict_to_json_serializable(
                variables
            )
        payload: dict[str, Any] = {
            "id": operation_id,
            "type": GraphQLTransportWSMessageType.SUBSCRIBE.value,
            "payload": payload_inner,
        }
        await websocket.send(json.dumps(payload))

    async def _handle_ws_message(
        self,
        message: Data,
        websocket: ClientConnection,
        expected_type: Optional[GraphQLTransportWSMessageType] = None,
    ) -> Optional[dict[str, Any]]:
        try:
            message_dict = json.loads(message)
        except json.JSONDecodeError as exc:
            raise GraphQLClientInvalidMessageFormat(message=message) from exc

        type_ = message_dict.get("type")
        payload = message_dict.get("payload", {})

        if not type_ or type_ not in {t.value for t in GraphQLTransportWSMessageType}:
            raise GraphQLClientInvalidMessageFormat(message=message)

        if expected_type and expected_type != type_:
            raise GraphQLClientInvalidMessageFormat(
                f"Invalid message received. Expected: {expected_type.value}"
            )

        if type_ == GraphQLTransportWSMessageType.NEXT:
            if "data" not in payload:
                raise GraphQLClientInvalidMessageFormat(message=message)
            return cast(dict[str, Any], payload["data"])

        if type_ == GraphQLTransportWSMessageType.COMPLETE:
            await websocket.close()
        elif type_ == GraphQLTransportWSMessageType.PING:
            await websocket.send(
                json.dumps({"type": GraphQLTransportWSMessageType.PONG.value})
            )
        elif type_ == GraphQLTransportWSMessageType.ERROR:
            raise GraphQLClientGraphQLMultiError.from_errors_dicts(
                errors_dicts=payload, data=message_dict
            )
        elif type_ == GraphQLTransportWSMessageType.CONNECTION_ACK:
            return {"connection_ack": True}

        return None
