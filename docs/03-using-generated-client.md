---
title: Using generated client
---

# Using generated client

Generated client can be imported from package:

```py
from {target_package_name}.{client_file_name} import {client_name}
```

Example with default settings:

```py
from graphql_client.client import Client
```

### Passing headers to client

Client (with default base client), takes passed headers and attaches them to every sent request.

```py
client = Client("https://example.com/graphql", {"Authorization": "Bearer token"})
```

For more complex scenarios, you can pass your own http client:

```py
client = Client(http_client=CustomComplexHttpClient())
```

`CustomComplexHttpClient` needs to be an instance of `httpx.AsyncClient` for async client, or `httpx.Client` for sync.

### Incremental delivery directives

You can use `@defer` and `@stream` directly in your `.graphql` operations.

For generated async clients, operations that use incremental delivery directives are exposed as async iterators. Each yielded value is a typed snapshot of the accumulated result after applying the latest patch.

Example:

```graphql
query GetProducts {
  products {
    id
    ...ProductDetails @defer
    reviews @stream(initialCount: 2) {
      id
      text
    }
  }
}

fragment ProductDetails on Product {
  description
  attributes {
    name
    value
  }
}
```

Generated async client usage:

```py
async for result in client.get_products():
		print(result)
```

The first yielded value is built from the initial payload. Later yields contain the same typed model after deferred fields or streamed items have been merged into the accumulated result.

Current support is split into two parts:

- Validation and code generation accept `@defer` and `@stream` even when the schema itself does not declare those directives.
- Generated result models treat deferred fields as optional, because they may be absent from the initial payload.
- Generated async client methods for operations that contain `@defer` or `@stream` return `AsyncIterator[ResultType]` instead of a single `ResultType`.
- The default async base clients can consume regular JSON responses and incremental `multipart/mixed` GraphQL responses.

Current limitation:

- Sync generated clients still expect a regular GraphQL JSON response with a top-level `data` or `errors` key. Incremental transport support is currently implemented for async generated clients only.

In practice this means:

- `@defer` changes generated typing and changes the async client method into an async iterator.
- `@stream` preserves the GraphQL field type, but async clients now merge streamed list items into later yielded snapshots.
- If a server answers with a normal JSON response instead of incremental multipart responses, the async iterator still yields exactly one typed result.
- If you use the sync client variant, you still need a custom base client or transport to consume incremental delivery at runtime.
