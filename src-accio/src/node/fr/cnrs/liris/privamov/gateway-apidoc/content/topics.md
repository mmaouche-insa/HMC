## Authentication



## Pagination

For methods supporting pagination, we support cursor-based pagination, via the `start` and `limit` parameters. A default limit on the number of objects is usually imposed implicitly, depending on the endpoint being considered.

| Query parameter | Description |
| --------------- | ----------- |
| `limit` | a limit on the number of objects to retrieve |
| `start` | an object identifier after which to start including results (exclusive), in chronological order |