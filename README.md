A Distrubuted Key Value Store using in-memory hash table and raft

## HTTP API

### Set a key

```
POST /key
Content-Type: application/json

{"key": "foo", "value": "bar"}
```

Response: `201 Created`

```sh
curl -X POST http://localhost:8080/key \
  -d '{"key":"foo","value":"bar"}'
```

### Get a key

```
GET /key/{key}
```

Response: `200 OK`

```json
{"key": "foo", "value": "bar"}
```

`404 Not Found` if the key does not exist.

```sh
curl http://localhost:8080/key/foo
```

### Delete a key

```
DELETE /key/{key}
```

Response: `200 OK`

```sh
curl -X DELETE http://localhost:8080/key/foo
```

### Health check

```
GET /health
```

Response: `200 OK` with body `healthy`.
