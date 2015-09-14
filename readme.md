into CartoDB
===

module for inserting data into cartodb

```js
var writeStream = intoCartoDB(user, key, table, method, callback);
```

`user` and `key` are the cartodb credentials, `table` is the destination table, `method` is one of `'create'`, `'append'`, or `'replace'` (default create) which selects the table creation strategy and `callback` which is called when the data is fully inserted into cartodb.  Note that listening for the streams `finish` event is not sufficient for knowing that it is fully uploaded due to the stream being buffered internally, if the callback is omitted then one can listen for the `uploaded` event.  Additionally an `inserted` event is emitted which tells you when features are successfully inserted, the event object is an integer telling you how many.

Returned stream is an object stream which takes geojson features, geometry may be null.
