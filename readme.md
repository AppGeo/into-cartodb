into CartoDB
===

module for inserting data into cartodb

```bash
# for use in a node.js package
npm install into-cartodb
# for use on the command line
npm install into-cartodb -g
```

# commmand line api

at it's simplest you can just point it to a file with the `-f` option and it will upload it to the table in cartodb that is the same as the file name minus the extension.

```bash
into-cartodb -k API_KEY -u USER_NAME -f path/to/file.ext -b 200
```

If you set the CARTODB_USER_NAME and CARTODB_API_KEY environmental variables you may omit the `-k` and `-u` options.

```bash
export CARTODB_USER_NAME=USER_NAME
export CARTODB_API_KEY=API_KEY
into-cartodb -f path/to/file.ext
```

if you need to import to a table that is named differently then the file use the `-t` option

```bash
into-cartodb -f path/to/file.ext -t table_name
```

if you want to stream from stdin you still need to tell us what kind of file it is and you can use the `-n` option to do so

```bash
getfile | into-cartodb -n file.ext
```

if you don't specify the table name then we default to the filename you said otherwise we only care about the extension, so the following are equivalent

```bash
getfile | into-cartodb -n tablename.ext
getfile | into-cartodb -n nobody_cares.ext -t tablename
```

By default we create a new table you can either use the --method (-m) flag to specify that you want to append or replace or use the --append (-a) or --replace (-r) flags.


```bash
# all three are equivalent
into-cartodb -f path/to/file.ext -m append
into-cartodb -f path/to/file.ext --append
into-cartodb -f path/to/file.ext -a
# all three are equivalent
into-cartodb -f path/to/file.ext -m replace
into-cartodb -f path/to/file.ext --replace
into-cartodb -f path/to/file.ext -r
# all four are equivalent
into-cartodb -f path/to/file.ext
into-cartodb -f path/to/file.ext -m create
into-cartodb -f path/to/file.ext --create
into-cartodb -f path/to/file.ext -c
```

Create mode throws an error if the table already exists in cartodb, replace and append modes throw an error if the table does not exist yet.

By default data is pushed into cartodb in batches of 200, you may use the `-b` argument to decrease the batch size if you are running out of memory or increase it if the upload is taking too long.

Supported formats are

- .geojson
- .csv
- .json
- .shp
- .kml
- .kmz
- .zip

Caveats:

- .shp can't be streamed in from stdin, it must be from the file system and it must have a .dbf in the same folder, if it isn't in unprojected WGS84 the prj file must also be in the same folder.
- the only geometry supported by .csv and .json are points encoded in fields named x and y or lat and lon (or lng). These must be WGS84 lat lons (even for x y).
- .json must have an array of objects as the top level element (aka not the same as geojson).
- shapefile must not be zipped.
- .kml, no styles, and only extended data
- .kmz, same as .kml but additionally like .shp must come from file system not stdin
- a .zip must be a path on the filesystem (no stdin) and may contain any other format (except .kmz), if the zip has more then one valid file then use the -n parameter to specify which one otherwise it'll pick the first one it can find.

This tool uploads to a temp table and then inserts them into the table after all rows have been uploaded, this is MUCH faster then importing directly to the table but if the upload doesn't finish can lead to a two minor issues.

1. orphan and invisible temp tables hanging around, you can delete these by using the `-C` (aka `-cleanup`) option from the command line which will drop all these.
2. a failed creation of a new table will leave a rump table of only 50 rows in cartodb, you'll need to delete that manually.

# programic api

```js
var writeStream = intoCartoDB(user, key, table, options, callback);
```

`user` and `key` are the cartodb credentials, `table` is the destination table, `options` is an object with various config options, if it's a string it's assumed to be method which is one of `'create'`, `'append'`, or `'replace'` (default create) which selects the table creation strategy and `callback` which is called when the data is fully inserted into cartodb.  Note that listening for the streams `finish` event is not sufficient for knowing that it is fully uploaded due to the stream being buffered internally, if the callback is omitted then one can listen for the `uploaded` event.  Additionally an `inserted` event is emitted which tells you when features are successfully inserted, the event object is an integer telling you how many.

Returned stream is an object stream which takes geojson features, geometry may be null.

Other options besides method which are supported include

- validations: an array of Promise returning functions, called in order with 3 arguments
    - the name of the table in cartodb
    - a map where each entries key is the field name to be inserted into the main table and the value is the value to get it out of the temp table
    - a [cartodb-tools](https://github.com/calvinmetcalf/cartodb) database object (same api as [knex]() minus transactions)
- batchSize: number of features to insert into cartodb at a time, defaults to 200, decrease if you are running out of memory.

  See bellow for more info.


Validations
---

If the promise rejects that the temporary table is cleaned up (and the stub table is cleaned up for create operations).  The field map works by the key being the name of the field to insert into the table and the value being the expressions in sql.  For instance usually the geometry value is the same as the key `the_geom` but if the geometry needs to be fixed it is instead `ST_MakeValid(the_geom) as the_geom`.

One validation is included by default and it is used to

- Transfer the geometry field over if any non null geometries
- check if any of the geometries are invalid and run `ST_MakeValid` on them if soo, the source of that function is

```js
var fixGeom = Bluebird.coroutine(function * fixGeom(table, fields, db) {
  var hasGeom = yield db(table).select(db.raw('bool_or(the_geom is not null) as hasgeom'));
  hasGeom = hasGeom.length === 1 && hasGeom[0].hasgeom;
  if (hasGeom) {
    debug('has geometry');
    let allValid = yield db(table).select(db.raw('bool_and(st_isvalid(the_geom)) as allvalid'));
    allValid = allValid.length === 1 && allValid[0].allvalid;
    if (allValid) {
      debug('geometry is all valid');
      fields.set('the_geom', 'the_geom');
    } else {
      debug('has invalid geometry');
      fields.set('the_geom', 'ST_MakeValid(the_geom) as the_geom');
    }
  }
});
```
