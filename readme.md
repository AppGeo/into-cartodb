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
into-cartodb -k API_KEY -u USER_NAME -f path/to/file.ext
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

Create mode throws an error if the table already exists in cartodb, replace and append modes throw an error if the table does not exist yet, replace won't leave your table in an inconsistent state if it fails (i.g. you stop it) but it will leave an extra table in your account.

Supported formats are

- geojson
- csv
- json
- .shp
- .kml

Caveats:

- .shp can't be streamed in from stdin, it must be from the file system and it must have a .dbf in the same folder, if it isn't in unprojected WGS84 the prj file must also be in the same folder.
- the only geometry supported by .csv and .json are points encoded in fields named x and y or lat and lon (or lng). These must be WGS84 lat lons (even for x y).
- .json must have an array of objects as the top level element (aka not the same as geojson).
- shapefile must not be zipped.
- .kml, no .kmz, no styles, and only extended data

# programic api

```js
var writeStream = intoCartoDB(user, key, table, method, callback);
```

`user` and `key` are the cartodb credentials, `table` is the destination table, `method` is one of `'create'`, `'append'`, or `'replace'` (default create) which selects the table creation strategy and `callback` which is called when the data is fully inserted into cartodb.  Note that listening for the streams `finish` event is not sufficient for knowing that it is fully uploaded due to the stream being buffered internally, if the callback is omitted then one can listen for the `uploaded` event.  Additionally an `inserted` event is emitted which tells you when features are successfully inserted, the event object is an integer telling you how many.

Returned stream is an object stream which takes geojson features, geometry may be null.
