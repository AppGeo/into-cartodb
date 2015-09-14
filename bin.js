#!/usr/bin/env node
'use strict';
require('colors');
var fs = require('fs');
var path = require('path');
var uploader = require('./');
var JsonStream = require('jsonstream3');
var csv = require('csv-parser');
var Transform = require('readable-stream').Transform;
var shapefile = require('shapefile').reader;
var Proj4Geojson = require('proj4geojson');
var argv = require('yargs')
  .usage('$0 [-f path/to/file.ext] [-n filename.ext] [-a apikey] [-u username] [-t tablename] [path/to/file.ext]')
  .alias('f', 'file')
  .describe('f', 'specify file to upload, -f flag is optional'.yellow)
  .alias('n', 'name')
  .describe('n', 'upload from stdin as file'.yellow).alias('u', 'user')
  .describe('u', 'specify cartodb username'.yellow)
  .default('u', null, '$CARTODB_USER_NAME')
  .alias('a', 'apikey')
  .describe('a', 'specify cartodb apikey'.yellow)
  .default('a', null, '$CARTODB_API_KEY')
  .alias('m', 'method')
  .default('m', 'create')
  .describe('m', 'choose import type'.yellow)
  .choices('m', ['create', 'append', 'replace'])
  .alias('t', 'table')
  .describe('t', 'tablename in cartodb'.yellow)
  .default('t', null, 'filename minus extention')
  .help('h', 'Show Help'.yellow)
  .alias('h', 'help')
  .argv;

var key = argv.apikey;
if (key === null) {
  key = process.env.CARTODB_API_KEY;
}
var user = argv.user;
if (user === null) {
  user = process.env.CARTODB_USER_NAME;
}
var exit = 0;
if (!key) {
  process.stdout.write('api key is required, please pass the -a option or set CARTODB_API_KEY'.red);
  process.stdout.write('\n');
  exit += 1;
}

if (!user) {
  process.stdout.write('username is required, please pass the -u option or set CARTODB_USER_NAME'.red);
  process.stdout.write('\n');
  exit += 2;
}
if (!argv.f && !argv.n && !argv._[0]) {
  process.stdout.write('name or file is required'.red);
  process.stdout.write('\n');
  exit += 4;
}

if (exit) {
  process.exit(exit);// eslint-disable-line no-process-exit
}
var fileName = argv.f || argv._[0];
var instream, name;
if (fileName) {
  fileName = path.resolve(fileName);
  instream = fs.createReadStream(fileName);
  name = path.basename(fileName);
}

if (argv.n) {
  name = argv.n;
  if (!instream) {
    if (path.extname(name) === '.shp') {
      console.log('must use full path with .shp'.red);
      process.exit(12);// eslint-disable-line no-process-exit
    }
    instream = process.stdin;
  }
}
var tablename = argv.t || path.basename(name, path.extname(name));
var middleStream = getMiddleStream(fileName);
function toGeoJson() {
  return new Transform({
    objectMode: true,
    transform: function (chunk, _, next) {
      this.push({
        type: 'Feature',
        properties: chunk,
        geometry: null
      });
      next();
    }
  });
}
function getMiddleStream(name) {
  var ext = path.extname(name);
  switch(ext) {
    case '.geojson':
      return instream.pipe(JsonStream.parse('features.*'));
    case '.csv':
      return instream.pipe(csv()).pipe(toGeoJson());
    case '.json':
      return instream.pipe(JsonStream.parse('*')).pipe(toGeoJson());
    case '.shp':
      var dbf = path.join(path.dirname(name), path.basename(name, '.shp') + '.dbf');
      return shapefile({
        shp: instream,
        dbf: dbf
      }).createReadStream().pipe(transformStream(path.join(path.dirname(name), path.basename(name, '.shp') + '.prj')));
    default:
      console.log(('unknown file type: ' + ext).red);
      process.exit(9);// eslint-disable-line no-process-exit
  }
}
function makeObject(path) {
  return new Promise(function (yes) {
    fs.readFile(path, {encoding: 'utf8'}, function (err, file) {
      if (err) {
        return yes({
          feature: function (thing) {
            return thing;
          }
        });
      }
      yes(new Proj4Geojson(file, true));
    });
  });
}
function transformStream (path) {
  var obj = makeObject(path);
  return new Transform({
    objectMode: true,
    transform: function (chunk, _, next) {
      var self = this;
      obj.then(function (transformer) {
        self.push(transformer.feature(chunk));
        next();
      });
    }
  });
}
middleStream.pipe(uploader(user, key, tablename, argv.m, function (err) {
  if (err) {
    console.log((err.stack || err.toString()).red);
    process.exit(8);// eslint-disable-line no-process-exit
  }
  console.log('done'.green);
  process.exit(0);// eslint-disable-line no-process-exit
})).on('inserted', function (n) {
  console.log('inserted', n);
});
