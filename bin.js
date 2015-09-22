#!/usr/bin/env node
'use strict';
var toGeojson = require('to-geojson-stream')();
var uploader = require('./');
require('colors');

var readline = require('readline');
var path = require('path');
var argv = toGeojson.args
  .usage('$0 [-f path/to/file.ext] [-n filename.ext] [-k key] [-u username] [-t tablename] [path/to/file.ext]')
  .describe('u', 'specify cartodb username'.yellow)
  .default('u', null, '$CARTODB_USER_NAME')
  .alias('k', 'key')
  .describe('k', 'specify cartodb api key'.yellow)
  .default('k', null, '$CARTODB_API_KEY')
  .alias('C', 'cleanup')
  .describe('C', 'clean up temp tables'.yellow)
  .alias('m', 'method')
  .default('m', 'create')
  .describe('m', 'choose import type'.yellow)
  .choices('m', ['create', 'append', 'replace'])
  .alias('r', 'replace')
  .describe('r', 'switch to replace mode'.yellow)
  .alias('a', 'append')
  .describe('a', 'switch to append mode'.yellow)
  .alias('c', 'create')
  .describe('c', 'switch to create mode'.yellow)
  .alias('t', 'table')
  .describe('t', 'tablename in cartodb'.yellow)
  .default('t', null, 'filename minus extention')
  .argv;

var key = argv.key;
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
if (!exit && argv.C) {
  return uploader.cleanUpTempTables(user, key).then(function (num) {
    if (num === 0) {
      process.stdout.write(`no tables to clean up\n`.green);
    } else {
      process.stdout.write(`cleaned up ${num} tables\n`.green);
    }
    process.exit(0);// eslint-disable-line no-process-exit
  }).catch(function (e) {
    process.stdout.write((e && e.stack || e || 'fail').red);
    process.exit(49);// eslint-disable-line no-process-exit
  });
}

if (exit) {
  process.exit(exit);// eslint-disable-line no-process-exit
}
function getMethod() {
  if (argv.c) {
    return 'create';
  }
  if (argv.a) {
    return 'append';
  }
  if (argv.r) {
    return 'replace';
  }
  return argv.m;
}
var total = 0;
toGeojson.filename.then(function (filename) {
  var tablename = argv.t || path.basename(filename, path.extname(filename));
  toGeojson.stream().pipe(uploader(user, key, tablename, getMethod(), function (err) {
    if (err) {
      process.stdout.write((err.stack || err.toString()).red);
      process.stdout.write('\n');
      process.exit(8);// eslint-disable-line no-process-exit
    }
    process.stdout.write('\ndone'.green);
    process.stdout.write('\n');
    process.exit(0);// eslint-disable-line no-process-exit
  })).on('inserted', function (n) {
    total += n;
  });
}).catch(function (e) {
  process.stdout.write('\noh no!'.red);
  process.stdout.write('\n');
  process.stdout.write((e && e.stack || e).red);
  process.exit(1);// eslint-disable-line no-process-exit
});
function clearLine() {
  readline.clearLine(process.stdout, 0);
  readline.cursorTo(process.stdout, 0);
}

// based upon https://github.com/helloIAmPau/node-spinner
var spinner = '⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏';
var stage = 0;
function updateCli() {
  clearLine();
  process.stdout.write('inserted ' + total + ' ' + spinner[stage] + '   ');
  stage++;
  stage %= spinner.length;
}
setInterval(updateCli, 60).unref();
