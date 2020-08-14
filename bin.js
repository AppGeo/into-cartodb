#!/usr/bin/env node
'use strict';
var toGeojson = require('to-geojson-stream')();
var uploader = require('./');
require('colors');
function handleError(e) {
  if (!e) {
    e = new Error('unknown error');
  }
  if (e.stack) {
    process.stdout.write((e.stack).red);
  } else if (typeof e === 'object') {
    process.stdout.write(JSON.stringify(e, false, 2).red);
  } else if (typeof e === 'string') {
    process.stdout.write(e.red);
  } else {
    process.stdout.write(e.toString().red);
  }
  process.stdout.write('\n');
}
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
  .alias('p', 'copy')
  .describe('p', 'use the copy api'.yellow)
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
  .alias('v', 'version')
  .describe('v', 'print version then exit'.yellow)
  .alias('b', 'batchsize')
  .describe('b', 'set the batch size'.yellow)
  .default('b', 200)
  .alias('d', 'direct')
  .describe('d', 'upload directly to the table (create/append only)'.yellow)
  .alias('s', 'subdomainless')
  .boolean('s')
  .default('s', undefined)
  .describe('s', 'whether to use subdomainless url mode'.yellow)
  .alias('D', 'domain')
  .describe('D', 'whether to use the non default domain'.yellow)
  .argv;

if (argv.version) {
  process.stdout.write(`v${require('./package.json').version}`.green);
  process.stdout.write('\n');
  process.exit(0);// eslint-disable-line no-process-exit
}

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
if (typeof user === 'string' && user.indexOf('@') > -1) {
  process.stdout.write('please provide account name, you provided login email'.red);
  process.stdout.write('\n');
  exit += 4;
}
if (!exit && argv.C) {
  return uploader.cleanUpTempTables(user, key, {
    subdomainless: argv.s,
    domain: argv.D
  }).then(function (num) {
    if (num === 0) {
      process.stdout.write('no tables to clean up\n'.green);
    } else {
      process.stdout.write(`cleaned up ${num} tables\n`.green);
    }
    process.exit(0);// eslint-disable-line no-process-exit
  }).catch(function (e) {
    process.stdout.write('error cleaning up temp tables\n'.red);
    handleError(e);
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
  toGeojson.stream().pipe(uploader(user, key, tablename, {
    method: getMethod(),
    batchSize: parseInt(argv.b, 10),
    direct: argv.d,
    copy: argv.p,
    subdomainless: argv.s,
    domain: argv.D
  }, function (err) {
    if (err) {
      handleError(err);
      process.exit(8);// eslint-disable-line no-process-exit
    }
    updateCli();
    process.stdout.write('\ndone'.green);
    process.stdout.write('\n');
    process.exit(0);// eslint-disable-line no-process-exit
  })).on('inserted', function (n) {
    total += n;
  });
}).catch(function (e) {
  process.stdout.write('\noh no!'.red);
  process.stdout.write('\n');
  handleError(e);
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
  process.stdout.write('uploaded ' + total + ' ' + spinner[stage] + '   ');
  stage++;
  stage %= spinner.length;
}
setInterval(updateCli, 60).unref();
