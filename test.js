'use strict';
var test = require('tape');
var intoCartodb = require('./');
var auth = require('./auth.json');
var cartodb = require('cartodb-tools')(auth.user, auth.key);

test('crud', function (t) {
  var table = 'test_table_into_carto';
  t.test('maybe delete', function (t) {
    t.plan(1);
    cartodb.schema.dropTableIfExists(table).exec(function (err) {
      t.error(err, 'no error');
    });
  });
  t.test('append to nonexistant table', function (t) {
    t.plan(1);
    intoCartodb(auth.user, auth.key, table, 'append', function (err) {
      t.ok(err, 'should error');
    });
  });
  t.test('replace to nonexistant table', function (t) {
    t.plan(1);
    intoCartodb(auth.user, auth.key, table, 'replace', function (err) {
      t.ok(err, 'should error');
    });
  });
  t.test('create', function (t) {
    var inserted = 0;
    var stream = intoCartodb(auth.user, auth.key, table, function (err) {
      t.error(err, 'no error');
      t.equals(inserted, 160);
      t.end();
    });
    stream.on('inserted', function (num) {
      t.ok(true, 'inserted');
      inserted += num;
    });
    var i = -1;
    while (++i < 160) {
      stream.write({
        type: 'Feature',
        properties: {
          num: i
        },
        geometry: null
      });
    }
    stream.end();
  });
  t.test('create again', function (t) {
    t.plan(1);
    intoCartodb(auth.user, auth.key, table, function (err) {
      t.ok(err, 'should error');
    });
  });
  t.test('correct ammount', function (t) {
    t.plan(2);
    cartodb(table).count('num').exec(function (err, resp) {
      t.error(err);
      t.deepEquals(resp, [{count: 160}]);
    });
  });
  t.test('append', function (t) {
    var inserted = 0;
    var stream = intoCartodb(auth.user, auth.key, table, 'append', function (err) {
      t.error(err, 'no error');
      t.equals(inserted, 230);
      t.end();
    });
    stream.on('inserted', function (num) {
      t.ok(true, 'inserted');
      inserted += num;
    });
    var i = -1;
    while (++i < 230) {
      stream.write({
        type: 'Feature',
        properties: {
          num: i
        },
        geometry: null
      });
    }
    stream.end();
  });
  t.test('correct ammount round 2', function (t) {
    t.plan(2);
    cartodb(table).count('num').exec(function (err, resp) {
      t.error(err);
      t.deepEquals(resp, [{count: 390}]);
    });
  });
  t.test('replace', function (t) {
    var inserted = 0;
    t.plan(2);
    var stream = intoCartodb(auth.user, auth.key, table, 'replace', function (err) {
      t.error(err, 'no error');
      t.equals(inserted, 75);
    });
    stream.on('inserted', function (num) {
      inserted += num;
    });
    var i = -1;
    while (++i < 75) {
      stream.write({
        type: 'Feature',
        properties: {
          num: i
        },
        geometry: null
      });
    }
    stream.end();
  });
  t.test('correct ammount round 3', function (t) {
    t.plan(2);
    cartodb(table).count('num').exec(function (err, resp) {
      t.error(err);
      t.deepEquals(resp, [{count: 75}]);
    });
  });
  t.test('maybe delete again', function (t) {
    t.plan(1);
    cartodb.schema.dropTableIfExists(table).exec(function (err) {
      t.error(err, 'no error');
    });
  });
  t.test('create less then 50', function (t) {
    var inserted = 0;
    t.plan(2);
    var stream = intoCartodb(auth.user, auth.key, table, function (err) {
      t.error(err, 'no error');
      t.equals(inserted, 40);
    });
    stream.on('inserted', function (num) {
      inserted += num;
    });
    var i = -1;
    while (++i < 40) {
      stream.write({
        type: 'Feature',
        properties: {
          num: i
        },
        geometry: null
      });
    }
    stream.end();
  });
});
