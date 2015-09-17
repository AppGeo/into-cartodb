'use strict';
var once = require('once');
var cartodb = require('cartodb-tools');
var uploader = require('cartodb-uploader');
var Bluebird = require('bluebird');
var FirstN = require('first-n-stream');
var Transform = require('readable-stream').Transform;
var uuid = require('node-uuid');
var inherits = require('inherits');
module.exports = intoCartoDB;
function append(db, table, toUser, cb) {
  return db.createWriteStream(table, {
    batchSize: 50
  })
    .once('error', cb)
    .once('uploaded', cb)
    .on('inserted', function (num) {
      toUser.emit('inserted', num);
    });
}
var createTemptTable = Bluebird.coroutine(function * createTemptTable(table, db){
  var id = `${table.slice(0, 21)}_temp_${uuid().replace(/-/g, '_')}`;
  yield db.raw(`
      create table ${id} as table ${table} with no data;
  `);
  return id;
});
var cleanUpTempTables = Bluebird.coroutine(function * cleanUp(user, key) {
  var db = cartodb(user, key);
  var done = 0;
  var tables = yield db(db.raw('INFORMATION_SCHEMA.tables')).select('table_name')
 .where('table_name', 'like', `%\_temp\_________\_____\_____\_____\_____________`)
 .groupBy('table_name');
  if (!tables.length) {
    return 0;
  }
  tables = tables.map(function (item) {
    return item.table_name;
  });
  for (let name of tables) {
    yield db.schema.dropTableIfExists(name);
    ++done;
    console.log(done, name);
  }
  return done;
});
module.exports.cleanUpTempTables = cleanUpTempTables;
var swap = Bluebird.coroutine(function * swap(table, tempTable, remove, db) {
  let fields = yield db(db.raw('INFORMATION_SCHEMA.COLUMNS')).select('column_name')
  .where({
    table_name: tempTable // eslint-disable-line camelcase
  })
  .whereNotIn('column_name', ['cartodb_id', 'the_geom_webmercator', 'created_at', 'updated_at']);
  fields = fields.map(function (item) {
    return item.column_name;
  }).join();
  return db.raw(`
    BEGIN;
      ${remove ? '' : `DELETE from ${table}`};
      INSERT into ${table} (${fields}) SELECT ${fields} from ${tempTable};
      DROP TABLE ${tempTable};
    COMMIT;
  `);
});

/*
{
  style: create|replace|append
}
*/
function exists(name, db) {
  return db(db.raw('information_schema.tables')).count('table_name').where('table_name', name).then(function (resp) {
    if (resp.length !== 1) {
      throw new Error('invalid response');
    }
    if (typeof resp[0].count !== 'number') {
      throw new Error('invalid response');
    }
    return resp[0].count;
  });
}
function part2(db, table, origTable, remove, toUser, done) {
  return append(db, table, toUser, function (err) {
     if (err) {
       return done(err);
     }
     if (!origTable) {
       return done();
     }
     swap(origTable, table, remove, db).then(function () {
       done();
     }).catch(done);
   });
}

function intoCartoDB(user, key, table, method, done) {
  table = table.toLowerCase();
  if (typeof method === 'function') {
    done = method;
    method = 'create';
  }
  var toUser = new Transform({
    objectMode: true,
    transform: function (chunk, _, next) {
      var oldProps = chunk.properties;
      chunk.properties = {};
      Object.keys(oldProps).forEach(function (key) {
        chunk.properties[key.toLowerCase()] = oldProps[key];
      });
      this.push(chunk);
      next();
    }
  });
  var cb = once(function (err, resp) {
    if (err) {
      if (done) {
        return done(err);
      }
      return toUser.emit('error', err);
    }
    if (done) {
      done(null, resp);
    }
    toUser.emit('uploaded');
  });
  var db = cartodb(user, key);
  exists(table, db).then(function (count) {
    if (method === 'create') {
      if (count !== 0) {
        throw new Error('table already exists');
      }
      let out = new FirstN(50, function (err, resp) {
        if (err) {
          return cb(err);
        }
        var uploadStream = uploader.geojson({
          user: user,
          key: key
        }, table, function (err) {
          if (err) {
            return cb(err);
          }
          toUser.emit('inserted', resp.length);
          return createTemptTable(table, db).then(function (id) {
            out.pipe(part2(db, id, table, false, toUser, cb));
          });
        });
        resp.forEach(function (item) {
          uploadStream.write(item);
        });
        uploadStream.end();
      });
      toUser.pipe(out);
      return;
    }
    if (count !== 1) {
      throw new Error('table must exist');
    }
    if (method === 'append') {
      return createTemptTable(table, db).then(function (id) {
        toUser.pipe(part2(db, id, table, false, toUser, cb));
      });
    } else if (method === 'replace') {
      return createTemptTable(table, db).then(function (id) {
        toUser.pipe(new Validator(id, db)).pipe(part2(db, id, table, true, toUser, cb));
      });
    }
  }).catch(cb);
  return toUser;
}
inherits(Validator, Transform);
function Validator(id, db) {
  Transform.call(this, {
    objectMode: true
  });
  this.schema = db(db.raw('INFORMATION_SCHEMA.COLUMNS')).select('column_name', 'data_type')
  .where('table_name', id )
  .whereNotIn('column_name', ['cartodb_id', 'the_geom_webmercator', 'created_at', 'updated_at', 'the_geom']).then(function (data) {
    var out = new Map();
    data.forEach(function (item) {
      out.set(item.column_name, item.data_type);
    });
    return out;
  });
  this.nope = Symbol('nope');
}
Validator.prototype._transform = function (chunk, _, next) {
  var self = this;
  var props = chunk.properties;
  chunk.properties = {};
  this.schema.then(function (schema) {
    Object.keys(props).forEach(function (key) {
      if (!schema.has(key)) {
        return;
      }
      var typed = self.coerceType(props[key], schema.get(key));
      if (typed === self.nope) {
        return;
      }
      chunk.properties[key] = typed;
    });
    self.push(chunk);
    next();
  }).catch(next);
};

Validator.prototype.coerceType = function (value, type) {
  if (typeof value === 'undefined') {
    return this.nope;
  }
  var out;
  switch(type) {
    case 'text':
      out = String(value);
      if (!out) {
        return this.nope;
      }
      return out;
    case 'double precision':
      out = parseFloat(value);
      if (out !== out) {
        return this.nope;
      }
      return out;
    case 'integer':
    case 'bigint':
      out = parseInt(value, 10);
      if (out !== out) {
        return this.nope;
      }
      return out;
    case 'timestamp with time zone':
      out = new Date(value);
      if (out.toString() === 'Invalid Date') {
        return this.nope;
      }
      return out;
    case 'boolean':
      if (value === 'NULL') {
        return this.nope;
      }
      return Boolean(value);
    default:
      return this.nope;
  }
};
