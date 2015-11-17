'use strict';
var once = require('once');
var cartodb = require('cartodb-tools');
var uploader = require('cartodb-uploader');
var Bluebird = require('bluebird');
var FirstN = require('first-n-stream');
var Transform = require('readable-stream').Transform;
var uuid = require('node-uuid');
var inherits = require('inherits');
var debug = require('debug')('into-cartodb');
var sanatize = require('./sanatize');
var validate = require('./validations');
var escape = require('pg-escape');
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
  yield db.raw(escape(`
      create table %I as table %I with no data;
  `, id, table));
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
  }
  return done;
});


module.exports.cleanUpTempTables = cleanUpTempTables;

var swap = Bluebird.coroutine(function * swap(table, tempTable, remove, db, config) {
  let fields = yield db(db.raw('INFORMATION_SCHEMA.COLUMNS')).select('column_name')
  .where({
    table_name: tempTable // eslint-disable-line camelcase
  })
  .whereNotIn('column_name', ['cartodb_id', 'the_geom_webmercator', 'created_at', 'updated_at', 'the_geom']);
  fields = fields.map(function (item) {
    return item.column_name;
  });
  var newFields, out;
  var group = new Set();
  try {
    newFields = yield validate(tempTable, fields, config, db, group);
  } catch(e) {
    debug(e);
    if (config.method === 'create') {
      out = db.raw(escape(`DROP TABLE %I, %I;`, table, tempTable));
    } else {
      out = db.raw(escape(`DROP TABLE %I;`, tempTable));
    }
    return out.then(function () {
      return Promise.reject(e || new Error('validation failed'));
    });
  }
  var fromFields = [];
  var toFields = [];
  newFields.forEach(function (value, key) {
    fromFields.push(value);
    toFields.push(key);
  });
  var groupFields = [];
  group.forEach(function (field) {
    groupFields.push(field);
  });
  return db.raw(escape(`
    BEGIN;
      ${remove ? escape(`DELETE from %I`, table) : ''};
      INSERT into %I (${toFields.join(',')})
      SELECT DISTINCT ${fromFields.join(',')} from %I
      ${groupFields.length ? `group by ${groupFields.join(',')}` : ''};
      DROP TABLE %I;
    COMMIT;
  `, table, tempTable, tempTable));
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
function part2(db, table, origTable, remove, toUser, config, done) {
  return append(db, table, toUser, function (err) {
     if (err) {
       return done(err);
     }
     if (!origTable) {
       return done();
     }
     swap(origTable, table, remove, db, config).then(function () {
       done();
     }).catch(done);
   });
}

function intoCartoDB(user, key, table, options, done) {
  table = sanatize(table).slice(0, 63);
  if (typeof options === 'function') {
    done = options;
    options = {};
  }
  if (typeof options === 'string') {
    options = {
      method: options
    };
  }
  options = options || {};
  options.method = options.method || 'create';
  var method = options.method;
  var toUser = new Transform({
    objectMode: true,
    transform: function (chunk, _, next) {
      var oldProps = chunk.properties;
      chunk.properties = {};
      Object.keys(oldProps).forEach(function (key) {
        chunk.properties[sanatize(key)] = oldProps[key];
      });
      this.push(chunk);
      next();
    }
  });
  function warning(msg) {
    toUser.emit('warning', msg);
  }
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
      let out = new FirstN(100, function (err, resp) {
        if (err) {
          return cb(err);
        }
        var uploadStream = uploader.geojson({
          user: user,
          key: key
        }, table, function (err, r) {
          if (err) {
            return cb(err);
          }
          if (r.table_name !== table) {
            return cb(new Error('table not created successfully'));
          }
          return createTemptTable(table, db).then(function (id) {
            var nextPart = part2(db, id, table, true, toUser, options, cb);
            resp.forEach(function (item) {
              nextPart.write(item);
            });
            out.pipe(new Validator(id, db, warning)).pipe(nextPart);
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
        toUser.pipe(new Validator(id, db, warning)).pipe(part2(db, id, table, false, toUser, options, cb));
      });
    } else if (method === 'replace') {
      return createTemptTable(table, db).then(function (id) {
        toUser.pipe(new Validator(id, db, warning)).pipe(part2(db, id, table, true, toUser, options, cb));
      });
    }
  }).catch(cb);
  return toUser;
}
inherits(Validator, Transform);
function Validator(id, db, warning) {
  Transform.call(this, {
    objectMode: true
  });
  this.warning = warning;
  this.extraKeys = new Set();
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
Validator.prototype.keyWarning = function (key) {
  if (this.extraKeys.has(key)) {
    return;
  }
  this.extraKeys.add(key);
  this.warning(`An unexpected field "${key}" was encountered. This field was not uploaded`);
};
Validator.prototype._transform = function (chunk, _, next) {
  var self = this;
  var props = chunk.properties;
  chunk.properties = {};
  this.schema.then(function (schema) {
    Object.keys(props).forEach(function (key) {
      if (!schema.has(key)) {
        self.keyWarning(key);
        return;
      }
      var typed = self.coerceType(props[key], schema.get(key));
      if (typed === self.nope) {
        return;
      }
      chunk.properties[key] = typed;
    });
    if (chunk.geometry || Object.keys(chunk.properties).length) {
      self.push(chunk);
    }
    next();
  }).catch(next);
};

Validator.prototype.coerceType = function (value, type) {
  if (typeof value === 'undefined' || value === null) {
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
      out = parseInt(value, 10);
      if (out !== out) {
        return this.nope;
      }
      if (out > 2147483647 || out < -2147483648 ) {
        return this.nope;
      }
      return out;
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
