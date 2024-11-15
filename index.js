'use strict';
var once = require('once');
var cartodb = require('cartodb-tools');
var uploader = require('cartodb-uploader');
var Bluebird = require('bluebird');
var FirstN = require('first-n-stream');
var uuid = require('uuid');
var debug = require('debug')('into-cartodb');
var sanatize = require('./sanatize');
var validate = require('./validations');
var cartoCopyStream = require('carto-copy-stream');
var validator = require('./validator');
var escape = require('pg-escape');
var createOutput = require('./create-output');
module.exports = intoCartoDB;
function append(db, table, toUser, options, cb) {
  if (options.copy) {
    const cartoOpts = {
      domain: options.domain,
      subdomainless: options.subdomainless
    };
    return cartoCopyStream(options.user, options.key, table, options.fields, cartoOpts, function (err, resp) {
      if (err) {
        return cb(err);
      }
      toUser.emit('inserted', resp.total_rows);
      cb();
    });
  }
  return db.createWriteStream(table, {
    batchSize: options.batchSize
  })
    .once('error', cb)
    .once('uploaded', cb)
    .on('inserted', function (num) {
      toUser.emit('inserted', num);
    });
}

var createTemptTable = Bluebird.coroutine(function * createTemptTable(table, db){
  var id = `${table.slice(0, 21)}_temp_${uuid().replace(/-/g, '_')}`;
  yield db.raw('create table ?? as table ?? with no data;', [id, table]);
  return id;
});
var cleanUpTempTables = Bluebird.coroutine(function * cleanUp(user, key, opts) {
  var db = cartodb(user, key, opts);
  var done = 0;
  var tables = yield db(db.raw('INFORMATION_SCHEMA.tables')).select('table_name')
    .where('table_name', 'like', '%\_temp\_________\_____\_____\_____\_____________')
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
  var newFields, out;
  const fields = config.fields;
  var group = new Set();
  try {
    newFields = yield validate(tempTable, fields, config, db, group);
  } catch(e) {
    debug(e);
    if (config.method === 'create') {
      out = db.raw(escape('DROP TABLE %I, %I;', table, tempTable));
    } else {
      out = db.raw(escape('DROP TABLE %I;', tempTable));
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
  // Modify fromFields to handle "trim" formatting correctly
  var modifiedFromFields = fromFields.map(field => {
    if (typeof field === 'string') {
      return `"${field}"`;
    }
    if (typeof field.toSQL === 'function') {
      const raw =  field.toSQL();
      return raw && raw.sql;
    }
    
  });
  return db.raw(`
    ${remove ? escape('DELETE from %I', table) : ''};
    INSERT into ?? ("${toFields.join('","')}")
    SELECT ${modifiedFromFields.join(',')} from ??
    ${groupFields.length ? `group by "${groupFields.join('","')}"` : ''};
  `, [table, tempTable]).batch().onSuccess(db.raw('DROP TABLE ??;', [tempTable])).onError(db.raw('DROP TABLE ??;', [tempTable]));
});

/*
{
  style: create|replace|append
}
*/
function _exists(name, db) {
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
function makeSchema(data) {
  var out = new Map();
  data.forEach(function (item) {
    out.set(item.column_name, item.data_type);
  });
  return out;
}
var getFields = Bluebird.coroutine(function * (db, tempTable) {
  let fields = yield db(db.raw('INFORMATION_SCHEMA.COLUMNS')).select('column_name', 'data_type')
    .where({
      table_name: tempTable // eslint-disable-line camelcase
    })
    .whereNotIn('column_name', ['cartodb_id', 'the_geom_webmercator', 'created_at', 'updated_at', 'the_geom']);
  return {
    fields: fields.map(function (item) {
      return item.column_name;
    }),
    schema: makeSchema(fields)
  }
})
const exists = Bluebird.coroutine(function * (name, db) {
  let count = yield _exists(name, db);
  if (count === 0) {
    return {count};
  }
  let {fields, schema} = yield getFields(db, name);
  return {count, fields, schema};
})
function part2(db, table, origTable, remove, toUser, config, done) {
  return append(db, table, toUser, config, function (err) {
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

  table = sanatize(table);
  if (table.match(/^[^a-z_]/)) {
    table = 'table_' + table
  } else if (table[0] === '_') {
    table = 'table' + table;
  }
  table = table.slice(0, 63);
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
  options = Object.assign({user, key}, options)
  options.method = options.method || 'create';
  options.batchSize = options.batchSize || 200;
  var direct = options.direct;
  var method = options.method;
  var toUser = createOutput(options.copy);
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
  var cartoOpts = {
    domain: options.domain,
    subdomainless: options.subdomainless
  };
  var db = cartodb(user, key, cartoOpts);
  exists(table, db).then(function (res) {
    let count = res.count;
    if (method === 'create') {
      if (count !== 0) {
        throw new Error('table already exists');
      }
      let out = new FirstN(options.batchSize, function (err, resp) {
        if (err) {
          return cb(err);
        }
        var uploadStream = uploader.geojson({
          user: user,
          key: key,
          domain: options.domain,
          subdomainless: options.subdomainless
        }, table, Bluebird.coroutine(function * (err, r) {
          if (err) {
            return cb(err);
          }
          if (r.table_name !== table) {
            return cb(new Error(`exptexted "${table}" but got "${r.table_name}"`));
          }
          try {
            let {fields, schema} = yield getFields(db, table)
            options.fields = fields;
            var nextPart;
            if (direct) {
              nextPart = part2(db, table, false, true, toUser, options, cb);
              return out.pipe(validator(warning, schema)).pipe(nextPart);
            } else {
              const id = yield createTemptTable(table, db)
              nextPart = part2(db, id, table, true, toUser, options, cb);
              resp.forEach(function (item) {
                nextPart.write(item);
              });
              out.pipe(validator(warning, schema)).pipe(nextPart);
            }
          } catch (e) {
            cb(e);
          }
        }));
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
    options.fields = res.fields;
    if (method === 'append') {
      if (direct) {
        return toUser.pipe(validator(warning, res.schema)).pipe(part2(db, table, false, false, toUser, options, cb));
      } else {
        return createTemptTable(table, db).then(function (id) {
          toUser.pipe(validator(warning, res.schema)).pipe(part2(db, id, table, false, toUser, options, cb));
        });
      }

    } else if (method === 'replace') {
      return createTemptTable(table, db).then(function (id) {
        toUser.pipe(validator(warning, res.schema)).pipe(part2(db, id, table, true, toUser, options, cb));
      });
    }
  }).catch(cb);
  return toUser;
}
