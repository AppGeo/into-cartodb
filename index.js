const once = require('once');
const cartodb = require('cartodb-tools');
const uploader = require('cartodb-uploader');
const FirstN = require('first-n-stream');
const uuid = require('uuid');
const debug = require('debug')('into-cartodb');
const sanatize = require('./sanatize');
const validate = require('./validations');
const cartoCopyStream = require('carto-copy-stream');
const validator = require('./validator');
const escape = require('pg-escape');
const createOutput = require('./create-output');

module.exports = intoCartoDB;
function append (db, table, toUser, options, cb) {
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

const createTemptTable = async (table, db) => {
  const id = `${table.slice(0, 21)}_temp_${uuid().replace(/-/g, '_')}`;
  await db.raw('create table ?? as table ?? with no data;', [id, table]);
  return id;
};
const cleanUpTempTables = async function * cleanUp (user, key, opts) {
  const db = cartodb(user, key, opts);
  let done = 0;
  let tables = await db(db.raw('INFORMATION_SCHEMA.tables')).select('table_name')
    .where('table_name', 'like', '%\_temp\_________\_____\_____\_____\_____________') // eslint-disable-line no-useless-escape
    .groupBy('table_name');
  if (!tables.length) {
    return 0;
  }
  tables = tables.map(function (item) {
    return item.table_name;
  });
  for (const name of tables) {
    await db.schema.dropTableIfExists(name);
    ++done;
  }
  return done;
};

module.exports.cleanUpTempTables = cleanUpTempTables;

const swap = async function swap (table, tempTable, remove, db, config) {
  let newFields, out;
  const fields = config.fields;
  const group = new Set();
  try {
    newFields = await validate(tempTable, fields, config, db, group);
  } catch (e) {
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
  const fromFields = [];
  const toFields = [];
  newFields.forEach(function (value, key) {
    fromFields.push(value);
    toFields.push(key);
  });
  const groupFields = [];
  group.forEach(function (field) {
    groupFields.push(field);
  });
  return db.raw(`
      ${remove ? escape('DELETE from %I', table) : ''};
      INSERT into ?? (${toFields.join(',')})
      SELECT ${fromFields.join(',')} from ??
      ${groupFields.length ? `group by ${groupFields.join(',')}` : ''};
  `, [table, tempTable]).batch().onSuccess(db.raw('DROP TABLE ??;', [tempTable])).onError(db.raw('DROP TABLE ??;', [tempTable]));
};

/*
{
  style: create|replace|append
}
*/
function _exists (name, db) {
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
function makeSchema (data) {
  const out = new Map();
  data.forEach(function (item) {
    out.set(item.column_name, item.data_type);
  });
  return out;
}
const getFields = async function (db, tempTable) {
  const fields = await db(db.raw('INFORMATION_SCHEMA.COLUMNS')).select('column_name', 'data_type')
    .where({
      table_name: tempTable // eslint-disable-line camelcase
    })
    .whereNotIn('column_name', ['cartodb_id', 'the_geom_webmercator', 'created_at', 'updated_at', 'the_geom']);
  return {
    fields: fields.map(function (item) {
      return item.column_name;
    }),
    schema: makeSchema(fields)
  };
};
const exists = async function (name, db) {
  const count = await _exists(name, db);
  if (count === 0) {
    return { count };
  }
  const { fields, schema } = await getFields(db, name);
  return { count, fields, schema };
};
function part2 (db, table, origTable, remove, toUser, config, done) {
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

function intoCartoDB (user, key, table, options, done) {
  table = sanatize(table);
  if (table.match(/^[^a-z_]/)) {
    table = 'table_' + table;
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
  options = Object.assign({ user, key }, options);
  options.method = options.method || 'create';
  options.batchSize = options.batchSize || 200;
  const direct = options.direct;
  const method = options.method;
  const toUser = createOutput(options.copy);
  function warning (msg) {
    toUser.emit('warning', msg);
  }
  const cb = once(function (err, resp) {
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
  const cartoOpts = {
    domain: options.domain,
    subdomainless: options.subdomainless
  };
  const db = cartodb(user, key, cartoOpts);
  exists(table, db).then(function (res) {
    const count = res.count;
    if (method === 'create') {
      if (count !== 0) {
        throw new Error('table already exists');
      }
      const out = new FirstN(options.batchSize, function (err, resp) {
        if (err) {
          return cb(err);
        }
        const uploadStream = uploader.geojson({
          user: user,
          key: key,
          domain: options.domain,
          subdomainless: options.subdomainless
        }, table, async function (err, r) {
          if (err) {
            return cb(err);
          }
          if (r.table_name !== table) {
            return cb(new Error(`exptexted "${table}" but got "${r.table_name}"`));
          }
          try {
            const { fields, schema } = await getFields(db, table);
            options.fields = fields;
            let nextPart;
            if (direct) {
              nextPart = part2(db, table, false, true, toUser, options, cb);
              return out.pipe(validator(warning, schema)).pipe(nextPart);
            } else {
              const id = await createTemptTable(table, db);
              nextPart = part2(db, id, table, true, toUser, options, cb);
              resp.forEach(function (item) {
                nextPart.write(item);
              });
              out.pipe(validator(warning, schema)).pipe(nextPart);
            }
          } catch (e) {
            cb(e);
          }
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
