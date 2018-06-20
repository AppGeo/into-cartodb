'use strict';
var Bluebird = require('bluebird');
var debug = require('debug')('into-cartodb:validations');

module.exports = Bluebird.coroutine(function * validate(tempTable, _fields, config, db, group) {
  let fields = new Map();
  _fields.forEach(function (field) {
    fields.set(field, field);
  });
  var validations = getValidations(config);
  for (let validation of validations) {
    yield validation(tempTable, fields, db, group);
  }
  return fields;
});
var fixGeom = Bluebird.coroutine(function * fixGeom(tempTable, fields, db) {
  var count = yield db(tempTable).count('*');
  count = count.length === 1 && count[0].count;
  if (!Number(count)) {
    throw new Error('no rows inserted');
  }
  var hasGeom = yield db(tempTable).select(db.raw('bool_or(the_geom is not null) as hasgeom'));
  hasGeom = hasGeom.length === 1 && hasGeom[0].hasgeom;
  if (hasGeom) {
    debug('has geometry');
    let allValid = yield db(tempTable).select(db.raw('bool_and(st_isvalid(the_geom)) as allvalid'));
    allValid = allValid.length === 1 && allValid[0].allvalid;
    if (allValid) {
      debug('geometry is all valid');
    } else {
      debug('has invalid geometry');
      yield db.raw('update ?? set the_geom = ST_MakeValid(the_geom) where not st_isvalid(the_geom)', [tempTable]).batch();
      yield db(tempTable).delete().whereRaw('GeometryType(the_geom) = \'GEOMETRYCOLLECTION\'');
    }
    fields.set('the_geom', 'the_geom');
  }
});
function getValidations(config) {
  if (!config.validations || !Array.isArray(config.validations) || !config.validations.length) {
    return [fixGeom];
  } else {
    return [fixGeom].concat(config.validations);
  }
}
