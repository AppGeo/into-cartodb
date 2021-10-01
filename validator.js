const Transform = require('readable-stream').Transform;
const inherits = require('inherits');

inherits(Validator, Transform);
module.exports = validator;
function validator (warning, schema) {
  return new Validator(warning, schema);
}
function Validator (warning, schema) {
  Transform.call(this, {
    objectMode: true
  });
  this.warning = warning;
  this.extraKeys = new Set();
  this.schema = schema;
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
  const self = this;
  const props = chunk.properties;
  chunk.properties = {};
  const schema = this.schema;
  Object.keys(props).forEach(function (key) {
    if (!schema.has(key)) {
      self.keyWarning(key);
      return;
    }
    const typed = self.coerceType(props[key], schema.get(key));
    if (typed === self.nope) {
      return;
    }
    chunk.properties[key] = typed;
  });
  if (chunk.geometry || Object.keys(chunk.properties).length) {
    self.push(chunk);
  }
  next();
};
const falses = new Set(['f', 'false', 'n', 'no', 'off', '0']);
const trues = new Set(['t', 'true', 'y', 'yes', 'on', '1']);
Validator.prototype.coerceType = function (value, type) {
  if (typeof value === 'undefined' || value === null) {
    return this.nope;
  }
  let out;
  switch (type) {
    case 'character':
    case 'text':
      out = String(value);
      if (!out) {
        return this.nope;
      }
      return out;
    case 'double precision':
      out = parseFloat(value);
      if (Number.isNaN(out)) {
        return this.nope;
      }
      return out;
    case 'integer':
      out = parseInt(value, 10);
      if (Number.isNaN(out)) {
        return this.nope;
      }
      if (out > 2147483647 || out < -2147483648) {
        return this.nope;
      }
      return out;
    case 'bigint':
      out = parseInt(value, 10);
      if (Number.isNaN(out)) {
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
      if (typeof value !== 'string') {
        return Boolean(value);
      }
      if (falses.has(value.trim().toLowerCase())) {
        return false;
      }
      if (trues.has(value.trim().toLowerCase())) {
        return true;
      }
      return this.nope;
    default:
      return this.nope;
  }
};
