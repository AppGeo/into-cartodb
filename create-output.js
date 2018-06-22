var stream = require('readable-stream')
var Transform = stream.Transform;
var PassThrough = stream.PassThrough
var sanatize = require('./sanatize');
class Output extends Transform {
  constructor(copy) {
    super({objectMode: true});
    this.i = 0;
    this.copy = copy;
    this.sanCache = new Map();
  }
  sanatize(key) {
    if (this.sanCache.has(key)) {
      return this.sanCache.get(key)
    }
    let out = sanatize(key);
    this.sanCache.set(key, out);
    return out;
  }
  fixProps(oldProps) {
    var out = {};
    var keys = Object.keys(oldProps);
    var i = -1;
    var key;
    while (++i < keys.length) {
      key = keys[i];
      out[this.sanatize(key)] = oldProps[key];
    }
    return out;
  }
  _transform(chunk, _, next) {
    chunk.properties = this.fixProps(chunk.properties)
    if (this.copy) {
      this.i++;
      if (!(this.i%100)) {
        this.emit('inserted', this.i);
      }
    }
    this.push(chunk);
    next();
  }
}
module.exports = function (copy) {
  return new Output(copy)
}
