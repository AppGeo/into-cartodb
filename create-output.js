var stream = require('readable-stream')
var Transform = stream.Transform;
var PassThrough = stream.PassThrough;
class Output extends Transform {
  constructor() {
    super({objectMode: true});
    this.i = 0;
  }
  _transform(chunk, _, next) {
    this.i++;
    if (!(this.i%100)) {
      this.emit('inserted', this.i);
    }
    this.push(chunk);
    next();
  }
}
module.exports = function (copy) {
  if (copy) {
    return new Output()
  } else {
    return new PassThrough({
      objectMode: true
    })
  }
}
