const progress = require('progress');
const progressBar = (total, description) => {
    return new progress(`${description} [:bar] :percent :etas`, {
      complete: '=',
      incomplete: ' ',
      width: 20,
      total: total,
    });
  };

module.exports = {
    progressBar
}