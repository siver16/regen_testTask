var readline = require('readline');
var redis = require('redis');
var fs = require('fs');
var minimist = require('minimist');

var client = redis.createClient(6379, '127.0.0.1');
var db = redis.createClient(6379, '127.0.0.1');
var args = minimist(process.argv.slice(2));//читаем параметры при запуске --sub или --pub
var messageDelay = 500;
var interface = readline.createInterface(process.stdin, process.srdout, null);
var logger = require('tracer').console({
  transport: function (data) {
    fs.open('./file.log', 'a', parseInt('0644', 8), function (e, id) {
      fs.write(id, data.output + "\n", null, 'utf8', function () {
        fs.close(id, function () {
        });
      });
    });
  }
});

if (args.sub) {
  createSub("mainChannel");
} else if (args.pub) {t
  createPub("mainChannel");
}
logger.log("runing on ", process.pid);

client.on('error', function (err) {
  logger.log("Error" + err);
});

interface.on('line', function (cmd) {
  if (cmd == 'pub') {
    createPub("mainChannel");
  } else if (cmd == 'sub') {
    createSub("mainChannel");
  } else if (cmd == 'getErrors') {
    db.lrange('errors', 0, -1, function (err, data) {
      if(err) logger.log("Error" + err);
      for (var val in data) {
        process.stdout.write(data[val] + '\n');
      }
      db.del('errors');
    });
  } else if (cmd == 'help' || cmd == 'h') {
    process.stdout.write("sub for subscribe,  pub for publish,  getErrors for get all messages processed with errors");
  } else {
    process.stdout.write("h or help  for help");
  }
});

function createPub(name) {
  db.set('generator', process.pid, function (err) {
    publish(name, 0);
  });
}

function publish(name, data) {
  if (data < 1000000) {
    var message = getMessage();
    db.mset('messages:' + message, 1, 'lastmessage', message, 'lasttime', Date.now(), 'generatorCreating', false, function (err){
      if(err) logger.log("Error" + err);
      client.publish(name, message);
      data++;
      setTimeout(function () {
        publish(name, data);
      }, messageDelay);
    });
  }
}
function createSub(name) {
  client.subscribe(name);
  client.on('message', function (channel, message) {
    setTimeout(function () {
      db.get('lasttime', function (err, val) {
        if(err) logger.log("Error" + err);
        if ((Date.now() - val) >= messageDelay * 3) {
          db.watch('generatorCreating');
          db.get('generatorCreating', function (err, value) {
            if(err) logger.log("Error" + err);
            if (value == "false") {
              var multi = db.multi();
              multi.set('generatorCreating', true, function (err) {
                if(err) logger.log("Error" + err);
                client.unsubscribe(name);
                db.set('generator', process.pid, function (err) {
                  if(err) logger.log("Error" + err);
                  db.get('lastmessage', function (err, value) {
                    if(err) logger.log("Error" + err);
                    db.set('generatorCreating', false, function (err) {
                      if(err) logger.log("Error" + err);
                      publish(name, value + 1);
                    });
                  });
                });
              });
              multi.exec();
            }
          });
        }
      });
    }, messageDelay * 4);
    processMessage(name, 'messages:' + message, 'generator');
  })
}
function processMessage(name, message, from) {
  db.watch(message);
  db.get(message, function (err, value) {
    if(err) logger.log("Error" + err);
    if (value == 1) {
      var multi = db.multi();
      var res = false;
      multi.set(message, process.pid, function () {
        res = true;
      });
      multi.exec(function (err, value) {
        if (res) {
          client.unsubscribe(name);
          eventHandler(message, from, function (error, msg) {
            if (error) {
              db.lpush('errors', msg.split(':')[1]);
            }
            db.del(message, function (error, msg) {
              db.keys('messages:*', function (err, val) {
                if(err) logger.log("Error" + err);
                if (val[0]) {
                  processMessage(name, val[0], 'queue');
                } else {
                  client.subscribe(name);
                }
              });
            });
          });
        } else {
          client.subscribe(name);
        }
      });
    } else {
      client.subscribe(name);
    }
  });
}

function getMessage() {
  this.cnt = this.cnt || 0;
  return this.cnt++;
}

function eventHandler(msg, from, callback) {
  function onComplete() {
    logger.info('message ' + msg + ' processed by ' + process.pid + ' from ' + from);
    var error = Math.random() > 0.85;
    callback(error, msg);
  }
  setTimeout(onComplete, Math.floor(Math.random() * 1000));
}