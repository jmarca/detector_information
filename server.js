var env = process.env;
var puser = process.env.PSQL_USER ;
var ppass = process.env.PSQL_PASS ;
var phost = process.env.PSQL_HOST ;
var cuser = process.env.COUCHDB_USER ;
var cpass = process.env.COUCHDB_PASS ;
var chost = process.env.COUCHDB_HOST ;
var connect = require('connect');

// for context
var RedisStore = require('connect-redis')(connect);

var detector_info = require('./lib/data_service');

var server = connect.createServer(
    connect.logger()
    ,connect.favicon(__dirname + '/public/favicon.ico')
    ,connect.bodyParser()
    ,connect.cookieParser()
    ,connect.session({ store: new RedisStore   //RedisStore or MemoryStore
                       , secret: '234kl 0aeyn9' })
    ,connect.router(vdsdata)

    ,connect.errorHandler({ dumpExceptions: true, showStack: true })
);



server.listen(3000);
console.log('Connect server listening on port 3000, working on '+__dirname+ ' but of course, there is always the fact that '+process.cwd());
// //server.listen(3000);
// console.log('Current gid: ' + process.getgid());
// try {
//     process.setgid(65533);
//     console.log('New gid: ' + process.getgid());
// }
// catch (err) {
//     console.log('Failed to set gid: ' + err);
//     throw(err);
// }
// console.log('Current uid: ' + process.getuid());
// try {
//     process.setuid(65534);
//     console.log('New uid: ' + process.getuid());
// }
// catch (err) {
//     console.log('Failed to set uid: ' + err);
//     throw(err);
// }

function vdsdata(app) {
  app.get('/vdsdata/:vdsid(\\d{6,7})/:startts/:endts?'
         ,detector_info.vds_data_service({})
         );
  app.get('/safety/:vdsid(\\d{6,7})/:startts/:endts?'
         ,detector_info.vds_safety_service({})
         );
  app.get('/vdsdata/hourly/:vdsid(\\d{6,7})/:hour/:start/:end?'
         ,detector_info.vds_hourly_service({})
         );
}


