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

var detector_info = require('./lib/detector_information');

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

function vdsdata(app) {
  app.get('/vdsdata/:vdsid(\\d{6,7})/:startts/:endts?'
         ,detector_info.data_service.vds_data_service({})
         );
  app.get('/risk/:vdsid(\\d{6,7})/:startts'
         ,detector_info.prediction_service.prediction_data_service({'dbname':'safetynet'
                                                                   ,'designdoc':'risk'
                                                                   ,'view':'risk'})
         );
}


