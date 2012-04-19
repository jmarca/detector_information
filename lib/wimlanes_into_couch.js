
var pg = require('pg');

var request = require('request');
var _ = require('underscore');
var async = require('async');

var env = process.env;

var tcuser = env.COUCHDB_USER ;
var tcpass = env.COUCHDB_PASS ;
var tchost = env.COUCHDB_HOST || '127.0.0.1' ;
var tcport = env.COUCHDB_PORT || 5984;
var puser = env.PSQL_USER ;
var ppass = env.PSQL_PASS ;
var phost = env.PSQL_HOST || '127.0.0.1' ;
var pport = env.PSQL_PORT || 5432 ;


function wim_twim_copy(options){

    var dbname = options.db || 'spatialvds';
    var host = options.host || phost;
    var user = options.username|| puser;
    var pass = options.password || ppass;
    var port = options.port || pport;
    var connectionString = 'pg://'+user+':'+pass+'@'+host+':'+port+'/'+dbname;
    var table = 'newctmlmap.twim twim';

    console.log(connectionString)

    // make the select statement
    var query = 'select site_no,direction,max(lane_no) as lanes from wim_lane_dir group by site_no,direction';


    var uri = 'http://'+tchost+':'+tcport+'/vdsdata%2ftracking';

    function handleErr(err,b){
        if(!b) {
            console.log('error '+JSON.stringify(err))

            throw new Error(err);

        }else{
            console.log('error '+JSON.stringify([err,b]))
        }
        return next(err);
    }
    function do_put(doc,cb){
        // put the modified document
        var url = [uri,doc._id].join('/');

        return request({ 'method' : 'PUT'
                       ,'uri' : url
                       ,'headers' : { 'authorization' : "Basic " + new Buffer(tcuser + ":" + tcpass).toString('base64')
                                    ,'content-type': 'application/json'
                                    ,'accept':'application/json'
                                    }
                       ,'body':JSON.stringify(doc)
                       },function(e,r,b){
                             if(e) return handleErr(e,b);
                             return cb();
                         }
                      );
    }

    function get_handler(detector,properties,cb){
        // handler for the get doc call.
        // parses the body into a json document
        return function(e,r,b){
            if(e) return handleErr(e,b);
            // b is the doc that needs to be modified
            var doc = JSON.parse(b);
            if(doc.error==="not_found"){
                doc = {'_id':detector};
            }
            if(doc.properties === undefined ){
                doc.properties = {}
            }
            var props_key = _.chain(doc.properties).keys().first().value();
            if(props_key === undefined){
                props_key = 'nogeo';
                doc.properties[props_key]={};
            }
            _.each(properties
                  ,function(val,key){
                       if(key === 'geojson'){
                           doc.properties[props_key][key]=JSON.parse(val);
                       }else if(['site_no', 'direction','last_modified'].indexOf(key) == -1){
                           doc.properties[props_key][key]=val;
                       }
                   });
            return do_put(doc,cb);
        }

    }
    function handleRow(row){
        var detector = ['wim',row.site_no,row.direction[0].toUpperCase()].join('.')
        // save the row data to the proper couchdb document
        // get the document, if it exists, modify it, if it doesn't exist, create it
        var url = [uri,detector].join('/');
        console.log(url)
        var x = request({'method' : 'GET'
                        ,'uri' : url
                        ,'headers' : { 'authorization' : "Basic " + new Buffer(tcuser + ":" + tcpass).toString('base64')
                                     ,'content-type': 'application/json'
                                     ,'accept':'application/json'
                                     }
                        }
                       ,get_handler(detector,row,function(err){
                            if(err) return next(err);
                            console.log('done with detector '+detector);
                            return null;
                        }));
        return x;
    }

    function doQuery(err,client){
        if(err) return handleErr(err);
        var q = client.query(query);
        q.on('error',handleErr);
        q.on('row',handleRow);
        q.on('end'
            , function(){
                  console.log('done with postgres query')
                  next();
              }
            );
        return q;
    }
    var next;
    return function(_cb){
        next = _cb;
        return pg.connect(connectionString
                         ,doQuery);
    };

};

async.series([wim_twim_copy({})
             ],
             function(){
                 console.log('all done with calls, waiting for async jobs to finish up');
                 return 1;
             });
1;
