var fs = require('fs');
var _ = require('underscore');
var couchdb = require('couchdb');


// this program is supposed to get data from couchdb for a day,
// collate the multiple docs into a single doc, and save it back

// first, what is the connection
var env = process.env;
// var puser = process.env.PSQL_USER ;
// var ppass = process.env.PSQL_PASS ;
// var phost = process.env.PSQL_HOST ;

var cuser = process.env.COUCHDB_USER ;
var cpass = process.env.COUCHDB_PASS ;
var chost = process.env.COUCHDB_HOST ;
var cport = process.env.COUCHDB_PORT ;

var cluser = process.env.COUCHDB_LOCALUSER ;
var clpass = process.env.COUCHDB_LOCALPASS ;
var clhost = process.env.COUCHDB_LOCALHOST ;
var clport = process.env.COUCHDB_LOCALPORT ;


var clientgetLocal1 = couchdb.createClient(clport, clhost, cluser, clpass);
var clientgetLocal2 = couchdb.createClient(clport, clhost, cluser, clpass);
var clientsaveLocal = couchdb.createClient(clport, clhost, cluser, clpass);
var clientRemote = couchdb.createClient(cport, chost, cuser, cpass);

var replicate_root = ['http://',[clhost,clport].join(':')].join('');

// loop over years and districts, extract the tracking database (if it
// exists), and copy to the master tracking database

var master_track = ['vdsdata','tracking']; // join with %2f or '/' as needed

var master_track_db = clientgetLocal1.db(master_track.join('%2f'));
var master_track_savedb = clientsaveLocal.db(master_track.join('%2f'));
master_track_db.create(function(e,r){
    // I don't care if e, because it probably just means that the
    // database already exists and can't be created

    // okay, now loop over years and districts

    var years = [2007,2008,2009];
    var districts = ['d03','d04','d05','d06','d07','d08','d09','d10','d11','d12'];

    // source db
    _.each(years,function(y){
        _.each(districts,function(d){
            var source_db = clientgetLocal2.db(['vdsdata',d,y].join('%2f'));
            source_db.allDocs(null,{'include_docs':true},function(e,r){
                // should get back a list of doc ids and docs
                _.each(r.rows,function(pulledrow){
                    var vdsid = row.id;
                    // load that doc, save it
                    master_track_db.get('vdsid',function(e,r){
                        // update the appropriate tracking bit
                        if(!r[y]) r[y]={};
                        _.each(pulledrow,function(key,value){
                            if (key !== '_id' && key !== '_rev'){
                                r[y][key]=value;
                            }
                        });
                        // save the update
                        master_track_savedb.saveDoc(r,function(e) {if(e){ console.log(e); throw new Error(e);}});
                    });
                });
            });
        });
    });
});