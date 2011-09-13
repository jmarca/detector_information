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
    var districts = [3,4,5,6,7,8,10,11,12];
    
    // source db
    var design_check = /^_design/;
    _.each(years,function(y){
        _.each(districts,function(dnum){
	    var d = dnum < 10 ? 'd0'+dnum : 'd'+dnum;
            var source_db = clientgetLocal2.db(['vdsdata',d,y].join('%2f'));
	    var district_check = new RegExp('^'+dnum+'\\d{5}');

            source_db.allDocs({},{'include_docs':true},function(e,r){
		if(e){
		    console.log(e);
		    throw new Error(e);
		}
                // should get back a list of doc ids and docs
                _.each(r.rows,function(pulledrow){
                    var vdsid = pulledrow.id;
		    var dres = district_check.exec(vdsid);
		    if(design_check.exec(vdsid) == null && dres  != null){
			// load that doc, save it
			// save function
			var newdoc = {};
			newdoc[y]={};
			_.each(pulledrow.doc,function(value,key){
                            if (key !== '_id' && key !== '_rev'){
				newdoc[y][key]=value;
                            }
			});
			master_track_db.saveDoc(vdsid,newdoc,function(e,r){
			    if(e){
				var anotherclient = couchdb.createClient(clport, clhost, cluser, clpass);
				var anotherdb =anotherclient.db(master_track.join('%2f'));
				anotherdb.getDoc(vdsid,function(e,r){
				    if(e){
					console.log('get failed too '+JSON.stringify(e));
					throw new Error(e);
				    }
				    if(r[y]){
					console.log('already have year '+y);
					console.log(JSON.stringify(r[y]) + ' vs ' +JSON.stringify(newdoc));
					throw new Error ('duped year');
				    }
				    r[y]=newdoc[y];
				    anotherdb.saveDoc(r,function(e) {if(e){ console.log(e); throw new Error(e);}});
				});
			    }
			});
		    }
                });
		      
            });
        });
    });
});