var fs = require('fs');
var _ = require('underscore');
var couchdb = require('couchdb');
var asyncMap = require('./utils/async-map');


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


var clientOuter = couchdb.createClient(cport, chost, cuser, cpass);
var clientBulk = couchdb.createClient(cport, chost, cuser, cpass);


var years = [2007];
var districts = ['d12'];
var d1 = 'var_27';
var v1 = 'complete'

// or whatever the f the for in loop syntax is
// and make this a function so I can call backy it

/* use a function for date formatting... */
function CDBDateString(d,time){

    function pad(n){return n<10 ? '0'+n : n}
    if(!time){
        return [d.getUTCFullYear()
               ,pad(d.getUTCMonth()+1)
               ,pad(d.getUTCDate())
               ].join('-');
    }
    return [d.getUTCFullYear()
           ,pad(d.getUTCMonth()+1)
           ,pad(d.getUTCDate())
           ].join('-')
                               + ' '
          +[ d.getUTCHours()
           ,pad(d.getUTCMinutes())
           ,pad(d.getUTCSeconds())
           ].join(':')
    ;

}

var vdsDayDocs = function(dayId,dyv_db,cb){
    return function(err,r){
        var bulkdocs=[];
        if (err){
            console.log('error: dayId '+dayId+' error: '+JSON.stringify(err));
            throw new Error (err);
        }
        if(r.rows.length){
            console.log('processing '+dayId+' with '+r.rows.length+' rows');
            // create a time sequence of each 30s period in the day
            var output={};
            var newdoc = {'_id': dayId};
            var time_index = 0;
            var start;
            for (var row_i in r.rows){
                var row = r.rows[row_i];
                if(!row.doc.ts) continue;
                var dropdoc={"_deleted": true};
                var rowTs = new Date(row.doc.ts);
                rowTs = rowTs.getTime();
                if(!start) start = rowTs;
                while ((start + 30*1000*time_index)<rowTs){time_index++}
                for (var key in row.doc){
                    if(key === '_id' || key === '_rev'){
                        dropdoc[key]=row.doc[key];
                    }else{
                        if(!output[key]){output[key]=[]}
                        output[key][time_index]=row.doc[key];
                    }
                }
                if(dropdoc['_id'] !== dayId ){
                    bulkdocs.push(dropdoc);
                }else{
                    newdoc['_rev'] = dropdoc['_rev'];
                }
            }
            if(!output.ts){ return cb('no data',dayId) ; }

            newdoc.data=output;

            var con = clientBulk.db(dyv_db);
            con.saveDoc(newdoc
                       ,function(err,r){
                           if(err){
                               console.log('croaked in save new doc '+JSON.stringify(err));
                           }
                               // bulk drop old docs
                               console.log('deleting 30s docs');
                               con.bulkDocs({'docs':bulkdocs},function(err,r){
                                   if(err){
                                       console.log('bulkdocs failed'+ err);
                                       cb(err);
                                       throw new Error();
                                   }else{
                                       // ignore the bulkdocs response
                                       cb(null,dayId);

                                   }
                                   //throw new Error();
                               });

                       });
        }
        return true;
    };
};


var doAllDocs= function(dyv_db,params){
    return function(q,cb){
        // make a client per day.  Not sure why, but it seems prudent
        console.log('firing for '+q.startkey );
        var db2 = clientOuter.db(dyv_db);
        var args = params;
        _.extend(args,q);
        db2.allDocs({}
                   ,args
                   ,vdsDayDocs(q.startkey,dyv_db,cb));

    }
};


var vdsFixer = function( vds_id, district, year ){

    var dy_db = ['vdsdata',district,year].join('%2f');
    // formulate the database name for the selected year
    var dyv_db = [dy_db,vds_id].join('%2f');

    console.log(dyv_db);

    //iterate over days in the year using a date object from year, jan, 1 00:00 to year+1, jan, 1 00:00
    var endymd = new Date(year+1, 0, 1, 0, 0, 0).getTime();

    var ymd=new Date(year, 0, 1, 0, 0, 0)
    var queries = [];
    while(ymd.getTime()<endymd){
        var dateString = CDBDateString(ymd);
        var timeString = '00:00:00';
        var dayId = [vds_id
                    ,dateString
                    ,timeString
                    ].join(' ');
        // tomorrow, and also increment ymd for the
        // loop here!
        ymd.setDate(ymd.getDate()+1)
        dateString = CDBDateString(ymd);

        var nextDayId =  [vds_id
                         ,dateString
                         ,timeString
                         ].join(' ');
        queries.push({'startkey':dayId
                     ,'endkey':nextDayId
                     });
    }
    var queries2 = [];
    var fn = doAllDocs(dyv_db,{'include_docs':true
                              ,'inclusive_end':false});
    var cb = function(err,other){
        if(err){console.log('outer callback error: '+JSON.stringify(err));}
        if(other){console.log('outer callback: '+JSON.stringify(other));}
    };

    // queries.forEach(function(arg){
    //     fn(arg,cb);
    // });

    // fn(queries[0],cb);
    asyncMap(queries,fn,cb);

};



var vdsViewHandler = function(year){
    return function(er, r) {
        if (er){
            console.log('error: '+JSON.stringify(er));
            return;
        }
        for (var row_i in r.rows){
            // each row identifies a detector with a database
            var row = r.rows[row_i];
            var vds_id = row['_id'];
            vdsFixer(vds_id,year);
        }
    };
};

// test out on 1201100
vdsFixer(1201653,districts[0],years[0]);


// for( var year in years ){
//     for( var district in districts ){
//         var dy_db = ['vdsdata',district,year].join('/');
//         var client = couchdb.createClient(cport, chost, cuser, cpass);
//         var db = client.db(dy_db);
//         // call the view that gets the vdsids
//         db.view(d1,v1,vdsViewHandler(year) );
//     }
// }


