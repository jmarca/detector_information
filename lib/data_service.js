var couchdb = require('couchdb');
//var detector_information = require('./detector_information');
var asyncMap = require('./utils/async-map');
var CDBDateString = require('./utils/formatting.js');
var _ = require('underscore');
var models = require('./utils/models')
// first, what is the connection
var env = process.env;
// var puser = process.env.PSQL_USER ;
// var ppass = process.env.PSQL_PASS ;
// var phost = process.env.PSQL_HOST ;


exports.vds_data_service = vds_data_service;

function vds_data_service(options,cb_){


    var cuser = options.user || process.env.COUCHDB_USER ;
    var cpass = options.pass || process.env.COUCHDB_PASS ;
    var chost = options.host || process.env.COUCHDB_HOST ;
    var cport = options.port || process.env.COUCHDB_PORT ;
    // bug this won't work if prefix is null or '', etc
    var dbprefix = options.dbprefix || 'vdsdata';
    if(typeof cb_ !== "function"){
        cb_ = function(res,next){
            return function(e,d){
                if(e){
                    next(e);
                }else{
                    res.end(JSON.stringify(d));
                }
            };
        };
    }
    function createCouchClient(){
        var client = couchdb.createClient(cport, chost, cuser, cpass);
        return client;
    }

    // maybe add createCouchLocalClient??
    // var cluser = process.env.COUCHDB_LOCALUSER ;
    // var clpass = process.env.COUCHDB_LOCALPASS ;
    // var clhost = process.env.COUCHDB_LOCALHOST ;
    // var clport = process.env.COUCHDB_LOCALPORT ;

    var possibleParams = ['vdsid','startts','endts'];

    return function vds_data_service(req,res,next){
        var cb__ = cb_(res,next);
        var activeParams = possibleParams.filter(function(a){return req.params[a];});
        var query = {};
        if(activeParams.length < 2 || !activeParams[0]==='vdsid') return next();
        // have to handle the fact that I am pulling down days of
        // data, so the day may be prior to the date. Do this by only
        // forming the date string with the date part, not the time
        // part.  Then when processing the response, filter out any
        // information before or after the desired date range
        var start = new Date(req.params.startts);
        var endts = req.params.startts;
        var end = start;
        var startkey = [req.params.vdsid,CDBDateString(start,false)].join(' ');
        var endkey =  startkey;
        if(req.params.endts){
            endts = req.params.endts;
            end = new Date(endts);
            endkey = [req.params.vdsid,CDBDateString(end,false)].join(' ');
        }
        endkey+=' 25';
        // year might be two
        var year1 = start.getFullYear();
        var year2 = end.getFullYear();

        if(year2 > year1+1){
            // complain, this is too much data
            res.end(JSON.stringify({'error':'Date range too large'}));
            return 0;
        }

        var district = match_district(req.params.vdsid);
        district = district < 10 ? 'd0'+district : 'd'+district;
        var data_handler = trimAndMergeData(start,end,res,cb__);
        // either one or two years passed in
        // logically easiest to use async_map here, I think

        // make a list of arguments to pass to db.allDocs
        argslist = [year1];

        if(year1 !== year2){
            argslist.push(year2);
        }

        asyncMap(argslist
                ,function(year,cb){
                    // do the couchdb query
                    var client = createCouchClient();
                    var dbname = ['vdsdata',district,year,req.params.vdsid].join('%2f');
                    var db = client.db(dbname);
                    db.allDocs({}
                                   ,{'startkey':startkey
                                    ,'endkey':endkey
                                    ,'include_docs':true}
                                   ,cb)}
                ,data_handler
                );
        return 1;
    }
}

function match_district (vdsid){
    var district_regex = /^(\d{1,2})\d{5}$/;
    var match = district_regex.exec(vdsid);
    console.log('vdsid '+vdsid+', regex '+district_regex+' '+JSON.stringify(match));
    return match[1];
}

// use from within async map
function trimAndMergeData(start,end,res,next){
    var start_string = CDBDateString(start,true,'/');
    var end_string = CDBDateString(end,true,'/');
    console.log(['start',start,start_string,'end',end,end_string].join(' '));


    return function(err,docsarray){
        if(err) return next(err);
        // logic:
        // 0. The docsarray is an array of responses from couchdb
        // 1. The first doc must be trimmed of data prior to desired timestamp.
        // 2. The last doc must be trimmed of data after desired timestamp.
        // 3. There might only be one doc.
        // 4. Not every doc has identical columns.
        // 5. Every doc should have a 'ts' column
        var first
          ,last
          ,idx
          ,result={};
        _.each(docsarray,function(docs){
            first = docs.rows[0];
            last = docs.rows[docs.rows.length-1];
            //
            // 3. There might only be one doc.
            //
            // I proved to myself that last is also a reference to docs[0]
            // if docs.length == 1, so manipulations of first.data also
            // change last.data and vice versa

            // find the timestamp that is equal to or greater than desired
            var s_idx = 0;
            for(s_idx = 0; first.doc.data.ts[s_idx] <= start_string; s_idx++){
                // no op
            }
            console.log(['sindex',s_idx,'start',start].join(' '));
            // slice away
            _.each(first.doc.data,function(val,key){
                first.doc.data[key]=val.slice(s_idx);
            });
            // that will also slice last if one doc only
            var t_idx = 0;
            for(t_idx = 0; last.doc.data.ts[t_idx] <= end_string; t_idx++){
                // no op
            }
            console.log(['tindex',t_idx,'end',end].join(' '));
            // slice away
            _.each(last.doc.data,function(val,key){
                last.doc.data[key]=val.slice(0,t_idx);
            });

            console.log(['last',last.length].join(' '));
            // fill up result arrays
            _.each(docs.rows,function(row){
                var doc = row.doc;
                // 3. There might only be one doc.
                // 5. Every doc should have a 'ts' column
                idx = result.ts ? result.ts.length : 0;
                _.each(doc.data,function(val,key){
                    //  4. Not every doc has identical columns
                    if(!result[key]){
                        result[key]=val;
                    }else{
                        if(result[key].length != idx){
                            result[key][idx]=val[0];
                            result[key].push(val.slice(1));
                        }else{
                            result[key].push(val);
                        }
                    }
                    //console.log(key+' val '+JSON.stringify(result));
                    //throw new Error('die');

                });
            });
        });
        // dump the result to the response
        return next(null,result);

    };
}



exports.vds_safety_service = vds_safety_service;
function vds_safety_service(options){

    var cb = function(res,next){
        return function(e,result){
            if(e){
                throw new Error(e);
            }else{
                var gen = models.any_accident(result);
                for (var i =0; i<result.ts.length;i++){
                    gen(i);
                }
                res.end(JSON.stringify(result));
            }
        };
    };
    return vds_data_service(options,cb);

}

