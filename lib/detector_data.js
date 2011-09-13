var fs = require('fs');
var _ = require('underscore');
var couchdb = require('couchdb');
var asyncMap = require('./utils/async-map');
var CDBDateString = require('./utils/formatting');
var helpers = require('./utils/couch_helpers');

// this program is supposed to get data from couchdb for a day,
// collate the multiple docs into a single doc, and save it back
//
// In this hopefully final version, I get from remote, work on the db
// locally, then replicate from local to remote
//

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


var clientLocal = couchdb.createClient(clport, clhost, cluser, clpass);
var clientLocalReplicate = couchdb.createClient(clport, clhost, cluser, clpass);
var clientRemote = couchdb.createClient(cport, chost, cuser, cpass);
var clientBulk = couchdb.createClient(cport, chost, cuser, cpass);
//var clientOuter = couchdb.createClient(cport, chost, cuser, cpass);

var pullReplicate = process.env.PULL_REPLICATE ;

var local_replicate_root = ['http://',[clhost,clport].join(':')].join('');
var remote_replicate_root = ['http://'
                            ,[ [cuser
                               ,cpass].join(':')
                               ,[chost
                                ,cport].join(':')
                             ].join('@')
                            ].join('');


var years = [2007];
var districts = ['d12'];
var d1 = 'var_27';
var v1 = 'complete'

var tracker = new process.EventEmitter();

// or whatever the f the for in loop syntax is
// and make this a function so I can call backy it

// work on a day's data, create a single doc, save it to local
var vdsDayDocs = function(dayId,dyv_db,cb){
    return function(err,r){
        var bulkdocs=[];
        if (err){
            console.log('error: dayId '+dayId+' error: '+JSON.stringify(err));
            throw new Error (err);
        }
        if(!r.rows.length){
            cb(null,dayId);
            return 1;
        }
        console.log('processing '+dayId+' with rows '+r.rows.length);
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
        if(!output.ts){
            console.log('no output.ts');
            return cb(null,dayId) ;
        }

        newdoc.data=output;

        var rcon = clientBulk.db(dyv_db);
        var lcon = clientLocal.db(dyv_db);
        lcon.saveDoc(newdoc
                   ,function(err,r){
                       if(err){
                           console.log('croaked in save new doc '+JSON.stringify(err));
                       }
                       // bulk drop old docs
                       if(bulkdocs.length){
                           return rcon.bulkDocs({'docs':bulkdocs},function(err,r){
                               if(err){
                                   console.log('bulkdocs failed'+ err);
                                   // don't bail out on err.  I'm going to delete the db anyway
                                   // cb(err,dayId);
                                   // throw new Error();
                               }
                               // ignore the bulkdocs response
                               console.log('bulkdocs done');
                               return cb(null,dayId);
                           });
                       }else{
                           return cb(null,dayId);
                       }
                   });
        return true;
    };
};


var doAllDocs= function(dyv_db,params){
    return function(q,cb){
        // use just one client to queue up the load
        var db2 = clientRemote.db(dyv_db);
        var args = params;
        _.extend(args,q);
        db2.allDocs({}
                   ,args
                   ,vdsDayDocs(q.startkey,dyv_db,cb));

    }
};


var vdsFixer = function( vds_id, district, year, cb ){
    return function(err,checkedout){
        var dyv_db = ['vdsdata',district,year,vds_id].join('%2f');
        var dyv_db_local = ['vdsdata',district,year,vds_id].join('/');
        if(err){
            console.log(dyv_db+' not going: '+JSON.stringify(err));
            return cb();
        }

        // couchdb wants the '/', but URLs need the '%2f'
        console.log(dyv_db);
        // make sure a clean local db exists
        helpers.fresh_db(dyv_db,clientLocal);

        //iterate over days in the year using a date object
        // from year, jan, 1 00:00 to year+1, jan, 1 00:00
        var endymd = new Date(year+1, 0, 1, 0, 0, 0).getTime();

        var ymd=new Date(year, 0, 1, 0, 0, 0)
        var queries = [];
        while(ymd.getTime()<endymd){
            var dateString = CDBDateString(ymd);
            var timeString = '00:00:00';
            var dayId = [vds_id,dateString,timeString].join(' ');
            // tomorrow, and also increment ymd for the
            // loop here!
            ymd.setDate(ymd.getDate()+1)
            dateString = CDBDateString(ymd);

            var nextDayId =  [vds_id,dateString,timeString].join(' ');
            queries.push({'startkey':dayId
                         ,'endkey':nextDayId
                         });
        }

        var fn = doAllDocs(dyv_db,{'include_docs':true
                                  ,'inclusive_end':false});
        var doneCount = 1;

        var cb2 = function(err,other){
            if(err){
                console.log('outer callback error: '+JSON.stringify(err));
            }else{
	        // never mind about compaction at this time
	        var dbremote = clientRemote.db(dyv_db);
                console.log('going to remove/recreate remote database');
                // throw new Error('stop now');
	        dbremote.remove(function(e,a){
		    if(e) console.log('delete error: e is ' + JSON.stringify(e));
		    if(a) console.log('delete repsonse is ' + JSON.stringify(a));
		    dbremote.create(function(e){
                        if(e) throw new Error('failed to create db for '+dyv_db+JSON.stringify(e))
                        if(pullReplicate){
                            clientRemote.replicate( [local_replicate_root
                                                    ,dyv_db].join('/')
                                                    ,dyv_db_local
					            ,{'create_target':true}
					            ,function(e,r){
					                if(e) console.log('replicate error: e is ' + JSON.stringify(e));
					                console.log('replication from  '
							           + [local_replicate_root,dyv_db].join('/')
                                                                   + ' pulled to '
                                                                   + dyv_db_local);
					                return 1;
					            });
                        }else{
                            // push replicate
                            clientLocal.replicate( dyv_db_local
                                         ,[remote_replicate_root
                                          ,dyv_db].join('/')
					  ,{'create_target':true}
					  ,function(e,r){
					      if(e) console.log('replicate error: e is ' + JSON.stringify(e));
					      console.log('replication from  '
							 + dyv_db_local
                                                         + ' pushed to '
                                                         + [remote_replicate_root,dyv_db].join('/'));
					      return 1;
					  });
                        }
                        cb(null,vds_id);
	            });
                });
            }
            return 1;
        };
        return asyncMap(queries,fn,cb2);
    };
};


// fake it
vdslist =[
 // 1209304
// ,
1209438
,1201705
,1201453
,1213892
,1201977
,1201853
,1203871
,1202527
,1213650
,1202537
,1211218
,1214118
,1208108
,1213680
,1214063
,1204153
,1208180
,1203850
,1204064
,1204142
,1204038
,1208110
,1214062
,1204105
,1203649
,1203922
,1208176
,1203825
,1203513
,1203549
,1204023
,1203524
,1203665
,1212725
,1212489
,1212484
,1210619
,1210488
,1202337
,1208945
,1208758
,1208703
,1208809
,1212472
,1210491
,1212075
,1208941
,1202464
,1213672
,1213618
,1202365
,1213105
,1202308
,1202278
,1213106
,1202040
,1202322
,1202078
,1202394
,1202132
,1202353
,1202475
,1202290
,1204750
,1204950
,1205567
,1213570
,1204472
,1213527
,1205493
,1204230
,1205250
,1204825
,1212588
,1204565
,1210190
,1203148
,1203076
,1202993
,1204501
,1204904
,1204244
,1205165
,1204672
,1204486
,1204766
,1205135
,1204731
,1204316
,1205157
,1204442
,1214020
,1205384
,1204216
,1204643
,1211829
,1205297
,1205139
,1213699
,1204688
,1204290
,1204193
,1204507
,1205440
,1210882
,1204841
,1213528
,1204737
,1205122
,1205276
,1210141
,1204552
,1205169
,1204697
,1204458
,1204869
,1204695
,1204967
,1204577
,1212001
,1204816
,1205310
,1204401
,1205583
,1212611
,1204521
,1204682
,1212645
,1204478
,1205246
,1205286
,1204586
,1205062
,1210841
,1210993
,1204428
,1204701
,1205269
,1205421
,1205189
,1205182
,1204415
,1204409
,1205364
,1210909
,1204937
,1211107
,1204798
,1205028
,1211816
,1211853
,1205290
,1205105
,1210856
,1205012
,1212662
,1205478
,1213571
,1204621
,1213229
,1205532
,1210172
,1203221
,1211173
,1203071
,1203021
,1203124
,1212955
,1203342
,1203400
,1212885
,1203288
,1212916
,1212865
,1203090
,1209888
,1203331
,1212901
,1212938
,1209860
,1203206
,1203050
,1212935
          ];

var compaction = [];

var vdsLoop = function(){};
function remote_compact(v,cb){
    var rdb = clientRemote.db(['vdsdata',districts[0],years[0],v].join('%2f'));
    return rdb.compact(cb);
}

var iterationer = function(err,vds_id){
    if(err){
        console.log('bail out on error condition '+JSON.stringify(err));
        throw new Error(err);
    }
    // if(vds_id){
    //     remote_compact(vds_id,function(e,b){
    //         console.log('done with compaction call '+ vds_id);
    //     });
    // }
    if(vdslist.length){
        vdsLoop();
    }
    return 1;
}

vdsLoop = function(){
    var vds_id = vdslist.shift();
    var dyv_db = ['vdsdata',districts[0],years[0],vds_id].join('%2f');
    helpers.checkout(dyv_db,clientRemote,vdsFixer(vds_id,districts[0],years[0],iterationer));
    return 1;
};

iterationer();


/** done vdsids
 *
 * // 1201054 ,1201087 ,1201100 ,1201112 ,1201125 ,1201159 ,1201171 ,
// 1201185 ,1201197 ,1201211 ,1201217 ,1201222 ,1201254 ,1201270
// ,1201283 ,1201292 ,1201298 ,1201333 ,1201350 ,1201365 ,1201382
// ,1201399 ,1201430 ,1201469 ,1201510 ,1201525 ,1201541 ,1201589
// ,1201606 ,1201620 ,1201637 ,1201671 ,1201687 ,1201735 ,1201751
// ,1201787 ,1201823 ,1201911 ,1201923 ,1201959 ,1201985 ,1201987
// ,1201998 ,1202011 ,1202024 ,1202053 ,1202093 ,1202105 ,1202118
// ,1202146 ,1202160 ,1202172 ,1202186 ,1202201 ,1202215 ,1202230
// ,1202248 ,1202263 ,1202373 ,1202380 ,1202408 ,1202422 ,1202436
// ,1202451 ,1202522 ,1202549 ,1203481 ,1203495 ,1203506 ,1203536
// ,1203561 ,1203589 ,1203615 ,1203631 ,1203642 ,1203654 ,1203679
// ,1203692 ,1203704 ,1203718 ,1203762 ,1203774 ,1203788 ,1203793
// ,1203799 ,1203813 ,1203831 ,1203845 ,1203861 ,1203866 ,1203886
// ,1203896 ,1203909 ,1203927 ,1203931 ,1203944 ,1203957 ,1203972
// ,1203984 ,1203998 ,1204010 ,1204052 ,1204076 ,1204091 1204117
// ,1204159 ,1204168 ,1204181 ,1204198 ,1204211 ,1204255 ,1204268 ,
// 1204273 ,1204279 ,1204295 ,1204301 ,1204306 ,1204328 ,1204340
// ,1204345 ,1204357 ,1204372 ,1204384 ,1204390 ,1204395 ,1204422
// ,1204436 , ,1204515 ,1204532 ,1204538 ,1204546 ,1204559 ,1204571
// ,1204615 ,1204650 ,1204665 ,1204703 ,1204761 ,1204781 ,1204787
// ,1204808 ,1204861 ,1204886 ,1204924 ,1204944 ,1204982 ,1204997
// ,1205045 ,1205071 ,1205152 ,1205166 ,1205168 ,1205175 ,1205193
// ,1205204 ,1205215 ,1205225 ,1205230 ,1205303 ,1205320 ,1205324
// ,1205330 ,1205335 ,1205341 ,1205375 ,1205380 ,1205395 ,1205409
// ,1205432 ,1205452 ,1205463 ,1205517 ,1205528 ,1205541 ,1205553
// ,1205562 ,1205572 ,1205590 ,1208121 ,1208134 ,1208147 ,1208151
// ,1208161 ,1208190 ,1208199 ,1208208 ,1208226 ,1208230 ,1208240
// ,1208260 ,1208701 ,1208760 ,1208789 ,1208886 ,1208942 ,1208943
// ,1208944 ,1208976 ,1209059 ,1209091 ,1209092 ,1209162 ,1209176
// ,1209178 ,1209189 ,1209204 ,1209243 ,1209259 ,1209261 ,1209274
// ,1209276 ,1209289 ,1209291 ,1209306 ,1209319 ,1209321 ,1209334
// ,1209353 ,1209355 ,1209372 ,1209374 ,1209407 ,1209424 ,1209454
// ,1210440 ,1210441 ,1210446 ,1210542 ,1210543 ,1210551 ,1210618
// ,1210872 ,1210895 ,1210908 ,1210926 ,1210955 , 1204453 ,1210972
// ,1210974 ,1210991 ,1211075 ,1211221 ,1211623 ,1211870 ,1211907
// ,1212074 ,1212094 ,1212115 ,1212216 ,1212218 ,1212423 ,1212454
// ,1212455 ,1212473 ,1212480 ,1212649 ,1213023 ,1213036 ,1213041
// ,1213060 ,1213087 ,1213088 ,1213121 ,1213122 ,1213133 ,1213215
// ,1213398 ,1213624 ,1213673 ,1213686 ,1213700 ,1213721 ,1213741
// ,1213742 ,1213891 ,1213963 ,1213985 ,1213986 ,1214005 ,1214006
// ,1214080 ,1214117
 *
 */