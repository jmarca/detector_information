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

var cluser = process.env.COUCHDB_LOCALUSER ;
var clpass = process.env.COUCHDB_LOCALPASS ;
var clhost = process.env.COUCHDB_LOCALHOST ;
var clport = process.env.COUCHDB_LOCALPORT ;


var clientOuter = couchdb.createClient(clport, clhost, cluser, clpass);
var clientBulk = couchdb.createClient(clport, clhost, cluser, clpass);
var clientRemote = couchdb.createClient(cport, chost, cuser, cpass);

var replicate_root = ['http://',[clhost,clport].join(':')].join('');


var years = [2007];
var districts = ['d12'];


// big list of detectors
vdslist = [// 1201054
	   //,1201087
	   //,1201100
	   //,1201112
	   //,1201125
	   //,1201159
	   ,1201171
	   ,1201185
	   ,1201197
	   ,1201211
	   ,1201217
	   ,1201222
	   ,1201254
	   ,1201270
	   ,1201283
	   ,1201292
	   ,1201298
	   ,1201333
	   ,1201350
	   ,1201365
	   ,1201382
	   ,1201399
	   ,1201430
	   ,1201469
	   ,1201510
	   ,1201525
	   ,1201541
	   ,1201589
	   ,1201606
	   ,1201620
	   ,1201637
	   ,1201671
	   ,1201687
	   ,1201735
	   ,1201751
	   ,1201787
	   ,1201823
	   ,1201911
	   ,1201923
	   ,1201959
	   ,1201985
	   ,1201987
	   ,1201998
	   ,1202011
	   ,1202024
	   ,1202053
	   ,1202093
	   ,1202105
	   ,1202118
	   ,1202146
	   ,1202160
	   ,1202172
	   ,1202186
	   ,1202201
	   ,1202215
	   ,1202230
	   ,1202248
	   ,1202263
	   ,1202373
	   ,1202380
	   ,1202408
	   ,1202422
	   ,1202436
	   ,1202451
	   ,1202522
	   ,1202549
	   ,1203481
	   ,1203495
	   ,1203506
	   ,1203536
	   ,1203561
	   ,1203589
	   ,1203615
	   ,1203631
	   ,1203642
	   ,1203654
	   ,1203679
	   ,1203692
	   ,1203704
	   ,1203718
	   ,1203762
	   ,1203774
	   ,1203788
	   ,1203793
	   ,1203799
	   ,1203813
	   ,1203831
	   ,1203845
	   ,1203861
	   ,1203866
	   ,1203886
	   ,1203896
	   ,1203909
	   ,1203927
	   ,1203931
	   ,1203944
	   ,1203957
	   ,1203972
	   ,1203984
	   ,1203998
	   ,1204010
	   ,1204052
	   ,1204076
	   ,1204091
	    ,1204117
	   ,1204159
	   ,1204168
	   ,1204181
	   ,1204198
	   ,1204211
	   ,1204255
	   ,1204268
	   ,1204273
	   ,1204279
	   ,1204295
	   ,1204301
	   ,1204306
	   ,1204328
	   ,1204340
	   ,1204345
	   ,1204357
	   ,1204372
	   ,1204384
	   ,1204390
	   ,1204395
	   ,1204422
	   ,1204436
	   ,1204515
	   ,1204532
	   ,1204538
	   ,1204546
	   ,1204559
	   ,1204571
	   ,1204615
	   ,1204650
	   ,1204665
	   ,1204703
	   ,1204761
	   ,1204781
	   ,1204787
	   ,1204808
	   ,1204861
	   ,1204886
	   ,1204924
	   ,1204944
	   ,1204982
	   ,1204997
	   ,1205045
	   ,1205071
	   ,1205152
	   ,1205166
	   ,1205168
	   ,1205175
	   ,1205193
	   ,1205204
	   ,1205215
	   ,1205225
	   ,1205230
	   ,1205303
	   ,1205320
	   ,1205324
	   ,1205330
	   ,1205335
	   ,1205341
	   ,1205375
	   ,1205380
	   ,1205395
	   ,1205409
	   ,1205432
	   ,1205452
	   ,1205463
	   ,1205517
	   ,1205528
	   ,1205541
	   ,1205553
	   ,1205562
	   ,1205572
	   ,1205590
	   ,1208121
	   ,1208134
	   ,1208147
	   ,1208151
	   ,1208161
	   ,1208190
	   ,1208199
	   ,1208208
	   ,1208226
	   ,1208230
	   ,1208240
	   ,1208260
	   ,1208701
	   ,1208760
	   ,1208789
	   ,1208886
	   ,1208942
	   ,1208943
	   ,1208944
	   ,1208976
	   ,1209059
	   ,1209091
	   ,1209092
	   ,1209162
	   ,1209176
	   ,1209178
	   ,1209189
	   ,1209204
	   ,1209243
	   ,1209259
	   ,1209261
	   ,1209274
	   ,1209276
	   ,1209289
	   ,1209291
	   ,1209306
	   ,1209319
	   ,1209321
	   ,1209334
	   ,1209353
	   ,1209355
	   ,1209372
	   ,1209374
	   ,1209407
	   ,1209424
	   ,1209454
	   ,1210440
	   ,1210441
	   ,1210446
	   ,1210542
	   ,1210543
	   ,1210551
	   ,1210618
	   ,1210872
	   ,1210895
	   ,1210908
	   ,1210926
	   ,1210955
          , 1204453
	   ,1210972
	   ,1210974
	   ,1210991
	   ,1211075
	   ,1211221
	   ,1211623
	   ,1211870
	   ,1211907
	   ,1212074
	   ,1212094
	   ,1212115
	   ,1212216
	   ,1212218
	   ,1212423
	   ,1212454
	   ,1212455
	   ,1212473
	   ,1212480
	   ,1212649
	   ,1213023
	   ,1213036
	   ,1213041
	   ,1213060
	   ,1213087
	   ,1213088
	   ,1213121
	   ,1213122
	   ,1213133
	   ,1213215
	   ,1213398
	   ,1213624
	   ,1213673
	   ,1213686
	   ,1213700
	   ,1213721
	   ,1213741
	   ,1213742
	   ,1213891
	   ,1213963
	   ,1213985
	   ,1213986
	   ,1214005
	   ,1214006
	   ,1214080
	   ,1214117
];


function replicate( vds_id, district, year, cb ){

    var local_dyv_db = ['vdsdata',district,year,vds_id].join('/');
    var remote_dyv_db = ['vdsdata',district,year,vds_id].join('%2f');
    // use remote version in urls

    clientRemote.replicate( [replicate_root,remote_dyv_db].join('/')
			    ,local_dyv_db
			    ,{'create_target':true}
			    // ,function(e,r){
			    // 	if(e) console.log('e is ' + JSON.stringify(e));
			    // 	if(r) console.log('r is ' + JSON.stringify(r));
			    // 	console.log(['called replication for' 
			    // 		     ,remote_dyv_db
			    // 		     ,local_dyv_db
			    // 		     ,vds_id].join(' '));
			    // 	return 1;
			    // }
			  );
}

function remote_compact(v,cb){
    var rdb = clientRemote.db(['vdsdata',districts[0],years[0],v].join('%2f'));
    return rdb.compact(cb);
}

function remote_tasks(cb){
    
    return clientRemote.activeTasks(cb, function(err,result){
	if(err) console.log('err is '+JSON.stringify(err));
	if(result) console.log('result is '+JSON.stringify(result));
	if(cb) cb(err,result);
    });

}

function replicate_thenext(vds){
    replicate(vds,districts[0],years[0],function(){
	console.log('called replicate');
    });
}
function compact_thenext(vds){
    remote_compact(vds,function(){
	console.log('called compact');
    });
}

function decide(err,status){
    if(err) throw new Error(JSON.stringify(err));
    var blarb = status.length ? status.length : 'zero' ;
    if(!status.length || status.length < 3 ){
	console.log('calling another replication, currently have '+blarb + ' jobs running');
	compact_thenext(vdslist.shift());
    }else{
	// console.log('wait, alread have '+blarb + ' jobs running');
    }
    semiInfiniteLoop();
}

function semiInfiniteLoop(){
    if(vdslist.length){
	setTimeout(remote_tasks, 5000, decide );
    }
}

remote_tasks(decide);
