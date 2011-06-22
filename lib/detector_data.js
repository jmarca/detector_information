var fs = require('fs');
var couchdb = require('couchdb');
var crypto = require('crypto');
var _ = require('underscore');

// this program is supposed to get data from couchdb for a day,
// collate the multiple docs into a single doc, and save it back

// first, what is the connection
var env = process.env;
var puser = process.env.PSQL_USER ;
var ppass = process.env.PSQL_PASS ;
var phost = process.env.PSQL_HOST ;

var cuser = process.env.COUCHDB_USER ;
var cpass = process.env.COUCHDB_PASS ;
var chost = process.env.COUCHDB_HOST ;
var cport = process.env.COUCHDB_PORT ;


var years = 2007,2008,2009];
var districts = [
  'd03'
  ,'d04'
  ,'d05'
  ,'d06'
  ,'d07'
  ,'d08'
  ,'d09'
  ,'d10'
  ,'d11'
  ,'d12'
];
var d1 = 'var_27';
var v1 = 'complete'

// or whatever the f the for in loop syntax is
// and make this a function so I can call backy it
for var year in years {
   for var district in districts {
     var dy_db = ['vdsdata',district,year].join('/');
     var client = couchdb.createClient(cport, chost, cuser, cpass);
     var db = client.db(dy_db);
     // call the view that gets the vdsids
     db.view(d1
            ,v1
            , function(er, r) {
              if (er){
                console.log('error: '+JSON.stringify(er));
                //}else{
                //console.log('done with query, r is '+JSON.stringify(r));
              }
              var features={};
              for (var row_i in r.rows){
                var row = r.rows[row_i];
                var vds_id = row['_id'];
                // now for each vds id, fix the data for the selected year
                var dyv_db = [dy_db,vds_id].join('/');
                var client2 = couchdb.createClient(cport, chost, cuser, cpass);
                var db2 = client2.db(dyv_db);

//iterate over days in the year using a date object from year, jan, 1 00:00 to year+1, jan, 1 00:00
                var day, nextday;//, combining vdsid and ts

                db2.allDocs({'startkey':day,'endkey':nextday}
                           ,{'include_docs':true}
                           ,function(err,r){
                             if (err){
                               console.log('error: '+JSON.stringify(er));
                             }else{
 //                              create a time sequence of each 30s period in the day
                              tsmap = {time sequence,position in array };
                               var daily_output = {};
                               var time_index = 0;
                               for (var row_i in r.rows){
                                 var row = r.rows[row_i];
                                 while ((start + 30*time_index)<row.ts){time_index++}
                                 for (var key in row){
                                   if(!output[key]){output[key]=[]}
                                   output[key][time_index]=row[key];
                                 }
                               }
                               // save that document as a new document for the entire day
                               var id='day'+start;
                               db2.saveDoc(id,output,function(err){next(err);})
                             }
                                   });
                      }






              }
              }

            });
