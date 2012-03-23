//var couchdb = require('couchdb');
var request = require("request");

var CDBDateString = require('./utils/formatting.js');
var _ = require('underscore');

// first, what is the connection
var env = process.env;
var  querystring = require('querystring');


exports.max_risk = max_risk;
exports.yearly_risk = yearly_risk;
exports.yearly_risk_models = yearly_risk_models;
exports.daily_risk = daily_risk;

// Encode only key, startkey and endkey as JSON
function couchQuery(query) {
        var q={};
        _.each(query,function(v,k){
            if (['key', 'startkey', 'endkey'].indexOf(k) != -1) {
                // if(! isNaN(v) ) v -= 0.0;
                q[k] = JSON.stringify(v)
            } else {
                q[k] = String(v);
            }
        });
        console.log(q);
        return querystring.stringify(q);
}

    /** create RequesObject creator
     *
     * initial call with uri
     *
     * usage: createRequestObject(target, method(optional), query(optional), callback);
     * returns: a Request object
     *
     */
function requestGenerator(_uri,cuser,cpass,debug){
    return function createRequestObject(){
        var args = Array.prototype.slice.call(arguments)
          , target = args.shift() || ''
          , cb_ = args.pop(); // last argument is always the callback, possibly null
        var method = args.shift() || 'GET';
        var query = couchQuery(args.shift());
        var uri = query ? _uri + '/'+target+ '?'+query : _uri + '/' + target ;
        if(debug) console.log('uri to couchdb is '+uri);
        var options={'headers':{}
                    ,'uri':uri
                    ,'method':method
                     //,'body':''
                    };
        options.headers.authorization = "Basic " + new Buffer(cuser + ":" + cpass).toString('base64');
        options.headers.accept='application/json';
        console.log(options)
	return cb_ ? request(options,cb_) : request(options);
    }
}
/**
 * max_risk
 *
 * simple client to get vdsid data between a start and end timestamp
 */
function max_risk(options,cb_){


    var cuser = options.user || process.env.COUCHDB_USER ;
    var cpass = options.pass || process.env.COUCHDB_PASS ;
    var chost = options.host || process.env.COUCHDB_HOST ;
    var cport = options.port || process.env.COUCHDB_PORT || 5984;
    // I imagine that maybe this will be run for multiple kinds of
    // predictions, so paramertize via the design doc and view names
    var dbname = options.dbname || 'safetynet';
    var designdoc = options.designdoc || 'risk';
    var view = options.view || 'risk';


    /** createRequesObject
     *
     * usage: createRequestObject(target, method(optional), query(optional), callback);
     * returns: a Request object
     *
     */
    var  createRequestObject = requestGenerator( 'http://'+chost+':'+cport,cuser,cpass , false);

    // I have a thought here that perhaps vdsid wants to be an array of items.
    // but first work out the bugs with a single vdsid

    var possibleParams = ['vdsid','year','month','day','level'];

    return function max_risk(req,res,next){

        console.log('handling request for max_risk');
        var activeParams = possibleParams.filter(function(a){return req.params[a] !==undefined;});
        var query = {};
        console.log(JSON.stringify(activeParams));
        //if(activeParams.length < 2 || !activeParams[0]==='vdsid') return next();
        // formulate for document id ranges from starte to end times

        var year = req.params.year;
        var month= req.params.month || '01';
        var day  = req.params.day || '01';
        console.log([year,month,day].join('/'));
        var start = [year,month-0.0,day-0.0];
        var vdsid = match_vdsid(req.params.vdsid);
        var district = match_district(req.params.vdsid);
        district = district < 10 ? 'd0'+district : 'd'+district;
        var startkey = [start[0],start[1],start[2],vdsid];

        // here is how a vdsid like "all" is handled...
        // just get everything
        if(!vdsid) startkey= [start[0],start[1],start[2]];

        // while testing, get one day
        if(req.params.day){
            start[2] += 1
        }else if(req.params.month){
            start[1] += 1
        }else{
            start[0] += 1
        }
        var endkey = [start[0],start[1],start[2],vdsid];

        // here is how a vdsid like "all" is handled...
        // just get everything in D12 ending with  vdsid 1300000
        if(!vdsid) endkey= [start[0],start[1],start[2]];

        var cdb_options = {'startkey':startkey
                          ,'endkey':endkey
                          ,'reduce':false
                         };


        // And finally, the last step in how a vdsid like "all" is handled...
        //
        // ask for reduce, and unless a level is asked for, get exact,
        // which will give the daily value for all detectors for the
        // given day
        //
        if(!vdsid){
            // cdb_options.reduce=true;
            // cdb_options.group=true;
            // if(req.params.level){
            //     cdb_options['group_level']=req.params.level;
            // }
        }

        console.log('calling couch with '+JSON.stringify({db:[dbname
                                                             ,'_design'
                                                             ,designdoc
                                                             ,'_view'
                                                             ,view].join('/')
                                                         ,options:cdb_options
                                                         }))
        var x = createRequestObject([dbname
                                    ,'_design'
                                    ,designdoc
                                    ,'_view'
                                    ,view].join('/')
                                    ,'GET'
                                    ,cdb_options
                                    ,null);
        x.pipe(res);
        return 1;
    };

    return 1;
}

/**
 * yearly_risk
 *
 * get the yearly risk by pulling the doc related to the year and vdsid from the couchdb
 */
function yearly_risk(options,cb_){

    var cuser = options.user || process.env.COUCHDB_USER ;
    var cpass = options.pass || process.env.COUCHDB_PASS ;
    var chost = options.host || process.env.COUCHDB_HOST ;
    var cport = options.port || process.env.COUCHDB_PORT || 5984;
    // I imagine that maybe this will be run for multiple kinds of
    // predictions, but no clue how to parameterize at the mo'
    var dbname = options.dbname || 'safetynet%2frisk';
    var designdoc = options.designdoc || 'anyaccident';
    var view = options.view || 'id_day';

    var createRequestObject = requestGenerator('http://'+chost+':'+cport,cuser,cpass,true);

    // I have a thought here that perhaps vdsid wants to be an array of items.
    // but first work out the bugs with a single vdsid

    var possibleParams = ['vdsid','year'];

    return function yearly_risk(req,res,next){

        // console.log('handling request for yearly_risk');
        var activeParams = possibleParams.filter(function(a){return req.params[a] !==undefined;});
        var query = {};
        console.log(JSON.stringify(activeParams));
        //if(activeParams.length < 2 || !activeParams[0]==='vdsid') return next();
        // formulate for document id ranges from starte to end times

        var vdsid = match_vdsid(req.params.vdsid);
        var year = req.params.year;
        var startkey = [vdsid,year];
        var endkey = [vdsid,year+1];

        var cdb_options = {'startkey':startkey
                          ,'endkey':endkey
                          ,'reduce':true
                          ,'group':true
                          ,'group_level':4
                         };
        var x = createRequestObject([dbname
                                    ,'_design'
                                    ,designdoc
                                    ,'_view'
                                    ,view].join('/')
                                   ,'GET'
                                   ,cdb_options
                                   ,null);
        x.pipe(res);
        return 1;
    };

    return 1;
}

/**
 * daily_risk
 *
 * get the daily risk by pulling the doc related to the year and vdsid from the couchdb
 */
function daily_risk(options,cb_){

    var cuser = options.user || process.env.COUCHDB_USER ;
    var cpass = options.pass || process.env.COUCHDB_PASS ;
    var chost = options.host || process.env.COUCHDB_HOST ;
    var cport = options.port || process.env.COUCHDB_PORT || 5984;
    // I imagine that maybe this will be run for multiple kinds of
    // predictions
    var designdoc = options.designdoc || 'risk';
    var view = options.view || 'risk';

    var createRequestObject = requestGenerator('http://'+chost+':'+cport,cuser,cpass,true);

    // I have a thought here that perhaps vdsid wants to be an array of items.
    // but first work out the bugs with a single vdsid

    var possibleParams = ['vdsid','year','month','day'];

    return function daily_risk(req,res,next){

        console.log('handling request for daily_risk');
        var activeParams = possibleParams.filter(function(a){return req.params[a] !==undefined;});
        var query = {};
        console.log(JSON.stringify(activeParams));
        //if(activeParams.length < 2 || !activeParams[0]==='vdsid') return next();
        var activeValues = _.map(activeParams,function(k){ return req.params[k] ; });
        var vdsid = activeValues.shift();
        var district = match_district(vdsid);
        district = district < 10 ? 'd0'+district : 'd'+district;
        var start = new Date(activeValues.join('-'));
        var startkey = [CDBDateString(start),vdsid];
        if(req.params.day){
            start.setDate( start.getDate() +1 )
        }else if(req.params.month){
            start.setMonth( start.getMonth() +1 )
        }
        var endkey =  [CDBDateString(start),vdsid];


        var cdb_options = {'startkey':startkey
                          ,'endkey':endkey
                          ,'reduce':false
                         };

        var x = createRequestObject([['vdsdata',district,activeValues[0],vdsid].join('%2f')
                                    ,'_design'
                                    ,designdoc
                                    ,'_view'
                                    ,view].join('/')
                                   ,'GET'
                                   ,cdb_options
                                   ,null);
        x.pipe(res);
        return 1;
    };

    return 1;
}

/**
 * daily_risk_ymd
 *
 * get the daily risk by pulling the doc related to the year and vdsid from the couchdb
 */
function daily_risk_ymd(options,cb_){

    var cuser = options.user || process.env.COUCHDB_USER ;
    var cpass = options.pass || process.env.COUCHDB_PASS ;
    var chost = options.host || process.env.COUCHDB_HOST ;
    var cport = options.port || process.env.COUCHDB_PORT || 5984;
    // I imagine that maybe this will be run for multiple kinds of
    // predictions
    var designdoc = options.designdoc || 'risk';
    var view = options.view || 'risk';

    var createRequestObject = requestGenerator('http://'+chost+':'+cport,cuser,cpass,true);

    // I have a thought here that perhaps vdsid wants to be an array of items.
    // but first work out the bugs with a single vdsid

    var possibleParams = ['vdsid','year','month','day'];

    return function daily_risk_ymd(req,res,next){

        console.log('handling request for daily_risk_ymd');
        var activeParams = possibleParams.filter(function(a){return req.params[a] !==undefined;});
        var query = {};
        console.log(JSON.stringify(activeParams));
        //if(activeParams.length < 2 || !activeParams[0]==='vdsid') return next();
        var activeValues = _.map(activeParams,function(k){ return req.params[k] ; });
        var vdsid = activeValues.shift();
        var district = match_district(vdsid);
        district = district < 10 ? 'd0'+district : 'd'+district;
        var startkey = _.flatten([activeValues,vdsid])
        var endkey = _.flatten([activeValues]) // lazy lazy copy by value
        if(req.params.day){
            endkey[2]+=1
        }else if(req.params.month){
            endkey[1]+=1
        }
        endkey =  _.flatten[endkey,vdsid];


        var cdb_options = {'startkey':startkey
                          ,'endkey':endkey
                          ,'reduce':false
                         };

        var x = createRequestObject([['vdsdata',district,activeValues[0],vdsid].join('%2f')
                                    ,'_design'
                                    ,designdoc
                                    ,'_view'
                                    ,view].join('/')
                                   ,'GET'
                                   ,cdb_options
                                   ,null);
        x.pipe(res);
        return 1;
    };

    return 1;
}


function db_view ( model ){
    var url;
    if(model == 'anyaccident'){
        url = ['safetynet%2frisk'
              ,'_design'
              ,'anyaccident'
              ,'_view'
              ,'id_day'].join('/')
    } else {
        var db = ['safetynet%2f',model].join('');
        var view = ['_design/',model,'/_view/id_day'].join('');
        url = [db,view].join('/')
    }
    return url;
}

/**
 * yearly_risk_models
 *
 * get the yearly risk by pulling the doc related to the year and vdsid from the couchdb
 * allow the model to be specified
 */
function yearly_risk_models(options,cb_){

    var cuser = options.user || process.env.COUCHDB_USER ;
    var cpass = options.pass || process.env.COUCHDB_PASS ;
    var chost = options.host || process.env.COUCHDB_HOST ;
    var cport = options.port || process.env.COUCHDB_PORT || 5984;
    // I imagine that maybe this will be run for multiple kinds of
    // predictions, but no clue how to parameterize at the mo'

    var createRequestObject = requestGenerator('http://'+chost+':'+cport,cuser,cpass,true);

    // I have a thought here that perhaps vdsid wants to be an array of items.
    // but first work out the bugs with a single vdsid
    // On thinkint more, it would require yet another change to couchdb
    // I would need to sort by freeway and milepost, not by vdsid.  And I can't do that without
    // a hack to the view to allow a look up of freeway and milepost.  Hmm.. wait a minute.
    var possibleParams = ['vdsid','fwy','pmstart','pmend','year','model','dir'];

    return function yearly_risk_models(req,res,next){

        // console.log('handling request for yearly_risk');
        var activeParams = possibleParams.filter(function(a){return req.params[a] !==undefined;});
        var query = {};
        //console.log(JSON.stringify(activeParams));
        //if(activeParams.length < 2 || !activeParams[0]==='vdsid') return next();
        // formulate for document id ranges from starte to end times
        var year = req.params.year;
        var startkey;
        var endkey;
        if(activeParams.indexOf('vdsid') != -1){
            var vdsid = match_vdsid(req.params.vdsid);
            startkey = [vdsid,year];
            endkey = [vdsid,year+1];
        }else{
            var fwy = +req.params.fwy;
            var dir = req.params.dir;

            if(activeParams.indexOf('pmstart')){
                startkey = [fwy,dir,+req.params.pmstart,year];
                endkey = [fwy,dir,+req.params.pmend,year+1];
            }else{
                startkey = [fwy,dir,0,year];
                endkey = [fwy,dir,'Z',year+1];
            }
            console.log([startkey,endkey]);

        }

        var cdb_options = {'startkey':startkey
                          ,'endkey':endkey
                          ,'group':true //exact grouping
                          };
        var url = db_view(req.params.model ? req.params.model : 'anyaccident');
        var x = createRequestObject(url
                                   ,'GET'
                                   ,cdb_options
                                   ,null);
        return x.pipe(res);
    };

    return null;
}

function match_district (vdsid){
    var district_regex = /(\d{1,2})\d{5}$/;
    var match = district_regex.exec(vdsid);
    if(match && match.length)    return match[1];
    return null;
}

function match_vdsid (id){
    var vdsid_regex = /(\d{6,7})$/;
    var match = vdsid_regex.exec(id);
    if(match && match.length)    return match[1];
    return null;
}


var d3 = {'stats':{}};
d3.max = function(array, f) {
  var i = -1,
      n = array.length,
      a,
      b;
  if (arguments.length === 1) {
    while (++i < n && ((a = array[i]) == null || a != a)) a = undefined;
    while (++i < n) if ((b = array[i]) != null && b > a) a = b;
  } else {
    while (++i < n && ((a = f.call(array, array[i], i)) == null || a != a)) a = undefined;
    while (++i < n) if ((b = f.call(array, array[i], i)) != null && b > a) a = b;
  }
  return a;
};
d3.min = function(array, f) {
  var i = -1,
      n = array.length,
      a,
      b;
  if (arguments.length === 1) {
    while (++i < n && ((a = array[i]) == null || a != a)) a = undefined;
    while (++i < n) if ((b = array[i]) != null && a > b) a = b;
  } else {
    while (++i < n && ((a = f.call(array, array[i], i)) == null || a != a)) a = undefined;
    while (++i < n) if ((b = f.call(array, array[i], i)) != null && a > b) a = b;
  }
  return a;
};

// Welford's algorithm.
d3.stats.mean = function(x) {
  var n = x.length;
  if (n === 0) return NaN;
  var m = 0,
      i = -1;
  while (++i < n) m += (x[i] - m) / (i + 1);
  return m;
};


