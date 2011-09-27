//var couchdb = require('couchdb');
var request = require("request");

var CDBDateString = require('./utils/formatting.js');
var _ = require('underscore');

// first, what is the connection
var env = process.env;
var  querystring = require('querystring');


exports.max_risk = max_risk;

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

    // Encode only key, startkey and endkey as JSON
    function couchQuery(query) {
    if(!query) return null;
    for (var k in query) {
        if (['key', 'startkey', 'endkey'].indexOf(k) != -1) {
        query[k] = JSON.stringify(query[k]);
        } else {
        query[k] = String(query[k]);
        }
    }
    return querystring.stringify(query);
    }

    /** createRequesObject
     *
     * usage: createRequestObject(target, method(optional), query(optional), callback);
     * returns: a Request object
     *
     */
    function createRequestObject(){
    var args = Array.prototype.slice.call(arguments)
    , target = args.shift() || ''
    , cb_ = args.pop(); // last argument is always the callback, possibly null
    var method = args.shift() || 'GET';
    var query = couchQuery(args.shift());
    var uri = 'http://'+chost+':'+cport+'/'+target;
    uri = query ? uri + '?'+query : uri ;
    console.log('uri to couchdb is '+uri);
    var options={
        'headers':{}
        ,'uri':uri
        ,'method':method
        //,'body':''
    };
    options.headers.authorization = "Basic " + new Buffer(cuser + ":" + cpass).toString('base64');
    return request(options,cb_);
    }

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
        var start = new Date([req.params.year,req.params.month,req.params.day].join('-'));
        var year = req.params.year;
        var vdsid = match_vdsid(req.params.vdsid);
        var district = match_district(req.params.vdsid);
        district = district < 10 ? 'd0'+district : 'd'+district;
        var startkey = [CDBDateString(start),vdsid];
        if(!vdsid) startkey= [CDBDateString(start),1200000];
        // while testing, get one day
        start.setDate( start.getDate() +1 )
        var endkey =  [CDBDateString(start),vdsid];
        if(!vdsid) endkey= [CDBDateString(start),1300000];
       var cdb_options = {'startkey':startkey
                          ,'endkey':endkey
                         };
        if(!vdsid){
            cdb_options.reduce=true;
            cdb_options.group=true;
            if(req.params.level){
                cdb_options['group_level']=req.params.level;
            }
        }
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

function brokenmmmcall(error,response,body){
                                        if(response.statusCode == 200){
                                            // find min, max, mean for each detector
                                            return minmaxmean(body, req, res);
                                        }else{
                                            console.log('error' + response.statusCode)
                                            console.log(body)
                                            return null;
                                        }
                                    }
function minmaxmean(body, req, res){
    // compute min max mean for all docs, all detectors, whatever, and dump those records
    var minmaxmean = {};

    console.log('computing min max mean');

    // first parse the body
    var docs = JSON.parse(body);
    body = null
    // result looks like
    // {"total_rows":1429429,"offset":686,"rows":[
    // {"id":"1201100 2007-01-02 00:00:00","key":"1201100 2007-01-02 00:00:00","value":["2007-01-02 04:41:30 UTC",0.205905]},
    // ...

    var vdsid_regex = /^(\d{6,7}) /;
    console.log('okay I have '+docs.rows.length)
    _.each(docs.rows,function(row){
        var match = vdsid_regex.exec(row.id);
        var id = 'vdsid_'+match[1];
        if(! minmaxmean[id]){
            minmaxmean[id]=[row.value[1],row.value[1],row.value[1],1];
            console.log(JSON.stringify(minmaxmean))
        }else{
            // add to the count
            minmaxmean[id][3] += 1;
            // min
            minmaxmean[id][0] = minmaxmean[id][0] > row.value[1] ? row.value[1]:minmaxmean[id][0];
            // max
            minmaxmean[id][1] = minmaxmean[id][1] < row.value[1] ? row.value[1]:minmaxmean[id][1];
            // mean
            var diff = row.value[1] - minmaxmean[id][2];
            var newmean = minmaxmean[id][2] + diff / minmaxmean[id][3];
            minmaxmean[id][2] = newmean;
        }
    });
    // get rid of the blob
    docs = null;

    //default is json  other formats handled upstream
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(minmaxmean));
    return 1;
};


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


