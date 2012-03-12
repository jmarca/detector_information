var couchdb = require('couchdb');
var request = require("request");

var CDBDateString = require('./utils/formatting.js');
var _ = require('underscore');

// first, what is the connection
var env = process.env;
var  querystring = require('querystring');


exports.prediction_data_service = prediction_data_service;

/**
 * prediction_data_service
 *
 * simple client to get vdsid data between a start and end timestamp
 */
function prediction_data_service(options,cb_){


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
        options.headers.accept='application/json';
	return cb_ ? request(options,cb_) : request(options);
    }

    // I have a thought here that perhaps vdsid wants to be an array of items.
    // but first work out the bugs with a single vdsid

    var possibleParams = ['vdsid','startts','endts'];

    return function prediction_data_service(req,res,next){

        console.log('handling request for prediction_data_service');
        var activeParams = possibleParams.filter(function(a){return req.params[a] !==undefined;});
        var query = {};
        if(activeParams.length < 2 || !activeParams[0]==='vdsid') return next();
        // formulate for document id ranges from starte to end times
        var start = new Date(req.params.start);
        var year = start.getFullYear();
        var district = match_district(req.params.vdsid);
        district = district < 10 ? 'd0'+district : 'd'+district;
        var startkey = [req.params.vdsid,CDBDateString(start,false)].join(' ');
        // while testing, get one month
        start.setMonth( start.getMonth() +1 )
        var endkey = [req.params.vdsid,CDBDateString(start ,false)].join(' ');

        // year might be two, which *might* mean different dbs, but if
        // so copy that from data_service later

        // use request library to stream output.  handle daily docs in browser
        var x = createRequestObject([dbname,'_design',designdoc,'_view',view].join('/')
                                           ,'GET'
                                           ,{'startkey':startkey
                                            ,'endkey':endkey
                                            ,'include_docs':true}
                                           ,null
                                   );
        x.pipe(res);
        return 1;
    };

return 1;
}


function match_district (vdsid){
    var district_regex = /^(\d{1,2})\d{5}$/;
    var match = district_regex.exec(vdsid);
    return match[1];
}

var d3max = function(array, f) {
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


