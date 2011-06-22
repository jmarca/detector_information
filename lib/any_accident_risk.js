var fs = require('fs');
var couchdb = require('couchdb');
var crypto = require('crypto');
var _ = require('underscore');


var any_accident = function(doc) {
  var anyaccidentrisk_exponent =
                               -3.194 +
    1.080 *  doc["cv.volocc.1"]    +
    0.627 *  doc["cv.volocc.m"]    +
    0.553 *  doc["cv.volocc.r"]    +
    1.439 *  doc["cor.volocc.1.m"] +
    0.658 *  doc["cor.volocc.m.r"] +
    0.412 * doc["autocor.occ.m"]   +
    1.424 * doc["autocor.occ.r"]   +
    0.038 * doc["mean.vol.1"]      +
    0.100 * doc["mean.vol.m"]      +
    (-0.168) * doc["cor.occ.1.m"] * doc["mean.vol.m"]  +
    0.479  * doc["cor.occ.1.m"] * doc["sd.vol.r" ]   +
    (-1.462) * doc["cor.occ.1.m"] * doc["autocor.occ.r"] ;

  var risk = Math.exp(anyaccidentrisk_exponent);
  return {"t":doc.EstimateTime
         ,"any_accident":risk};
};

// get a doc, compute the risk, spit out the risk as a list
exports.get_any_accident_risk = function get_any_accident_risk(options){
    var root =  options.root ?  options.root :  process.cwd();
    var unique_doc_property = options.unique_doc_property ? options.unique_doc_property : 'components';
    var pathParams = options.pathParams ? options.pathParams : ['district','year','id'];
    var fileParam  = options.fileParam ;

    var design = options.design ? options.design : 'detector';
    var view   = options.view   ? options.view   : 'notprocessed';

    function connectToCouch(path,next){
        var client = couchdb.createClient(options.port, options.host, options.user, options.pass);
        var db = client.db(options.db);
        next(null,db);
    }

    function couchView(c,postprocess,next){
        // console.log('setting up view query '+JSON.stringify(c));
        return function(err,db){
            if (err){
                console.log('error: '+JSON.stringify(err));
                next( new Error(JSON.stringify(err)));
                return;
            }
            db.view(
                design,view,c
                , function(er, r) {
                    if (er){
                        console.log('error: '+JSON.stringify(er));
                    }
                    else{
                        //console.log('done with query, r is '+JSON.stringify(r));
                    }
                    var features=[];
                    for (var row_i in r.rows){
                        var row = r.rows[row_i];
                        features.push(postprocess(row.value));
                    }
                    next(null,features);
                });
        };
    }

  function getViewQuery(req){

    var activeParams = pathParams.filter(function(a){return req.params[a];});
    var startkey=[];
    var endkey=[];
    var last = activeParams[activeParams.length - 1];

    // coouchdb likes startkey and endkey to be something like
    // startkey=[12,34,12,2008,1]&endkey=[12,34,12,2008,1,{}] that
    // is, from some key, represented by a sequence of array
    // elements, to the very end.
    //
    // for example, if you have year, month, day in your view, you
    // can ask for all of 2008 by putting
    // startkey=[2008]&endkey=[2009], but it is faster to ask for
    // startkey=[2008]&endkey=[2008,{}].  And besides, the key
    // might be a letter or a number, and I don't really know how
    // to increment by one in all cases.  So instead, I just use
    // the push {} onto the end trick, which sorts as an object,
    // and therefore is always greater than everything

    activeParams.map(
      function(q){
        startkey.push(req.params[q]);
        endkey.push(req.params[q]);
        if (q == last){
          endkey.push({});
        }
      }
    );

    return {'startkey':startkey,'endkey':endkey};

  }




  return function get_any_accident_risk(req,next){

    var query = getViewQuery(req);
    var db = getPath(req);

    return function(err){
      if (err){
        console.log('error: '+JSON.stringify(err));
        next( err );
        return;
      }
      var fetch = couchView(query,any_accident,next);
      connectToCouch(fetch);
    };
  };
};
