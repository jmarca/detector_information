var fs = require('fs');
var couchdb = require('couchdb');
var crypto = require('crypto');
var _ = require('underscore');

function getPath(req){
    var activeParams = pathParams.filter(function(a){return req.params[a];});
    var dirs={};
    activeParams.map(
        function(q,r){
            dirs[q] =  req.params[q];
            dirs._id = dirs._id ?  [dirs._id, req.params[q]].join('/') : req.params[q];
        }
    );

    return dirs;
}
function getFile(req){
    var format = req.format || 'json';
    var filename = req.params[fileParam]+'.'+format;
    return filename;
}

exports.bulksave_detector_information = function bulksave_detector_information(options){
    var root =  options.root ?  options.root :  process.cwd();
    var unique_doc_properties = options.unique_doc_properties ? options.unique_doc_properties : ['ts','components'];
    var pathParams = options.pathParams ? options.pathParams : ['zoom','column'];
    var fileParam  = options.fileParam ;
    function connectToCouch(next){
        var client = couchdb.createClient(options.port, options.host, options.user, options.pass);
        var db = client.db(options.db);

        next(null,db);
    }

    /**
     * couchBulk -- save data using the couchdb bulk docs api
     * parameters
     *
     *  c -- an object containing
     *
     *  c.targetpath : an object with details about the query path (for example,
     *     :year/:zoom/:column/:row type url would generate {year:2008,
     *     zoom:3, row:3, column3}
     *
     *  c.root_id : a string that will serve as the root document id
     *     for all documents being saved.  It will be combined with
     *     the unique_properties of each document to generate the
     *     unique id for couchdb.
     *
     *  c.unique_properties : an array holding the unique properties
     *     in the doc that should be appended to the doc id formed by the
     *     query path
     *
     *  c.docs : an array holding the documents to save
     *
     *  c.docs[0], etc : each document is an object that must have an
     *     element feature, which is an object containing at least an
     *     element properties, with entries for each of the
     *     c.unique_properties.  For example, if c.unique_properties =
     *     ['ts','components'], the at a minimum the document object
     *     should look like
     *     doc={'feature':{'properties':{'ts':something,'components':[some,other,thing]}}};
     *
     **/
    function couchBulk(c,next){
        var buffer = [];
        for (var d in c.docs){
            var features = c.docs[d];
            for (var f in features){
                var doc = {};
                for(var k in c.targetpath){ doc[k] = c.targetpath[k] ; }
                doc.feature = features[f];
                var unique_bits = c.unique_properties.map(function(p){return doc.feature.properties[p];});
                doc._id = [c.root_id,unique_bits].join('/');
                buffer.push(doc);
            }
        }
        return function(err,db){
            if (err){
                console.log('error: '+JSON.stringify(err));
                next( new Error(JSON.stringify(err)));
                return;
            }
            next(null,c.docs);

            db.bulkDocs({
                docs: buffer
            }, function(er, r) {
                if (er){
                    console.log('error: '+JSON.stringify(er));
                }
                else{
                   // console.log('done with bulk docs, r is '+JSON.stringify(r));
                }
            });
        };
    }

    return function bulksave_detector_docs(req,next){

        var targetpath = getPath(req);

        var filename = getFile(req);
        targetpath.filename=filename;
        var localWriteEnd = function(docs){
            var writeOut = couchBulk({
                'targetpath':targetpath
                ,'unique_properties' : unique_doc_properties
                ,'root_id' : targetpath._id
                ,'docs':docs
            },next);
            connectToCouch(writeOut);
        };
        return function(err,docs){
            if (err){
                console.log('error: '+JSON.stringify(err));
                next( err );
                return;
            }
            //console.log('direct call to couch cache with '+filename+' and docs '+JSON.stringify(docs));
            localWriteEnd(docs);
        };
    };

};

/**
 * get_detector_information
 *
 *  The doc is a detector stats doc that tells me what is going on
 *  with the detector.  In the short term I am interested in
 *  imputation of truck data, and also imputing missing data on a
 *  yearly basis, so at a minimum, the doc should contain whether the
 *  detector has been imputed against itself, and whether it has been
 *  imputed for trucks, and how many times each, because of course I
 *  can do multiple imputations.  The other things it should have are
 *  the file names or perhaps the pattern for the different kinds of
 *  binary data...the RData object of the raw, the RData object for
 *  the imputed object, the RData ojbect for the truck imputations,
 *  etc.  And also the CSV file for the truck imputations.
 *
 */
exports.get_detector_information = function get_detector_information(options){
    var root =  options.root ?  options.root :  process.cwd();
    var unique_doc_property = options.unique_doc_property ? options.unique_doc_property : 'components';
    var pathParams = options.pathParams ? options.pathParams : ['district','year','id'];
    var fileParam  = options.fileParam ;

    var design = options.design ? options.design : 'detector';
    var view   = options.view   ? options.view   : 'notprocessed';

    function connectToCouch(next){
        var client = couchdb.createClient(options.port, options.host, options.user, options.pass);
        var db = client.db(options.db);
        next(null,db);
    }

    function couchView(c,next){
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
                    var features={};
                    for (var row_i in r.rows){
                        var row = r.rows[row_i];

                        if(!features[row.value.properties[unique_doc_property]]){
                            features[row.value.properties[unique_doc_property]] = [];
                        }
                        features[row.value.properties[unique_doc_property]].push(row.value);
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

    return function get_detector_information(req,next){

        var query = getViewQuery(req);

        return function(err){
            if (err){
                console.log('error: '+JSON.stringify(err));
                next( err );
                return;
            }
            var fetch = couchView(query ,next);
            connectToCouch(fetch);
        };
    };
};


