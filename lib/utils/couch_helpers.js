exports.fresh_db = fresh_db;

function fresh_db(dyv_db,client){
    var cdb = client.db(dyv_db);
    var create_handler = function(){
        cdb.create(function(e){
            if(e) throw new Error('failed to create db for '+dyv_db+JSON.stringify(e))
            return cdb.saveDoc('redo',{'redo':1},function(){
                cdb.saveDoc('shrink',{'shrink':1});
                return console.log('created db for '+dyv_db);
            });
        });
    };
    return cdb.exists(function(er,exists){
        console.log('exists is '+exists);
        if(exists){
            // delete it then recreate it if it does not contain the
            // redo document
            cdb.getDoc('redo',function(e,doc){
                if(e){
                    cdb.remove( create_handler );
                }
            });
        }else{
            // just create it
            create_handler();
        }
        return 1;
    });
}

exports.checkout = checkout;
function checkout(dyv_db,client,cb){
    var cdb = client.db(dyv_db);
    cdb.saveDoc('shrink',{'shrink':1},cb);
    // note that if e is non null, then failed to save.  If e is null,
    // then all good.  Just make sure cb ignores the result of save,
    // but pays attention to the error state
}