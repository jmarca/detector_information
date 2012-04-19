var yajl = require('yajl');

var h = new yajl.Handle();
h.on( 'error',      function (_) { console.log(_)     });

h.on( 'startMap',   function ( ) { console.log('{')   });
h.on( 'endMap',     function ( ) { console.log('}')   });
h.on( 'startArray', function ( ) { console.log('[')   });
h.on( 'endArray',   function ( ) { console.log(']')   });
h.on( 'mapKey',     function (_) { console.log('key is here  '+_+':') });
h.on( 'null',       function (_) { console.log('null')});
h.on( 'boolean',    function (_) { console.log(_)     });
h.on( 'string',     function (_) { console.log(_)     });
h.on( 'integer',    function (_) { console.log(_)     });
h.on( 'double',     function (_) { console.log(_)     });

var data = [
    '{"some"',
    ':["JSON',
    '"],"data',
    '":[-3.5]}'
];

for ( var i in data ) {
    h.parse( data[i] );
}

h.completeParse();
