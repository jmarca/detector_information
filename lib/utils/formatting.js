module.exports = CDBDateString

/* use a function for date formatting... */
function CDBDateString(d,time,datesep){
    if(!datesep) datesep='-';
    function pad(n){return n<10 ? '0'+n : n}
    if(!time){
        return [d.getUTCFullYear()
               ,pad(d.getUTCMonth()+1)
               ,pad(d.getUTCDate())
               ].join(datesep);
    }
    return [d.getUTCFullYear()
           ,pad(d.getUTCMonth()+1)
           ,pad(d.getUTCDate())
           ].join(datesep)
          + ' '
          +[pad(d.getUTCHours())
           ,pad(d.getUTCMinutes())
           ,pad(d.getUTCSeconds())
           ].join(':')
    ;

}
