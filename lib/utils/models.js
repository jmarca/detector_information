/** this contains the models for computing the relative
 * risk of different kinds of accident.
 *
 * The variables stored in couchdb are:
 *
 * {"nl1","ol1","nr4","or4","nr3","or3","nr2","or2","nr1","or1"
 * ,"ts","mean.vol.1","mean.vol.m","mean.vol.r","sd.vol.1","sd.vol.m"
 * ,"sd.vol.r","cv.occ.1","cv.occ.m","cv.occ.r","cv.volocc.1","cv.volocc.m"
 * ,"cv.volocc.r","cor.vol.1.m","cor.vol.1.r","cor.vol.m.r","cor.occ.1.m"
 * ,"cor.occ.1.r","cor.occ.m.r","cor.volocc.1.m","cor.volocc.1.r"
 * ,"cor.volocc.m.r","autocor.vol.1","autocor.vol.m","autocor.vol.r"
 * ,"autocor.occ.1","autocor.occ.m","autocor.occ.r" ,"any_accident"}
 *
 */

exports.any_accident = any_accident;
exports.injury = severityinjury;
// module.exports. = ;
// module.exports. = ;
// module.exports. = ;
// module.exports. = ;

function any_accident(doc) {
    doc['any_accident']=[];
    return function(i){
        var risk_exponent = null;
	try {
	    risk_exponent =
            doc["cv.volocc.1"][i]
                                     && doc["cv.volocc.m"][i]
                                     && doc["cv.volocc.r"][i]
                                     && doc["cor.volocc.1.m"][i]
                                     && doc["cor.volocc.m.r"][i]
                                     && doc["autocor.occ.m"][i]
                                     && doc["autocor.occ.r"][i]
                                     && doc["mean.vol.1"][i]
                                     && doc["mean.vol.m"][i]
                                     && doc["cor.occ.1.m"][i]
                                     && doc["mean.vol.m"][i]
                                     && doc["cor.occ.1.m"][i]
                                     && doc["sd.vol.r" ][i]
                                     && doc["cor.occ.1.m"][i]
                                     && doc["autocor.occ.r"][i] ?
                                                            -3.194 +
            1.080 *  doc["cv.volocc.1"][i]    +
            0.627 *  doc["cv.volocc.m"][i]    +
            0.553 *  doc["cv.volocc.r"][i]    +
            1.439 *  doc["cor.volocc.1.m"][i] +
            0.658 *  doc["cor.volocc.m.r"][i] +
            0.412 * doc["autocor.occ.m"][i]   +
            1.424 * doc["autocor.occ.r"][i]   +
            0.038 * doc["mean.vol.1"][i]      +
            0.100 * doc["mean.vol.m"][i]      +
            (-0.168) * doc["cor.occ.1.m"][i] * doc["mean.vol.m"][i]  +
            0.479  * doc["cor.occ.1.m"][i] * doc["sd.vol.r" ][i]   +
            (-1.462) * doc["cor.occ.1.m"][i] * doc["autocor.occ.r"][i]
                                         : null;
	}catch (err) {
	    // gulp the error for now
	    //throw(err);
	}

        doc['any_accident'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}

function severityinjury(doc) {
    doc['severityinjury']=[];
    return function(i){
        var risk_exponent = null;
	try {
	    risk_exponent =
            doc["cor.occ.1.m"][i]
               && doc["cv.volocc.1"][i]
               && doc["cv.volocc.m"][i]
               && doc["cor.volocc.1.m"][i]
               && doc["cor.volocc.m.r"][i]
               && doc["autocor.occ.r"][i]
               && doc["mean.vol.1"][i]
               && doc["mean.vol.m"][i]
               && doc["mean.vol.r"][i]
               && doc["mean.vol.m"][i]
            ?
                 ( -4.571)
               + ( 0.627 ) * doc["cor.occ.1.m"][i]
               + ( 1.190 ) * doc["cv.volocc.1"][i]
               + ( 0.348 ) * doc["cv.volocc.m"][i]
               + ( 1.461 ) * doc["cor.volocc.1.m"][i]
               + ( 0.575 ) * doc["cor.volocc.m.r"][i]
               + ( 1.566 ) * doc["autocor.occ.r"][i]
               + ( 0.050 ) * doc["mean.vol.1"][i]
               + ( 0.087 ) * doc["mean.vol.m"][i]
               + ( 0.002 ) * doc["mean.vol.r"][i]
               + ( -0.172) * doc["mean.vol.m"][i]* doc["cor.occ.1.m"][i]
               + (  0.510) * doc["mean.vol.m"][i]* doc["cor.occ.1.m"][i]
               + ( -1.631) * doc["mean.vol.m"][i]* doc["cor.occ.1.m"][i]
             : null;
	}catch (err) {
	    console.log('Failed to set gid: ' + err);
	    //throw(err);
	}

        doc['severityinjury'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}

function interiorlanes(doc) {
    doc['interiorlanes']=[];
    return function(i){
        var risk_exponent =
   doc["cv.volocc.1"][i]
&& doc["cv.volocc.m"][i]
&& doc["cor.vol.1.r"][i]
&& doc["cor.occ.1.m"][i]
&& doc["cor.occ.m.r"][i]
&& doc["cor.volocc.1.m"][i]
&& doc["cor.volocc.1.r"][i]
&& doc["cor.volocc.m.r"][i]
&& doc["autocor.vol.1"][i]
&& doc["autocor.occ.r"][i]
&& doc["mean.vol.1"][i]
&& doc["mean.vol.m"][i]
&& doc["sd.vol.1"][i]
&& doc["sd.vol.r"][i]
&& doc["cor.occ.1.m"][i]
            ?
                                          ( -4.039)
+ doc["cv.volocc.1"][i]                 * ( 1.356)
+ doc["cv.volocc.m"][i]                 * ( 1.082)
+ doc["cor.vol.1.r"][i]                 * ( -0.185)
+ doc["cor.occ.1.m"][i]                 * ( -0.062)
+ doc["cor.occ.m.r"][i]                 * ( -0.549)
+ doc["cor.volocc.1.m"][i]              * ( 1.243)
+ doc["cor.volocc.1.r"][i]              * ( 0.462)
+ doc["cor.volocc.m.r"][i]              * ( 0.688)
+ doc["autocor.vol.1"][i]                  * ( -0.130)
+ doc["autocor.occ.r"][i]                  * ( 1.705)
+ doc["mean.vol.1"][i]                    * ( 0.047)
+ doc["mean.vol.m"][i]                    * ( 0.123)
+ doc["sd.vol.1"][i]                    * ( -0.028)
+ doc["sd.vol.r"][i]                    * ( -0.079)
+ doc["cor.occ.1.m"][i] * doc["mean.vol.m"][i]      * ( -0.262)
+ doc["cor.occ.1.m"][i] * doc["sd.vol.r"][i]      * ( 0.924)
+ doc["cor.occ.1.m"][i] * doc["autocor.occ.r"][i]    * ( -1.704)
                                         : null;
        doc['interiorlanes'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}

function severitypdo(doc) {
    doc['severitypdo']=[];
    return function(i){
        var risk_exponent =
   doc["cv_occ_1"][i]
&& doc["cv_volocc_1"][i]
&& doc["cv_volocc_m"][i]
&& doc["corr_vol_1m"][i]
&& doc["corr_vol_mr"][i]
&& doc["corr_occ_1m"][i]
&& doc["lag1_occ_r"][i]
&& doc["mu_vol_1"][i]
&& doc["mu_vol_m"][i]
&& doc["mu_vol_r"][i]
&& doc["sd_vol_r"][i]
            ?
                                                    (-3.504 )
+ doc["cv_occ_1"][i]                              * (-0.105 )
+ doc["cv_volocc_1"][i]                           * ( 1.257 )
+ doc["cv_volocc_m"][i]                           * ( 0.990 )
+ doc["corr_vol_1m"][i]                           * ( 1.576 )
+ doc["corr_vol_mr"][i]                           * ( 0.786 )
+ doc["lag1_occ_r"][i]                            * ( 1.551 )
+ doc["mu_vol_1"][i]                              * ( 0.040 )
+ doc["mu_vol_m"][i]                              * ( 0.105 )
+ doc["mu_vol_r"][i]                              * (-0.006 )
+ doc["corr_occ_1m"][i] * doc["mu_vol_m"][i]      * (-0.166 )
+ doc["corr_occ_1m"][i] * doc["sd_vol_r"][i]      * ( 0.479 )
+ doc["corr_occ_1m"][i] * doc["lag1_occ_r"][i]    * (-1.328 )
                                         : null;
        doc['severitypdo'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}

function oneveh(doc) {
    doc['oneveh']=[];
    return function(i){
        var risk_exponent =
   doc["cv_occ_1"][i]
&& doc["cv_volocc_1"][i]
&& doc["cv_volocc_r"][i]
&& doc["corr_vol_mr"][i]
&& doc["corr_occ_1m"][i]
&& doc["corr_volocc_1m"][i]
&& doc["corr_volocc_mr"][i]
&& doc["lag1_vol_1"][i]
&& doc["lag1_occ_r"][i]
&& doc["mu_vol_1"][i]
&& doc["mu_vol_m"][i]
&& doc["mu_vol_r"][i]
&& doc["sd_vol_r"][i]
            ?
                                                    (-4.076 )
+ doc["cv_occ_1"][i]                              * ( 0.837 )
+ doc["cv_volocc_1"][i]                           * (-0.508 )
+ doc["cv_volocc_r"][i]                           * (-0.138 )
+ doc["corr_vol_mr"][i]                           * ( 0.022 )
+ doc["corr_occ_1m"][i]                           * (-0.087 )
+ doc["corr_volocc_1m"][i]                        * ( 0.312 )
+ doc["corr_volocc_mr"][i]                        * ( 0.429 )
+ doc["lag1_vol_1"][i]                            * (-0.051 )
+ doc["lag1_occ_r"][i]                            * ( 0.305 )
+ doc["mu_vol_1"][i]                              * ( 0.002 )
+ doc["mu_vol_m"][i]                              * ( 0.043 )
+ doc["mu_vol_r"][i]                              * (-0.053 )
+ doc["sd_vol_r"][i]                              * ( 0.095 )
+ doc["corr_occ_1m"][i] * doc["mu_vol_m"][i]      * (-0.015 )
+ doc["corr_occ_1m"][i] * doc["sd_vol_r"][i]      * ( 0.472 )
                                         : null;
        doc['oneveh'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}

function twoveh(doc) {
    doc['twoveh']=[];
    return function(i){
        var risk_exponent =
   doc["cv_occ_1"][i]
&& doc["cv_volocc_1"][i]
&& doc["cv_volocc_r"][i]
&& doc["corr_vol_mr"][i]
&& doc["corr_occ_1m"][i]
&& doc["corr_volocc_1m"][i]
&& doc["corr_volocc_mr"][i]
&& doc["lag1_vol_1"][i]
&& doc["lag1_occ_r"][i]
&& doc["mu_vol_1"][i]
&& doc["mu_vol_m"][i]
&& doc["mu_vol_r"][i]
&& doc["sd_vol_r"][i]
            ?
                                                    (-3.167 )
+ doc["cv_occ_1"][i]                              * (-0.557 )
+ doc["cv_volocc_1"][i]                           * ( 2.003 )
+ doc["cv_volocc_r"][i]                           * ( 0.643 )
+ doc["corr_vol_mr"][i]                           * ( 0.167 )
+ doc["corr_occ_1m"][i]                           * (-0.730 )
+ doc["corr_volocc_1m"][i]                        * ( 1.611 )
+ doc["corr_volocc_mr"][i]                        * ( 0.610 )
+ doc["lag1_vol_1"][i]                            * (-0.155 )
+ doc["lag1_occ_r"][i]                            * ( 0.947 )
+ doc["mu_vol_1"][i]                              * ( 0.045 )
+ doc["mu_vol_m"][i]                              * ( 0.092 )
+ doc["mu_vol_r"][i]                              * ( 0.020 )
+ doc["sd_vol_r"][i]                              * (-0.235 )
+ doc["corr_occ_1m"][i] * doc["mu_vol_m"][i]      * (-0.192 )
+ doc["corr_occ_1m"][i] * doc["sd_vol_r"][i]      * ( 0.786 )
                                         : null;
        doc['twoveh'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}

function threeplusveh(doc) {
    doc['threeplusveh']=[];
    return function(i){
        var risk_exponent =
   doc["cv_occ_1"][i]
&& doc["cv_volocc_1"][i]
&& doc["cv_volocc_r"][i]
&& doc["corr_vol_mr"][i]
&& doc["corr_occ_1m"][i]
&& doc["corr_volocc_1m"][i]
&& doc["corr_volocc_mr"][i]
&& doc["lag1_vol_1"][i]
&& doc["lag1_occ_r"][i]
&& doc["mu_vol_1"][i]
&& doc["mu_vol_m"][i]
&& doc["mu_vol_r"][i]
&& doc["sd_vol_r"][i]
            ?
                                                    (-5.295 )
+ doc["cv_occ_1"][i]                              * (-0.012 )
+ doc["cv_volocc_1"][i]                           * ( 0.386 )
+ doc["cv_volocc_r"][i]                           * ( 1.145 )
+ doc["corr_vol_mr"][i]                           * (-0.823 )
+ doc["corr_occ_1m"][i]                           * ( 0.938 )
+ doc["corr_volocc_1m"][i]                        * ( 2.217 )
+ doc["corr_volocc_mr"][i]                        * ( 0.826 )
+ doc["lag1_vol_1"][i]                            * ( 0.768 )
+ doc["lag1_occ_r"][i]                            * ( 0.790 )
+ doc["mu_vol_1"][i]                              * ( 0.061 )
+ doc["mu_vol_m"][i]                              * ( 0.137 )
+ doc["mu_vol_r"][i]                              * ( 0.045 )
+ doc["sd_vol_r"][i]                              * (-0.020 )
+ doc["corr_occ_1m"][i] * doc["mu_vol_m"][i]      * (-0.306 )
+ doc["corr_occ_1m"][i] * doc["sd_vol_r"][i]      * ( 0.439 )
                                         : null;
        doc['threeplusveh'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}

function offroad(doc) {
    doc['offroad']=[];
    return function(i){
        var risk_exponent =
   doc["cv_volocc_1"][i]
&& doc["cv_volocc_m"][i]
&& doc["corr_vol_1r"][i]
&& doc["corr_occ_1m"][i]
&& doc["corr_occ_mr"][i]
&& doc["corr_volocc_1m"][i]
&& doc["corr_volocc_1r"][i]
&& doc["corr_volocc_mr"][i]
&& doc["lag1_vol_1"][i]
&& doc["lag1_occ_r"][i]
&& doc["mu_vol_1"][i]
&& doc["mu_vol_m"][i]
&& doc["sd_vol_1"][i]
&& doc["sd_vol_r"][i]
            ?
                                                    (-3.516 )
+ doc["cv_volocc_1"][i]                           * ( 0.537 )
+ doc["cv_volocc_m"][i]                           * (-0.650 )
+ doc["corr_vol_1r"][i]                           * (-0.367 )
+ doc["corr_occ_1m"][i]                           * (-0.127 )
+ doc["corr_occ_mr"][i]                           * (-0.128 )
+ doc["corr_volocc_1m"][i]                        * ( 1.007 )
+ doc["corr_volocc_1r"][i]                        * ( 0.503 )
+ doc["corr_volocc_mr"][i]                        * ( 0.383 )
+ doc["lag1_vol_1"][i]                            * (-0.017 )
+ doc["lag1_occ_r"][i]                            * ( 1.383 )
+ doc["mu_vol_1"][i]                              * (-0.026 )
+ doc["mu_vol_m"][i]                              * ( 0.093 )
+ doc["sd_vol_1"][i]                              * ( 0.190 )
+ doc["sd_vol_r"][i]                              * (-0.255 )
+ doc["corr_occ_1m"][i] * doc["mu_vol_m"][i]      * (-0.136 )
+ doc["corr_occ_1m"][i] * doc["sd_vol_r"][i]      * ( 0.632 )
+ doc["corr_occ_1m"][i] * doc["lag1_occ_r"][i]    * (-1.078 )
                                         : null;
        doc['offroad'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}

function severitypdo(doc) {
    doc['severitypdo']=[];
    return function(i){
        var risk_exponent =
   doc["cv_occ_1"][i]
&& doc["cv_occ_m"][i]
&& doc["cv_occ_r"][i]
&& doc["cv_volocc_1"][i]
&& doc["cv_volocc_m"][i]
&& doc["cv_volocc_r"][i]
&& doc["corr_vol_1m"][i]
&& doc["corr_vol_1r"][i]
&& doc["corr_vol_mr"][i]
&& doc["corr_occ_1m"][i]
&& doc["corr_occ_1r"][i]
&& doc["corr_occ_mr"][i]
&& doc["corr_volocc_1m"][i]
&& doc["corr_volocc_1r"][i]
&& doc["corr_volocc_mr"][i]
&& doc["lag1_vol_1"][i]
&& doc["lag1_vol_m"][i]
&& doc["lag1_vol_r"][i]
&& doc["lag1_occ_1"][i]
&& doc["lag1_occ_m"][i]
&& doc["lag1_occ_r"][i]
&& doc["mu_vol_1"][i]
&& doc["mu_vol_m"][i]
&& doc["mu_vol_r"][i]
&& doc["sd_vol_1"][i]
&& doc["sd_vol_m"][i]
&& doc["sd_vol_r"][i]
            ?
                                                    (-3.504 )
+ doc["cv_occ_1"][i]                              * (-0.105 )
+ doc["cv_occ_m"][i]                              * ( 0.000 )
+ doc["cv_occ_r"][i]                              * ( 0.000 )
+ doc["cv_volocc_1"][i]                           * ( 1.257 )
+ doc["cv_volocc_m"][i]                           * ( 0.990 )
+ doc["cv_volocc_r"][i]                           * ( 0.000 )
+ doc["corr_vol_1m"][i]                           * ( 1.576 )
+ doc["corr_vol_1r"][i]                           * ( 0.000 )
+ doc["corr_vol_mr"][i]                           * ( 0.786 )
+ doc["corr_occ_1m"][i]                           * ( 0.000 )
+ doc["corr_occ_1r"][i]                           * ( 0.000 )
+ doc["corr_occ_mr"][i]                           * ( 0.000 )
+ doc["corr_volocc_1m"][i]                        * ( 0.000 )
+ doc["corr_volocc_1r"][i]                        * ( 0.000 )
+ doc["corr_volocc_mr"][i]                        * ( 0.000 )
+ doc["lag1_vol_1"][i]                            * ( 0.000 )
+ doc["lag1_vol_m"][i]                            * ( 0.000 )
+ doc["lag1_vol_r"][i]                            * ( 0.000 )
+ doc["lag1_occ_1"][i]                            * ( 0.000 )
+ doc["lag1_occ_m"][i]                            * ( 0.000 )
+ doc["lag1_occ_r"][i]                            * ( 1.551 )
+ doc["mu_vol_1"][i]                              * ( 0.040 )
+ doc["mu_vol_m"][i]                              * ( 0.105 )
+ doc["mu_vol_r"][i]                              * (-0.006 )
+ doc["sd_vol_1"][i]                              * ( 0.000 )
+ doc["sd_vol_m"][i]                              * ( 0.000 )
+ doc["sd_vol_r"][i]                              * ( 0.000 )
+ doc["corr_occ_1m"][i] * doc["mu_vol_m"][i]      * (-0.166 )
+ doc["corr_occ_1m"][i] * doc["sd_vol_r"][i]      * ( 0.479 )
+ doc["corr_occ_1m"][i] * doc["lag1_occ_r"][i]    * (-1.328 )
+ doc["cv_volocc_1"][i] * doc["corr_volocc_1m"][i]* ( 0.000 )

                                         : null;
        doc['severitypdo'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}
function severitypdo(doc) {
    doc['severitypdo']=[];
    return function(i){
        var risk_exponent =
   doc["cv_occ_1"][i]
&& doc["cv_occ_m"][i]
&& doc["cv_occ_r"][i]
&& doc["cv_volocc_1"][i]
&& doc["cv_volocc_m"][i]
&& doc["cv_volocc_r"][i]
&& doc["corr_vol_1m"][i]
&& doc["corr_vol_1r"][i]
&& doc["corr_vol_mr"][i]
&& doc["corr_occ_1m"][i]
&& doc["corr_occ_1r"][i]
&& doc["corr_occ_mr"][i]
&& doc["corr_volocc_1m"][i]
&& doc["corr_volocc_1r"][i]
&& doc["corr_volocc_mr"][i]
&& doc["lag1_vol_1"][i]
&& doc["lag1_vol_m"][i]
&& doc["lag1_vol_r"][i]
&& doc["lag1_occ_1"][i]
&& doc["lag1_occ_m"][i]
&& doc["lag1_occ_r"][i]
&& doc["mu_vol_1"][i]
&& doc["mu_vol_m"][i]
&& doc["mu_vol_r"][i]
&& doc["sd_vol_1"][i]
&& doc["sd_vol_m"][i]
&& doc["sd_vol_r"][i]
            ?
                                                    (-3.504 )
+ doc["cv_occ_1"][i]                              * (-0.105 )
+ doc["cv_occ_m"][i]                              * ( 0.000 )
+ doc["cv_occ_r"][i]                              * ( 0.000 )
+ doc["cv_volocc_1"][i]                           * ( 1.257 )
+ doc["cv_volocc_m"][i]                           * ( 0.990 )
+ doc["cv_volocc_r"][i]                           * ( 0.000 )
+ doc["corr_vol_1m"][i]                           * ( 1.576 )
+ doc["corr_vol_1r"][i]                           * ( 0.000 )
+ doc["corr_vol_mr"][i]                           * ( 0.786 )
+ doc["corr_occ_1m"][i]                           * ( 0.000 )
+ doc["corr_occ_1r"][i]                           * ( 0.000 )
+ doc["corr_occ_mr"][i]                           * ( 0.000 )
+ doc["corr_volocc_1m"][i]                        * ( 0.000 )
+ doc["corr_volocc_1r"][i]                        * ( 0.000 )
+ doc["corr_volocc_mr"][i]                        * ( 0.000 )
+ doc["lag1_vol_1"][i]                            * ( 0.000 )
+ doc["lag1_vol_m"][i]                            * ( 0.000 )
+ doc["lag1_vol_r"][i]                            * ( 0.000 )
+ doc["lag1_occ_1"][i]                            * ( 0.000 )
+ doc["lag1_occ_m"][i]                            * ( 0.000 )
+ doc["lag1_occ_r"][i]                            * ( 1.551 )
+ doc["mu_vol_1"][i]                              * ( 0.040 )
+ doc["mu_vol_m"][i]                              * ( 0.105 )
+ doc["mu_vol_r"][i]                              * (-0.006 )
+ doc["sd_vol_1"][i]                              * ( 0.000 )
+ doc["sd_vol_m"][i]                              * ( 0.000 )
+ doc["sd_vol_r"][i]                              * ( 0.000 )
+ doc["corr_occ_1m"][i] * doc["mu_vol_m"][i]      * (-0.166 )
+ doc["corr_occ_1m"][i] * doc["sd_vol_r"][i]      * ( 0.479 )
+ doc["corr_occ_1m"][i] * doc["lag1_occ_r"][i]    * (-1.328 )
+ doc["cv_volocc_1"][i] * doc["corr_volocc_1m"][i]* ( 0.000 )

                                         : null;
        doc['severitypdo'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}
function severitypdo(doc) {
    doc['severitypdo']=[];
    return function(i){
        var risk_exponent =
   doc["cv_occ_1"][i]
&& doc["cv_occ_m"][i]
&& doc["cv_occ_r"][i]
&& doc["cv_volocc_1"][i]
&& doc["cv_volocc_m"][i]
&& doc["cv_volocc_r"][i]
&& doc["corr_vol_1m"][i]
&& doc["corr_vol_1r"][i]
&& doc["corr_vol_mr"][i]
&& doc["corr_occ_1m"][i]
&& doc["corr_occ_1r"][i]
&& doc["corr_occ_mr"][i]
&& doc["corr_volocc_1m"][i]
&& doc["corr_volocc_1r"][i]
&& doc["corr_volocc_mr"][i]
&& doc["lag1_vol_1"][i]
&& doc["lag1_vol_m"][i]
&& doc["lag1_vol_r"][i]
&& doc["lag1_occ_1"][i]
&& doc["lag1_occ_m"][i]
&& doc["lag1_occ_r"][i]
&& doc["mu_vol_1"][i]
&& doc["mu_vol_m"][i]
&& doc["mu_vol_r"][i]
&& doc["sd_vol_1"][i]
&& doc["sd_vol_m"][i]
&& doc["sd_vol_r"][i]
            ?
                                                    (-3.504 )
+ doc["cv_occ_1"][i]                              * (-0.105 )
+ doc["cv_occ_m"][i]                              * ( 0.000 )
+ doc["cv_occ_r"][i]                              * ( 0.000 )
+ doc["cv_volocc_1"][i]                           * ( 1.257 )
+ doc["cv_volocc_m"][i]                           * ( 0.990 )
+ doc["cv_volocc_r"][i]                           * ( 0.000 )
+ doc["corr_vol_1m"][i]                           * ( 1.576 )
+ doc["corr_vol_1r"][i]                           * ( 0.000 )
+ doc["corr_vol_mr"][i]                           * ( 0.786 )
+ doc["corr_occ_1m"][i]                           * ( 0.000 )
+ doc["corr_occ_1r"][i]                           * ( 0.000 )
+ doc["corr_occ_mr"][i]                           * ( 0.000 )
+ doc["corr_volocc_1m"][i]                        * ( 0.000 )
+ doc["corr_volocc_1r"][i]                        * ( 0.000 )
+ doc["corr_volocc_mr"][i]                        * ( 0.000 )
+ doc["lag1_vol_1"][i]                            * ( 0.000 )
+ doc["lag1_vol_m"][i]                            * ( 0.000 )
+ doc["lag1_vol_r"][i]                            * ( 0.000 )
+ doc["lag1_occ_1"][i]                            * ( 0.000 )
+ doc["lag1_occ_m"][i]                            * ( 0.000 )
+ doc["lag1_occ_r"][i]                            * ( 1.551 )
+ doc["mu_vol_1"][i]                              * ( 0.040 )
+ doc["mu_vol_m"][i]                              * ( 0.105 )
+ doc["mu_vol_r"][i]                              * (-0.006 )
+ doc["sd_vol_1"][i]                              * ( 0.000 )
+ doc["sd_vol_m"][i]                              * ( 0.000 )
+ doc["sd_vol_r"][i]                              * ( 0.000 )
+ doc["corr_occ_1m"][i] * doc["mu_vol_m"][i]      * (-0.166 )
+ doc["corr_occ_1m"][i] * doc["sd_vol_r"][i]      * ( 0.479 )
+ doc["corr_occ_1m"][i] * doc["lag1_occ_r"][i]    * (-1.328 )
+ doc["cv_volocc_1"][i] * doc["corr_volocc_1m"][i]* ( 0.000 )

                                         : null;
        doc['severitypdo'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}
function severitypdo(doc) {
    doc['severitypdo']=[];
    return function(i){
        var risk_exponent =
   doc["cv_occ_1"][i]
&& doc["cv_occ_m"][i]
&& doc["cv_occ_r"][i]
&& doc["cv_volocc_1"][i]
&& doc["cv_volocc_m"][i]
&& doc["cv_volocc_r"][i]
&& doc["corr_vol_1m"][i]
&& doc["corr_vol_1r"][i]
&& doc["corr_vol_mr"][i]
&& doc["corr_occ_1m"][i]
&& doc["corr_occ_1r"][i]
&& doc["corr_occ_mr"][i]
&& doc["corr_volocc_1m"][i]
&& doc["corr_volocc_1r"][i]
&& doc["corr_volocc_mr"][i]
&& doc["lag1_vol_1"][i]
&& doc["lag1_vol_m"][i]
&& doc["lag1_vol_r"][i]
&& doc["lag1_occ_1"][i]
&& doc["lag1_occ_m"][i]
&& doc["lag1_occ_r"][i]
&& doc["mu_vol_1"][i]
&& doc["mu_vol_m"][i]
&& doc["mu_vol_r"][i]
&& doc["sd_vol_1"][i]
&& doc["sd_vol_m"][i]
&& doc["sd_vol_r"][i]
            ?
                                                    (-3.504 )
+ doc["cv_occ_1"][i]                              * (-0.105 )
+ doc["cv_occ_m"][i]                              * ( 0.000 )
+ doc["cv_occ_r"][i]                              * ( 0.000 )
+ doc["cv_volocc_1"][i]                           * ( 1.257 )
+ doc["cv_volocc_m"][i]                           * ( 0.990 )
+ doc["cv_volocc_r"][i]                           * ( 0.000 )
+ doc["corr_vol_1m"][i]                           * ( 1.576 )
+ doc["corr_vol_1r"][i]                           * ( 0.000 )
+ doc["corr_vol_mr"][i]                           * ( 0.786 )
+ doc["corr_occ_1m"][i]                           * ( 0.000 )
+ doc["corr_occ_1r"][i]                           * ( 0.000 )
+ doc["corr_occ_mr"][i]                           * ( 0.000 )
+ doc["corr_volocc_1m"][i]                        * ( 0.000 )
+ doc["corr_volocc_1r"][i]                        * ( 0.000 )
+ doc["corr_volocc_mr"][i]                        * ( 0.000 )
+ doc["lag1_vol_1"][i]                            * ( 0.000 )
+ doc["lag1_vol_m"][i]                            * ( 0.000 )
+ doc["lag1_vol_r"][i]                            * ( 0.000 )
+ doc["lag1_occ_1"][i]                            * ( 0.000 )
+ doc["lag1_occ_m"][i]                            * ( 0.000 )
+ doc["lag1_occ_r"][i]                            * ( 1.551 )
+ doc["mu_vol_1"][i]                              * ( 0.040 )
+ doc["mu_vol_m"][i]                              * ( 0.105 )
+ doc["mu_vol_r"][i]                              * (-0.006 )
+ doc["sd_vol_1"][i]                              * ( 0.000 )
+ doc["sd_vol_m"][i]                              * ( 0.000 )
+ doc["sd_vol_r"][i]                              * ( 0.000 )
+ doc["corr_occ_1m"][i] * doc["mu_vol_m"][i]      * (-0.166 )
+ doc["corr_occ_1m"][i] * doc["sd_vol_r"][i]      * ( 0.479 )
+ doc["corr_occ_1m"][i] * doc["lag1_occ_r"][i]    * (-1.328 )
+ doc["cv_volocc_1"][i] * doc["corr_volocc_1m"][i]* ( 0.000 )

                                         : null;
        doc['severitypdo'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}
function severitypdo(doc) {
    doc['severitypdo']=[];
    return function(i){
        var risk_exponent =
   doc["cv_occ_1"][i]
&& doc["cv_occ_m"][i]
&& doc["cv_occ_r"][i]
&& doc["cv_volocc_1"][i]
&& doc["cv_volocc_m"][i]
&& doc["cv_volocc_r"][i]
&& doc["corr_vol_1m"][i]
&& doc["corr_vol_1r"][i]
&& doc["corr_vol_mr"][i]
&& doc["corr_occ_1m"][i]
&& doc["corr_occ_1r"][i]
&& doc["corr_occ_mr"][i]
&& doc["corr_volocc_1m"][i]
&& doc["corr_volocc_1r"][i]
&& doc["corr_volocc_mr"][i]
&& doc["lag1_vol_1"][i]
&& doc["lag1_vol_m"][i]
&& doc["lag1_vol_r"][i]
&& doc["lag1_occ_1"][i]
&& doc["lag1_occ_m"][i]
&& doc["lag1_occ_r"][i]
&& doc["mu_vol_1"][i]
&& doc["mu_vol_m"][i]
&& doc["mu_vol_r"][i]
&& doc["sd_vol_1"][i]
&& doc["sd_vol_m"][i]
&& doc["sd_vol_r"][i]
            ?
                                                    (-3.504 )
+ doc["cv_occ_1"][i]                              * (-0.105 )
+ doc["cv_occ_m"][i]                              * ( 0.000 )
+ doc["cv_occ_r"][i]                              * ( 0.000 )
+ doc["cv_volocc_1"][i]                           * ( 1.257 )
+ doc["cv_volocc_m"][i]                           * ( 0.990 )
+ doc["cv_volocc_r"][i]                           * ( 0.000 )
+ doc["corr_vol_1m"][i]                           * ( 1.576 )
+ doc["corr_vol_1r"][i]                           * ( 0.000 )
+ doc["corr_vol_mr"][i]                           * ( 0.786 )
+ doc["corr_occ_1m"][i]                           * ( 0.000 )
+ doc["corr_occ_1r"][i]                           * ( 0.000 )
+ doc["corr_occ_mr"][i]                           * ( 0.000 )
+ doc["corr_volocc_1m"][i]                        * ( 0.000 )
+ doc["corr_volocc_1r"][i]                        * ( 0.000 )
+ doc["corr_volocc_mr"][i]                        * ( 0.000 )
+ doc["lag1_vol_1"][i]                            * ( 0.000 )
+ doc["lag1_vol_m"][i]                            * ( 0.000 )
+ doc["lag1_vol_r"][i]                            * ( 0.000 )
+ doc["lag1_occ_1"][i]                            * ( 0.000 )
+ doc["lag1_occ_m"][i]                            * ( 0.000 )
+ doc["lag1_occ_r"][i]                            * ( 1.551 )
+ doc["mu_vol_1"][i]                              * ( 0.040 )
+ doc["mu_vol_m"][i]                              * ( 0.105 )
+ doc["mu_vol_r"][i]                              * (-0.006 )
+ doc["sd_vol_1"][i]                              * ( 0.000 )
+ doc["sd_vol_m"][i]                              * ( 0.000 )
+ doc["sd_vol_r"][i]                              * ( 0.000 )
+ doc["corr_occ_1m"][i] * doc["mu_vol_m"][i]      * (-0.166 )
+ doc["corr_occ_1m"][i] * doc["sd_vol_r"][i]      * ( 0.479 )
+ doc["corr_occ_1m"][i] * doc["lag1_occ_r"][i]    * (-1.328 )
+ doc["cv_volocc_1"][i] * doc["corr_volocc_1m"][i]* ( 0.000 )

                                         : null;
        doc['severitypdo'][i]= risk_exponent
            ? Math.exp(risk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}
