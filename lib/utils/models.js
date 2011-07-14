module.exports.any_accident = any_accident;
function any_accident(doc) {
    doc['any_accident']=[];
    return function(i){
        var anyaccidentrisk_exponent =
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
        doc['any_accident'][i]= anyaccidentrisk_exponent
            ? Math.exp(anyaccidentrisk_exponent).toFixed(6)
                  : null;
        return 1;
    };
}

