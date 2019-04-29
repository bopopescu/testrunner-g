function OnUpdate(doc,meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 180);
    var time_rand = random_gen();
    var doc_id = time_rand + ' ';
    var context = {docID : doc_id, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6"};
    createTimer(DocTimerCallback, expiry, meta.id, context);
}
function DocTimerCallback(context) {
    try {
        src_bucket[context.docID] = context.random_text;
    } catch(e) {
        //var time_rand = random_gen();
        //dst_bucket[time_rand] = 'from DocTimerCallback';
    }
}
function random_gen(){
    var rand = Math.floor(Math.random() * 20000000) * Math.floor(Math.random() * 20000000);
    var time_rand = Math.round((new Date()).getTime() / 1000) + rand;
    return time_rand;
}