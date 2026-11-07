'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../static/collection-native-api.js'), 'utf8');
const calls = [];
const client = {
    get(url) { calls.push({method: 'GET', url}); return Promise.resolve({}); },
    post(url, payload) { calls.push({method: 'POST', url, payload}); return Promise.resolve({}); },
    defaults: {baseURL: 'https://edith.xiaohongshu.com'},
    interceptors: {request: {handlers: [{}]}, response: {handlers: []}},
};
const require_ = {c: {fixture: {exports: {default: client}}}};
const window = {webpackChunkxhs_pc_web: {push(chunk) { chunk[2](require_); }}};
vm.runInNewContext(source, {window, location: {hostname: 'www.xiaohongshu.com'}, URL, Date, Promise});
assert.equal(window.__xhsNativeApi.probe().available, true);
assert.equal(calls.length, 0, 'setup/probe must not issue a request');
const note = 'a'.repeat(24), root = 'b'.repeat(24);
const base = 'https://edith.xiaohongshu.com';
const comment = `${base}/api/sns/web/v2/comment/page?note_id=${note}&cursor=&xsec_token=fixture`;
assert.equal(window.__xhsNativeApi.start({method:'GET',url:comment}).started, true);
assert.equal(window.__xhsNativeApi.start({method:'POST',url:base+'/api/sns/web/v1/feed',payload:{source_note_id:note,xsec_token:'fixture'}}).started, true);
assert.equal(window.__xhsNativeApi.start({method:'GET',url:base+`/api/sns/web/v2/comment/sub/page?note_id=${note}&root_comment_id=${root}&cursor=&xsec_token=fixture&num=10`}).started, true);
assert.equal(calls.length, 3);
for (const request of [
    {method:'POST',url:base+'/api/sns/web/v1/comment/post',payload:{note_id:note,content:'forbidden'}},
    {method:'GET',url:comment.replace('edith.xiaohongshu.com','example.com')},
    {method:'GET',url:comment.replace('xsec_token=fixture','xsec_token=')},
    {method:'DELETE',url:comment},
    {method:'GET',url:comment+'#fragment'},
    {method:'POST',url:base+'/api/sns/web/v1/feed',payload:{source_note_id:'invalid',xsec_token:'fixture'}},
    {method:'GET',url:base+`/api/sns/web/v2/comment/sub/page?note_id=${note}&root_comment_id=invalid&xsec_token=fixture`},
]) assert.equal(window.__xhsNativeApi.start(request).started, false);
assert.equal(calls.length, 3, 'rejected requests must not reach the native client');
console.log('native API bridge: local-only probe, three read endpoints, and seven rejected requests passed; platform requests: 0');
