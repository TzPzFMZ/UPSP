// Production modules and HTML; controlled delayed responses, no Runtime/provider.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join, relative, extname } from 'node:path';
import { pathToFileURL } from 'node:url';

const gui = resolve(import.meta.dirname, '..');
const { build } = await import(pathToFileURL(join(gui, 'node_modules/esbuild/lib/main.js')));
const { availablePort, browserExecutable, connectCdp, pollJson } = await import(pathToFileURL(join(gui, 'scripts/browser-test-utils.mjs')));
const entry = `
import * as runtime from './src/runtime.ts';
import * as view from './src/view.ts';
import * as markdown from './src/markdown.ts';
import { initEvents } from './src/events.ts';
import { state, els, runtimeProjection, depositionProjection, taskProjection, settingsProjection } from './src/state.ts';
window.qaErrors = [];
window.addEventListener('error', e => window.qaErrors.push(e.message));
window.addEventListener('unhandledrejection', e => window.qaErrors.push(String(e.reason)));
initEvents();
runtimeProjection.host = 'connected';
runtimeProjection.hostSession = 'fixture-old';
runtimeProjection.status = {schema_version:'seed_gui_runtime_status.v3', host_session:'fixture-old', stage:'idle', current_round:null, can_stop:false};
state.systemWindowOpen = true;
els.app.hidden = false;
els.app.classList.add('system-open');
els.bootstrapRoot.hidden = true;
view.render();
window.qa = {runtime, view, markdown, state, els, runtimeProjection, depositionProjection, taskProjection, settingsProjection, fetch:window.fetch.bind(window), ready:true};
`;
const built = await build({stdin:{contents:entry, resolveDir:gui, loader:'ts'}, bundle:true, write:false, platform:'browser', format:'iife', target:'es2022', logLevel:'silent', plugins:[{name:'test-cache-counts', setup(b) {
  b.onLoad({filter:/[\\/]src[\\/]markdown\.ts$/}, async args => ({loader:'ts', contents: await readFile(args.path,'utf8') +
    '\nexport function testCacheCounts(){return {html:htmlCache.size,prefixes:documentPrefixes.size,images:imageLoads.size,mermaid:mermaidRenders.size,approved:approvedImageDocuments.size}}'}));
}}]});
const html = (await readFile(join(gui, 'index.html'), 'utf8')).replace(/<script[^>]*src="\.\/app\.js[^\"]*"[^>]*><\/script>/, '<script src="/qa.js"></script>');
assert.ok(html.includes('/qa.js'));
const temp = await mkdtemp(join(tmpdir(), 'upsp-defense-browser-'));
const port = await availablePort(), debug = await availablePort();
const requests = [];
let faultMode = '', savedRevision = 'before', releaseOldSettings;
const requestIds = new Set();
let mutationCount = 0;
const statusFixture = {schema_version:'seed_gui_runtime_status.v3', host_session:'fixture-old', stage:'idle',
  current_round:null, can_stop:false, send_in_flight:false, relay_in_flight:false, mutation_in_flight:false,
  restart_requested:false, interrupted_recovery:{pending:true, outcome_unknown:1}};
const settingsFixture = revision => ({schema_version:'seed_gui_settings.v3',
  files:Object.fromEntries(['system','now','lately','periodic','high_freq','relation'].map(id=>[id,{revision,values:{}}])),
  interface:{revision,values:{locale:'zh-CN'}}, model_catalog:{revision,connections:[],models:[]},
  persona:{model_routing:{revision,values:{}},setup_model_ready:true}});
const server = createServer(async (req, res) => {
  const path = new URL(req.url, 'http://localhost').pathname;
  requests.push({method:req.method, path});
  if (faultMode && path.startsWith('/api/')) {
    res.setHeader('Content-Type','application/json');
    res.setHeader('X-UPSP-Request-Session','fixture-session');
    if (req.method === 'POST') {
      for await (const _ of req) { /* consume the submitted request before losing the reply */ }
      assert.equal(req.headers['x-upsp-request-session'],'fixture-session');
      const id=req.headers['x-upsp-request-id']; assert.ok(id);
      if (requestIds.has(id)) {res.writeHead(409);res.end(JSON.stringify({error:'submission_outcome_unconfirmed'}));return;}
      requestIds.add(id); mutationCount++;
      if (faultMode === 'disconnect') {savedRevision='after-disconnect'; req.socket.destroy(); return;}
      if (faultMode === 'non-json') {res.end('not JSON'); return;}
      if (path === '/api/settings') {savedRevision='saved';res.end(JSON.stringify(settingsFixture(savedRevision)));return;}
      res.end('{}'); return;
    }
    if (path === '/api/settings' && faultMode === 'old-settings') {
      releaseOldSettings = () => res.end(JSON.stringify(settingsFixture('obsolete'))); return;
    }
    if (path === '/api/runtime/status' && faultMode === 'bad-status') {res.end('{"schema_version":"seed_gui_runtime_status.v3"}');return;}
    const payload = path === '/api/runtime/status' ? statusFixture
      : path === '/api/live/state' ? {round:null,state:null}
      : path === '/api/rounds' ? {rounds:[]}
      : path === '/api/settings' ? settingsFixture(savedRevision)
      : path === '/api/workbench/task' ? {schema_version:'seed_gui_task_projection.v1',task:null} : {};
    res.end(JSON.stringify(payload)); return;
  }
  if (req.method !== 'GET' || path.startsWith('/api/')) {res.writeHead(403);res.end('fixture denies APIs');return;}
  try {
    if (path === '/') {res.setHeader('Content-Type','text/html; charset=utf-8');res.end(html);return;}
    if (path === '/qa.js') {res.setHeader('Content-Type','text/javascript');res.end(built.outputFiles[0].text);return;}
    const file = resolve(gui, '.' + path);
    assert.ok(!relative(gui, file).startsWith('..'));
    res.setHeader('Content-Type', ({'.css':'text/css','.js':'text/javascript','.png':'image/png','.woff2':'font/woff2','.md':'text/plain'})[extname(file)] || 'application/octet-stream');
    res.end(await readFile(file));
  } catch {res.writeHead(404);res.end();}
});
await new Promise(done => server.listen(port, '127.0.0.1', done));
const process = spawn(browserExecutable, ['--headless=new','--edge-skip-compat-layer-relaunch','--disable-gpu','--disable-extensions','--disable-background-networking','--no-first-run',`--user-data-dir=${join(temp,'profile')}`,`--remote-debugging-port=${debug}`,'about:blank'], {windowsHide:true,stdio:'ignore'});
let cdp, passed = false;
try {
  const tabs = await pollJson(`http://127.0.0.1:${debug}/json`);
  cdp = await connectCdp(tabs.find(t=>t.type==='page').webSocketDebuggerUrl);
  const evaluate = async expression => {
    const r = await cdp.call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});
    if (r.exceptionDetails) throw new Error(JSON.stringify(r.exceptionDetails));
    return r.result.value;
  };
  await cdp.call('Network.enable');
  await cdp.call('Network.setBlockedURLs',{urls:['https://*','http://*/api/*']});
  await cdp.call('Emulation.setDeviceMetricsOverride',{width:1440,height:900,deviceScaleFactor:1,mobile:false});
  await cdp.call('Page.navigate',{url:`http://127.0.0.1:${port}/`});
  for(let i=0;i<100;i++){if(await evaluate('Boolean(window.qa?.ready)'))break;await new Promise(r=>setTimeout(r,50));}
  assert.equal(await evaluate('Boolean(window.qa?.ready)'),true);
  await evaluate('document.fonts.ready.then(()=>true)');

  const manualRace = await evaluate(`(async()=>{
    const pending={}; window.fetch=url=>new Promise(done=>pending[String(url)]=done);
    const first=qa.view.openManual('intro.md');
    const second=qa.view.openManual('memory-bus.md');
    pending['./manual/memory-bus.md'](new Response('---\\ntitle: NEW MANUAL\\n---\\nSelected'));
    await second;
    pending['./manual/intro.md'](new Response('---\\ntitle: OLD MANUAL\\n---\\nStale'));
    await first;
    return document.querySelector('#manualTitle').textContent;
  })()`);
  assert.equal(manualRace,'NEW MANUAL');
  for (const fails of [false,true]) {
    const closed = await evaluate(`(async()=>{
      let finish; window.fetch=()=>new Promise(done=>finish=done);
      const read=qa.view.openManual('intro.md'); qa.view.closeManual();
      finish(new Response('closed request',{status:${fails ? 500 : 200}}));await read;
      return document.querySelector('#manualOverlay').hidden;
    })()`);
    assert.equal(closed,true,'closed pane must stay closed');
  }
  const sibling = await evaluate(`(async()=>{
    let finish; window.fetch=()=>new Promise(done=>finish=done);
    const read=qa.view.openManual('intro.md');
    qa.view.openLedgerEvent(802,'later',{type:'event',event_type:'SELECTED EVENT'});
    const title=document.querySelector('#manualTitle').textContent;
    finish(new Response('obsolete failure',{status:500}));await read;
    return {title,current:document.querySelector('#manualTitle').textContent};
  })()`);
  assert.equal(sibling.current,sibling.title,'old manual error must not replace another detail');
  const protocol = await evaluate(`(async()=>{
    let finish; window.fetch=()=>new Promise(done=>finish=done);
    const read=qa.view.openProtocolDocument('doc','old.md');
    window.fetch=async()=>new Response('---\\ntitle: CURRENT MANUAL\\n---\\nSelected');
    await qa.view.openManual('memory-bus.md');
    finish(Response.json({schema_version:'seed_gui_protocol_document.v1',kind:'doc',id:'old.md',content_md:'Old protocol'}));
    await read; return document.querySelector('#manualTitle').textContent;
  })()`);
  assert.equal(protocol,'CURRENT MANUAL');
  for (const close of [false,true]) {
    const memory = await evaluate(`(async()=>{
      qa.depositionProjection.index={memory:[{id:'MEM-RACE',title:'Old memory'}],containers:[],relations:[]};
      delete qa.depositionProjection.details.memory['MEM-RACE'];
      let finish;window.fetch=()=>new Promise(done=>finish=done);
      const button=document.createElement('button');
      button.dataset.depositionKind='memory';button.dataset.depositionId='MEM-RACE';
      document.body.append(button);button.click();button.remove();
      if (${close}) qa.view.closeManual();
      else {
        window.fetch=async()=>new Response('---\\ntitle: NEW DETAIL\\n---\\nCurrent');
        await qa.view.openManual('intro.md');
      }
      finish(Response.json({schema_version:'seed_gui_deposition_detail.v1',kind:'memory',item:{id:'MEM-RACE',body:'Old memory'}}));
      await new Promise(done=>setTimeout(done,0));
      return {hidden:document.querySelector('#manualOverlay').hidden,title:document.querySelector('#manualTitle').textContent};
    })()`);
    assert.equal(memory.hidden,close);
    if (!close) assert.equal(memory.title,'NEW DETAIL');
  }
  const locale = await evaluate(`(async()=>{
    const pending={};window.fetch=url=>new Promise(done=>pending[String(url)]=done);
    const read=qa.view.openManual('intro.md'); qa.view.changeLocale('en-US');
    pending['./manual/intro.en-US.md'](new Response('---\\ntitle: ENGLISH MANUAL\\n---\\nEnglish'));
    await new Promise(done=>setTimeout(done,0));
    pending['./manual/intro.md'](new Response('---\\ntitle: OLD CHINESE MANUAL\\n---\\nChinese'));
    await read;return document.querySelector('#manualTitle').textContent;
  })()`);
  assert.equal(locale,'ENGLISH MANUAL');
  await evaluate(`qa.view.closeManual();qa.view.changeLocale('zh-CN')`);
  const liveChecks=[];
  for (const body of ['{broken-json','{}','{"round":null}','{"round":0,"state":{}}','null','{"round":802,"state":{"schema_version":"round_live_state.v3","round":801}}','{"round":null,"state":null}','{"round":null,"state":{"schema_version":"round_live_state.v3","round":null}}']) {
    const observed=await evaluate(`(async()=>{
      qa.runtimeProjection.round=802;
      qa.runtimeProjection.live={schema_version:'round_live_state.v3',round:802,last_event_index:8};
      window.fetch=async url=>{
        if(String(url).includes('/runtime/status')) return Response.json({schema_version:'seed_gui_runtime_status.v3',host_session:'fixture-old',stage:'idle',can_stop:false,send_in_flight:false,relay_in_flight:false,mutation_in_flight:false,restart_requested:false,current_round:null});
        if(String(url).includes('/live/state'))return new Response(${JSON.stringify(body)});
        if(String(url).includes('/rounds'))return Response.json({rounds:[]});
        if(String(url).includes('/workbench/task'))return Response.json({schema_version:'seed_gui_task_projection.v1',task:null});
        return Response.json({});
      };
      await qa.runtime.pollRuntime({forceFull:true,ignoreVisibility:true});
      return {round:qa.runtimeProjection.round,live:qa.runtimeProjection.live,liveError:qa.runtimeProjection.liveError,host:qa.runtimeProjection.host};
    })()`);
    if (body === '{"round":null,"state":null}') {
      assert.deepEqual(observed,{round:null,live:null,liveError:'',host:'connected'});
    } else if (body === '{"round":null,"state":{"schema_version":"round_live_state.v3","round":null}}') {
      assert.equal(observed.round,null);
      assert.equal(observed.live.round,null);
      assert.equal(observed.liveError,'');
    } else {
      assert.equal(observed.round,802);
      assert.equal(observed.live.last_event_index,8);
      assert.ok(observed.liveError);
    }
    liveChecks.push({body,...observed});
  }
  assert.deepEqual(await evaluate('window.qaErrors'),[]);
  for (const payload of [
    {schema_version:'round_live_events.v2'},
    {schema_version:'round_live_events.v2',round:802,state:{schema_version:'round_live_state.v3',round:801}},
    {schema_version:'round_live_events.v2',round:802,state:null},
  ]) {
    const observed=await evaluate(`(async()=>{
      qa.runtimeProjection.round=802;
      qa.runtimeProjection.live={schema_version:'round_live_state.v3',round:802,last_event_index:8};
      qa.runtimeProjection.fullRefreshNeeded=false;
      qa.runtimeProjection.liveError='';qa.runtimeProjection.liveRetryAfter=0;
      window.fetch=async url=>{
        if(String(url).includes('/runtime/status')) return Response.json({schema_version:'seed_gui_runtime_status.v3',host_session:'fixture-old',stage:'idle',can_stop:false,send_in_flight:false,relay_in_flight:false,mutation_in_flight:false,restart_requested:false,current_round:802});
        if(String(url).includes('/live/events')) return Response.json(${JSON.stringify(payload)});
        if(String(url).includes('/rounds'))return Response.json({rounds:[]});
        return Response.json({});
      };
      await qa.runtime.pollRuntime({ignoreVisibility:true});
      return {round:qa.runtimeProjection.round,event:qa.runtimeProjection.live.last_event_index,error:qa.runtimeProjection.liveError};
    })()`);
    assert.equal(observed.round,802);
    assert.equal(observed.event,8);
    assert.equal(Boolean(observed.error),payload.state !== null);
  }
  await evaluate(`window.installSession=host=>{window.fetch=async url=>{
    const s=String(url);
    if(s.includes('/runtime/status'))return Response.json({schema_version:'seed_gui_runtime_status.v3',host_session:host,stage:'idle',can_stop:false,send_in_flight:false,relay_in_flight:false,mutation_in_flight:false,restart_requested:false,current_round:803});
    if(s.includes('/live/state'))return Response.json({round:803,state:{schema_version:'round_live_state.v3',round:803,last_event_index:0,frame_catalog:[{frame_id:'reaction-1'}]}});
    if(s.includes('/rounds'))return Response.json({rounds:[]});return Response.json({});
  }};window.diffPayload=label=>({schema_version:'seed_gui_request_prefix_diff.v1',state:'identical',current:{round:803,frame_id:'reaction-1'},previous:{frame_id:'setup'},common_prefix_bytes:10,current_wire_bytes:10,audit_label:label});`);
  const sessionRaces=[];
  for (const oldError of [false,true]) {
    const observed=await evaluate(`(async()=>{
      qa.state.activePage='overview';installSession('before-${oldError}');
      await qa.runtime.pollRuntime({forceFull:true,ignoreVisibility:true});
      const old={},fresh={};let detailReads=0;
      window.fetch=url=>new Promise(done=>old[String(url)]=done);
      const prefix=qa.runtime.pollContextPrefixDiffForSelection();
      const detail=qa.runtime.loadDepositionDetail('memory','MEM-803',{render:false});
      const forced=qa.runtime.loadDepositionDetail('memory','MEM-803',{force:true,render:false});
      qa.state.conversationStickToBottom=false;
      installSession('after-${oldError}');await qa.runtime.pollRuntime({forceFull:true,ignoreVisibility:true});
      if(!qa.state.conversationStickToBottom)throw new Error('new session retained the old reading position');
      window.fetch=url=>new Promise(done=>{fresh[String(url)]=done;if(String(url).includes('/deposition/'))detailReads++});
      const newPrefix=qa.runtime.pollContextPrefixDiffForSelection();
      const newDetail=qa.runtime.loadDepositionDetail('memory','MEM-803',{render:false});
      old['./api/context/request-prefix-diff?round=803&frame_id=reaction-1'](${oldError} ? new Response('old failure',{status:500}) : Response.json(diffPayload('old')));
      old['./api/deposition/memory?id=MEM-803'](${oldError} ? new Response('old failure',{status:500}) : Response.json({schema_version:'seed_gui_deposition_detail.v1',kind:'memory',item:{id:'MEM-803',content:'old'}}));
      await Promise.all([prefix,detail,forced]);
      const waiting={prefix:qa.runtimeProjection.contextPrefixDiffLoading,detail:qa.depositionProjection.pendingDetails.has('memory:MEM-803'),prefixError:qa.runtimeProjection.contextPrefixDiffError,detailError:qa.depositionProjection.detailErrors['memory:MEM-803']||'',oldBody:Boolean(qa.depositionProjection.details.memory['MEM-803'])};
      fresh['./api/context/request-prefix-diff?round=803&frame_id=reaction-1'](Response.json(diffPayload('new')));
      fresh['./api/deposition/memory?id=MEM-803'](Response.json({schema_version:'seed_gui_deposition_detail.v1',kind:'memory',item:{id:'MEM-803',content:'new'}}));
      await Promise.all([newPrefix,newDetail]);
      await qa.runtime.loadDepositionDetail('memory','MEM-803',{render:false});
      return {waiting,detailReads,prefix:qa.runtimeProjection.contextPrefixDiff.audit_label,body:qa.depositionProjection.details.memory['MEM-803'].item.content};
    })()`);
    assert.deepEqual(observed.waiting,{prefix:true,detail:true,prefixError:'',detailError:'',oldBody:false});
    assert.equal(observed.detailReads,1);assert.equal(observed.prefix,'new');assert.equal(observed.body,'new');
    sessionRaces.push(observed);
  }
  const indexRace=await evaluate(`(async()=>{
    let oldFinish;window.fetch=()=>new Promise(done=>oldFinish=done);
    const old=qa.runtime.pollDeposition({force:true,ignoreVisibility:true});
    installSession('index-new');await qa.runtime.pollRuntime({forceFull:true,ignoreVisibility:true});
    let newFinish;window.fetch=()=>new Promise(done=>newFinish=done);
    const fresh=qa.runtime.pollDeposition({force:true,ignoreVisibility:true});
    oldFinish(Response.json({schema_version:'seed_gui_deposition_index.v1',memory:[{id:'OLD'}],containers:[],relations:[]}));await old;
    const waiting=qa.depositionProjection.loading&&qa.depositionProjection.index===null;
    newFinish(Response.json({schema_version:'seed_gui_deposition_index.v1',memory:[],containers:[],relations:[]}));
    return {waiting,loaded:await fresh,memory:qa.depositionProjection.index.memory.length};
  })()`);
  assert.deepEqual(indexRace,{waiting:true,loaded:true,memory:0});
  for (const indexFirst of [false,true]) {
    const firstConnection=await evaluate(`(async()=>{
      qa.runtimeProjection.hostSession='';
      let finish;window.fetch=()=>new Promise(done=>finish=done);
      const index=qa.runtime.pollDeposition({force:true,ignoreVisibility:true});
      const payload={schema_version:'seed_gui_deposition_index.v1',memory:[],containers:[],relations:[]};
      if(${indexFirst}){finish(Response.json(payload));await index}
      installSession('first-connection-${indexFirst}');await qa.runtime.pollRuntime({forceFull:true,ignoreVisibility:true});
      if(!${indexFirst}){finish(Response.json(payload));await index}
      let reads=0;window.fetch=async()=>{reads++;return Response.json(payload)};
      const loaded=await qa.runtime.pollDeposition({force:true,ignoreVisibility:true});
      return {reads,loaded,loading:qa.depositionProjection.loading};
    })()`);
    assert.deepEqual(firstConnection,{reads:1,loaded:true,loading:false});
  }
  const cacheLifecycle=await evaluate(`(async()=>{
    qa.view.closeManual();qa.markdown.clearMarkdownDocuments();
    const box=document.createElement('div');box.className='chat-bubble';document.body.append(box);
    let max=0;
    for(let i=0;i<800;i++){
      box.innerHTML=qa.markdown.renderMarkdownDocument('dialogue-'+i,'材料'+i+'\\n\\n'+'内容'.repeat(1024));
      qa.markdown.hydrateMarkdownDocuments(box);await Promise.resolve();
      max=Math.max(max,qa.markdown.testCacheCounts().html);
    }
    const content='原文[^1]\\n\\n[^1]: 注释';
    const rendered=qa.markdown.renderMarkdownDocument('copy-doc',content);
    box.innerHTML='<button data-markdown-document-copy>Copy</button>'+rendered;
    qa.markdown.hydrateMarkdownDocuments(box);await Promise.resolve();
    let copied='';Object.defineProperty(navigator,'clipboard',{configurable:true,value:{writeText:async text=>{copied=text}}});
    box.querySelector('button').click();await Promise.resolve();
    const reused=rendered===qa.markdown.renderMarkdownDocument('copy-doc',content);
    const footnote=box.querySelector('a[href^="#"]');
    const validFootnote=Boolean(footnote&&document.getElementById(decodeURIComponent(footnote.hash.slice(1))));
    box.remove();qa.markdown.scheduleMarkdownPrune();await Promise.resolve();
    return {max,copied,reused,validFootnote,remaining:qa.markdown.testCacheCounts()};
  })()`);
  assert.equal(cacheLifecycle.max,1);assert.equal(cacheLifecycle.copied,'原文[^1]\n\n[^1]: 注释');
  assert.equal(cacheLifecycle.reused,true);assert.equal(cacheLifecycle.validFootnote,true);
  assert.deepEqual(cacheLifecycle.remaining,{html:0,prefixes:0,images:0,mermaid:0,approved:0});
  await cdp.call('HeapProfiler.collectGarbage');
  const heapAfterRelease=await cdp.call('Runtime.getHeapUsage');
  await evaluate(`window.mediaBox=document.createElement('div');document.body.append(mediaBox);
    mediaBox.innerHTML=qa.markdown.renderMarkdownDocument('media-doc','![本地测试图片](http://127.0.0.1:${port}/assets/upsp-logo.png)\\n\\n'+String.fromCharCode(96).repeat(3)+'mermaid\\nflowchart LR\\n A --> B\\n'+String.fromCharCode(96).repeat(3));
    qa.markdown.hydrateMarkdownDocuments(mediaBox);`);
  assert.equal(await evaluate('mediaBox.querySelectorAll("img").length'),0,'images require an explicit click');
  await evaluate('mediaBox.querySelector("button.md-image-placeholder").click()');
  for(let i=0;i<100;i++){
    if(await evaluate('Boolean(mediaBox.querySelector("img")&&mediaBox.querySelector("[data-mermaid-state=ready]"))'))break;
    await new Promise(r=>setTimeout(r,50));
  }
  assert.equal(await evaluate('Boolean(mediaBox.querySelector("img")&&mediaBox.querySelector("[data-mermaid-state=ready]"))'),true);
  const releasedMedia=await evaluate(`(async()=>{mediaBox.remove();qa.markdown.scheduleMarkdownPrune();await Promise.resolve();return qa.markdown.testCacheCounts()})()`);
  assert.deepEqual(releasedMedia,{html:0,prefixes:0,images:0,mermaid:0,approved:0});
  const chatScroll=await evaluate(`(async()=>{
    const frame=()=>new Promise(done=>requestAnimationFrame(()=>requestAnimationFrame(done)));
    const chat=document.querySelector('#chatThread');
    const distance=()=>chat.scrollHeight-chat.clientHeight-chat.scrollTop;
    const reasoning={node_id:'reason-805',type:'reasoning',first_event_index:1,status:'succeeded',content_raw:'思考\\n'.repeat(60)};
    const reply={node_id:'reply-805',type:'final',first_event_index:2,status:'succeeded',content_raw:'正文段落\\n\\n'.repeat(60)};
    const live={schema_version:'round_live_state.v3',round:805,
      dialogue_timeline:{schema_version:'round_dialogue_timeline.v1',order:[reasoning.node_id,reply.node_id],nodes:[reasoning,reply]},
      dialogue_activity:{schema_version:'round_dialogue_activity.v1',terminal:true}};
    qa.runtimeProjection.conversationRounds=new Map([[805,live]]);
    qa.runtimeProjection.conversationRoundOrder=[805];qa.runtimeProjection.round=805;
    qa.runtimeProjection.liveError='';qa.runtimeProjection.conversationHistoryError='';
    qa.runtimeProjection.conversationHistoryHasMore=false;qa.state.conversationStickToBottom=true;
    qa.view.renderChat();await frame();
    const initial=distance()<=1&&chat.scrollHeight>chat.clientHeight;
    const node=chat.querySelector('[data-dialogue-node-id="reply-805"]');
    const boundaries=[];
    for(const gap of [24,25]){
      chat.scrollTop=chat.scrollHeight-chat.clientHeight-gap;chat.dispatchEvent(new Event('scroll'));
      boundaries.push(qa.state.conversationStickToBottom);
    }
    chat.scrollTop=100;chat.dispatchEvent(new Event('scroll'));
    const detached=!qa.state.conversationStickToBottom;
    const top=chat.scrollTop;reply.content_raw+='新增段落\\n\\n'.repeat(10);qa.view.renderChat();await frame();
    const readingPreserved=chat.scrollTop===top;
    chat.scrollTop=chat.scrollHeight;chat.dispatchEvent(new Event('scroll'));
    reply.content_raw+='更多正文\\n\\n'.repeat(10);qa.view.renderChat();await frame();
    const sticky=qa.state.conversationStickToBottom&&distance()<=1;
    const details=chat.querySelector('details[data-dialogue-node-id="reason-805"]');
    const anchoring=chat.style.overflowAnchor;chat.style.overflowAnchor='none';
    details.querySelector('summary').click();await frame();
    const toggleSticky=distance()<=1;chat.style.overflowAnchor=anchoring;
    qa.view.renderChat();await frame();
    return {initial,boundaries,detached,readingPreserved,sticky,toggleSticky,
      sameReply:node===chat.querySelector('[data-dialogue-node-id="reply-805"]'),
      disclosurePreserved:chat.querySelector('details[data-dialogue-node-id="reason-805"]').open};
  })()`);
  assert.deepEqual(chatScroll,{initial:true,boundaries:[true,false],detached:true,readingPreserved:true,sticky:true,toggleSticky:true,sameReply:true,disclosurePreserved:true});
  // Synthetic immutable detail responses exercise production selectors/rendering;
  // the backend's recorded-vs-active-layer behavior is verified separately.
  const recordedFrames=await evaluate(`(async()=>{
    const frame=(iteration,label,time)=>({round:807,frame_id:'R000807:reaction:'+iteration,
      phase:'reaction',iteration,label:'Synthetic F'+iteration,created_at:time,layer_source:'layers_snapshot',
      context_panes:[{id:'50_now',title:'NOW',content_md:'SYNTHETIC RECORDED '+label},
        {id:'40_high_freq',title:'High frequency',content_blocks:[
          {block_id:'high_freq:task_board',content_md:'SYNTHETIC TASK '+label}]}]});
    const first=frame(1,'A','2026-09-05T12:34:56+08:00');
    const second=frame(2,'B','2026-09-05T12:35:56+08:00');
    const catalog=({context_panes,...entry})=>entry;
    const live={schema_version:'round_live_state.v3',round:807,last_event_index:1,
      latest_frame_id:first.frame_id,frame_catalog:[catalog(first)]};
    const reads=[];
    window.fetch=async url=>{
      const query=new URL(String(url),location.href);
      if(query.pathname==='/api/context/request-prefix-diff')return Response.json({
        schema_version:'seed_gui_request_prefix_diff.v1',state:'unavailable',reason:'synthetic_fixture'});
      if(query.pathname!=='/api/live/detail')throw new Error('unexpected fixture request: '+url);
      const kind=query.searchParams.get('kind'),ref=query.searchParams.get('ref')||'';
      if(kind!=='frame'&&kind!=='ledger')throw new Error('unexpected detail kind: '+kind);
      const payload=kind==='ledger'?{items:[]}:[first,second].find(item=>item.frame_id===ref);
      if(!payload)throw new Error('unknown fixture frame: '+ref);
      if(kind==='frame')reads.push(ref);
      return Response.json({schema_version:'round_live_detail.v1',round:807,kind,ref,payload});
    };
    Object.assign(qa.runtimeProjection,{round:807,live,frameDetail:null,frameDetailLoading:'',frameDetailError:'',
      conversationRounds:new Map([[807,live]]),conversationRoundOrder:[807]});
    Object.assign(qa.state,{selectedContextRound:null,selectedContextFrame:null,
      selectedTaskRound:null,selectedTaskFrame:null,activeRuntimePane:'50_now'});
    Object.assign(qa.taskProjection,{loading:false,error:'',
      data:{schema_version:'seed_gui_task_projection.v1',task:null,summary:{}}});
    const context=()=>{
      qa.view.setPage('context','content');qa.view.renderStage('context');
      const article=document.querySelector('.runtime-context-workspace > article');
      return {body:article.textContent,time:article.querySelector('time').getAttribute('datetime'),
        selected:article.querySelector('[data-context-frame]').value};
    };
    const choose=async(selector,value)=>{
      const control=document.querySelector(selector);control.value=value;
      if(control.value!==value)throw new Error('frame option missing: '+value);
      control.dispatchEvent(new Event('change',{bubbles:true}));
      for(let i=0;i<50&&qa.runtimeProjection.frameDetailLoading;i++)await new Promise(done=>setTimeout(done,0));
      if(qa.runtimeProjection.frameDetailLoading||qa.runtimeProjection.frameDetailError)throw new Error('frame detail did not load');
    };
    await qa.runtime.loadFrameDetail(807,first.frame_id);
    const initial=context();
    qa.view.setPage('run','tools');
    live.frame_catalog.push(catalog(second));live.latest_frame_id=second.frame_id;live.last_event_index=2;
    qa.runtime.refreshRuntimeUi();
    await choose('[data-task-frame]',first.frame_id);
    const cached={body:document.querySelector('.task-pane').textContent,
      selected:document.querySelector('[data-task-frame]').value,reads:[...reads]};
    await choose('[data-task-frame]','latest');
    const latest=context();
    await choose('[data-context-frame]',first.frame_id);
    const returned=context();
    return {initial,cached,latest,returned,reads,response_source:'synthetic'};
  })()`);
  for(const [observed,label,time,selected] of [
    [recordedFrames.initial,'A','2026-09-05T12:34:56+08:00','latest'],
    [recordedFrames.latest,'B','2026-09-05T12:35:56+08:00','latest'],
    [recordedFrames.returned,'A','2026-09-05T12:34:56+08:00','R000807:reaction:1'],
  ]) {
    assert.ok(observed.body.includes('SYNTHETIC RECORDED '+label));
    assert.ok(!observed.body.includes('SYNTHETIC RECORDED '+(label==='A'?'B':'A')));
    assert.equal(observed.time,time);assert.equal(observed.selected,selected);
  }
  assert.ok(recordedFrames.cached.body.includes('该帧任务快照'));
  assert.ok(recordedFrames.cached.body.includes('SYNTHETIC TASK A'));
  assert.ok(!recordedFrames.cached.body.includes('SYNTHETIC TASK B'));
  assert.equal(recordedFrames.cached.selected,'R000807:reaction:1');
  assert.deepEqual(recordedFrames.cached.reads,['R000807:reaction:1']);
  assert.deepEqual(recordedFrames.reads,['R000807:reaction:1','R000807:reaction:2','R000807:reaction:1']);
  assert.deepEqual(await evaluate('window.qaErrors'),[]);
  assert.equal(requests.filter(r=>r.method!=='GET'||r.path.startsWith('/api/')).length,0);
  await cdp.call('Network.setBlockedURLs',{urls:['https://*']});
  await evaluate(`window.fetch=qa.fetch; window.alert=()=>{}; qa.view.closeManual(); qa.state.globalSettingsOpen=false;`);
  const boundaryChecks=[];
  for (const mode of ['disconnect','non-json']) {
    faultMode=mode;
    const before=mutationCount;
    const result=await evaluate(`(async()=>{
      qa.settingsProjection.data=${JSON.stringify(settingsFixture('before'))};
      qa.runtimeProjection.awaitingProjection=false; qa.els.messageInput.value='isolated failure check';
      await qa.runtime.submitRuntimeMessage(new Event('submit'));
      return {feedback:qa.runtimeProjection.sendFeedback,host:qa.runtimeProjection.host,
        recovery:document.querySelector('[data-timeline-key="interrupted-recovery"]')?.textContent};
    })()`);
    assert.equal(mutationCount-before,1,'transport retry must not execute twice');
    assert.ok(result.feedback.includes('提交结果未确认')); assert.equal(result.host,'connected');
    assert.ok(result.recovery.includes('不确定动作'));
    boundaryChecks.push({mode,...result});
  }
  statusFixture.interrupted_recovery={pending:true,outcome_unknown:1,reason:'persistent_effect_recovery_required',affected_ids:['MEM-80800001']};
  await evaluate('qa.runtime.pollRuntime({forceFull:true,ignoreVisibility:true})');
  const persistentRecovery=await evaluate(`document.querySelector('[data-timeline-key="interrupted-recovery"]')?.textContent`);
  assert.ok(persistentRecovery.includes('本地写入恢复尚未确认'));
  assert.ok(persistentRecovery.includes('MEM-80800001'));
  faultMode='bad-status';
  assert.equal(await evaluate(`qa.runtime.pollRuntime({forceFull:true,ignoreVisibility:true}).then(()=>qa.runtimeProjection.host)`),'error');
  faultMode='old-settings';
  await evaluate('window.oldSettingsRead=qa.runtime.pollSettings({force:true});true');
  for(let i=0;i<100&&!releaseOldSettings;i++)await new Promise(r=>setTimeout(r,10));
  assert.ok(releaseOldSettings);
  faultMode='settings';
  await evaluate(`qa.runtime.submitSettings([['interface',{locale:'zh-CN'}]])`);
  releaseOldSettings(); await evaluate('oldSettingsRead');
  assert.equal(await evaluate('qa.settingsProjection.data.interface.revision'),'saved');
  faultMode='disconnect';
  const before=mutationCount;
  await evaluate(`qa.runtime.submitSettings([['interface',{locale:'zh-CN'}]])`);
  assert.equal(mutationCount-before,1);
  assert.equal(await evaluate('qa.settingsProjection.data.interface.revision'),'after-disconnect');
  assert.ok((await evaluate('qa.settingsProjection.feedback')).includes('提交结果未确认'));
  assert.deepEqual(await evaluate('window.qaErrors'),[]);
  console.log(JSON.stringify({boundaryChecks,staleSettingsIgnored:true,settingsReadback:true,provider_called:false}));
  console.log(JSON.stringify({ok:true,manualRace,closedSuccessAndError:true,sibling,protocol,locale,liveChecks,sessionRaces,indexRace,cacheLifecycle,heapAfterRelease,releasedMedia,chatScroll,recordedFrames,provider_called:false}));

  passed = true;
} finally {
  if (cdp) await Promise.race([cdp.call('Browser.close'), new Promise(r=>setTimeout(r,1500))]).catch(()=>{});
  cdp?.close();
  process.kill();
  server.closeAllConnections();
  await new Promise(done=>server.close(done));
  const checked = resolve(temp);
  assert.ok(relative(resolve(tmpdir()),checked).startsWith('upsp-defense-browser-'));
  if (passed) await rm(checked,{recursive:true,force:true,maxRetries:10,retryDelay:100});
  else console.error('Failure site retained:',checked);
}
