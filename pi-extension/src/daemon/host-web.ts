import { createServer, type Server } from "node:http";
import type { HostService } from "./host-service.js";
import type { HostRelay } from "./host-relay.js";

/** Loopback operator UI. Browser mutations require a same-origin custom header. */
export async function startHostWeb(
  host: HostService,
  relay: HostRelay | undefined,
  port: number,
): Promise<Server> {
  const server = createServer(async (request, response) => {
    response.setHeader("X-Content-Type-Options", "nosniff");
    response.setHeader("Cache-Control", "no-store");
    const address = `127.0.0.1:${port}`;
    if (request.headers.host !== address) {
      response.writeHead(403).end();
      return;
    }
    if (request.method === "GET" && request.url === "/") {
      response.setHeader("Content-Type", "text/html; charset=utf-8");
      response.end(page);
      return;
    }
    if (
      request.method !== "POST" ||
      request.url !== "/api" ||
      request.headers["x-pitato-local"] !== "1" ||
      (request.headers.origin && request.headers.origin !== `http://${address}`)
    ) {
      response.writeHead(403).end();
      return;
    }
    let body = "";
    try {
      for await (const chunk of request) {
        body += chunk.toString();
        if (Buffer.byteLength(body) > 1500000) throw new Error("Request too large");
      }
      const command = JSON.parse(body);
      const data =
        command.action === "pair"
          ? (relay?.pairing() ?? { error: "Relay disabled" })
          : command.action === "relay"
            ? { connected: relay?.connected ?? false }
            : await host.handle("local-operator", command);
      response.setHeader("Content-Type", "application/json");
      response.end(JSON.stringify({ ok: true, data }));
    } catch (error) {
      response.setHeader("Content-Type", "application/json");
      response.end(
        JSON.stringify({
          ok: false,
          error: error instanceof Error ? error.message : "Request failed",
        }),
      );
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", resolve);
  });
  return server;
}

const page = `<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>pitato · 电脑端</title>
<style>body{font:16px system-ui;background:#f8f5ed;color:#25291c;max-width:900px;margin:48px auto;padding:0 24px}button,select,textarea,input{font:inherit;padding:12px;border-radius:18px;border:1px solid #c3cbb5;margin:4px}button{background:#365e28;color:white;cursor:pointer}article{background:#fff;border-radius:28px;padding:24px;margin:20px 0}textarea{width:90%;min-height:72px}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#edf0e4;padding:16px;border-radius:16px}small{color:#59614e}#events{max-height:400px;overflow:auto}</style>
<h1>pitato</h1><p id="status">正在连接电脑…</p><article><h2>连接手机</h2><p>在 pitato Android 客户端粘贴下面的配对链接。链接两分钟内有效，只能使用一次。</p><button id="pair">生成配对链接</button><pre id="pairing">尚未生成</pre><button id="copy">复制链接</button></article>
<article><h2>项目与会话</h2><select id="projects"></select><button id="histories">查看历史</button><button id="fresh">新建聊天</button><div id="history"></div><p id="session"></p><textarea id="prompt" placeholder="给电脑上的助手发消息"></textarea><div><button id="send">发送</button><button id="abort">停止生成</button><button id="stop">关闭会话</button></div><div id="approvals"></div><pre id="events"></pre></article><p id="error" role="alert"></p>
<script type="module">
const $=id=>document.getElementById(id); let host,connection,cursor,creating,operation;
async function api(command){const r=await fetch('/api',{method:'POST',headers:{'Content-Type':'application/json','X-Pitato-Local':'1'},body:JSON.stringify(command)});const j=await r.json();if(!j.ok)throw Error(j.error);return j.data}
async function run(work){try{$('error').textContent='';await work()}catch(e){$('error').textContent=e.message}}
host=await api({action:'describe'});$('status').textContent=host.name+' · 电脑端已就绪'; for(const p of host.projects){const o=document.createElement('option');o.value=p.id;o.textContent=p.name;$('projects').append(o)}
$('pair').onclick=()=>run(async()=>{const p=await api({action:'pair'});$('pairing').textContent=p.uri||p.error});$('copy').onclick=()=>navigator.clipboard.writeText($('pairing').textContent);
async function open(command){const result=await api(command);if(!result.lease)throw Error('会话状态：'+result.status+'，请在电脑上检查仍存活的进程');connection=await api({action:'connect',binding:result.lease});cursor=undefined;$('session').textContent='已连接，手机与电脑共享这段聊天。';const snapshot=await api({action:'snapshot',connection});$('events').textContent=messageText(snapshot)}
$('fresh').onclick=()=>run(async()=>{if(creating)return;creating=true;operation??=crypto.randomUUID();try{await open({action:'create',projectId:$('projects').value,generation:host.generation,operationId:operation});operation=undefined}finally{creating=false}});
$('histories').onclick=()=>run(async()=>{const list=await api({action:'histories',projectId:$('projects').value});$('history').replaceChildren();for(const h of list){const b=document.createElement('button');b.textContent=h.name||new Date(h.modified).toLocaleString();b.onclick=()=>run(()=>open({action:'select',projectId:$('projects').value,historyId:h.id,generation:host.generation}));$('history').append(b)}});
$('send').onclick=()=>run(async()=>{await api({action:'prompt',connection,text:$('prompt').value,operationId:crypto.randomUUID()});$('prompt').value=''});$('abort').onclick=()=>run(()=>api({action:'abort',connection}));$('stop').onclick=()=>run(()=>api({action:'stop',connection}));
function messageText(snapshot){return(snapshot.messages||[]).map(message=>{const content=typeof message.content==='string'?message.content:(message.content||[]).filter(block=>block.type==='text').map(block=>block.text).join(String.fromCharCode(10));return content?((message.role==='user'?'你':message.role==='toolResult'?'工具':'助手')+'：'+String.fromCharCode(10)+content):''}).filter(Boolean).join(String.fromCharCode(10,10))}let polling=false;setInterval(async()=>{if(!connection||polling)return;polling=true;try{const result=await api({action:'poll',connection,cursor});cursor=result.cursor;if(result.gap){$('events').textContent=messageText(await api({action:'snapshot',connection}))}if(result.events.length){$('events').textContent=messageText(await api({action:'snapshot',connection}))}$('events').textContent=$('events').textContent.slice(-200000);$('approvals').replaceChildren();for(const a of result.approvals){const box=document.createElement('article'),title=document.createElement('p');title.textContent=a.request.title+' '+(a.request.message||'');box.append(title);function answer(fields){run(()=>api({action:'respond',connection,response:{type:'extension_ui_response',id:a.request.id,...fields}}))}for(const value of a.request.method==='select'?a.request.options:[]){const b=document.createElement('button');b.textContent=value;b.onclick=()=>answer({value});box.append(b)}if(a.request.method==='confirm'){const b=document.createElement('button');b.textContent='确认这一次';b.onclick=()=>answer({confirmed:true});box.append(b)}if(['input','editor'].includes(a.request.method)){const input=document.createElement('textarea');input.value=a.request.prefill||'';box.append(input);const b=document.createElement('button');b.textContent='提交';b.onclick=()=>answer({value:input.value});box.append(b)}const cancel=document.createElement('button');cancel.textContent='取消';cancel.onclick=()=>answer({cancelled:true});box.append(cancel);$('approvals').append(box)}}catch(e){$('error').textContent=e.message;connection=undefined}finally{polling=false}},1000);
</script></html>`;
