export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const dst = url.searchParams.get("dst");
    const dc = url.searchParams.get("dc");
    if (!dst || !dc) return new Response("Missing params", { status: 400 });
    const tcpSocket = await connect({ hostname: dst, port: 443 });
    const webSocketPair = new WebSocketPair();
    const client = webSocketPair.client;
    const server = webSocketPair.server;
    client.accept();
    ctx.waitUntil(handleConnection(server, tcpSocket));
    return new Response(null, { status: 101, webSocket: client });
  },
};
async function handleConnection(ws, tcp) {
  const writer = tcp.writable.getWriter();
  for await (const chunk of ws.readable) {
    await writer.write(chunk);
  }
  ws.close();
  tcp.close();
}