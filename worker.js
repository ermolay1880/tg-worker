export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const dst = url.searchParams.get("dst");
    const dc = url.searchParams.get("dc");
    
    if (!dst || !dc) return new Response("Missing params", { status: 400 });

    try {
      const tcpSocket = await connect({ hostname: dst, port: 443 });
      const webSocketPair = new WebSocketPair();
      const client = webSocketPair.client;
      const server = webSocketPair.server;

      client.accept();
      ctx.waitUntil(handleConnection(server, tcpSocket));

      return new Response(null, {
        status: 101,
        webSocket: client,
      });
    } catch (e) {
      return new Response(`Worker Error: ${e.message}`, { status: 500 });
    }
  },
};

async function handleConnection(ws, tcp) {
  try {
    // Используем pipeTo для более эффективной пересылки бинарных данных
    const wsReadable = ws.readable;
    const tcpWritable = tcp.writable;
    const tcpReadable = tcp.readable;
    const wsWritable = ws.writable;

    // Запускаем два потока одновременно
    await Promise.all([
      wsReadable.pipeTo(tcpWritable),
      tcpReadable.pipeTo(wsWritable)
    ]);
  } catch (e) {
    // Игнорируем ошибки закрытия соединения клиентом
  } finally {
    try { ws.close(); } catch {}
    try { tcp.close(); } catch {}
  }
}