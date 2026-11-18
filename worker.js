export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const dst = url.searchParams.get("dst");
    
    if (!dst) {
      return new Response("Missing dst parameter", { status: 400 });
    }

    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);
    server.accept();

    ctx.waitUntil(handleConnection(server, dst));

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  },
};

async function handleConnection(ws, dst) {
  try {
    // Подключаемся к серверу Telegram
    const tcp = await connect({ hostname: dst, port: 443 });
    
    // Безопасная перекачка данных из WebSocket в TCP
    const pumpWsToTcp = async () => {
      const writer = tcp.writable.getWriter();
      try {
        for await (const chunk of ws.readable) {
          await writer.write(chunk);
        }
      } finally {
        writer.releaseLock();
      }
    };

    // Безопасная перекачка данных из TCP в WebSocket
    const pumpTcpToWs = async () => {
      const writer = ws.writable.getWriter();
      try {
        for await (const chunk of tcp.readable) {
          await writer.write(chunk);
        }
      } finally {
        writer.releaseLock();
      }
    };

    // Запускаем оба процесса параллельно
    await Promise.all([pumpWsToTcp(), pumpTcpToWs()]);
  } catch (e) {
    // Игнорируем ошибки при закрытии соединения клиентом
  } finally {
    try { ws.close(); } catch {}
  }
}