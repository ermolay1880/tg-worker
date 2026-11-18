export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    // Берем dst из параметров, если нет - используем дефолтный IP DC2 для теста
    const dst = url.searchParams.get("dst") || "149.154.167.51"; 
    
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
    // Пытаемся подключиться к Telegram
    const tcp = await connect({ hostname: dst, port: 443 });
    
    // Поток 1: От клиента (Telegram Desktop) к серверу (Telegram DC)
    const pumpWsToTcp = async () => {
      const writer = tcp.writable.getWriter();
      try {
        for await (const chunk of ws.readable) {
          await writer.write(chunk);
        }
      } catch (e) {
        // Игнорируем ошибки при разрыве соединения клиентом
      } finally {
        try { writer.close(); } catch {}
      }
    };

    // Поток 2: От сервера (Telegram DC) к клиенту (Telegram Desktop)
    const pumpTcpToWs = async () => {
      const writer = ws.writable.getWriter();
      try {
        for await (const chunk of tcp.readable) {
          await writer.write(chunk);
        }
      } catch (e) {
        // Игнорируем ошибки
      } finally {
        try { writer.close(); } catch {}
      }
    };

    // Запускаем оба потока параллельно
    await Promise.all([pumpWsToTcp(), pumpTcpToWs()]);
  } catch (e) {
    // Если connect не сработал, пишем ошибку (она будет видна в логах Cloudflare)
    console.error("Worker connect failed for dst:", dst, e);
  } finally {
    try { ws.close(); } catch {}
  }
}