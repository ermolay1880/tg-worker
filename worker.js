import { connect } from "cloudflare:sockets";

function toBytes(data) {
	if (data instanceof ArrayBuffer) return new Uint8Array(data);
	if (typeof data === "string") return new TextEncoder().encode(data);
	if (data && typeof data.arrayBuffer === "function") return data.arrayBuffer().then(ab => new Uint8Array(ab));
	return new Uint8Array();
}

export default {
	async fetch(request, env, ctx) {
		if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
			return new Response("Expected websocket", { status: 426 });
		}
		
		const url = new URL(request.url);
		const dst = url.searchParams.get("dst");
		
		if (!dst) {
			return new Response("Missing dst parameter", { status: 400 });
		}

		const pair = new WebSocketPair();
		const [client, server] = Object.values(pair);
		server.accept();

		// Запускаем обработку в фоне, чтобы не блокировать ответ 101
		ctx.waitUntil((async () => {
			try {
				const socket = connect({ hostname: dst, port: 443 });
				const tcpReader = socket.readable.getReader();
				const tcpWriter = socket.writable.getWriter();

				server.addEventListener("message", async (event) => {
					try { 
						await tcpWriter.write(await toBytes(event.data)); 
					} catch (e) { 
						console.error("Write error:", e);
						try { server.close(1011, "tcp write failed"); } catch {} 
					}
				});

				server.addEventListener("close", async () => {
					try { await tcpWriter.close(); } catch {}
					try { socket.close(); } catch {}
				});

				while (true) {
					const { value, done } = await tcpReader.read();
					if (done) break;
					if (value) server.send(value);
				}
			} catch (e) {
				console.error("Worker connection error for dst:", dst, e);
				try { server.close(1011, "Worker connect failed"); } catch {}
			} finally {
				try { server.close(); } catch {}
			}
		})());

		return new Response(null, { status: 101, webSocket: client });
	},
};