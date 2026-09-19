// UUID اختصاصی شما
let userID = '24b4b24b-3241-4824-a15d-8b093112c314';
// یک آی‌پی تمیز برای پروکسی (می‌توانی بعداً تغییر دهی)
let proxyIP = 'cdn.anycast.eu.org';

export default {
  async fetch(request, env, ctx) {
    try {
      userID = env.UUID || userID;
      proxyIP = env.PROXYIP || proxyIP;
      
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        return new Response("EdgeOne VLESS is Active and Running!", { status: 200 });
      }
      
      return await vlessOverWSHandler(request);
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  }
};

async function vlessOverWSHandler(request) {
  const webSocketPair = new WebSocketPair();
  const [client, webSocket] = Object.values(webSocketPair);
  webSocket.accept();

  let address = '';
  let portWithRandomLog = '';
  const log = (info, event) => {
    console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
  };
  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
  const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader, log);
  let remoteSocketWapper = { value: null };
  let isDns = false;

  readableWebSocketStream.pipeTo(new WritableStream({
    async write(chunk, controller) {
      if (remoteSocketWapper.value) {
        const writer = remoteSocketWapper.value.writable.getWriter()
        await writer.write(chunk);
        writer.releaseLock();
        return;
      }
      
      const {
        hasError,
        message,
        portRemote = 443,
        addressRemote = '',
        rawDataIndex,
        vlessVersion = new Uint8Array([0, 0]),
        isUDP,
      } = processVlessHeader(chunk, userID);
      
      address = addressRemote;
      portWithRandomLog = `${portRemote}-${Math.random()} ${isUDP ? 'udp' : 'tcp'} `;
      
      if (hasError) {
        throw new Error(message);
      }
      if (isUDP) {
        throw new Error('UDP is not supported on this edge function yet');
      }
      
      const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
      const rawClientData = chunk.slice(rawDataIndex);
      
      handleTCPOutBound(remoteSocketWapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log);
    },
    close() { log(`readableWebSocketStream is close`); },
    abort(reason) { log(`readableWebSocketStream is abort`, JSON.stringify(reason)); },
  })).catch((err) => {
    log('readableWebSocketStream pipeTo error', err);
  });

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

function makeReadableWebSocketStream(webSocketServer, earlyDataHeader, log) {
  let readableStreamCancel = false;
  const stream = new ReadableStream({
    start(controller) {
      webSocketServer.addEventListener('message', (event) => {
        if (readableStreamCancel) return;
        const message = event.data;
        controller.enqueue(message);
      });
      webSocketServer.addEventListener('close', () => {
        safeCloseWebSocket(webSocketServer);
        if (readableStreamCancel) return;
        readableStreamCancel = true;
        controller.close();
      });
      webSocketServer.addEventListener('error', (err) => {
        log('webSocketServer has error');
        controller.error(err);
      });
      if (earlyDataHeader) {
        controller.enqueue(base64ToArrayBuffer(earlyDataHeader));
      }
    },
    pull(controller) {},
    cancel(reason) {
      if (readableStreamCancel) return;
      readableStreamCancel = true;
      safeCloseWebSocket(webSocketServer);
    }
  });
  return stream;
}

function processVlessHeader(vlessBuffer, userID) {
  if (vlessBuffer.byteLength < 24) {
    return { hasError: true, message: 'invalid data' };
  }
  const version = new Uint8Array(vlessBuffer.slice(0, 1));
  let isValidUser = false;
  let isUDP = false;
  
  const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
  const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 18 + optLength + 1))[0];
  
  if (command === 1) {} 
  else if (command === 2) { isUDP = true; } 
  else {
    return { hasError: true, message: `command ${command} is not supported` };
  }
  
  const portIndex = 18 + optLength + 1;
  const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
  const portRemote = new DataView(portBuffer).getUint16(0);
  
  let addressIndex = portIndex + 2;
  const addressBuffer = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1));
  const addressType = addressBuffer[0];
  let addressLength = 0;
  let addressValue = '';
  let addressRemote = '';
  
  addressIndex += 1;
  if (addressType === 1) {
    addressLength = 4;
    addressValue = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + addressLength)).join('.');
  } else if (addressType === 2) {
    addressLength = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1))[0];
    addressIndex += 1;
    addressValue = new TextDecoder().decode(vlessBuffer.slice(addressIndex, addressIndex + addressLength));
  } else if (addressType === 3) {
    addressLength = 16;
    const dataView = new DataView(vlessBuffer.slice(addressIndex, addressIndex + addressLength));
    const ipv6 = [];
    for (let i = 0; i < 8; i++) { ipv6.push(dataView.getUint16(i * 2).toString(16)); }
    addressValue = ipv6.join(':');
  }
  addressRemote = addressValue;
  
  return {
    hasError: false,
    addressRemote,
    portRemote,
    rawDataIndex: addressIndex + addressLength,
    vlessVersion: version,
    isUDP,
  };
}

async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log) {
  async function connectAndWrite(address, port) {
    const ws = new WebSocket(`wss://${proxyIP}/proxy?host=${address}&port=${port}`);
    ws.addEventListener('open', () => {
      ws.send(rawClientData);
    });
    return ws;
  }
  
  const tcpSocket = await connectAndWrite(addressRemote, portRemote);
  remoteSocket.value = tcpSocket;
  
  tcpSocket.addEventListener('message', (event) => {
    webSocket.send(event.data);
  });
  tcpSocket.addEventListener('close', () => {
    safeCloseWebSocket(webSocket);
  });
}

function safeCloseWebSocket(socket) {
  try {
    if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
      socket.close();
    }
  } catch (error) {
    console.error('safeCloseWebSocket error', error);
  }
}

function base64ToArrayBuffer(base64) {
  const binary_string = atob(base64);
  const len = binary_string.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) {
    bytes[i] = binary_string.charCodeAt(i);
  }
  return bytes.buffer;
}
