// Using built-in globalThis.WebSocket in Node 26+

export class RemoteClient {
  constructor(baseUrl = 'http://localhost:8088') {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.cookies = '';
    this.csrf = '';
    this.ws = null;
    this.publicId = null;
    this.msgId = 1;
    this.pendingCallbacks = new Map();
    this.otListeners = [];
  }

  async login(email = 'admin@example.com', password = 'Password123!') {
    const firstRes = await fetch(`${this.baseUrl}/login`);
    const initialCookies = firstRes.headers.getSetCookie().map(c => c.split(';')[0]).join('; ');
    const html = await firstRes.text();
    const csrfMatch = html.match(/name="ol-csrfToken" content="([^"]+)/);
    if (!csrfMatch) throw new Error('CSRF token not found');
    this.csrf = csrfMatch[1];

    const loginRes = await fetch(`${this.baseUrl}/login`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Csrf-Token': this.csrf,
        'Cookie': initialCookies
      },
      body: JSON.stringify({ email, password }),
      redirect: 'manual'
    });

    this.cookies = loginRes.headers.getSetCookie().map(c => c.split(';')[0]).join('; ');
    return this.cookies;
  }

  async fastSync(projectId, docId, content, source = 'remote-collaborator') {
    const res = await fetch(`${this.baseUrl}/project/${projectId}/doc/${docId}/fast-sync`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Cookie': this.cookies,
        'X-Csrf-Token': this.csrf
      },
      body: JSON.stringify({ content, source })
    });
    return await res.json();
  }

  async connectWs(projectId) {
    const hsRes = await (await fetch(`${this.baseUrl}/socket.io/1/?projectId=${projectId}&t=${Date.now()}`, {
      headers: { 'Cookie': this.cookies }
    })).text();
    const sid = hsRes.split(':')[0];

    const wsUrl = `ws://localhost:8088/socket.io/1/websocket/${sid}?projectId=${projectId}`;
    this.ws = new globalThis.WebSocket(wsUrl, {
      headers: { 'Cookie': this.cookies }
    });

    await new Promise((resolve, reject) => {
      this.ws.onopen = () => resolve();
      this.ws.onerror = (e) => reject(e);
      this.ws.onclose = () => {
        for (const cb of this.pendingCallbacks.values()) {
          cb(null);
        }
        this.pendingCallbacks.clear();
      };
      this.ws.onmessage = (event) => {
        this.handleMessage(event.data);
      };
    });

    // Wait for joinProjectResponse
    await new Promise((resolve) => {
      const check = setInterval(() => {
        if (this.publicId) {
          clearInterval(check);
          resolve();
        }
      }, 50);
    });
  }

  handleMessage(data) {
    if (data.startsWith('2::')) {
      this.ws.send('2::');
      return;
    }
    if (data.startsWith('5:::')) {
      try {
        const payload = JSON.parse(data.slice(4));
        if (payload.name === 'joinProjectResponse') {
          this.publicId = payload.args[0].publicId;
          this.project = payload.args[0].project;
        } else if (payload.name === 'otUpdateApplied') {
          for (const listener of this.otListeners) {
            listener(payload.args[0]);
          }
        }
      } catch (e) {}
    } else if (data.startsWith('6:::')) {
      const parts = data.slice(4).split('+');
      const id = parts[0];
      const rest = parts.slice(1).join('+');
      if (this.pendingCallbacks.has(id)) {
        const cb = this.pendingCallbacks.get(id);
        this.pendingCallbacks.delete(id);
        try {
          cb(JSON.parse(rest));
        } catch (e) {
          cb(null);
        }
      }
    }
  }

  async joinDoc(docId, projectId = '6afacb5e0082f5af0cb6d6f7') {
    if (!this.ws || this.ws.readyState !== 1) {
      await this.connectWs(projectId);
    }
    const id = this.msgId++;
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pendingCallbacks.delete(String(id));
        resolve({ lines: [], version: 0 });
      }, 3000);
      this.pendingCallbacks.set(String(id), (args) => {
        clearTimeout(timer);
        if (args && args[1]) {
          resolve({ lines: args[1], version: args[2] });
        } else {
          resolve({ lines: [], version: 0 });
        }
      });
      const packet = `5:${id}+::{"name":"joinDoc","args":["${docId}",{"encodeRanges":true}]}`;
      try {
        this.ws.send(packet);
      } catch (e) {
        clearTimeout(timer);
        resolve({ lines: [], version: 0 });
      }
    });
  }

  async sendOt(docId, version, op) {
    const id = this.msgId++;
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pendingCallbacks.delete(String(id));
        resolve();
      }, 2000);
      this.pendingCallbacks.set(String(id), () => {
        clearTimeout(timer);
        resolve();
      });
      const payload = {
        name: 'applyOtUpdate',
        args: [docId, { doc: docId, v: version, op }]
      };
      const packet = `5:${id}+::${JSON.stringify(payload)}`;
      try {
        this.ws.send(packet);
      } catch (e) {
        clearTimeout(timer);
        resolve();
      }
    });
  }

  async getFreshProject(projectId = '6afacb5e0082f5af0cb6d6f7') {
    const freshClient = new RemoteClient(this.baseUrl);
    await freshClient.login();
    await freshClient.connectWs(projectId);
    const proj = freshClient.project;
    freshClient.close();
    return proj;
  }

  onOt(listener) {
    this.otListeners.push(listener);
  }

  close() {
    if (this.ws) {
      try { this.ws.close(); } catch (e) {}
    }
  }
}
