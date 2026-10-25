/* Promise adapter used only inside the trusted Qt management pages. */
(() => {
    if (!window.qt?.webChannelTransport) return;
    document.documentElement.classList.add('qt-shell');
    const script = document.createElement('script');
    script.src = 'qrc:///qtwebchannel/qwebchannel.js';
    script.onload = () => new QWebChannel(qt.webChannelTransport, channel => {
        const rpc = channel.objects.rpc, pending = new Map();
        let sequence = 0;
        rpc.replied.connect((id, payload) => {
            const item = pending.get(id);
            if (!item) return;
            pending.delete(id);
            clearTimeout(item.timer);
            const result = JSON.parse(payload);
            if (result.ok) item.resolve(result.value);
            else item.reject(new Error(result.error));
        });
        window.pywebview = {api: new Proxy({}, {get: (_, method) => (...args) => new Promise((resolve, reject) => {
            const id = String(++sequence);
            const timer = setTimeout(() => { pending.delete(id); reject(new Error('操作超时；请刷新状态，勿重复提交。')); }, 75000);
            pending.set(id, {resolve, reject, timer});
            rpc.invoke(id, method, JSON.stringify(args));
        })})};
        const ready = () => window.dispatchEvent(new Event('pywebviewready'));
        if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', ready, {once: true});
        else ready();
    });
    document.head.appendChild(script);
})();
