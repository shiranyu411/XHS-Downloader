/* Serial, visibility-aware state polling shared by both local UI pages. */
(() => {
    'use strict';
    window.createUiPoller = ({refresh, isBusy = () => false, activeDelay = 1000, idleDelay = 5000}) => {
        let timer = null, flight = null, again = false, started = false;
        const visible = () => window.__xhsUiVisible ?? !document.hidden;
        const cancel = () => {clearTimeout(timer); timer = null;};
        const schedule = () => {
            cancel();
            if (started && visible()) timer = setTimeout(() => request(false), isBusy() ? activeDelay : idleDelay);
        };
        function request(force = true) {
            if (!force && !visible()) return Promise.resolve();
            cancel();
            if (flight) {again = true; return flight;}
            // Defer refresh so synchronous callers can also coalesce into this flight.
            flight = Promise.resolve().then(async () => {
                do {again = false; await refresh();} while (again);
            }).finally(() => {flight = null; schedule();});
            return flight;
        }
        function visibilityChanged() {
            cancel();
            if (started && visible()) void request();
        }
        window.addEventListener('xhs-ui-visibility', visibilityChanged);
        document.addEventListener('visibilitychange', visibilityChanged);
        window.addEventListener('pagehide', () => {started = false; cancel();});
        return {
            request, visible,
            start() {if (!started) {started = true; schedule();}},
        };
    };
})();
