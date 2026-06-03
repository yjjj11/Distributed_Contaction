/**
 * 即时通讯 - SPA 单文件应用
 * 登录/注册、聊天、好友请求、WebSocket 直连 msgServer
 */
(function() {
    'use strict';

    // ==============================
    //  共享工具
    // ==============================
    const API_BASE = '/api';

    async function apiPost(endpoint, body) {
        const resp = await fetch(API_BASE + endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
            credentials: 'same-origin',
        });
        return resp.json();
    }

    async function apiGet(endpoint) {
        const resp = await fetch(API_BASE + endpoint, {
            method: 'GET',
            credentials: 'same-origin',
        });
        if (!resp.ok) return { valid: false };
        return resp.json();
    }

    function getCookie(name) {
        const m = document.cookie.match(new RegExp('(?:^|;\\s*)' + name + '=([^;]*)'));
        return m ? decodeURIComponent(m[1]) : null;
    }

    function getCurrentUsername() {
        return getCookie('username');
    }

    function getToken() {
        return getCookie('token');
    }

    function clearAuth() {
        document.cookie = 'token=; path=/; max-age=0';
        document.cookie = 'username=; path=/; max-age=0';
        document.cookie = 'msg_addr=; path=/; max-age=0';
        document.cookie = 'user_id=; path=/; max-age=0';
    }

    function escapeHtml(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }

    // ==============================
    //  WebSocket 管理
    // ==============================
    let ws = null;
    let wsReconnectTimer = null;
    let wsConnected = false;

    function connectWebSocket() {
        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

        const token = getToken();
        if (!token) return;

        // 从 cookie 获取 msg_addr（登录时保存）
        let msgAddr = getCookie('msg_addr');
        if (!msgAddr) {
            msgAddr = 'ws://localhost:50055/ws';
        }

        try {
            ws = new WebSocket(msgAddr);
        } catch (e) {
            console.error('WebSocket connect error:', e);
            scheduleReconnect();
            return;
        }

        ws.onopen = function() {
            console.log('[WS] connected');
            // 发送鉴权
            ws.send(JSON.stringify({ action: 'auth', token: token }));
        };

        ws.onmessage = function(event) {
            try {
                const data = JSON.parse(event.data);
                handleWSMessage(data);
            } catch (e) {
                console.error('[WS] parse error:', e);
            }
        };

        ws.onclose = function() {
            console.log('[WS] disconnected');
            wsConnected = false;
            ws = null;
            scheduleReconnect();
        };

        ws.onerror = function(e) {
            console.error('[WS] error:', e);
        };
    }

    function scheduleReconnect() {
        if (wsReconnectTimer) clearTimeout(wsReconnectTimer);
        wsReconnectTimer = setTimeout(() => {
            if (getToken()) connectWebSocket();
        }, 3000);
    }

    function sendWS(obj) {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify(obj));
            return true;
        }
        return false;
    }

    // WebSocket 心跳
    let pingTimer = null;
    function startPing() {
        stopPing();
        pingTimer = setInterval(() => {
            sendWS({ action: 'ping' });
        }, 15000);
    }
    function stopPing() {
        if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
    }

    // ==============================
    //  WebSocket 消息处理
    // ==============================
    function handleWSMessage(data) {
        const type = data.type;

        switch (type) {
            case 'need_auth':
                // 等待 auth
                break;

            case 'auth_ok':
                wsConnected = true;
                console.log('[WS] authenticated as', data.username);
                // 保存用户信息
                if (data.user_id) {
                    document.cookie = 'user_id=' + data.user_id + '; path=/';
                }
                startPing();
                // 触发连接成功事件
                window.dispatchEvent(new CustomEvent('ws_ready', { detail: data }));
                break;

            case 'auth_error':
                console.error('[WS] auth error:', data.message);
                break;

            case 'pong':
                break;

            case 'new_msg':
                // 收到新消息
                handleNewMessage(data);
                break;

            case 'msg_ack':
                // 消息发送确认
                handleMsgAck(data);
                break;

            case 'offline_msgs':
                // 离线消息批量推送
                if (data.messages) {
                    console.log('[WS] offline_msgs count:', data.messages.length);
                    data.messages.forEach(m => handleNewMessage(m));
                }
                break;

            case 'new_friend_request':
                // 收到好友请求 — 累加计数 + 通知
                pendingReqCount++;
                updateFriendReqBadge(pendingReqCount);
                showFriendRequestNotification(data);
                break;

            case 'friend_request_ack':
                // 好友请求发送成功
                if (data.success) {
                    showToast('好友请求已发送');
                }
                break;

            case 'handle_friend_ack':
                if (data.success) {
                    showToast(data.message);
                    // 刷新好友列表
                    loadFriendList();
                }
                break;

            case 'friend_request_accepted':
                showToast(data.friend_username + ' 已接受你的好友请求');
                loadFriendList();
                break;

            case 'typing':
                // 对方正在输入
                if (data.from_username && currentChat === data.from_username) {
                    showTyping(data.from_username);
                }
                break;

            case 'error':
                showToast('错误: ' + data.message);
                break;

            default:
                console.log('[WS] unknown message:', data);
        }
    }

    // ==============================
    //  消息管理
    // ==============================
    let messages = {};       // { friendName: [ {text, self, time, from_user_id}, ... ] }
    let friends = [];        // { id, name, avatar, lastMsg, online }
    let currentChat = null;  // friend name
    let currentChatUserId = null;
    let unreadCounts = {};   // { friendName: count } 未读消息数
    let pendingReqCount = 0; // 待处理好友请求数

    // 当前正在查看的好友请求页面
    let pendingRequests = [];

    function handleNewMessage(data) {
        const fromId = data.from_user_id;
        // 如果发件人是自己且正在查看该聊天，跳过（已在 sendMessage 中本地添加）
        const myId = parseInt(getCookie('user_id') || '0');
        if (fromId === myId) return;
        const fromName = data.from_username || ('user_' + fromId);
        const content = data.content;
        const time = data.time || '';
        const now = new Date();
        const ts = time || now.getHours().toString().padStart(2,'0') + ':' + now.getMinutes().toString().padStart(2,'0');

        // 确认好友列表中是否有此人
        let friend = friends.find(f => f.id === fromId);
        if (!friend) {
            // 自动添加陌生人到好友列表（但标记为未添加）
            friend = { id: fromId, name: fromName, avatar: fromName.charAt(0).toUpperCase(), lastMsg: content, online: true };
            friends.push(friend);
            renderFriendList();
        }
        friend.lastMsg = content;

        // 存消息
        if (!messages[fromName]) messages[fromName] = [];
        messages[fromName].push({ text: content, self: false, time: ts, from_user_id: fromId });
        saveChatState();

        // 如果当前正在和此人聊天，渲染；否则累加未读计数
        if (currentChat === fromName) {
            renderMessages();
        } else {
            unreadCounts[fromName] = (unreadCounts[fromName] || 0) + 1;
        }
        renderFriendList();
    }

    function handleMsgAck(data) {
        // 消息已确认送达
    }

    // ==============================
    //  路由 & 视图切换
    // ==============================
    const views = {
        login: document.getElementById('login-view'),
        home: document.getElementById('home-view'),
        profile: document.getElementById('profile-view'),
    };
    const navbar = document.getElementById('navbar');
    const navLinks = document.querySelectorAll('.nav-link');

    function showView(name) {
        Object.keys(views).forEach(k => {
            views[k].classList.toggle('hidden', k !== name);
        });
        navbar.classList.toggle('hidden', name === 'login');
        navLinks.forEach(link => {
            link.classList.toggle('active', link.dataset.route === name);
        });
    }

    function handleHash() {
        const hash = location.hash.slice(1) || '/home';
        const token = getToken();
        const username = getCurrentUsername();
        console.log('[handleHash] hash=', hash, 'token=', token ? token.substring(0, 20) + '...' : null, 'username=', username);

        if (!token || !username) {
            console.log('[handleHash] missing token or username, showing login');
            showView('login');
            disconnectWS();
            return;
        }

        if (hash === '/home') {
            console.log('[handleHash] showing home view');
            showView('home');
            initChat();
        } else if (hash === '/profile') {
            showView('profile');
            initProfile();
        } else {
            showView('home');
            initChat();
        }
    }

    window.addEventListener('hashchange', handleHash);

    // ==============================
    //  鉴权检查（启动时）
    // ==============================
    (async function bootAuth() {
        const token = getToken();
        const username = getCurrentUsername();
        if (token && username) {
            try {
                const data = await apiGet('/validate');
                if (data.valid) {
                    connectWebSocket();
                    location.hash = '/home';
                    return;
                }
            } catch (_) {}
            clearAuth();
        }
        showView('login');
    })();

    // ==============================
    //  登录 / 注册
    // ==============================
    const card = document.getElementById('auth-card');
    const loginForm = document.getElementById('login-form');
    const registerForm = document.getElementById('register-form');
    const showRegisterLink = document.getElementById('show-register');
    const showLoginLink = document.getElementById('show-login');
    const loginError = document.getElementById('login-error');
    const registerError = document.getElementById('register-error');

    function showError(el, msg) {
        el.textContent = msg;
        el.classList.add('visible');
    }
    function hideError(el) {
        el.classList.remove('visible');
        el.textContent = '';
    }

    // 卡片翻转
    showRegisterLink.addEventListener('click', (e) => {
        e.preventDefault();
        hideError(loginError);
        hideError(registerError);
        card.classList.add('flipped');
    });
    showLoginLink.addEventListener('click', (e) => {
        e.preventDefault();
        hideError(loginError);
        hideError(registerError);
        card.classList.remove('flipped');
    });

    loginForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        hideError(loginError);
        const username = document.getElementById('login-username').value.trim();
        const password = document.getElementById('login-password').value.trim();
        if (!username || !password) {
            showError(loginError, '请输入用户名和密码');
            return;
        }
        try {
            const data = await apiPost('/login', { username, password });
            console.log('[login] response:', data);
            if (data.success) {
                document.cookie = 'username=' + encodeURIComponent(username) + '; path=/';
                if (data.token) {
                    document.cookie = 'token=' + encodeURIComponent(data.token) + '; path=/';
                    console.log('[login] token cookie set');
                } else {
                    console.error('[login] no token in response!');
                }
                if (data.msg_addr) {
                    document.cookie = 'msg_addr=' + encodeURIComponent(data.msg_addr) + '; path=/';
                }
                // 重置状态
                resetChatState();
                // 连接 WebSocket
                console.log('[login] connecting WebSocket, token=', getToken());
                connectWebSocket();
                console.log('[login] navigating to /home');
                // 重置聊天面板到初始状态
                chatPlaceholder.classList.remove('hidden');
                chatPanel.classList.add('hidden');
                currentChat = null;
                currentChatUserId = null;
                // 直接切换视图
                showView('home');
                initChat();
                location.hash = '/home';
            } else {
                showError(loginError, data.message || '登录失败');
            }
        } catch (err) {
            console.error('[login] error:', err);
            showError(loginError, '网络错误，请稍后重试');
        }
    });

    registerForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        hideError(registerError);
        const username = document.getElementById('reg-username').value.trim();
        const password = document.getElementById('reg-password').value.trim();
        if (!username || !password) {
            showError(registerError, '请输入用户名和密码');
            return;
        }
        try {
            const data = await apiPost('/register', { username, password });
            if (data.success) {
                // 提示成功后自动翻转回登录面
                showToast('注册成功，请登录');
                card.classList.remove('flipped');
                document.getElementById('login-username').value = username;
                document.getElementById('login-password').focus();
            } else {
                showError(registerError, data.message || '注册失败');
            }
        } catch (err) {
            showError(registerError, '网络错误，请稍后重试');
        }
    });

    // ==============================
    //  Toast 提示
    // ==============================
    function showToast(msg) {
        let toast = document.getElementById('toast');
        if (!toast) {
            toast = document.createElement('div');
            toast.id = 'toast';
            document.body.appendChild(toast);
        }
        toast.textContent = msg;
        toast.className = 'toast visible';
        setTimeout(() => { toast.className = 'toast'; }, 2500);
    }

    // ==============================
    //  聊天模块
    // ==============================
    let chatInitialized = false;
    let chatFriendListInitialized = false;

    const sidebarUsername = document.getElementById('sidebar-username');
    const sidebarAvatar = document.getElementById('sidebar-avatar');
    const friendListEl = document.getElementById('friend-list');
    const chatPlaceholder = document.getElementById('chat-placeholder');
    const chatPanel = document.getElementById('chat-panel');
    const chatFriendName = document.getElementById('chat-friend-name');
    const chatMessages = document.getElementById('chat-messages');
    const chatInput = document.getElementById('chat-input');
    const chatSendBtn = document.getElementById('chat-send-btn');
    const friendSearch = document.getElementById('friend-search');
    const addFriendBtn = document.getElementById('add-friend-btn');
    const addFriendModal = document.getElementById('add-friend-modal');
    const addFriendInput = document.getElementById('add-friend-input');
    const addFriendCancel = document.getElementById('add-friend-cancel');
    const addFriendConfirm = document.getElementById('add-friend-confirm');
    const addFriendError = document.getElementById('add-friend-error');

    function resetChatState() {
        chatInitialized = false;
        chatFriendListInitialized = false;
        messages = {};
        friends = [];
        currentChat = null;
        currentChatUserId = null;
        pendingRequests = [];
        unreadCounts = {};
        pendingReqCount = 0;
        profileInitialized = false;
    }

    function initChat() {
        const username = getCurrentUsername();
        if (!username) return;
        if (chatInitialized) return;
        chatInitialized = true;

        sidebarUsername.textContent = username;
        if (username) sidebarAvatar.textContent = username.charAt(0).toUpperCase();

        // 加载好友列表（从服务端）
        loadFriendList();

        // 事件绑定
        chatSendBtn.addEventListener('click', () => sendMessage(chatInput.value));
        chatInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') sendMessage(chatInput.value);
        });

        // 正在输入 — 3 秒防抖发送 typing 通知
        let typingTimer = null;
        chatInput.addEventListener('input', () => {
            if (!currentChatUserId) return;
            if (!typingTimer) {
                sendWS({ action: 'typing', to_user_id: currentChatUserId });
            }
            clearTimeout(typingTimer);
            typingTimer = setTimeout(() => { typingTimer = null; }, 3000);
        });

        friendSearch.addEventListener('input', () => {
            renderFriendList(friendSearch.value.trim());
        });

        addFriendBtn.addEventListener('click', () => {
            addFriendModal.classList.remove('hidden');
            addFriendInput.value = '';
            addFriendInput.focus();
            hideError(addFriendError);
        });

        addFriendCancel.addEventListener('click', () => {
            addFriendModal.classList.add('hidden');
        });

        addFriendModal.addEventListener('click', (e) => {
            if (e.target === addFriendModal) addFriendModal.classList.add('hidden');
        });

        addFriendConfirm.addEventListener('click', () => {
            const name = addFriendInput.value.trim();
            if (!name) {
                showError(addFriendError, '请输入好友用户名');
                return;
            }
            if (name === getCurrentUsername()) {
                showError(addFriendError, '不能添加自己为好友');
                return;
            }
            // 通过 WebSocket 发送好友请求
            const sent = sendWS({
                action: 'send_friend_request',
                to_username: name
            });
            if (!sent) {
                showError(addFriendError, 'WebSocket 未连接');
                return;
            }
            addFriendModal.classList.add('hidden');
            showToast('好友请求已发送');
        });

        // 监听 WebSocket ready 事件
        window.addEventListener('ws_ready', () => {
            loadFriendList();
        });
    }

    // ==============================
    //  好友列表（从服务端加载）
    // ==============================
    async function loadFriendList() {
        try {
            const fdata = await apiGet('/friends');
            if (fdata.success && fdata.friends) {
                friends = fdata.friends.map(f => ({
                    id: f.friend_id,
                    name: f.friend_username,
                    avatar: f.friend_username.charAt(0).toUpperCase(),
                    lastMsg: '',
                    online: f.online || false,
                }));
                renderFriendList();
            }
        } catch (_) {}

        try {
            const rdata = await apiGet('/pending-requests');
            if (rdata.success && rdata.requests) {
                pendingReqCount = rdata.requests.length;
                updateFriendReqBadge(pendingReqCount);
                renderPendingRequests(rdata.requests);
            } else {
                pendingReqCount = 0;
                updateFriendReqBadge(0);
            }
        } catch (_) {}
    }

    function renderFriendList(filter) {
        friendListEl.innerHTML = '';
        const list = filter
            ? friends.filter(f => f.name.toLowerCase().includes(filter.toLowerCase()))
            : friends;

        if (list.length === 0) {
            friendListEl.innerHTML = '<div class="friend-empty">暂无好友</div>';
            return;
        }

        list.forEach(f => {
            const unread = unreadCounts[f.name] || 0;
            const div = document.createElement('div');
            div.className = 'friend-item' + (currentChat === f.name ? ' active' : '');
            div.innerHTML = `
                <div class="friend-avatar ${f.online ? 'online' : ''}">${escapeHtml(f.avatar)}</div>
                <div class="friend-info">
                    <div class="friend-name">${escapeHtml(f.name)}</div>
                    <div class="friend-last-msg">${escapeHtml(f.lastMsg || '')}</div>
                </div>
                ${unread > 0 ? `<span class="unread-badge">${unread > 99 ? '99+' : unread}</span>` : ''}
                <div class="friend-online-dot ${f.online ? 'online' : ''}"></div>
            `;
            div.addEventListener('click', () => selectChat(f.name, f.id));
            friendListEl.appendChild(div);
        });
    }

    // ==============================
    //  好友请求
    // ==============================
    function showFriendRequestNotification(data) {
        showToast('收到来自 ' + data.from_username + ' 的好友请求');
        // 刷新请求列表
        sendWS({ action: 'get_pending_requests' });
    }

    function renderPendingRequests(requests) {
        if (!requests || requests.length === 0) {
            pendingRequests = [];
            updateFriendReqBadge(0);
            return;
        }
        pendingRequests = requests;
        updateFriendReqBadge(requests.length);
        // 显示在界面上
        showPendingRequestsUI(requests);
    }

    function updateFriendReqBadge(count) {
        let badge = document.getElementById('friend-req-badge');
        if (!badge) {
            badge = document.createElement('span');
            badge.id = 'friend-req-badge';
            badge.className = 'friend-req-badge';
            addFriendBtn.appendChild(badge);
        }
        badge.textContent = count > 99 ? '99+' : count;
        badge.style.display = count > 0 ? 'inline' : 'none';
    }

    function showPendingRequestsUI(requests) {
        // 如果已经有弹窗，更新内容
        let modal = document.getElementById('pending-req-modal');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'pending-req-modal';
            modal.className = 'modal-overlay hidden';
            modal.innerHTML = `
                <div class="modal-content">
                    <h3>好友请求</h3>
                    <div id="pending-req-list"></div>
                    <div class="modal-actions">
                        <button id="pending-req-close" class="btn btn-secondary">关闭</button>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
            document.getElementById('pending-req-close').addEventListener('click', () => {
                modal.classList.add('hidden');
            });
            modal.addEventListener('click', (e) => {
                if (e.target === modal) modal.classList.add('hidden');
            });
        }

        const list = document.getElementById('pending-req-list');
        if (requests.length === 0) {
            list.innerHTML = '<p style="text-align:center;color:rgba(255,255,255,0.4);padding:24px;">暂无待处理的请求</p>';
            return;
        }

        list.innerHTML = '';
        requests.forEach(req => {
            const div = document.createElement('div');
            div.className = 'pending-req-item';
            div.innerHTML = `
                <span class="pending-req-user">${escapeHtml(req.from_username)}</span>
                <span class="pending-req-time">${escapeHtml(req.created_at || '')}</span>
                <div class="pending-req-actions">
                    <button class="btn btn-small btn-accept" data-id="${req.request_id}">接受</button>
                    <button class="btn btn-small btn-reject" data-id="${req.request_id}">拒绝</button>
                </div>
            `;
            div.querySelector('.btn-accept').addEventListener('click', () => {
                sendWS({ action: 'handle_friend_request', request_id: req.request_id, accept: true });
                modal.classList.add('hidden');
            });
            div.querySelector('.btn-reject').addEventListener('click', () => {
                sendWS({ action: 'handle_friend_request', request_id: req.request_id, accept: false });
                modal.classList.add('hidden');
            });
            list.appendChild(div);
        });

        // 如果有请求且弹窗未显示，自动显示
        if (!modal.classList.contains('hidden')) return;
        // 只在有新的请求时显示（通过通知触发）
    }

    // 扩展添加好友按钮点击：显示好友请求弹窗
    // 在 addFriendBtn 旁边加一个好友请求按钮
    (function initFriendReqUI() {
        window.addEventListener('load', () => {
            const actions = document.querySelector('.sidebar-actions');
            if (!actions) return;
            const reqBtn = document.createElement('button');
            reqBtn.id = 'friend-req-btn';
            reqBtn.innerHTML = '📨 请求';
            reqBtn.style.marginTop = '6px';
            reqBtn.addEventListener('click', () => {
                const modal = document.getElementById('pending-req-modal');
                if (modal) {
                    modal.classList.remove('hidden');
                    sendWS({ action: 'get_pending_requests' });
                }
            });
            actions.appendChild(reqBtn);
        });
    })();

    // ==============================
    //  选择聊天
    // ==============================
    async function selectChat(friendName, friendId) {
        hideTyping();
        currentChat = friendName;
        currentChatUserId = friendId || null;
        // 清除该好友的未读计数
        unreadCounts[friendName] = 0;
        renderFriendList();

        chatPlaceholder.classList.add('hidden');
        chatPanel.classList.remove('hidden');
        chatFriendName.textContent = friendName;
        chatMessages.innerHTML = '';

        if (friendId) {
            try {
                console.log('[selectChat] fetching history for friend_id=', friendId);
                const data = await apiPost('/history', {
                    friend_id: friendId,
                    limit: 50,
                    offset: 0
                });
                console.log('[selectChat] history response:', data);
                if (data.success && data.messages) {
                    messages[currentChat] = data.messages.map(m => ({
                        text: m.content,
                        self: parseInt(m.from_user_id) === parseInt(getCookie('user_id') || '0'),
                        time: m.created_at ? m.created_at.substring(11, 16) : '',
                        from_user_id: m.from_user_id,
                    })).reverse(); // 服务端返回倒序，翻转后旧的在上、新的在下
                    renderMessages();
                } else {
                    console.log('[selectChat] no messages or success=false');
                    messages[currentChat] = [];
                    renderMessages();
                }
            } catch (_) {
                messages[currentChat] = [];
                renderMessages();
            }
        } else {
            renderMessages();
        }
    }

    function renderMessages() {
        if (!currentChat) return;
        const msgs = messages[currentChat] || [];
        chatMessages.innerHTML = '';
        if (msgs.length === 0) {
            chatMessages.innerHTML = '<div class="chat-empty">开始聊天吧</div>';
            return;
        }
        msgs.forEach(m => {
            const row = document.createElement('div');
            row.className = 'msg-row' + (m.self ? ' self' : '');
            const avatarLetter = m.self
                ? (getCurrentUsername() || '我').charAt(0).toUpperCase()
                : (currentChat || '?').charAt(0).toUpperCase();
            row.innerHTML = `
                <div class="msg-avatar ${m.self ? 'self' : 'other'}">${escapeHtml(avatarLetter)}</div>
                <div class="msg-body">
                    <div class="msg-bubble">${escapeHtml(m.text)}</div>
                    <div class="msg-time">${escapeHtml(m.time || '')}</div>
                </div>
            `;
            chatMessages.appendChild(row);
        });
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    let typingTimeout = null;
    function showTyping(username) {
        hideTyping();
        const el = document.createElement('div');
        el.id = 'typing-indicator';
        el.className = 'typing-indicator';
        el.textContent = username + ' 正在输入...';
        chatMessages.appendChild(el);
        chatMessages.scrollTop = chatMessages.scrollHeight;
        typingTimeout = setTimeout(hideTyping, 4000);
    }
    function hideTyping() {
        const el = document.getElementById('typing-indicator');
        if (el) el.remove();
        if (typingTimeout) { clearTimeout(typingTimeout); typingTimeout = null; }
    }

    function sendMessage(text) {
        const content = text.trim();
        if (!content) return;
        chatInput.value = '';

        if (currentChatUserId) {
            // 通过 WebSocket 发送
            sendWS({
                action: 'send_msg',
                to_user_id: currentChatUserId,
                content: content
            });
            // 立即显示在本地
            const now = new Date();
            const ts = now.getHours().toString().padStart(2,'0') + ':' + now.getMinutes().toString().padStart(2,'0');
            if (!messages[currentChat]) messages[currentChat] = [];
            messages[currentChat].push({ text: content, self: true, time: ts, from_user_id: parseInt(getCookie('user_id') || '0') });
            const friend = friends.find(f => f.name === currentChat);
            if (friend) friend.lastMsg = content;
            renderMessages();
            renderFriendList();
            saveChatState();
        } else {
            // 本地模式（旧兼容）
            const now = new Date();
            const ts = now.getHours().toString().padStart(2,'0') + ':' + now.getMinutes().toString().padStart(2,'0');
            if (!messages[currentChat]) messages[currentChat] = [];
            messages[currentChat].push({ text: content, self: true, time: ts });
            renderMessages();
            saveChatState();
        }
    }

    // ==============================
    //  本地存储
    // ==============================
    function saveChatState() {
        const username = getCurrentUsername();
        if (!username) return;
        try {
            localStorage.setItem('chat_msgs_' + username, JSON.stringify(messages));
        } catch (_) {}
    }

    // ==============================
    //  个人中心模块
    // ==============================
    let profileInitialized = false;

    function initProfile() {
        if (profileInitialized) return;
        profileInitialized = true;

        const username = getCurrentUsername();
        const profileUsername = document.getElementById('profile-username');
        const profileUserid = document.getElementById('profile-userid');
        const profileAvatarText = document.getElementById('profile-avatar-text');
        const logoutBtn = document.getElementById('logout-btn');

        profileUsername.textContent = username;
        if (username) profileAvatarText.textContent = username.charAt(0).toUpperCase();

        apiGet('/user/info').then(data => {
            profileUserid.textContent = data.user_id || '-';
        }).catch(() => {
            profileUserid.textContent = '-';
        });

        logoutBtn.addEventListener('click', async () => {
            try {
                await apiPost('/logout', {});
            } catch (_) {}
            clearAuth();
            disconnectWS();
            resetChatState();
            showView('login');
            location.hash = '';
        });
    }

    function disconnectWS() {
        stopPing();
        if (wsReconnectTimer) { clearTimeout(wsReconnectTimer); wsReconnectTimer = null; }
        if (ws) {
            try { ws.close(); } catch(_) {}
            ws = null;
        }
        wsConnected = false;
    }

    // ==============================
    //  添加 Toast 样式
    // ==============================
    const toastStyle = document.createElement('style');
    toastStyle.textContent = `
        .toast {
            position: fixed;
            bottom: 30px;
            left: 50%;
            transform: translateX(-50%) translateY(20px);
            background: rgba(20, 20, 50, 0.9);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(167, 139, 250, 0.2);
            color: #fff;
            padding: 12px 24px;
            border-radius: 12px;
            font-size: 14px;
            z-index: 999;
            opacity: 0;
            transition: all 0.3s ease;
            pointer-events: none;
        }
        .toast.visible {
            opacity: 1;
            transform: translateX(-50%) translateY(0);
        }
        .friend-req-badge {
            display: inline-block;
            background: #ef4444;
            color: #fff;
            font-size: 11px;
            font-weight: 700;
            padding: 1px 6px;
            border-radius: 10px;
            margin-left: 4px;
            vertical-align: top;
        }
        .pending-req-item {
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 12px 0;
            border-bottom: 1px solid rgba(167, 139, 250, 0.08);
        }
        .pending-req-user {
            flex: 1;
            font-weight: 600;
            font-size: 15px;
        }
        .pending-req-time {
            color: rgba(255,255,255,0.3);
            font-size: 12px;
        }
        .pending-req-actions {
            display: flex;
            gap: 8px;
        }
        .btn-small {
            padding: 6px 14px;
            border: none;
            border-radius: 8px;
            font-size: 13px;
            font-weight: 600;
            cursor: pointer;
        }
        .btn-accept {
            background: linear-gradient(135deg, #34d399, #60a5fa);
            color: #fff;
        }
        .btn-reject {
            background: rgba(255,255,255,0.1);
            color: rgba(255,255,255,0.6);
        }
        .friend-online-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: rgba(255,255,255,0.15);
            flex-shrink: 0;
        }
        .friend-online-dot.online {
            background: #34d399;
            box-shadow: 0 0 8px rgba(52, 211, 153, 0.5);
        }
        .friend-avatar.online {
            box-shadow: 0 0 0 2px #34d399;
        }
        .chat-empty {
            text-align: center;
            color: rgba(255,255,255,0.3);
            padding: 48px;
        }
        #friend-req-btn {
            width: 100%;
            padding: 10px;
            border: none;
            border-radius: 12px;
            background: rgba(167, 139, 250, 0.12);
            color: rgba(255,255,255,0.7);
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            transition: background 0.2s;
        }
        #friend-req-btn:hover {
            background: rgba(167, 139, 250, 0.2);
        }
        .unread-badge {
            background: #ef4444;
            color: #fff;
            font-size: 11px;
            font-weight: 700;
            min-width: 18px;
            height: 18px;
            line-height: 18px;
            text-align: center;
            border-radius: 9px;
            padding: 0 5px;
            flex-shrink: 0;
            margin-right: 6px;
        }
        .typing-indicator {
            color: rgba(167, 139, 250, 0.8);
            font-size: 13px;
            padding: 6px 16px 10px;
            font-style: italic;
            animation: typingPulse 1.5s ease-in-out infinite;
        }
        @keyframes typingPulse {
            0%, 100% { opacity: 0.4; }
            50% { opacity: 1; }
        }
    `;
    document.head.appendChild(toastStyle);
})();
