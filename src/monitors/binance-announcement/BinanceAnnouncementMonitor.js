/**
 * Binance公告监控器
 * 基于官方WebSocket API文档实现
 * 支持实时公告推送
 */
import { BaseMonitor } from '../base/BaseMonitor.js';
import WebSocket from 'ws';
import crypto from 'crypto';
import { SocksProxyAgent } from 'socks-proxy-agent';
import * as deepl from 'deepl-node';

export class BinanceAnnouncementMonitor extends BaseMonitor {
    constructor(sharedServices, config = {}) {
        super('binance-websocket', sharedServices, config);

        // 从环境变量读取API密钥
        this.apiKey = process.env.BINANCE_API_KEY;
        this.secretKey = process.env.BINANCE_SECRET_KEY;
        this.proxyUrl = process.env.BINANCE_PROXY_URL;

        // 验证必需配置
        if (!this.apiKey || !this.secretKey) {
            throw new Error('请在.env文件中设置 BINANCE_API_KEY 和 BINANCE_SECRET_KEY');
        }
        this.baseUrl = 'wss://api.binance.com/sapi/wss';

        // 配置代理
        this.agent = null;
        if (this.proxyUrl) {
            console.log(`🌐 配置代理: ${this.proxyUrl}`);
            this.agent = new SocksProxyAgent(this.proxyUrl);
        }

        // WebSocket配置
        this.recvWindow = config.recvWindow || 30000; // 30秒窗口
        this.topics = config.topics || ['com_announcement_en']; // 默认订阅英文公告主题（官方只提供英文推送）
        this.maxReconnectAttempts = config.maxReconnectAttempts || 10;
        this.reconnectDelay = config.reconnectDelay || 5000; // 5秒重连延迟

        // 连接状态
        this.ws = null;
        this.isConnected = false;
        this.isRunning = false;
        this.reconnectAttempts = 0;
        this.lastPingTime = null;
        this.pingInterval = null;
        this.reconnectTimeout = null;
        this.connectionStartTime = null;
        this.dailyReconnectTimeout = null;

        // 去重机制 - 混合内存+数据库方案
        this.processedAnnouncements = new Set(); // 内存缓存，用于快速查询
        this.announcementCacheTimeout = 24 * 60 * 60 * 1000; // 24小时缓存
        this.memoryCache = {
            maxSize: 1000, // 最大缓存1000条记录
            cleanupInterval: 60 * 60 * 1000 // 每小时清理一次过期缓存
        };

        // 统计信息
        this.stats = {
            connections: 0,
            reconnections: 0,
            messagesReceived: 0,
            announcementsProcessed: 0,
            errors: 0,
            lastConnectTime: null,
            uptime: 0,
            connectionDurations: [] // 记录连接持续时间，用于分析稳定性
        };

        console.log('🔌 币安公告监控器已初始化');
        console.log(`📋 配置: 主题=${this.topics.join('|')}, 接收窗口=${this.recvWindow}ms`);
    }

    /**
     * 子类启动方法 - 由基础监控器调用
     */
    async onStart() {
        console.log('🚀 启动Binance WebSocket监控器...');

        // 从数据库加载最近的已处理公告到内存缓存
        await this.loadRecentProcessedAnnouncements();

        // 启动内存缓存清理定时器
        this.startMemoryCacheCleanup();

        // 同步内部状态与基础监控器状态
        this.isRunning = true;
        await this.connect();
    }

    /**
     * 从数据库加载最近处理的公告到内存缓存
     */
    async loadRecentProcessedAnnouncements() {
        try {
            console.log('📥 从数据库加载最近处理的公告...');

            if (!this.sharedServices || !this.sharedServices.database) {
                console.warn('⚠️  数据库服务未配置，跳过加载已处理公告');
                return;
            }

            const recentAnnouncements = await this.sharedServices.database.getRecentProcessedAnnouncements(
                'binance_announcement',
                24 // 加载最近24小时的记录
            );

            // 清空现有缓存并加载新数据
            this.processedAnnouncements.clear();
            recentAnnouncements.forEach(announcementId => {
                this.processedAnnouncements.add(announcementId);
            });

            console.log(`✅ 已加载 ${recentAnnouncements.length} 条最近处理的公告到内存缓存`);

        } catch (error) {
            console.error('❌ 加载最近处理公告失败:', error.message);
        }
    }

    /**
     * 启动内存缓存清理定时器
     */
    startMemoryCacheCleanup() {
        // 每小时清理一次内存缓存
        this.memoryCacheCleanupInterval = setInterval(() => {
            this.cleanupMemoryCache();
        }, this.memoryCache.cleanupInterval);

        console.log('🧹 内存缓存清理定时器已启动');
    }

    /**
     * 清理内存缓存，保持缓存大小在合理范围内
     */
    cleanupMemoryCache() {
        try {
            const currentSize = this.processedAnnouncements.size;

            if (currentSize > this.memoryCache.maxSize) {
                // 如果缓存过大，清空一半（简单的LRU策略）
                const itemsToRemove = Math.floor(currentSize / 2);
                const iterator = this.processedAnnouncements.values();

                for (let i = 0; i < itemsToRemove; i++) {
                    const item = iterator.next();
                    if (!item.done) {
                        this.processedAnnouncements.delete(item.value);
                    }
                }

                console.log(`🧹 内存缓存清理完成: ${currentSize} -> ${this.processedAnnouncements.size}`);
            }
        } catch (error) {
            console.error('❌ 内存缓存清理失败:', error.message);
        }
    }

    /**
     * 子类停止方法 - 由基础监控器调用
     */
    async onStop() {
        console.log('⏹️  停止Binance WebSocket监控器...');
        // 同步内部状态与基础监控器状态
        this.isRunning = false;

        // 清理定时器
        if (this.pingInterval) {
            clearInterval(this.pingInterval);
            this.pingInterval = null;
        }

        if (this.reconnectTimeout) {
            clearTimeout(this.reconnectTimeout);
            this.reconnectTimeout = null;
        }

        if (this.dailyReconnectTimeout) {
            clearTimeout(this.dailyReconnectTimeout);
            this.dailyReconnectTimeout = null;
        }

        if (this.memoryCacheCleanupInterval) {
            clearInterval(this.memoryCacheCleanupInterval);
            this.memoryCacheCleanupInterval = null;
        }

        // 关闭WebSocket连接
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.close(1000, 'Monitor stopped');
        }

        this.isConnected = false;
        console.log('✅ WebSocket监控器已停止');
        this.printStats();
    }

    /**
     * 建立WebSocket连接
     */
    async connect() {
        try {
            console.log('🔗 建立WebSocket连接...');

            const connectionUrl = await this.buildConnectionUrl();
            console.log('🌐 连接URL已生成');

            const wsOptions = {
                headers: {
                    'X-MBX-APIKEY': this.apiKey
                }
            };

            // 如果配置了代理，添加代理agent
            if (this.agent) {
                wsOptions.agent = this.agent;
                console.log('🌐 使用代理连接WebSocket');
            }

            this.ws = new WebSocket(connectionUrl, [], wsOptions);

            this.setupEventHandlers();

        } catch (error) {
            console.error('❌ 建立连接失败:', error.message);
            this.stats.errors++;

            if (this.isRunning) {
                this.scheduleReconnect();
            }
        }
    }

    /**
     * 获取Binance服务器时间
     */
    async getBinanceServerTime() {
        try {
            const fetchOptions = {};

            // 如果配置了代理，添加代理agent
            if (this.agent) {
                fetchOptions.agent = this.agent;
                console.log('🌐 使用代理获取服务器时间');
            }

            const response = await fetch('https://api.binance.com/api/v3/time', fetchOptions);
            const data = await response.json();
            console.log('⏰ 获取Binance服务器时间成功');
            console.log(`🕐 服务器时间戳: ${data.serverTime}`);
            return data.serverTime;
        } catch (error) {
            console.warn('⚠️  获取服务器时间失败，使用本地时间:', error.message);
            return Date.now();
        }
    }

    /**
     * 构建WebSocket连接URL
     */
    async buildConnectionUrl() {
        const timestamp = await this.getBinanceServerTime();
        const random = this.generateRandomString(32);
        const topic = this.topics.join('|');

        console.log(`🕐 使用时间戳: ${timestamp}`);

        // 构建签名参数（不包含apiKey）
        const params = {
            random: random,
            topic: topic,
            recvWindow: 30000, // 减少到30秒，提高稳定性
            timestamp: timestamp
        };

        // 生成签名
        const signature = this.generateSignature(params);
        params.signature = signature;

        // 构建完整URL - 按字母顺序排列参数（与签名一致）
        const queryString = Object.keys(params)
            .sort()
            .map(key => `${key}=${params[key]}`)
            .join('&');

        const fullUrl = `${this.baseUrl}?${queryString}`;
        console.log(`🔐 签名生成完成, 时间戳: ${timestamp}`);
        console.log(`🌐 完整连接URL: ${fullUrl}`);

        return fullUrl;
    }

    /**
     * 生成HMAC SHA256签名
     * 按照官方文档要求：参数按字母顺序排序
     */
    generateSignature(params) {
        // 按字母顺序排序参数（排除signature字段）
        const sortedParams = Object.keys(params)
            .filter(key => key !== 'signature')
            .sort()
            .map(key => `${key}=${params[key]}`)
            .join('&');

        console.log(`🔑 签名字符串: ${sortedParams}`);

        // 使用HMAC SHA256生成签名
        const signature = crypto
            .createHmac('sha256', this.secretKey)
            .update(sortedParams)
            .digest('hex');

        console.log(`🔐 生成签名: ${signature}`);
        return signature;
    }

    /**
     * 生成随机字符串
     */
    generateRandomString(length) {
        const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
        let result = '';
        for (let i = 0; i < length; i++) {
            result += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        return result;
    }

    /**
     * 翻译文本到中文
     */
    async translateToChineseWithRetry(text, maxRetries = 3) {
        if (!text || typeof text !== 'string') {
            return text;
        }

        // 初始化DeepL翻译器
        const deeplApiKey = process.env.DEEPL_API_KEY;

        if (!deeplApiKey) {
            console.warn('⚠️  DeepL API密钥未配置，返回原文');
            return text;
        }

        const translator = new deepl.Translator(deeplApiKey);

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                console.log(`🌐 使用DeepL翻译文本 (尝试 ${attempt}/${maxRetries}): ${text.substring(0, 50)}...`);

                const result = await translator.translateText(text, 'en', 'zh');
                const translatedText = result.text;

                console.log(`✅ DeepL翻译成功: ${translatedText.substring(0, 50)}...`);
                return translatedText;

            } catch (error) {
                console.log(`❌ DeepL翻译失败 (尝试 ${attempt}/${maxRetries}):`, error.message);

                if (attempt === maxRetries) {
                    console.log(`⚠️  翻译最终失败，返回原文: ${text}`);
                    return text; // 翻译失败时返回原文
                }

                // 等待后重试
                await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
            }
        }

        return text;
    }

    /**
     * 设置WebSocket事件处理器
     */
    setupEventHandlers() {
        this.ws.on('open', () => {
            console.log('✅ WebSocket连接已建立');
            this.isConnected = true;
            this.reconnectAttempts = 0;
            this.stats.connections++;
            this.stats.lastConnectTime = new Date();
            this.connectionStartTime = new Date();

            // 启动心跳
            this.startHeartbeat();

            // 启动24小时重连定时器
            this.scheduleDailyReconnect();

            // 订阅主题
            this.subscribeToTopics();

            // 如果是重连（不是首次连接），异步重新加载已处理的公告缓存
            // 使用 setImmediate 避免阻塞 onOpen 事件处理
            if (this.stats.reconnections > 0) {
                setImmediate(async () => {
                    try {
                        console.log('🔄 检测到重连，重新加载已处理公告缓存...');
                        await this.loadRecentProcessedAnnouncements();
                    } catch (error) {
                        console.error('❌ 重连时加载缓存失败:', error.message);
                    }
                });
            }
        });

        this.ws.on('message', (data) => {
            try {
                this.stats.messagesReceived++;
                const message = JSON.parse(data.toString());
                this.handleMessage(message);
            } catch (error) {
                console.error('❌ 处理消息失败:', error.message);
                this.stats.errors++;
            }
        });

        this.ws.on('close', (code, reason) => {
            const closeReasons = {
                1000: '正常关闭',
                1001: '端点离开',
                1002: '协议错误',
                1003: '不支持的数据类型',
                1006: '连接异常关闭（可能是网络问题或服务器主动断开）',
                1011: '服务器错误',
                1012: '服务重启',
                1013: '稍后重试',
                1014: '网关错误',
                1015: 'TLS握手失败'
            };

            const reasonText = closeReasons[code] || '未知原因';
            console.log(`🔌 WebSocket连接已关闭: ${code} - ${reasonText}`);
            if (reason) {
                console.log(`📝 关闭原因: ${reason}`);
            }

            this.isConnected = false;

            // 记录连接持续时间
            if (this.connectionStartTime) {
                const duration = Date.now() - this.connectionStartTime.getTime();
                this.stats.connectionDurations.push(duration);
                console.log(`⏱️  连接持续时间: ${Math.round(duration / 1000)}秒`);

                // 只保留最近10次连接记录
                if (this.stats.connectionDurations.length > 10) {
                    this.stats.connectionDurations.shift();
                }

                // 如果连接持续时间太短（小于30秒），可能有问题
                if (duration < 30000) {
                    console.warn('⚠️  连接持续时间过短，可能存在配置问题');

                    // 如果连续多次短连接，增加重连延迟
                    const recentShortConnections = this.stats.connectionDurations
                        .slice(-3) // 最近3次连接
                        .filter(d => d < 30000).length;

                    if (recentShortConnections >= 2) {
                        console.warn('⚠️  检测到连续短连接，可能是代理或网络问题，增加重连延迟');
                        // 临时增加重连延迟
                        this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
                    }
                } else {
                    // 连接稳定，重置重连延迟
                    this.reconnectDelay = 5000;
                }
            }

            // 停止心跳
            if (this.pingInterval) {
                clearInterval(this.pingInterval);
                this.pingInterval = null;
            }

            // 停止24小时重连定时器
            if (this.dailyReconnectTimeout) {
                clearTimeout(this.dailyReconnectTimeout);
                this.dailyReconnectTimeout = null;
            }

            // 如果监控器仍在运行，尝试重连
            if (this.isRunning) {
                this.scheduleReconnect();
            }
        });

        this.ws.on('error', (error) => {
            console.error('❌ WebSocket错误:', error.message);
            console.error('❌ 错误详情:', error);
            this.stats.errors++;
        });

        this.ws.on('pong', () => {
            console.log('🏓 收到PONG响应');
        });
    }

    /**
     * 启动心跳机制
     */
    startHeartbeat() {
        console.log('💓 启动心跳机制 (每30秒)');

        this.pingInterval = setInterval(() => {
            if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                this.ws.ping(); // 发送空载荷PING
                this.lastPingTime = new Date();
                console.log('🏓 发送PING心跳');
            }
        }, 30000); // 每30秒发送一次PING
    }

    /**
     * 启动24小时重连定时器
     * 根据官方文档：每个连接有效期不超过24小时
     */
    scheduleDailyReconnect() {
        // 清除现有定时器
        if (this.dailyReconnectTimeout) {
            clearTimeout(this.dailyReconnectTimeout);
        }

        // 设置23小时后重连（提前1小时确保不会断线）
        const reconnectDelay = 23 * 60 * 60 * 1000; // 23小时

        this.dailyReconnectTimeout = setTimeout(() => {
            console.log('⏰ 24小时连接限制，主动重连...');
            if (this.isRunning) {
                // 先断开现有连接，然后重新连接
                if (this.ws) {
                    this.ws.close();
                }
                this.connect();
            }
        }, reconnectDelay);

        console.log('⏰ 已设置24小时重连定时器');
    }

    /**
     * 订阅主题
     */
    subscribeToTopics() {
        for (const topic of this.topics) {
            const subscribeMessage = {
                command: 'SUBSCRIBE',
                value: topic
            };

            this.ws.send(JSON.stringify(subscribeMessage));
            console.log(`📡 订阅主题: ${topic}`);
        }
    }

    /**
     * 处理接收到的消息
     */
    handleMessage(message) {
        const receiveTime = new Date();
        console.log(`📨 收到消息 [${receiveTime.toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })}]:`, JSON.stringify(message, null, 2));

        // 详细分析消息结构
        console.log('🔍 消息分析:');
        console.log(`   接收时间: ${receiveTime.toISOString()} (UTC)`);
        console.log(`   接收时间: ${receiveTime.toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })} (北京时间)`);
        console.log(`   类型: ${message.type}`);
        console.log(`   子类型: ${message.subType || '无'}`);
        console.log(`   数据: ${message.data ? '有数据' : '无数据'}`);
        console.log(`   代码: ${message.code || '无'}`);

        // 处理命令响应
        if (message.type === 'COMMAND') {
            this.handleCommandResponse(message);
            return;
        }

        // 处理公告数据 - 扩展条件判断
        if (message.type === 'ANNOUNCEMENT' ||
            message.type === 'DATA' ||
            (message.data && message.data !== 'SUCCESS')) {
            console.log('🎯 检测到可能的公告数据，开始处理...');
            this.handleAnnouncementData(message);
            return;
        }

        console.log('ℹ️  未知消息类型，完整消息:', JSON.stringify(message, null, 2));
    }

    /**
     * 处理命令响应
     */
    handleCommandResponse(message) {
        if (message.subType === 'SUBSCRIBE' && message.data === 'SUCCESS') {
            console.log(`✅ 订阅成功: ${message.code}`);
        } else if (message.subType === 'UNSUBSCRIBE' && message.data === 'SUCCESS') {
            console.log(`✅ 取消订阅成功: ${message.code}`);
        } else {
            console.log(`⚠️  命令响应: ${message.subType} - ${message.data}`);
        }
    }

    /**
     * 处理公告数据
     */
    async handleAnnouncementData(message) {
        try {
            console.log('📢 收到公告数据，开始处理...');

            // 生成消息唯一标识符用于去重
            const messageId = this.generateMessageId(message);
            console.log(`🔍 生成消息ID: ${messageId}`);

            // 先检查内存缓存（快速检查）
            if (this.processedAnnouncements.has(messageId)) {
                console.log(`⚠️  消息已处理过（内存缓存），跳过重复处理: ${messageId}`);
                return;
            }

            // 检查数据库（确保程序重启后的去重）
            if (this.sharedServices && this.sharedServices.database) {
                const isProcessed = await this.sharedServices.database.isAnnouncementProcessed(
                    messageId,
                    'binance_announcement'
                );

                if (isProcessed) {
                    console.log(`⚠️  消息已处理过（数据库），跳过重复处理: ${messageId}`);
                    // 同时添加到内存缓存，避免下次再查数据库
                    this.processedAnnouncements.add(messageId);
                    return;
                }
            }

            // 验证消息是否包含有效的公告内容
            if (!this.isValidAnnouncementMessage(message)) {
                console.log('⚠️  消息不包含有效公告内容，跳过处理');
                return;
            }

            // 解析公告数据用于保存
            let announcementData = {};
            if (message.type === 'DATA' && message.data) {
                try {
                    announcementData = JSON.parse(message.data);
                } catch (parseError) {
                    console.warn('⚠️  解析公告数据失败:', parseError.message);
                }
            }

            // 标记消息为已处理（内存缓存）
            this.processedAnnouncements.add(messageId);

            // 保存到数据库
            if (this.sharedServices && this.sharedServices.database) {
                await this.sharedServices.database.saveProcessedAnnouncement(
                    messageId,
                    announcementData,
                    'binance_announcement'
                );
            }

            this.stats.announcementsProcessed++;

            // 发送通知
            await this.sendAnnouncementNotification(message);

        } catch (error) {
            console.error('❌ 处理公告数据失败:', error.message);
            this.stats.errors++;
        }
    }

    /**
     * 生成消息唯一标识符
     */
    generateMessageId(message) {
        // 尝试从消息中提取唯一标识符
        let identifier = '';

        if (message.type === 'DATA' && message.data) {
            try {
                const announcementData = JSON.parse(message.data);

                // 优先使用公告ID（如果存在）
                if (announcementData.id) {
                    identifier = `binance_${announcementData.id}`;
                } else if (announcementData.title) {
                    // 使用标题的哈希值作为稳定标识符
                    // 避免使用时间戳，因为重连时可能导致同一公告生成不同ID
                    const titleHash = crypto.createHash('md5')
                        .update(announcementData.title.trim())
                        .digest('hex')
                        .substring(0, 16); // 取前16位

                    // 如果有发布时间，也加入标识符增强唯一性
                    const publishDate = announcementData.publishDate || announcementData.releaseDate;
                    if (publishDate) {
                        identifier = `binance_${titleHash}_${publishDate}`;
                    } else {
                        identifier = `binance_${titleHash}`;
                    }
                } else {
                    // 如果没有标题，使用消息内容的哈希
                    identifier = crypto.createHash('md5').update(JSON.stringify(announcementData)).digest('hex');
                }
            } catch (error) {
                // 如果解析失败，使用消息内容的哈希
                identifier = crypto.createHash('md5').update(JSON.stringify(message)).digest('hex');
            }
        } else {
            // 使用消息内容的哈希作为标识符
            identifier = crypto.createHash('md5').update(JSON.stringify(message)).digest('hex');
        }

        return identifier;
    }

    /**
     * 验证消息是否包含有效的公告内容
     */
    isValidAnnouncementMessage(message) {
        // 只处理包含实际公告数据的消息
        if (message.type === 'DATA' && message.data) {
            try {
                const announcementData = JSON.parse(message.data);
                // 必须包含标题才认为是有效公告
                return announcementData.title && announcementData.title.trim() !== '';
            } catch (error) {
                console.warn('⚠️  解析公告数据失败:', error.message);
                return false;
            }
        }

        // 其他类型的消息暂时不处理
        return false;
    }

    /**
     * 发送公告通知
     */
    async sendAnnouncementNotification(message) {
        try {
            // 解析公告数据
            let announcementData = null;
            let title = '未知公告';
            let publishTime = new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' });
            let catalogName = '';

            if (message.type === 'DATA' && message.data) {
                try {
                    announcementData = JSON.parse(message.data);
                    title = announcementData.title || '未知公告';
                    catalogName = announcementData.catalogName || '';
                    if (announcementData.publishDate) {
                        publishTime = new Date(announcementData.publishDate).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' });
                    }
                } catch (parseError) {
                    console.warn('⚠️  解析公告数据失败:', parseError.message);
                }
            }

            // 翻译标题到中文
            console.log(`🌐 开始翻译公告标题: "${title}"`);
            const titleChinese = await this.translateToChineseWithRetry(title);
            console.log(`🌐 标题翻译结果: "${titleChinese}"`);

            // 翻译分类名称到中文（如果存在）
            let catalogChinese = '';
            if (catalogName) {
                console.log(`🌐 开始翻译分类名称: "${catalogName}"`);
                catalogChinese = await this.translateToChineseWithRetry(catalogName);
                console.log(`🌐 分类翻译结果: "${catalogChinese}"`);
            }

            // 构建可点击的链接
            const binanceUrl = 'https://www.binance.com/en/support/announcement';

            // 使用统一的Binance消息格式化器
            if (this.sharedServices && this.sharedServices.notifier) {
                // 构建标准化的公告数据
                const announcementData = {
                    title: titleChinese && titleChinese !== title && titleChinese.trim() !== '' ? titleChinese : title,
                    originalTitle: title,
                    category: catalogChinese || catalogName || '未分类',
                    publishTime: publishTime,
                    url: binanceUrl,
                    content: '', // 公告通常没有详细内容
                    priorityScore: 5 // 默认优先级
                };

                // 使用统一格式化器格式化消息
                const formatter = this.sharedServices.notifier.messageFormatters.binance;
                const message = formatter.format(announcementData);

                await this.sharedServices.notifier.sendToRecipients(message, {
                    recipients: ['dingtalk']
                });
                console.log('📤 双语公告通知已发送到钉钉');
                console.log(`📢 英文标题: ${title}`);
                console.log(`📢 中文标题: ${titleChinese}`);
            } else {
                console.warn('⚠️  通知器未配置，跳过通知发送');
            }

        } catch (error) {
            console.error('❌ 发送通知失败:', error.message);
        }
    }

    /**
     * 安排重连
     */
    scheduleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error(`❌ 重连失败，已达到最大重连次数: ${this.maxReconnectAttempts}`);
            this.isRunning = false;
            return;
        }

        this.reconnectAttempts++;
        this.stats.reconnections++;

        // 指数退避策略：每次重连延迟翻倍，最大30秒
        const delay = Math.min(this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1), 30000);

        console.log(`🔄 安排重连 (${this.reconnectAttempts}/${this.maxReconnectAttempts}) 在 ${delay}ms 后...`);

        this.reconnectTimeout = setTimeout(() => {
            if (this.isRunning) {
                this.connect();
            }
        }, delay);
    }

    /**
     * 打印统计信息
     */
    printStats() {
        console.log('\n📊 WebSocket监控统计:');
        console.log('====================');
        console.log(`连接次数: ${this.stats.connections}`);
        console.log(`重连次数: ${this.stats.reconnections}`);
        console.log(`接收消息: ${this.stats.messagesReceived}`);
        console.log(`处理公告: ${this.stats.announcementsProcessed}`);
        console.log(`错误次数: ${this.stats.errors}`);
        console.log(`最后连接: ${this.stats.lastConnectTime ? this.stats.lastConnectTime.toLocaleString('zh-CN') : '未连接'}`);
        console.log(`最后心跳: ${this.lastPingTime ? this.lastPingTime.toLocaleString('zh-CN') : '未发送'}`);
    }

    /**
     * 检查监控器是否健康
     */
    isHealthy() {
        // 如果监控器正在运行且连接正常，则认为是健康的
        // 或者如果监控器已停止（正常关闭），也认为是健康的
        return this.isRunning ? this.isConnected : true;
    }

    /**
     * 获取监控状态 - 扩展基础监控器的状态信息
     */
    getStatus() {
        const baseStatus = super.getStatus();
        return {
            ...baseStatus,
            // 添加Binance特有的状态信息
            binanceSpecific: {
                isConnected: this.isConnected,
                reconnectAttempts: this.reconnectAttempts,
                topics: this.topics,
                stats: this.stats,
                lastPingTime: this.lastPingTime
            }
        };
    }
}
