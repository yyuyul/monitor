import { MonitorOrchestrator } from './orchestrator.js';
import { unifiedConfigManager } from './core/config.js';
import { unifiedDatabaseManager } from './core/database.js';
import { createUnifiedNotifier } from './core/notifier.js';
import http from 'http';

/**
 * 简化的多监控源系统主程序
 * 专注于核心功能，减少复杂性
 */
class MultiSourceMonitorApp {
    constructor() {
        this.isRunning = false;
        this.startTime = null;
        this.httpServer = null;
        this.orchestrator = null;
        this.sharedServices = {};
    }

    /**
     * 初始化共享服务
     * @returns {Promise<boolean>} 是否初始化成功
     */
    async initializeSharedServices() {
        try {
            console.log('🔧 初始化共享服务...');

            // 1. 初始化配置管理器
            console.log('📋 加载统一配置...');
            const config = unifiedConfigManager.loadConfig();
            if (!config) {
                throw new Error('配置加载失败');
            }
            this.sharedServices.config = unifiedConfigManager;

            // 2. 初始化数据库管理器（Twitter监控和Binance去重都需要）
            const needsDatabase = config.system.enabledModules.includes('twitter-official') ||
                config.system.enabledModules.includes('twitter-openapi') ||
                config.system.enabledModules.includes('binance-announcement') ||
                config.system.enabledModules.includes('binance-price');

            if (needsDatabase) {
                console.log('🗄️  初始化数据库连接...');
                const dbSuccess = await unifiedDatabaseManager.initialize(config.shared.database, config.system.enabledModules);
                if (!dbSuccess) {
                    throw new Error('数据库初始化失败');
                }
                this.sharedServices.database = unifiedDatabaseManager;
                console.log('✅ 数据库连接成功');
            } else {
                console.log('ℹ️  跳过数据库初始化（无启用的模块需要数据库）');
                this.sharedServices.database = null;
            }

            // 3. 通知系统
            console.log('📢 初始化通知系统...');
            this.sharedServices.notifier = createUnifiedNotifier(config.shared.notification);
            console.log('✅ 通知系统初始化成功');

            return true;

        } catch (error) {
            console.error('❌ 共享服务初始化失败:', error.message);
            return false;
        }
    }

    /**
     * 启动应用
     */
    async start() {
        try {
            console.log('🚀 多监控源系统启动中...');
            this.startTime = new Date();

            // 1. 初始化共享服务
            const servicesInitialized = await this.initializeSharedServices();
            if (!servicesInitialized) {
                throw new Error('共享服务初始化失败');
            }

            // 2. 创建并启动编排器
            console.log('🎭 创建监控编排器...');
            this.orchestrator = new MonitorOrchestrator(this.sharedServices);

            const orchestratorStarted = await this.orchestrator.start();
            if (!orchestratorStarted) {
                throw new Error('监控编排器启动失败');
            }

            this.isRunning = true;
            console.log('🎉 多监控源系统启动成功！');

            // 3. 启动HTTP健康检查服务器
            this.startHealthCheckServer();

            // 4. 设置优雅关闭
            this.setupGracefulShutdown();

            // 5. 显示系统状态
            setTimeout(() => {
                this.showSystemStatus();
            }, 3000);

            // 6. 定期内存检查（可选，用于排查内存泄漏）
            if (process.env.ENABLE_MEMORY_CHECK === 'true') {
                setInterval(() => {
                    const usage = process.memoryUsage();
                    const heapUsedMB = Math.round(usage.heapUsed / 1024 / 1024);
                    const rssMB = Math.round(usage.rss / 1024 / 1024);
                    console.log(`💾 内存使用: Heap ${heapUsedMB}MB, RSS ${rssMB}MB`);
                }, 5 * 60 * 1000); // 每 5 分钟
            }

        } catch (error) {
            console.error('❌ 系统启动失败:', error.message);
            process.exit(1);
        }
    }

    /**
     * 启动HTTP健康检查服务器
     */
    startHealthCheckServer() {
        const port = this.sharedServices.config.config.system.port;

        // 请求日志频率限制
        this.lastLogTime = 0;
        this.logInterval = 10000; // 10秒内只记录一次健康检查请求

        this.httpServer = http.createServer(async (req, res) => {
            // 记录请求日志，包含来源信息（带频率限制）
            const now = Date.now();
            const userAgent = req.headers['user-agent'] || 'Unknown';
            const clientIP = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'Unknown';

            // 对于健康检查请求，限制日志频率
            if (req.url === '/health' || req.url === '/') {
                if (now - this.lastLogTime > this.logInterval) {
                    // 识别Render平台健康检查
                    const isRenderHealthCheck = userAgent.includes('Render/');
                    const source = isRenderHealthCheck ? 'Render平台' : '外部监控';
                    console.log(`💓 健康检查: ${source} | ${req.method} ${req.url} `);
                    this.lastLogTime = now;
                }
            } else {
                // 非健康检查请求正常记录
                console.log(`📡 HTTP请求: ${req.method} ${req.url} | IP: ${clientIP} | UA: ${userAgent.substring(0, 50)}`);
            }

            // 设置CORS头
            res.setHeader('Access-Control-Allow-Origin', '*');
            res.setHeader('Access-Control-Allow-Methods', 'GET');
            res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
            res.setHeader('Content-Type', 'application/json');

            if ((req.method === 'GET' || req.method === 'HEAD') && (req.url === '/health' || req.url === '/healthz' || req.url === '/')) {
                // 健康检查端点（支持 /, /health, /healthz）
                try {
                    const status = this.orchestrator.getSystemStatus();
                    const isHealthy = status.orchestrator.status === 'running' &&
                        status.orchestrator.activeModules > 0;

                    res.statusCode = isHealthy ? 200 : 503;

                    // 设置响应头，便于外部保活/监控（GitHub Actions/Render Cron/等）识别服务状态
                    res.setHeader('X-Health-Status', isHealthy ? 'healthy' : 'unhealthy');
                    res.setHeader('X-Active-Modules', status.orchestrator.activeModules.toString());

                    // HEAD请求只返回头部，不返回响应体
                    if (req.method === 'HEAD') {
                        res.end();
                    } else {
                        // GET请求返回简洁的JSON响应
                        res.end(JSON.stringify({
                            status: isHealthy ? 'healthy' : 'unhealthy',
                            timestamp: new Date().toISOString(),
                            activeModules: status.orchestrator.activeModules,
                            totalModules: status.orchestrator.enabledModules.length
                        }, null, 2));
                    }
                } catch (error) {
                    console.error('❌ 健康检查异常:', error.message);
                    res.statusCode = 500;
                    res.setHeader('X-Health-Status', 'error');

                    if (req.method === 'HEAD') {
                        res.end();
                    } else {
                        res.end(JSON.stringify({ error: error.message }, null, 2));
                    }
                }

            } else if (req.method === 'GET' && req.url === '/status') {
                // 详细状态端点
                try {
                    const status = this.orchestrator.getSystemStatus();
                    res.statusCode = 200;
                    res.end(JSON.stringify(status, null, 2));

                } catch (error) {
                    res.statusCode = 500;
                    res.end(JSON.stringify({ error: error.message }, null, 2));
                }

            } else {
                // 404 - 未找到
                res.statusCode = 404;
                res.end(JSON.stringify({
                    error: 'Not Found',
                    availableEndpoints: ['/health', '/status']
                }, null, 2));
            }
        });

        this.httpServer.listen(port, () => {
            console.log(`🌐 HTTP健康检查服务器启动，端口: ${port}`);
            console.log(`   健康检查: http://localhost:${port}/health`);
            console.log(`   详细状态: http://localhost:${port}/status`);
        });

        this.httpServer.on('error', (error) => {
            if (error.code === 'EADDRINUSE') {
                console.warn(`⚠️  端口 ${port} 被占用，尝试使用其他端口...`);
                // 尝试使用随机端口
                this.httpServer.listen(0, () => {
                    const actualPort = this.httpServer.address().port;
                    console.log(`🌐 HTTP健康检查服务器启动，端口: ${actualPort}`);
                    console.log(`   健康检查: http://localhost:${actualPort}/health`);
                    console.log(`   详细状态: http://localhost:${actualPort}/status`);
                });
            } else {
                console.error('❌ HTTP服务器错误:', error.message);
            }
        });
    }

    /**
     * 显示系统状态
     */
    showSystemStatus() {
        try {
            const status = this.orchestrator.getSystemStatus();

            console.log('\n📊 系统状态报告:');
            console.log('================');
            console.log(`编排器状态: ${status.orchestrator.status === 'running' ? '✅ 运行中' : '❌ 已停止'}`);
            console.log(`总模块数: ${status.orchestrator.enabledModules.length} 个`);
            console.log(`运行中模块: ${status.orchestrator.activeModules} 个`);

            // 显示各模块状态
            for (const [moduleName, moduleStatus] of Object.entries(status.modules)) {
                const statusIcon = moduleStatus.status === 'running' ? '✅' : '❌';
                const healthIcon = moduleStatus.isHealthy !== false ? '✅' : '❌';
                console.log(`\n📦 模块 ${moduleName}:`);
                console.log(`  状态: ${statusIcon} ${moduleStatus.status}`);
                console.log(`  健康: ${healthIcon} ${moduleStatus.isHealthy !== false ? '健康' : '异常'}`);
                if (moduleStatus.statistics) {
                    console.log(`  处理总数: ${moduleStatus.statistics.totalProcessed || 0}`);
                    console.log(`  成功率: ${moduleStatus.statistics.successCount || 0}/${moduleStatus.statistics.totalProcessed || 0}`);
                }
            }

            // 显示共享服务状态
            console.log(`\n🔧 共享服务:`);
            console.log(`  配置管理: ${status.sharedServices.config}`);
            console.log(`  数据库: ${status.sharedServices.database}`);
            console.log(`  通知系统: ${status.sharedServices.notifier}`);
            console.log('================\n');

        } catch (error) {
            console.error('❌ 获取系统状态失败:', error.message);
        }
    }



    /**
     * 设置优雅关闭
     */
    setupGracefulShutdown() {
        const shutdown = async (signal) => {
            console.log(`\n🛑 收到 ${signal} 信号，开始优雅关闭...`);
            console.log(`📍 时间: ${new Date().toISOString()}`);
            console.log(`📍 进程ID: ${process.pid}`);
            console.log(`📍 父进程ID: ${process.ppid}`);
            console.log(`📍 运行时间: ${Math.floor((Date.now() - this.startTime.getTime()) / 1000)}秒`);
            console.log(`📍 内存使用: ${Math.round(process.memoryUsage().rss / 1024 / 1024)}MB`);
            
            // 记录环境变量（排查是否有特殊配置）
            console.log(`📍 NODE_ENV: ${process.env.NODE_ENV}`);
            console.log(`📍 RENDER: ${process.env.RENDER || 'false'}`);
            console.log(`📍 RENDER_SERVICE_NAME: ${process.env.RENDER_SERVICE_NAME || 'N/A'}`);
            
            // 记录调用栈
            const stack = new Error().stack;
            console.log(`📍 调用栈:\n${stack}`);

            try {
                this.isRunning = false;

                // 关闭HTTP服务器
                if (this.httpServer) {
                    console.log('🌐 关闭HTTP服务器...');
                    this.httpServer.close();
                }

                // 停止监控编排器
                if (this.orchestrator) {
                    await this.orchestrator.stop();
                }

                // 显示运行统计
                const runTime = Date.now() - this.startTime.getTime();
                const runTimeFormatted = this.formatDuration(runTime);
                console.log(`📊 系统运行时间: ${runTimeFormatted}`);

                console.log('✅ 系统已优雅关闭');
                process.exit(0);

            } catch (error) {
                console.error('❌ 关闭过程中出错:', error.message);
                process.exit(1);
            }
        };

        // 监听关闭信号
        process.on('SIGINT', () => shutdown('SIGINT'));
        process.on('SIGTERM', () => shutdown('SIGTERM'));

        // 监听未捕获的异常
        process.on('uncaughtException', (error) => {
            console.error('❌ 未捕获的异常:', error);

            // 如果是数据库连接错误，不退出程序，让重连机制处理
            if (error.message && (
                error.message.includes('Connection terminated') ||
                error.message.includes('connection closed') ||
                error.code === 'ECONNRESET'
            )) {
                console.log('🔄 数据库连接错误，等待重连机制处理...');
                return;
            }

            // 其他严重错误才退出程序
            console.error('🛑 严重错误，开始优雅关闭...');
            shutdown('uncaughtException');
        });

        process.on('unhandledRejection', (reason) => {
            console.error('❌ 未处理的Promise拒绝:', reason);
            shutdown('unhandledRejection');
        });
    }

    /**
     * 格式化持续时间
     * @param {number} ms - 毫秒数
     * @returns {string} 格式化后的时间
     */
    formatDuration(ms) {
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);

        if (days > 0) {
            return `${days}天 ${hours % 24}小时 ${minutes % 60}分钟`;
        } else if (hours > 0) {
            return `${hours}小时 ${minutes % 60}分钟`;
        } else if (minutes > 0) {
            return `${minutes}分钟 ${seconds % 60}秒`;
        } else {
            return `${seconds}秒`;
        }
    }
}

// 创建应用实例
const app = new MultiSourceMonitorApp();

// 如果直接运行此文件，启动应用
if (import.meta.url === `file://${process.argv[1]}`) {
    app.start().catch(error => {
        console.error('应用启动失败:', error);
        process.exit(1);
    });
}

// 导出应用实例供其他模块使用
export { app as multiSourceMonitorApp };