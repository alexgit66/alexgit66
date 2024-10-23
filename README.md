const { Worker, isMainThread, workerData, parentPort } = require('worker_threads');
const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const axios = require('axios');
const faker = require('faker');
const { chromium } = require('playwright');
const UserAgent = require('user-agents');
const path = require('path');
const { Mutex } = require('async-mutex');

// 参数设置部分
const dailiFilePath = path.join(__dirname, 'daili.txt');
const dbFilePath = path.join(__dirname, 'keywords.db');

// 命令行参数获取
const numThreads = parseInt(process.argv[2], 10) || 1; // 初始线程数，默认1线程
const headless = process.argv[3] !== undefined ? parseInt(process.argv[3]) === 0 : false; // 无头模式，1表示开启，0表示关闭，默认关闭
const successMultiplier = parseInt(process.argv[4], 10) || 1; // 成功倍数参数，默认1
const clickUrlRecognition = parseInt(process.argv[5], 10) || 2; // 点击URL识别特征参数，1表示使用网址，2表示使用顶级域名，默认2

// 定义城市列表，用于模拟地理位置
const cities = [
    "北京", "上海", "成都", "重庆", "长沙", "长春", "大连", "东莞", "福州", "佛山",
    "广州", "贵阳", "桂林", "杭州", "惠州", "哈尔滨", "合肥", "呼和浩特", "海口",
    "济南", "南京", "宁波", "南宁", "南昌", "兰州", "昆明", "青岛", "深圳", "沈阳",
    "石家庄", "苏州", "天津", "太原", "武汉", "无锡", "威海", "西安", "厦门", "西宁",
    "银川", "宜昌", "烟台", "郑州", "珠海"
];

// 定义互斥锁，避免多线程读写冲突
const dbMutex = new Mutex();
const sucMutex = new Mutex();

// 日志记录函数，只输出到控制台，节约系统资源
function log(message, threadId = '主线程') {
    const logLine = `【线程${threadId}】 ${message}`;
    console.log(logLine);
}

// 封装数据库运行函数，返回Promise
function dbRunAsync(db, sql, params) {
    return new Promise((resolve, reject) => {
        db.run(sql, params, function (err) {
            if (err) reject(err);
            else resolve(this);
        });
    });
}

// 封装数据库查询函数，返回Promise
function dbGetAsync(db, sql, params) {
    return new Promise((resolve, reject) => {
        db.get(sql, params, function (err, row) {
            if (err) reject(err);
            else resolve(row);
        });
    });
}

// 初始化数据库
function initializeDatabase() {
    const db = new sqlite3.Database(dbFilePath, (err) => {
        if (err) {
            log(`数据库连接失败: ${err.message}`, "主线程");
            process.exit(1);
        }
    });
    db.serialize(() => {
        db.run("PRAGMA journal_mode = WAL;");
        db.run("CREATE TABLE IF NOT EXISTS keywords (keyword TEXT, domain TEXT, url TEXT, UNIQUE(keyword, domain, url))");

        // 检查 success_counts 表结构
        db.get("PRAGMA table_info(success_counts)", async (err, rows) => {
            if (err) {
                log(`检查 success_counts 表结构时出错: ${err.message}`, "主线程");
            } else if (!rows || rows.length === 0) {
                // 表不存在，创建表
                db.run("CREATE TABLE success_counts (date TEXT PRIMARY KEY, count INTEGER)", (err) => {
                    if (err) {
                        log(`创建 success_counts 表时出错: ${err.message}`, "主线程");
                    } else {
                        log("成功创建 success_counts 表", "主线程");
                    }
                });
            } else {
                // 表存在，检查 date 列是否为 PRIMARY KEY
                db.all("PRAGMA table_info(success_counts)", (err, columns) => {
                    if (err) {
                        log(`获取 success_counts 表信息时出错: ${err.message}`, "主线程");
                    } else {
                        let dateColumn = columns.find(column => column.name === 'date');
                        if (dateColumn && dateColumn.pk === 0) {
                            // date 列不是 PRIMARY KEY，需要重新创建表
                            db.serialize(() => {
                                db.run("ALTER TABLE success_counts RENAME TO old_success_counts");
                                db.run("CREATE TABLE success_counts (date TEXT PRIMARY KEY, count INTEGER)");
                                db.run("INSERT INTO success_counts(date, count) SELECT date, count FROM old_success_counts");
                                db.run("DROP TABLE old_success_counts");
                                log("已重新创建 success_counts 表，添加 PRIMARY KEY", "主线程");
                            });
                        }
                    }
                });
            }
        });
    });
    return db;
}

// 将根目录下的 pm.txt 文件中的关键词同步到数据库
async function syncKeywordsToDb(db) {
    if (!isMainThread) return;
    try {
        const filePath = path.join(__dirname, 'pm.txt');
        if (fs.existsSync(filePath)) {
            const data = fs.readFileSync(filePath, 'utf-8').split('\n');
            // 使用事务保证数据一致性
            db.serialize(() => {
                db.run("BEGIN TRANSACTION");
                const stmt = db.prepare("INSERT OR IGNORE INTO keywords VALUES (?, ?, ?)");
                data.forEach(line => {
                    const [keyword, domain, url] = line.split('###').map(str => str.trim());
                    if (keyword && domain && url) {
                        stmt.run(keyword, domain, url);
                    }
                });
                stmt.finalize(async (err) => {
                    if (err) {
                        db.run("ROLLBACK");
                        log(`数据库插入错误: ${err.message}`, "主线程");
                    } else {
                        db.run("COMMIT");
                        log(`数据库同步完成，来自文件 pm.txt`, "主线程");
                    }
                    // 删除 pm.txt 文件
                    try {
                        fs.unlinkSync(filePath);
                        log(`已删除文件 pm.txt`, "主线程");
                    } catch (err) {
                        log(`删除文件 pm.txt 时出错: ${err.message}`, "主线程");
                    }
                });
            });
        } else {
            log(`文件 pm.txt 不存在，等待下次同步`, "主线程");
        }
    } catch (error) {
        log(`同步关键词到数据库时出错: ${error.message}`, "主线程");
        // 尝试删除 pm.txt 文件
        const filePath = path.join(__dirname, 'pm.txt');
        if (fs.existsSync(filePath)) {
            try {
                fs.unlinkSync(filePath);
                log(`已删除文件 pm.txt`, "主线程");
            } catch (err) {
                log(`删除文件 pm.txt 时出错: ${err.message}`, "主线程");
            }
        }
    }
}

// 每10分钟从根目录下同步关键词到数据库
function scheduleKeywordSync(db) {
    syncKeywordsToDb(db); // 立即执行一次
    setInterval(() => {
        syncKeywordsToDb(db);
    }, 600000); // 600,000 毫秒，即 10 分钟
}

// 读取 raw特征.txt 文件
function readRawFeatures() {
    const filePath = path.join(__dirname, 'raw特征.txt');
    if (fs.existsSync(filePath)) {
        const data = fs.readFileSync(filePath, 'utf-8').trim();
        const features = data.split('|').map(str => str.trim());
        return features;
    } else {
        log('raw特征.txt 不存在', '主线程');
        return [];
    }
}

// 获取代理IP
async function getProxyIp(threadId) {
    const dailiUrl = fs.readFileSync(dailiFilePath, 'utf-8').trim();
    if (!dailiUrl) {
        log("daili.txt 代理IP接口地址为空，使用本地IP", threadId);
        return '';
    }

    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            const response = await axios.get(dailiUrl, { timeout: 5000 });
            if (response.data) {
                log(`成功获取代理IP: ${response.data}`, threadId);
                return response.data.trim();
            }
        } catch (error) {
            log(`获取代理IP失败，第${attempt}次重试...`, threadId);
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
    }

    log("代理IP获取失败，等待60秒后结束线程", threadId);
    await new Promise(resolve => setTimeout(resolve, 60000));
    process.exit(0); // 结束子线程，重新初始化
}

// 获取当天的日期字符串（格式 YYYY-MM-DD）
function getTodayDateString() {
    const now = new Date();
    now.setHours(now.getHours() + 8); // 调整为北京时间
    return now.toISOString().split('T')[0];
}

// 获取今天的成功数，从数据库中获取
async function getTodaySuccessCount(db) {
    const today = getTodayDateString(); // 获取当前日期
    try {
        const row = await dbGetAsync(db, "SELECT count FROM success_counts WHERE date = ?", [today]);
        if (row) {
            return row.count;
        } else {
            return 0;
        }
    } catch (err) {
        log(`读取今天成功数时出错: ${err.message}`);
        return 0;
    }
}

// 增加今天的成功数，保存在数据库中
async function incrementTodaySuccessCount(db, threadId) {
    await sucMutex.runExclusive(async () => {
        const today = getTodayDateString();
        try {
            // 先尝试更新
            const result = await dbRunAsync(db, "UPDATE success_counts SET count = count + 1 WHERE date = ?", [today]);
            if (result.changes === 0) {
                // 如果没有更新到行，插入新行
                await dbRunAsync(db, "INSERT INTO success_counts(date, count) VALUES(?, 1)", [today]);
            }
            log(`成功次数增加`, threadId);
        } catch (err) {
            log(`更新今天成功数时出错: ${err.message}`, threadId);
        }
    });
}

// 判断是否可以执行任务
async function canExecuteTask(db, threadId) {
    let successCount = 0;
    let keywordCount = 0;

    try {
        successCount = await getTodaySuccessCount(db);
    } catch (error) {
        log(`读取今天成功数时出错: ${error.message}`, threadId);
        successCount = 0;
    }

    try {
        const row = await dbGetAsync(db, "SELECT COUNT(*) as count FROM keywords", []);
        keywordCount = row.count;
    } catch (err) {
        log(`数据库查询错误: ${err.message}`, threadId);
        keywordCount = 0;
    }

    const expectedTaskVolume = keywordCount * successMultiplier;
    log(`当日成功数: ${successCount}, 预期任务量: ${expectedTaskVolume}`, threadId);

    if (successCount >= expectedTaskVolume) {
        log("当日已全部完成", threadId);
        await new Promise(resolve => setTimeout(resolve, 600000)); // 等待600秒
        return false;
    }

    return true;
}

// 重置成功次数：在新的日期出现时添加新的一行记录
function resetSuccessCount(db) {
    if (!isMainThread) return;

    sucMutex.runExclusive(async () => {
        const today = getTodayDateString(); // 获取当前日期
        try {
            const row = await dbGetAsync(db, "SELECT date FROM success_counts WHERE date = ?", [today]);
            if (!row) {
                try {
                    await dbRunAsync(db, "INSERT INTO success_counts(date, count) VALUES(?, 0)", [today]);
                    log(`日期变更，添加新日期记录 ${today}|0`, "主线程");
                } catch (err) {
                    log(`插入今天成功数记录时出错: ${err.message}`, "主线程");
                }
            }
        } catch (err) {
            log(`重置成功次数时出错: ${err.message}`, "主线程");
        }
    }).catch(error => {
        log(`重置成功次数时出错: ${error.message}`, "主线程");
    });
}

// 模拟人工操作（鼠标移动和随机等待）
async function simulateHumanActions(page) {
    if (page.isClosed()) return;
    // 随机移动鼠标
    const viewportSize = page.viewportSize();
    if (viewportSize) {
        const x = Math.floor(Math.random() * viewportSize.width);
        const y = Math.floor(Math.random() * viewportSize.height);
        await page.mouse.move(x, y, { steps: 10 });
    }
    // 随机等待200-500毫秒
    await page.waitForTimeout(Math.floor(Math.random() * 300) + 200);
}

// 模拟人工浏览，滚动页面和鼠标移动，增强模拟细节
async function simulateHumanBrowsing(page, duration) {
    const endTime = Date.now() + duration;
    while (Date.now() < endTime) {
        if (page.isClosed()) break;
        const viewportSize = page.viewportSize();
        if (!viewportSize) break;

        // 模拟鼠标在页面上的随机移动
        const startX = Math.floor(Math.random() * viewportSize.width);
        const startY = Math.floor(Math.random() * viewportSize.height);
        const endX = Math.floor(Math.random() * viewportSize.width);
        const endY = Math.floor(Math.random() * viewportSize.height);
        await page.mouse.move(startX, startY, { steps: 5 }).catch(() => { });
        await page.mouse.move(endX, endY, { steps: 5 }).catch(() => { });

        // 随机滚动页面
        const scrollAmount = Math.floor(Math.random() * 500) - 250;
        await page.mouse.wheel(0, scrollAmount).catch(() => { });

        // 随机等待200-500毫秒
        await page.waitForTimeout(Math.floor(Math.random() * 300) + 200);
    }
}

// 关闭百度弹窗
async function closeBaiduPopup(page, threadId) {
    try {
        // 查找包含特征文字的弹窗
        const popupSelector = 'div[aria-label*="想要"]';
        const popups = await page.$$(popupSelector);
        if (popups.length > 0) {
            for (const popup of popups) {
                const closeButton = await popup.$('button, .close, .icon-close');
                if (closeButton) {
                    await closeButton.click({ force: true });
                    log("关闭百度弹窗", threadId);
                }
            }
        }
    } catch (error) {
        log(`关闭百度弹窗时出错: ${error.message}`, threadId);
    }
}

// 获取数据库中关键词的数量
async function getKeywordCount(db) {
    try {
        const row = await dbGetAsync(db, "SELECT COUNT(*) as count FROM keywords", []);
        return row.count;
    } catch (err) {
        log(`数据库查询错误: ${err.message}`);
        return 0;
    }
}

// 定义内页设置的多个方案
const innerPageSchemes = [
    {
        name: '方案1',
        searchToolSelector: 'div.tool_3HMbZ.pointer_32dlN.c_font_2AD7M.hovering_1RCgm',
        siteSearchSelector: 'span.c_font_2AD7M:has-text("站点内检索")',
        siteInputSelector: 'input.input_3npy4.custdom_input_25IQU',
        confirmButtonSelector: 'button.btn_2wuOJ.btn_custom_QJplx'
    },
    {
        name: '方案2',
        searchToolSelector: 'text=搜索工具',
        siteSearchSelector: 'text=站点内检索',
        siteInputSelector: 'input[placeholder="例如:baidu.com"]',
        confirmButtonSelector: 'button:has-text("确认")'
    },
    {
        name: '方案3',
        searchToolSelector: 'div.options_2Vntk > div',
        siteSearchSelector: 'span.title_pos_2AOrh.pointer_32dlN',
        siteInputSelector: 'input[type="text"][placeholder*="例如"]',
        confirmButtonSelector: 'button:has-text("确认")'
    },
    {
        name: '方案4',
        searchToolSelector: 'div[aria-label="搜索工具"]',
        siteSearchSelector: 'span[aria-label="站点内检索"]',
        siteInputSelector: 'input[type="text"][class*="custdom_input"]',
        confirmButtonSelector: 'button[class*="btn_custom"]'
    },
    {
        name: '方案5',
        searchToolSelector: 'div[title="搜索工具"]',
        siteSearchSelector: 'span[title="站点内检索"]',
        siteInputSelector: 'input[type="text"]',
        confirmButtonSelector: 'button[type="button"]'
    }
];

// 重试操作的通用函数
async function retryOperation(operation, retries, delay, schemeName, stepDescription, threadId) {
    for (let i = 0; i < retries; i++) {
        try {
            await operation();
            return;
        } catch (error) {
            if (i < retries - 1) {
                log(`${schemeName}: ${stepDescription} 第${i + 1}次重试失败，等待${delay}ms后重试`, threadId);
                await page.waitForTimeout(delay);
            } else {
                throw new Error(`${schemeName}: ${stepDescription} 失败，已重试${retries}次`);
            }
        }
    }
}

// 执行任务主函数
async function executeTask(workerData) {
    const { threadId, rawFeatures } = workerData;
    const db = new sqlite3.Database(dbFilePath, sqlite3.OPEN_READWRITE, (err) => {
        if (err) {
            log(`数据库连接失败: ${err.message}`, threadId);
            process.exit(1);
        }
    });

    while (true) {
        try {
            // 每次子线程运行时都判断是否可以执行任务
            const shouldExecute = await canExecuteTask(db, threadId);
            if (!shouldExecute) {
                continue;
            }

            let proxy = await getProxyIp(threadId);
            if (proxy === '') {
                log("未获取到代理IP，重新初始化", threadId);
                continue;
            }

            let browser;
            try {
                // 初始化浏览器和上下文
                const userAgent = new UserAgent({ deviceCategory: 'desktop' }).toString();
                let fingerprintCity = faker.address.city();
                if (!cities.includes(fingerprintCity)) {
                    fingerprintCity = cities[Math.floor(Math.random() * cities.length)];
                }
                const contextOptions = {
                    userAgent: userAgent,
                    locale: 'zh-CN',
                    viewport: { width: 1920, height: 1080 },
                    proxy: proxy ? { server: `http://${proxy}` } : undefined
                };
                log(`浏览器指纹设置 - 城市: ${fingerprintCity}`, threadId);
                log(`浏览器指纹设置 - UserAgent: ${userAgent}`, threadId);

                browser = await chromium.launch({ headless: headless });
                const context = await browser.newContext(contextOptions);

                // 清理浏览器缓存和Cookie
                await context.clearCookies();
                await context.clearPermissions();
                log("浏览器缓存已清理", threadId);

                const page = await context.newPage();
                log(`浏览器初始化完成，当前IP: ${proxy}`, threadId);

                // 定义已匹配的原始特征集合，防止重复日志输出
                const matchedUrls = new Set();

                // 开始监听请求以收集参数和监控 raw 特征
                page.on('request', (request) => {
                    const url = request.url();
                    const hostname = new URL(url).hostname;

                    // 检查请求方法和主机名
                    if (request.method() === 'GET') {
                        // 如果目标流数据包含所有特征值，则认为匹配成功
                        const isMatch = rawFeatures.every(feature => url.includes(feature));
                        if (isMatch && !matchedUrls.has(url)) {
                            matchedUrls.add(url);
                            log(`raw数据匹配成功: ${url}`, threadId);
                        }
                    }

                    // 收集百度请求的参数
                    if (/(\.|^)baidu\.com$/.test(hostname)) {
                        const params = new URLSearchParams(new URL(url).search);
                        for (const [key, value] of params.entries()) {
                            // 可以在此处处理参数，如存储或日志
                        }
                    }
                });

                // 获取数据库中关键词的总数
                const totalKeywords = await getKeywordCount(db);

                // 定义已使用的关键词集合，避免重复
                let usedKeywords = new Set();

                // 进入内层循环，持续提取新关键词并执行任务，直到出现页面加载失败
                let pageLoadFailed = false;
                while (!pageLoadFailed) {
                    let keywordData;

                    // 从数据库中随机获取一个未使用的关键词
                    await dbMutex.runExclusive(async () => {
                        while (true) {
                            keywordData = await dbGetAsync(db, "SELECT keyword, domain, url FROM keywords ORDER BY RANDOM() LIMIT 1", []);
                            if (!keywordData) {
                                log("数据库中无关键词数据可供执行，等待600秒后重新检查", threadId);
                                await new Promise(resolve => setTimeout(resolve, 600000));
                                continue;
                            }

                            const keyword = keywordData.keyword.trim();

                            if (!usedKeywords.has(keyword)) {
                                usedKeywords.add(keyword);
                                break;
                            }

                            if (usedKeywords.size >= totalKeywords) {
                                usedKeywords.clear();
                                log("所有关键词已使用，重置已使用的关键词集合", threadId);
                            }
                        }
                    });

                    // 提取关键词、顶级域名和网址，并去除前后不可见符号
                    let { keyword, domain, url } = keywordData;
                    keyword = keyword.replace(/^\s+|\s+$/g, '');
                    domain = domain.replace(/^\s+|\s+$/g, '');
                    url = url.replace(/^\s+|\s+$/g, '');

                    // 输出5个数据
                    const successCount = await getTodaySuccessCount(db);
                    const keywordCount = await getKeywordCount(db);
                    const expectedTaskVolume = keywordCount * successMultiplier;
                    log(`当日成功数: ${successCount}, 预期任务量: ${expectedTaskVolume}, 关键词:【${keyword}】, 域名:【${domain}】, 网址:【${url}】`, threadId);

                    try {
                        // 打开百度PC首页
                        try {
                            await page.goto('https://www.baidu.com', { waitUntil: 'load', timeout: 20000 });
                            log("成功打开百度PC首页", threadId);
                        } catch (error) {
                            log(`打开百度首页失败: ${error.message} at https://www.baidu.com/`, threadId);
                            pageLoadFailed = true;
                            await browser.close();
                            break;
                        }

                        // 确保百度首页完全加载
                        try {
                            await page.waitForSelector('input[name="wd"]', { timeout: 10000 });
                            log("百度首页搜索框加载完成", threadId);
                        } catch (error) {
                            log(`百度首页加载不完全: ${error.message}`, threadId);
                            pageLoadFailed = true;
                            await browser.close();
                            break;
                        }

                        // 关闭百度弹窗
                        await closeBaiduPopup(page, threadId);

                        // 模拟人工操作
                        await simulateHumanActions(page);

                        // 输入关键词
                        try {
                            const searchInputSelector = 'input[name="wd"]';
                            await page.focus(searchInputSelector);
                            await page.fill(searchInputSelector, '');
                            for (const char of keyword) {
                                await page.type(searchInputSelector, char, { delay: Math.floor(Math.random() * 150) + 200 }); // 每个字符间隔200-350毫秒
                                // 模拟人类操作
                                await simulateHumanActions(page);
                            }
                            log(`输入关键词: ${keyword}`, threadId);
                        } catch (error) {
                            log(`输入关键词时出错: ${error.message}`, threadId);
                            pageLoadFailed = true;
                            await browser.close();
                            break;
                        }

                        // 模拟人类操作
                        await simulateHumanActions(page);

                        // 点击搜索按钮
                        try {
                            const searchButtonSelector = 'input#su, input[type="submit"]';
                            await page.waitForSelector(searchButtonSelector, { timeout: 10000 });
                            // 点击搜索按钮前关闭弹窗
                            await closeBaiduPopup(page, threadId);
                            // 模拟人类操作
                            await simulateHumanActions(page);
                            // 移动鼠标到搜索按钮并点击
                            const searchButton = await page.$(searchButtonSelector);
                            const buttonBox = await searchButton.boundingBox();
                            if (buttonBox) {
                                await page.mouse.move(buttonBox.x + buttonBox.width / 2, buttonBox.y + buttonBox.height / 2, { steps: 10 });
                            }
                            await searchButton.click({ force: true });
                            log("点击搜索按钮", threadId);
                        } catch (error) {
                            log(`点击搜索按钮时出错: ${error.message}`, threadId);
                            pageLoadFailed = true;
                            await browser.close();
                            break;
                        }

                        // 判断是否触发验证码
                        let captchaTriggered = false;
                        try {
                            await page.waitForLoadState('networkidle', { timeout: 20000 });
                            const currentURL = page.url();
                            if (currentURL.includes('wappass.baidu.com') || currentURL.includes('verify.baidu.com')) {
                                log("触发百度安全验证码，重新初始化浏览器", threadId);
                                captchaTriggered = true;
                                pageLoadFailed = true;
                                await browser.close();
                                break;
                            } else {
                                // 确保搜索结果页面的主要内容加载完成
                                await page.waitForSelector('#content_left', { timeout: 10000 });
                                log("搜索结果页面加载完成", threadId);
                            }
                        } catch (error) {
                            log(`搜索结果页面加载失败: ${error.message}`, threadId);
                            pageLoadFailed = true;
                            await browser.close();
                            break;
                        }

                        if (captchaTriggered) {
                            pageLoadFailed = true;
                            break;
                        }

                        // 模拟人类浏览
                        await simulateHumanBrowsing(page, Math.floor(Math.random() * 3000) + 2000); // 浏览2-5秒

                        // 在搜索结果页面关闭弹窗
                        await closeBaiduPopup(page, threadId);

                        // 获取搜索结果
                        const resultDivs = await page.$$('div.result.c-container.xpath-log.new-pmd');
                        let matchedElement = null;

                        for (const div of resultDivs) {
                            const mu = await div.getAttribute('mu');
                            if (mu) {
                                let matchFound = false;
                                switch (clickUrlRecognition) {
                                    case 1:
                                        // 参数1：仅使用网址
                                        if (mu.includes(url)) {
                                            matchFound = true;
                                        }
                                        break;
                                    case 2:
                                        // 参数2：仅使用顶级域名
                                        if (mu.includes(domain)) {
                                            matchFound = true;
                                        }
                                        break;
                                    default:
                                        // 默认使用顶级域名
                                        if (mu.includes(domain)) {
                                            matchFound = true;
                                        }
                                        break;
                                }
                                if (matchFound) {
                                    matchedElement = div;
                                    log(`找到匹配的链接: ${mu}`, threadId);
                                    break;
                                }
                            }
                        }

                        if (matchedElement) {
                            // 在匹配成功的 div 中找到目标 <a> 标签
                            const linkElement = await matchedElement.$('a[href^="http://www.baidu.com/link?url="]');
                            if (linkElement) {
                                // 确保链接在同一标签页打开
                                await linkElement.evaluate(node => node.setAttribute('target', '_self'));

                                // 移动鼠标到链接位置并点击
                                const linkBox = await linkElement.boundingBox();
                                if (linkBox) {
                                    await page.mouse.move(linkBox.x + linkBox.width / 2, linkBox.y + linkBox.height / 2, { steps: 10 });
                                }

                                // 模拟人类操作
                                await simulateHumanActions(page);

                                // 在点击进站URL前关闭弹窗
                                await closeBaiduPopup(page, threadId);

                                await linkElement.click({ force: true });
                                log("成功点击进站URL", threadId);

                                // 等待新页面加载，最多10秒
                                try {
                                    await page.waitForLoadState('load', { timeout: 10000 });
                                    const finalUrl = page.url();
                                    if (finalUrl.includes(domain)) {
                                        log("点击进站URL成功，包含顶级域名", threadId);
                                        await incrementTodaySuccessCount(db, threadId); // 增加今天的成功数

                                        // 模拟浏览2-3秒
                                        await simulateHumanBrowsing(page, Math.floor(Math.random() * 1000) + 2000);

                                        await browser.close();
                                        break; // 结束线程
                                    } else {
                                        log("点击进站URL后URL不包含顶级域名，视为失败", threadId);
                                        await browser.close();
                                        break; // 结束线程
                                    }
                                } catch (error) {
                                    log(`等待点击的页面加载时出错: ${error.message}`, threadId);
                                    // 页面加载失败，不视为失败
                                    await browser.close();
                                    break; // 结束线程
                                }
                            } else {
                                log("未找到匹配的 <a> 标签，结束线程", threadId);
                                await browser.close();
                                break; // 结束线程
                            }
                        } else {
                            // 进入内页设置环节
                            log("未找到匹配的链接，进入内页设置环节", threadId);

                            let innerPageSuccess = false;

                            for (const scheme of innerPageSchemes) {
                                try {
                                    log(`尝试${scheme.name}`, threadId);

                                    // Step 1: 点击搜索工具按钮
                                    await retryOperation(async () => {
                                        await page.waitForSelector(scheme.searchToolSelector, { timeout: 5000 });
                                        const toolButton = await page.$(scheme.searchToolSelector);
                                        if (toolButton) {
                                            await toolButton.click({ force: true });
                                            log(`${scheme.name}: 点击搜索工具按钮`, threadId);
                                        } else {
                                            throw new Error(`${scheme.name}: 未找到搜索工具按钮`);
                                        }
                                    }, 3, 1000, scheme.name, '点击搜索工具按钮', threadId);

                                    // Step 2: 点击站点内检索按钮
                                    await retryOperation(async () => {
                                        await page.waitForSelector(scheme.siteSearchSelector, { timeout: 5000 });
                                        const siteSearchButton = await page.$(scheme.siteSearchSelector);
                                        if (siteSearchButton) {
                                            await siteSearchButton.click({ force: true });
                                            log(`${scheme.name}: 点击站点内检索按钮`, threadId);
                                        } else {
                                            throw new Error(`${scheme.name}: 未找到站点内检索按钮`);
                                        }
                                    }, 3, 1000, scheme.name, '点击站点内检索按钮', threadId);

                                    // Step 3: 在输入框内输入顶级域名
                                    await retryOperation(async () => {
                                        await page.waitForSelector(scheme.siteInputSelector, { timeout: 5000 });
                                        const siteInput = await page.$(scheme.siteInputSelector);
                                        if (siteInput) {
                                            await siteInput.fill(domain);
                                            log(`${scheme.name}: 输入顶级域名: ${domain}`, threadId);
                                        } else {
                                            throw new Error(`${scheme.name}: 未找到站点输入框`);
                                        }
                                    }, 3, 1000, scheme.name, '输入顶级域名', threadId);

                                    // Step 4: 点击确认按钮
                                    await retryOperation(async () => {
                                        await page.waitForSelector(scheme.confirmButtonSelector, { timeout: 5000 });
                                        const confirmButton = await page.$(scheme.confirmButtonSelector);
                                        if (confirmButton) {
                                            await confirmButton.click({ force: true });
                                            log(`${scheme.name}: 点击确认按钮`, threadId);
                                        } else {
                                            throw new Error(`${scheme.name}: 未找到确认按钮`);
                                        }
                                    }, 3, 1000, scheme.name, '点击确认按钮', threadId);

                                    // 如果所有步骤成功，标记为成功并退出循环
                                    innerPageSuccess = true;
                                    break;

                                } catch (error) {
                                    log(`${scheme.name}: 出现错误: ${error.message}`, threadId);
                                    // 继续尝试下一个方案
                                }
                            }

                            if (!innerPageSuccess) {
                                log("内页设置环节全部方案尝试失败", threadId);
                                await browser.close();
                                break; // 结束线程
                            }

                            // 后续步骤与之前类似，判断是否触发验证码，获取新的搜索结果，执行点击等
                            // 判断是否触发验证码
                            let captchaTriggeredInner = false;
                            try {
                                await page.waitForLoadState('networkidle', { timeout: 20000 });
                                const currentURLInner = page.url();
                                if (currentURLInner.includes('wappass.baidu.com') || currentURLInner.includes('verify.baidu.com')) {
                                    log("触发百度安全验证码，重新初始化浏览器", threadId);
                                    captchaTriggeredInner = true;
                                    pageLoadFailed = true;
                                    await browser.close();
                                    break;
                                } else {
                                    // 确保搜索结果页面的主要内容加载完成
                                    await page.waitForSelector('#content_left', { timeout: 10000 });
                                    log("内页设置后的搜索结果页面加载完成", threadId);
                                }
                            } catch (error) {
                                log(`内页设置后的搜索结果页面加载失败: ${error.message}`, threadId);
                                pageLoadFailed = true;
                                await browser.close();
                                break;
                            }

                            if (captchaTriggeredInner) {
                                pageLoadFailed = true;
                                break;
                            }

                            // 模拟人类浏览
                            await simulateHumanBrowsing(page, Math.floor(Math.random() * 2000) + 3000); // 浏览3-5秒

                            // 获取新的搜索结果
                            const newResultDivs = await page.$$('div.result.c-container.xpath-log.new-pmd');
                            let newMatchedElement = null;

                            for (const div of newResultDivs) {
                                const mu = await div.getAttribute('mu');
                                if (mu) {
                                    let matchFound = false;
                                    switch (clickUrlRecognition) {
                                        case 1:
                                            // 参数1：仅使用网址
                                            if (mu.includes(url)) {
                                                matchFound = true;
                                            }
                                            break;
                                        case 2:
                                            // 参数2：仅使用顶级域名
                                            if (mu.includes(domain)) {
                                                matchFound = true;
                                            }
                                            break;
                                        default:
                                            // 默认使用顶级域名
                                            if (mu.includes(domain)) {
                                                matchFound = true;
                                            }
                                            break;
                                    }
                                    if (matchFound) {
                                        newMatchedElement = div;
                                        log(`内页设置后找到匹配的链接: ${mu}`, threadId);
                                        break;
                                    }
                                }
                            }

                            if (newMatchedElement) {
                                // 模拟人类浏览
                                await simulateHumanBrowsing(page, Math.floor(Math.random() * 2000) + 3000); // 浏览3-5秒

                                // 在匹配成功的 div 中找到目标 <a> 标签
                                const linkElement = await newMatchedElement.$('a[href^="http://www.baidu.com/link?url="]');
                                if (linkElement) {
                                    // 确保链接在同一标签页打开
                                    await linkElement.evaluate(node => node.setAttribute('target', '_self'));

                                    // 移动鼠标到链接位置并点击
                                    const linkBox = await linkElement.boundingBox();
                                    if (linkBox) {
                                        await page.mouse.move(linkBox.x + linkBox.width / 2, linkBox.y + linkBox.height / 2, { steps: 10 });
                                    }

                                    // 模拟人类操作
                                    await simulateHumanActions(page);

                                    // 在点击进站URL前关闭弹窗
                                    await closeBaiduPopup(page, threadId);

                                    await linkElement.click({ force: true });
                                    log("成功点击内页设置后的进站URL", threadId);

                                    // 等待新页面加载，最多10秒
                                    try {
                                        await page.waitForLoadState('load', { timeout: 10000 });
                                        const finalUrl = page.url();
                                        if (finalUrl.includes(domain)) {
                                            log("点击内页设置后的进站URL成功，包含顶级域名", threadId);
                                            await incrementTodaySuccessCount(db, threadId); // 增加今天的成功数

                                            // 模拟浏览2-3秒
                                            await simulateHumanBrowsing(page, Math.floor(Math.random() * 1000) + 2000);

                                            await browser.close();
                                            break; // 结束线程
                                        } else {
                                            log("点击内页设置后的进站URL后URL不包含顶级域名，视为失败", threadId);
                                            await browser.close();
                                            break; // 结束线程
                                        }
                                    } catch (error) {
                                        log(`等待点击的页面加载时出错: ${error.message}`, threadId);
                                        // 页面加载失败，不视为失败
                                        await browser.close();
                                        break; // 结束线程
                                    }
                                } else {
                                    log("未找到匹配的 <a> 标签，结束线程", threadId);
                                    await browser.close();
                                    break; // 结束线程
                                }
                            } else {
                                // 未找到匹配的链接，删除当前关键词网址数据
                                log("未找到相关结果，删除当前关键词网址数据", threadId);
                                await dbMutex.runExclusive(async () => {
                                    try {
                                        await dbRunAsync(db, "DELETE FROM keywords WHERE keyword = ? AND domain = ? AND url = ?", [keyword, domain, url]);
                                        log(`已删除关键词【${keyword}】相关数据`, threadId);
                                    } catch (err) {
                                        log(`删除关键词网址数据时出错: ${err.message}`, threadId);
                                    }
                                });
                                await browser.close();
                                break; // 结束线程
                            }
                        }
                    } catch (error) {
                        log(`执行任务时出现错误: ${error.message}`, threadId);
                        // 判断是否是页面加载失败导致的错误
                        if (error.message.includes('Page load failed') || error.message.includes('navigation timeout')) {
                            pageLoadFailed = true;
                            await browser.close();
                            break;
                        } else {
                            // 其他错误，继续下一个关键词
                            continue;
                        }
                    }
                }

                // 如果出现页面加载失败，重新初始化浏览器和页面
                if (pageLoadFailed) {
                    log('页面加载失败，重新初始化浏览器', threadId);
                    pageLoadFailed = false;
                    continue; // 继续外层循环，重新初始化浏览器
                }

            } catch (error) {
                log(`执行任务时出现错误: ${error.message}`, threadId);
                if (browser) {
                    await browser.close();
                }
                // 等待10秒后继续
                await new Promise(resolve => setTimeout(resolve, 10000));
                continue;
            }

        } catch (error) {
            log(`线程${threadId} 出现错误: ${error.message}`, threadId);
            // 等待10秒后继续
            await new Promise(resolve => setTimeout(resolve, 10000));
            continue;
        }
    }
}

// 主线程启动多个工作线程
if (isMainThread) {
    const db = initializeDatabase();
    scheduleKeywordSync(db); // 安排关键词同步任务
    resetSuccessCount(db);

    // 读取 raw 特征数据
    const rawFeatures = readRawFeatures();

    // 每分钟检查一次是否过了0点，重置成功次数
    setInterval(() => {
        resetSuccessCount(db);
    }, 60000); // 60,000 毫秒，即 1 分钟

    // 启动线程
    for (let i = 1; i <= numThreads; i++) {
        const worker = new Worker(__filename, { workerData: { threadId: i, rawFeatures } });
        worker.on('exit', (code) => {
            log(`线程${i} 退出，代码: ${code}`, "主线程");
            // 重启线程
            const newWorker = new Worker(__filename, { workerData: { threadId: i, rawFeatures } });
            attachWorkerEvents(newWorker, i);
        });
        worker.on('error', (err) => {
            log(`线程${i} 出现错误: ${err.message}`, "主线程");
        });
    }

    // 附加工作线程事件处理
    function attachWorkerEvents(worker, threadId) {
        worker.on('exit', (code) => {
            log(`线程${threadId} 退出，代码: ${code}`, "主线程");
            // 重启线程
            const newWorker = new Worker(__filename, { workerData: { threadId, rawFeatures } });
            attachWorkerEvents(newWorker, threadId);
        });
        worker.on('error', (err) => {
            log(`线程${threadId} 出现错误: ${err.message}`, "主线程");
        });
    }

} else {
    executeTask(workerData);
}
