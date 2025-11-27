using System;
using System.Buffers;
using System.Collections.Generic;
using System.Drawing;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace ProxyServer
{
    // 日志级别枚举
    public enum LogLevel
    {
        Critical,  // 严重错误
        Error,     // 错误
        Warning,   // 警告
        Info,      // 关键信息
        Debug      // 调试信息（高并发时关闭）
    }

    // 单个代理配置项（支持HTTP/SOCKS5）
    public class ProxyItem
    {
        public string ProxyType { get; set; } = "HTTP"; // 默认HTTP
        public string ProxyIp { get; set; } = "";
        public int ProxyPort { get; set; } = 8080; // HTTP默认8080，SOCKS5默认1080
        public bool NeedAuth { get; set; } = false;
        public string ProxyUsername { get; set; } = "";
        public string ProxyPassword { get; set; } = "";
    }

    // 全局配置类
    public class ProxyConfig
    {
        public string LocalIp { get; set; } = "0.0.0.0";
        public int MinRandomPort { get; set; } = 10000;
        public int MaxRandomPort { get; set; } = 10005;
        public Dictionary<int, ProxyItem> PortProxyMap { get; set; } = new Dictionary<int, ProxyItem>();
        public LogLevel LogLevel { get; set; } = LogLevel.Info; // 默认只输出关键日志

        public void Save(string path = "proxy_config.json")
        {
            try
            {
                var json = JsonSerializer.Serialize(this, new JsonSerializerOptions { WriteIndented = true });
                System.IO.File.WriteAllText(path, json);
            }
            catch (Exception ex)
            {
                Log($"❌ 配置保存失败：{ex.Message}", LogLevel.Error);
            }
        }

        public static ProxyConfig Load(string path = "proxy_config.json")
        {
            try
            {
                if (!System.IO.File.Exists(path)) return new ProxyConfig();
                var json = System.IO.File.ReadAllText(path);
                var config = JsonSerializer.Deserialize<ProxyConfig>(json) ?? new ProxyConfig();
                if (config.PortProxyMap == null) config.PortProxyMap = new Dictionary<int, ProxyItem>();
                return config;
            }
            catch (Exception ex)
            {
                Log($"❌ 配置加载失败：{ex.Message}", LogLevel.Error);
                return new ProxyConfig();
            }
        }

        public static void Log(string message, LogLevel level = LogLevel.Info)
        {
            if (level >= Load().LogLevel)
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] [{level}] {message}");
            }
        }
    }

    // 服务端核心转发逻辑（兼容C# 10.0及以下）
    public class ProxyServerCore
    {
        private const int BUFFER_SIZE = 16384; // 16KB缓冲区
        private const int SOCKET_BUFFER_SIZE = 65536; // 64KB Socket缓冲区
        private const int CONNECT_TIMEOUT_MS = 20000; // 20秒连接超时
        private const int READ_WRITE_TIMEOUT_MS = 30000; // 30秒读写超时
        private const string USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

        // 流量统计（线程安全）
        private long _totalReceivedBytes = 0;
        private long _totalSentBytes = 0;

        // 对外提供流量数据
        public long TotalReceivedBytes => Volatile.Read(ref _totalReceivedBytes);
        public long TotalSentBytes => Volatile.Read(ref _totalSentBytes);
        public long TotalTrafficBytes => TotalReceivedBytes + TotalSentBytes;

        private List<TcpListener> _listeners = new List<TcpListener>();
        private ProxyConfig _config;
        private bool _isRunning;

        public ProxyServerCore(ProxyConfig config)
        {
            _config = config;
            // 初始化所有端口的监听器
            foreach (var port in config.PortProxyMap.Keys)
            {
                try
                {
                    var listener = new TcpListener(IPAddress.Parse(config.LocalIp), port);
                    // 增大监听Socket缓冲区
                    ((Socket)listener.Server).SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendBuffer, SOCKET_BUFFER_SIZE);
                    ((Socket)listener.Server).SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer, SOCKET_BUFFER_SIZE);
                    _listeners.Add(listener);
                }
                catch (Exception ex)
                {
                    ProxyConfig.Log($"⚠️  端口 {port} 初始化失败：{ex.Message}", LogLevel.Warning);
                }
            }
        }

        // 重置流量统计
        public void ResetTraffic()
        {
            Interlocked.Exchange(ref _totalReceivedBytes, 0);
            Interlocked.Exchange(ref _totalSentBytes, 0);
            ProxyConfig.Log("[流量统计] 已重置所有流量", LogLevel.Info);
        }

        // 启动服务
        public async Task StartAsync()
        {
            _isRunning = true;
            var listenTasks = new List<Task>();

            foreach (var listener in _listeners)
            {
                var port = ((IPEndPoint)listener.LocalEndpoint).Port;
                if (!_config.PortProxyMap.TryGetValue(port, out var proxyItem))
                {
                    ProxyConfig.Log($"⚠️  端口 {port} 无对应代理配置，跳过启动", LogLevel.Warning);
                    continue;
                }

                try
                {
                    listener.Start();
                    ProxyConfig.Log($"✅ 端口 {port} 启动成功 | 代理：{proxyItem.ProxyType} {proxyItem.ProxyIp}:{proxyItem.ProxyPort}（认证：{proxyItem.NeedAuth}）", LogLevel.Info);
                    listenTasks.Add(AcceptClientsAsync(listener, port, proxyItem));
                }
                catch (Exception ex)
                {
                    ProxyConfig.Log($"⚠️  端口 {port} 启动失败：{ex.Message}", LogLevel.Warning);
                }
            }

            ProxyConfig.Log($"\n📡 服务端启动成功！", LogLevel.Info);
            ProxyConfig.Log($"监听IP：{_config.LocalIp}", LogLevel.Info);
            ProxyConfig.Log($"端口范围：{_config.MinRandomPort}-{_config.MaxRandomPort}", LogLevel.Info);
            ProxyConfig.Log($"已启动端口数：{listenTasks.Count}\n", LogLevel.Info);

            await Task.WhenAll(listenTasks);
        }

        // 循环接收客户端连接
        private async Task AcceptClientsAsync(TcpListener listener, int port, ProxyItem proxyItem)
        {
            while (_isRunning)
            {
                try
                {
                    var clientTcp = await listener.AcceptTcpClientAsync();
                    // 禁用Nagle算法，增大缓冲区
                    clientTcp.NoDelay = true;
                    clientTcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendBuffer, SOCKET_BUFFER_SIZE);
                    clientTcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer, SOCKET_BUFFER_SIZE);
                    clientTcp.ReceiveTimeout = READ_WRITE_TIMEOUT_MS;
                    clientTcp.SendTimeout = READ_WRITE_TIMEOUT_MS;
                    var clientIp = ((IPEndPoint)clientTcp.Client.RemoteEndPoint!).Address.ToString();
                    ProxyConfig.Log($"[端口 {port}] 📥 新客户端连接：{clientIp}（代理类型：{proxyItem.ProxyType}）", LogLevel.Info);
                    // 异步处理客户端连接
                    _ = HandleClientAsync(clientTcp, port, proxyItem, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    if (_isRunning)
                    {
                        ProxyConfig.Log($"[端口 {port}] ⚠️  监听异常：{ex.Message}", LogLevel.Warning);
                    }
                }
            }
        }

        // 处理单个客户端连接（移除Memory<T>，兼容C# 10.0）
        private async Task HandleClientAsync(TcpClient clientTcp, int port, ProxyItem proxyItem, CancellationToken token)
        {
            var clientIp = ((IPEndPoint)clientTcp.Client.RemoteEndPoint!).Address.ToString();
            NetworkStream clientStream = null;
            var buffer = ArrayPool<byte>.Shared.Rent(BUFFER_SIZE);
            try
            {
                clientStream = clientTcp.GetStream();
                clientStream.ReadTimeout = READ_WRITE_TIMEOUT_MS;
                clientStream.WriteTimeout = READ_WRITE_TIMEOUT_MS;

                // 兼容修改：使用传统ReadAsync（数组+偏移量）
                var readCount = await clientStream.ReadAsync(buffer, 0, buffer.Length, token);
                if (readCount == 0)
                {
                    ProxyConfig.Log($"[端口 {port}] 📤 客户端主动断开：{clientIp}（未发送数据）", LogLevel.Debug);
                    return;
                }

                // 协议判断（使用数组切片兼容低版本）
                int maxCheckLength = Math.Min(10, readCount);
                var requestStart = Encoding.ASCII.GetString(buffer, 0, maxCheckLength).TrimStart();
                if (proxyItem.ProxyType == "HTTP")
                {
                    if (requestStart.StartsWith("GET") || requestStart.StartsWith("POST") ||
                        requestStart.StartsWith("PUT") || requestStart.StartsWith("DELETE") ||
                        requestStart.StartsWith("CONNECT") || requestStart.StartsWith("HEAD"))
                    {
                        await HandleHttpClientAsync(clientTcp, clientStream, buffer, readCount, port, proxyItem, token);
                    }
                    else
                    {
                        ProxyConfig.Log($"[端口 {port}] ❌ HTTP代理不支持非HTTP协议：{clientIp}（请求开头：{requestStart}）", LogLevel.Error);
                        var errorResp = Encoding.ASCII.GetBytes("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
                        await clientStream.WriteAsync(errorResp, 0, errorResp.Length, token);
                    }
                }
                else if (proxyItem.ProxyType == "SOCKS5")
                {
                    if (buffer[0] == 0x05)
                    {
                        await HandleSocks5ClientAsync(clientTcp, clientStream, buffer, readCount, port, proxyItem, token);
                    }
                    else
                    {
                        ProxyConfig.Log($"[端口 {port}] ❌ SOCKS5代理不支持非SOCKS5协议：{clientIp}（首字节：0x{buffer[0]:X2}）", LogLevel.Error);
                        var errorResp = Encoding.ASCII.GetBytes("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
                        await clientStream.WriteAsync(errorResp, 0, errorResp.Length, token);
                    }
                }
                else
                {
                    ProxyConfig.Log($"[端口 {port}] ❌ 不支持的代理类型：{proxyItem.ProxyType}", LogLevel.Error);
                    var errorResp = Encoding.ASCII.GetBytes("HTTP/1.1 501 Not Implemented\r\nContent-Length: 0\r\n\r\n");
                    await clientStream.WriteAsync(errorResp, 0, errorResp.Length, token);
                }
            }
            catch (TimeoutException ex)
            {
                ProxyConfig.Log($"[端口 {port}] ⚠️  客户端超时 {clientIp}：{ex.Message}", LogLevel.Warning);
            }
            catch (Exception ex)
            {
                ProxyConfig.Log($"[端口 {port}] ❌ 客户端处理异常 {clientIp}：{ex.Message}", LogLevel.Error);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
                clientStream?.Dispose();
                clientTcp.Dispose();
                ProxyConfig.Log($"[端口 {port}] 📤 客户端连接已释放：{clientIp}", LogLevel.Debug);
            }
        }

        // SOCKS5代理处理逻辑（兼容修改）
        private async Task HandleSocks5ClientAsync(TcpClient clientTcp, NetworkStream clientStream, byte[] buffer, int readCount, int port, ProxyItem proxyItem, CancellationToken token)
        {
            TcpClient proxyTcp = null;
            NetworkStream proxyStream = null;
            try
            {
                // 1. SOCKS5握手响应（无认证）
                await clientStream.WriteAsync(new byte[] { 0x05, 0x00 }, 0, 2, token);

                // 2. 读取连接请求（兼容修改）
                readCount = await clientStream.ReadAsync(buffer, 0, buffer.Length, token);
                if (readCount == 0)
                {
                    ProxyConfig.Log($"[端口 {port}] 📤 客户端断开：{((IPEndPoint)clientTcp.Client.RemoteEndPoint!).Address}（未发送连接请求）", LogLevel.Debug);
                    return;
                }

                if (buffer[1] != 0x01) // 只支持CONNECT命令
                {
                    await clientStream.WriteAsync(new byte[] { 0x05, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, 0, 10, token);
                    return;
                }

                // 3. 解析目标地址
                string dstHost;
                int dstPort = (buffer[8] << 8) | buffer[9];
                if (buffer[3] == 0x01) // IPv4
                {
                    dstHost = $"{buffer[4]}.{buffer[5]}.{buffer[6]}.{buffer[7]}";
                }
                else if (buffer[3] == 0x03) // 域名
                {
                    var domainLength = buffer[4];
                    dstHost = Encoding.ASCII.GetString(buffer, 5, domainLength);
                }
                else // 不支持的地址类型
                {
                    await clientStream.WriteAsync(new byte[] { 0x05, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, 0, 10, token);
                    return;
                }
                ProxyConfig.Log($"[端口 {port}] 🚀 SOCKS5转发：{dstHost}:{dstPort} | 代理：{proxyItem.ProxyIp}:{proxyItem.ProxyPort}", LogLevel.Info);

                // 4. 连接SOCKS5代理
                proxyTcp = new TcpClient();
                proxyTcp.NoDelay = true;
                proxyTcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendBuffer, SOCKET_BUFFER_SIZE);
                proxyTcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer, SOCKET_BUFFER_SIZE);
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token))
                {
                    cts.CancelAfter(CONNECT_TIMEOUT_MS);
                    await proxyTcp.ConnectAsync(proxyItem.ProxyIp, proxyItem.ProxyPort, cts.Token);
                }
                proxyTcp.ReceiveTimeout = READ_WRITE_TIMEOUT_MS;
                proxyTcp.SendTimeout = READ_WRITE_TIMEOUT_MS;
                proxyStream = proxyTcp.GetStream();

                // 5. 代理认证（如果需要）
                if (proxyItem.NeedAuth)
                {
                    var authReq = new byte[] { 0x05, 0x02, 0x00, 0x01 };
                    await proxyStream.WriteAsync(authReq, 0, authReq.Length, token);

                    var authResp = new byte[2];
                    var authRead = await proxyStream.ReadAsync(authResp, 0, authResp.Length, token);
                    if (authRead != 2 || authResp[1] != 0x01)
                    {
                        await clientStream.WriteAsync(new byte[] { 0x05, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, 0, 10, token);
                        ProxyConfig.Log($"[端口 {port}] ❌ SOCKS5代理认证失败：不支持用户名密码认证", LogLevel.Error);
                        return;
                    }

                    // 发送用户名密码
                    var userBytes = Encoding.ASCII.GetBytes(proxyItem.ProxyUsername);
                    var pwdBytes = Encoding.ASCII.GetBytes(proxyItem.ProxyPassword);
                    var authDataLength = 1 + userBytes.Length + 1 + pwdBytes.Length;
                    var authData = ArrayPool<byte>.Shared.Rent(authDataLength);
                    try
                    {
                        authData[0] = (byte)userBytes.Length;
                        userBytes.CopyTo(authData, 1);
                        authData[1 + userBytes.Length] = (byte)pwdBytes.Length;
                        pwdBytes.CopyTo(authData, 1 + userBytes.Length + 1);
                        await proxyStream.WriteAsync(authData, 0, authDataLength, token);
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(authData);
                    }

                    // 读取认证结果
                    var authResult = new byte[2];
                    var resultRead = await proxyStream.ReadAsync(authResult, 0, authResult.Length, token);
                    if (resultRead != 2 || authResult[1] != 0x00)
                    {
                        await clientStream.WriteAsync(new byte[] { 0x05, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, 0, 10, token);
                        ProxyConfig.Log($"[端口 {port}] ❌ SOCKS5代理认证失败：用户名或密码错误", LogLevel.Error);
                        return;
                    }
                }

                // 6. 转发请求和响应
                await proxyStream.WriteAsync(buffer, 0, readCount, token);
                var proxyResp = new byte[10];
                var respRead = await proxyStream.ReadAsync(proxyResp, 0, proxyResp.Length, token);
                if (respRead > 0)
                    await clientStream.WriteAsync(proxyResp, 0, respRead, token);

                // 7. 双向流转发
                var forwardTask1 = ForwardStreamAsync(clientStream, proxyStream, $"[端口 {port}] 客户端→SOCKS5代理", token);
                var forwardTask2 = ForwardStreamAsync(proxyStream, clientStream, $"[端口 {port}] SOCKS5代理→客户端", token);
                await Task.WhenAll(forwardTask1, forwardTask2);
            }
            catch (OperationCanceledException)
            {
                ProxyConfig.Log($"[端口 {port}] ⚠️  SOCKS5代理连接超时：{proxyItem.ProxyIp}:{proxyItem.ProxyPort}（{CONNECT_TIMEOUT_MS}ms）", LogLevel.Warning);
                await clientStream.WriteAsync(new byte[] { 0x05, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }, 0, 10, token);
            }
            catch (IOException ex)
            {
                if (!ex.Message.Contains("已中止 I/O 操作") && !ex.Message.Contains("无法读取传输连接"))
                {
                    ProxyConfig.Log($"[端口 {port}] ❌ SOCKS5转发异常：{ex.Message}", LogLevel.Error);
                }
            }
            finally
            {
                proxyStream?.Dispose();
                proxyTcp?.Dispose();
            }
        }

        // HTTP代理处理逻辑（兼容修改）
        private async Task HandleHttpClientAsync(TcpClient clientTcp, NetworkStream clientStream, byte[] buffer, int readCount, int port, ProxyItem proxyItem, CancellationToken token)
        {
            TcpClient proxyTcp = null;
            NetworkStream proxyStream = null;
            try
            {
                // 1. 解析HTTP请求（兼容低版本：使用数组+索引）
                int firstLineEnd = -1;
                for (int i = 0; i < readCount; i++)
                {
                    if (buffer[i] == '\n')
                    {
                        firstLineEnd = i;
                        break;
                    }
                }
                if (firstLineEnd == -1)
                {
                    ProxyConfig.Log($"[端口 {port}] ❌ 无效HTTP请求（无换行符）", LogLevel.Error);
                    var errorResp = Encoding.ASCII.GetBytes("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
                    await clientStream.WriteAsync(errorResp, 0, errorResp.Length, token);
                    return;
                }
                var firstLine = Encoding.ASCII.GetString(buffer, 0, firstLineEnd).Trim();
                var parts = firstLine.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 2)
                {
                    ProxyConfig.Log($"[端口 {port}] ❌ 无效HTTP请求：{firstLine}", LogLevel.Error);
                    var errorResp = Encoding.ASCII.GetBytes("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
                    await clientStream.WriteAsync(errorResp, 0, errorResp.Length, token);
                    return;
                }

                string httpMethod = parts[0].ToUpper();
                string target = parts[1];
                ProxyConfig.Log($"[端口 {port}] 🚀 HTTP代理-{httpMethod}转发：{target} | 代理：{proxyItem.ProxyIp}:{proxyItem.ProxyPort}", LogLevel.Info);

                // 2. 连接HTTP代理
                proxyTcp = new TcpClient();
                proxyTcp.NoDelay = true;
                proxyTcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendBuffer, SOCKET_BUFFER_SIZE);
                proxyTcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer, SOCKET_BUFFER_SIZE);
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token))
                {
                    cts.CancelAfter(CONNECT_TIMEOUT_MS);
                    ProxyConfig.Log($"[端口 {port}] 🔌 正在连接HTTP代理：{proxyItem.ProxyIp}:{proxyItem.ProxyPort}（超时{CONNECT_TIMEOUT_MS}ms）", LogLevel.Debug);
                    await proxyTcp.ConnectAsync(proxyItem.ProxyIp, proxyItem.ProxyPort, cts.Token);
                    ProxyConfig.Log($"[端口 {port}] ✅ HTTP代理连接成功", LogLevel.Debug);
                }
                proxyTcp.ReceiveTimeout = READ_WRITE_TIMEOUT_MS;
                proxyTcp.SendTimeout = READ_WRITE_TIMEOUT_MS;
                proxyStream = proxyTcp.GetStream();

                // 3. 处理代理认证（Basic认证）
                int finalReadCount = readCount;
                if (proxyItem.NeedAuth)
                {
                    ProxyConfig.Log($"[端口 {port}] 🔐 启用HTTP代理Basic认证", LogLevel.Debug);
                    var authStr = $"{proxyItem.ProxyUsername}:{proxyItem.ProxyPassword}";
                    var authBase64 = Convert.ToBase64String(Encoding.ASCII.GetBytes(authStr));

                    // 插入认证头+User-Agent
                    var requestStr = Encoding.ASCII.GetString(buffer, 0, readCount);
                    var requestLines = requestStr.Split('\n').ToList();
                    requestLines.RemoveAll(line => line.TrimStart().StartsWith("Proxy-Authorization:") || line.TrimStart().StartsWith("User-Agent:"));
                    requestLines.Insert(1, $"Proxy-Authorization: Basic {authBase64}");
                    requestLines.Insert(2, $"User-Agent: {USER_AGENT}");
                    requestStr = string.Join("\n", requestLines);
                    buffer = Encoding.ASCII.GetBytes(requestStr);
                    finalReadCount = buffer.Length;
                }
                else
                {
                    // 无认证时添加User-Agent
                    var requestStr = Encoding.ASCII.GetString(buffer, 0, readCount);
                    var requestLines = requestStr.Split('\n').ToList();
                    requestLines.RemoveAll(line => line.TrimStart().StartsWith("User-Agent:"));
                    requestLines.Insert(1, $"User-Agent: {USER_AGENT}");
                    requestStr = string.Join("\n", requestLines);
                    buffer = Encoding.ASCII.GetBytes(requestStr);
                    finalReadCount = buffer.Length;
                }

                // 4. 处理CONNECT隧道（HTTPS网站）
                if (httpMethod == "CONNECT")
                {
                    string standardConnectReq = $"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\nUser-Agent: {USER_AGENT}\r\nProxy-Connection: Keep-Alive\r\n\r\n";
                    byte[] connectReqBytes = Encoding.ASCII.GetBytes(standardConnectReq);
                    await proxyStream.WriteAsync(connectReqBytes, 0, connectReqBytes.Length, token);
                    ProxyConfig.Log($"[端口 {port}] 📤 发送标准化CONNECT请求：{standardConnectReq.Trim()}", LogLevel.Debug);

                    // 读取代理响应（兼容修改）
                    var proxyRespBuffer = ArrayPool<byte>.Shared.Rent(BUFFER_SIZE);
                    try
                    {
                        var proxyRespRead = await proxyStream.ReadAsync(proxyRespBuffer, 0, proxyRespBuffer.Length, token);
                        var proxyRespStr = Encoding.ASCII.GetString(proxyRespBuffer, 0, proxyRespRead);

                        if (proxyRespStr.IndexOf("200 Connection established", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            ProxyConfig.Log($"[端口 {port}] ✅ HTTP代理CONNECT隧道建立成功：{target}", LogLevel.Info);
                            var clientResp = Encoding.ASCII.GetBytes("HTTP/1.1 200 Connection Established\r\n\r\n");
                            await clientStream.WriteAsync(clientResp, 0, clientResp.Length, token);

                            // 双向流转发
                            var forwardTunnelTask1 = ForwardStreamAsync(clientStream, proxyStream, $"[端口 {port}] 客户端→HTTP代理（隧道）", token);
                            var forwardTunnelTask2 = ForwardStreamAsync(proxyStream, clientStream, $"[端口 {port}] HTTP代理（隧道）→客户端", token);
                            await Task.WhenAll(forwardTunnelTask1, forwardTunnelTask2);
                        }
                        else if (proxyRespStr.Contains("407 Proxy Authentication Required"))
                        {
                            ProxyConfig.Log($"[端口 {port}] ❌ HTTP代理认证失败：{proxyRespStr.Trim()}", LogLevel.Error);
                            await clientStream.WriteAsync(proxyRespBuffer, 0, proxyRespRead, token);
                            return;
                        }
                        else
                        {
                            ProxyConfig.Log($"[端口 {port}] ❌ HTTP代理CONNECT失败：{proxyRespStr.Trim()}", LogLevel.Error);
                            await clientStream.WriteAsync(proxyRespBuffer, 0, proxyRespRead, token);
                            return;
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(proxyRespBuffer);
                    }
                }
                else
                {
                    // 普通HTTP请求转发
                    await proxyStream.WriteAsync(buffer, 0, finalReadCount, token);
                    ProxyConfig.Log($"[端口 {port}] 📤 普通HTTP请求已转发：{target}", LogLevel.Debug);

                    // 双向流转发
                    var forwardTask1 = ForwardStreamAsync(clientStream, proxyStream, $"[端口 {port}] 客户端→HTTP代理", token);
                    var forwardTask2 = ForwardStreamAsync(proxyStream, clientStream, $"[端口 {port}] HTTP代理→客户端", token);
                    await Task.WhenAll(forwardTask1, forwardTask2);
                }
            }
            catch (OperationCanceledException)
            {
                ProxyConfig.Log($"[端口 {port}] ⚠️ HTTP代理连接超时：{proxyItem.ProxyIp}:{proxyItem.ProxyPort}（{CONNECT_TIMEOUT_MS}ms）", LogLevel.Warning);
                var errorResp = Encoding.ASCII.GetBytes("HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n");
                await clientStream.WriteAsync(errorResp, 0, errorResp.Length, token);
            }
            catch (IOException ex)
            {
                if (!ex.Message.Contains("已中止 I/O 操作") && !ex.Message.Contains("无法读取传输连接") && !ex.Message.Contains("远程主机强迫关闭了一个现有的连接"))
                {
                    ProxyConfig.Log($"[端口 {port}] ❌ HTTP转发异常：{ex.Message}", LogLevel.Error);
                    var errorResp = Encoding.ASCII.GetBytes("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n");
                    await clientStream.WriteAsync(errorResp, 0, errorResp.Length, token);
                }
            }
            finally
            {
                proxyStream?.Dispose();
                proxyTcp?.Dispose();
            }
        }

        // 流转发方法（核心优化+兼容C# 10.0）
        private async Task ForwardStreamAsync(Stream srcStream, Stream dstStream, string logPrefix, CancellationToken token)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(BUFFER_SIZE);
            try
            {
                int readCount;
                // 兼容修改：使用传统ReadAsync（数组+偏移量）
                while ((readCount = await srcStream.ReadAsync(buffer, 0, buffer.Length, token)) > 0)
                {
                    // 直接写入，移除不必要的Flush
                    await dstStream.WriteAsync(buffer, 0, readCount, token);

                    // 流量统计（原子操作，无锁）
                    if (logPrefix.Contains("→代理"))
                    {
                        Interlocked.Add(ref _totalSentBytes, readCount);
                    }
                    else if (logPrefix.Contains("代理→"))
                    {
                        Interlocked.Add(ref _totalReceivedBytes, readCount);
                    }
                }
                ProxyConfig.Log($"{logPrefix} 流正常结束", LogLevel.Debug);
            }
            catch (OperationCanceledException)
            {
                ProxyConfig.Log($"{logPrefix} 流转发被取消", LogLevel.Debug);
            }
            catch (TimeoutException)
            {
                ProxyConfig.Log($"{logPrefix} 读写超时（{READ_WRITE_TIMEOUT_MS}ms）", LogLevel.Warning);
            }
            catch (IOException ex)
            {
                if (ex.Message.Contains("已中止 I/O 操作") ||
                    ex.Message.Contains("无法读取传输连接") ||
                    ex.Message.Contains("远程主机强迫关闭了一个现有的连接"))
                {
                    ProxyConfig.Log($"{logPrefix} 连接正常断开", LogLevel.Debug);
                }
                else
                {
                    ProxyConfig.Log($"{logPrefix} 转发异常：{ex.Message}", LogLevel.Error);
                }
            }
            catch (Exception ex)
            {
                ProxyConfig.Log($"{logPrefix} 未捕获异常：{ex.Message}", LogLevel.Error);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        // 端口可用性检测（修复TcpListener不支持IDisposable）
        public bool IsPortAvailable(int port, ProxyConfig config)
        {
            if (port < config.MinRandomPort || port > config.MaxRandomPort)
                return false;
            if (config.PortProxyMap.ContainsKey(port))
                return false;
            try
            {
                var listener = new TcpListener(IPAddress.Any, port);
                try
                {
                    listener.Start();
                    return true;
                }
                finally
                {
                    listener.Stop();
                }
            }
            catch (SocketException)
            {
                return false;
            }
        }

        // 停止服务
        public void Stop()
        {
            _isRunning = false;
            foreach (var listener in _listeners)
            {
                try
                {
                    listener.Stop();
                    var port = ((IPEndPoint)listener.LocalEndpoint).Port;
                    ProxyConfig.Log($"❌ 端口 {port} 监听已停止（代理类型：{_config.PortProxyMap[port].ProxyType}）", LogLevel.Info);
                }
                catch (Exception ex)
                {
                    ProxyConfig.Log($"⚠️  停止端口监听异常：{ex.Message}", LogLevel.Warning);
                }
            }
            _listeners.Clear();
            ProxyConfig.Log("\n🛑 服务端已完全停止", LogLevel.Info);
        }
    }

    // UI窗口（完整实现）
    public class ServerMainForm : Form
    {
        private ProxyConfig _config;
        private ProxyServerCore _proxyServer;
        private Task _serverTask;

        // 控件声明
        private GroupBox gbPortRange;
        private Label lblMinPort;
        private NumericUpDown nudMinPort;
        private Label lblMaxPort;
        private NumericUpDown nudMaxPort;
        private Button btnGenPorts;
        private Label lblPortCount;
        private NumericUpDown nudPortCount;

        private GroupBox gbPortProxyMap;
        private DataGridView dgvPortProxy;
        private Button btnAddProxy;
        private Button btnDeleteProxy;
        private Button btnSaveMap;

        private Label lblLocalIp;
        private ComboBox cboLocalIp;
        private Button btnStart;
        private Button btnStop;

        private RichTextBox rtLog;
        private Label lblReceived;
        private Label lblSent;
        private Label lblTotal;

        // 日志级别控件
        private Label lblLogLevel;
        private ComboBox cboLogLevel;

        // 列名常量
        private const string COL_PORT = "Port";
        private const string COL_PROXY_TYPE = "ProxyType";
        private const string COL_PROXY_IP = "ProxyIp";
        private const string COL_PROXY_PORT = "ProxyPort";
        private const string COL_NEED_AUTH = "NeedAuth";
        private const string COL_USERNAME = "ProxyUsername";
        private const string COL_PASSWORD = "ProxyPassword";

        public ServerMainForm()
        {
            InitializeForm();
            CreateAllControls();
            _config = ProxyConfig.Load();
            InitControlValues();
            LoadLocalIps();
        }

        private void InitializeForm()
        {
            this.Text = "局域网多端口代理服务端（兼容版：HTTP/SOCKS5+高速转发）";
            this.Size = new Size(780, 780);
            this.StartPosition = FormStartPosition.CenterScreen;
            this.FormClosing += ServerMainForm_FormClosing;
            this.MaximizeBox = false;
            this.FormBorderStyle = FormBorderStyle.FixedSingle;
        }

        // 创建所有UI控件
        private void CreateAllControls()
        {
            // 1. 端口范围分组
            gbPortRange = new GroupBox
            {
                Text = "端口范围与生成设置",
                Location = new Point(20, 20),
                Size = new Size(720, 100)
            };
            this.Controls.Add(gbPortRange);

            var portRangeTable = new TableLayoutPanel
            {
                Dock = DockStyle.Fill,
                ColumnCount = 6,
                ColumnStyles = {
                    new ColumnStyle(SizeType.Absolute, 80),
                    new ColumnStyle(SizeType.Absolute, 100),
                    new ColumnStyle(SizeType.Absolute, 80),
                    new ColumnStyle(SizeType.Absolute, 100),
                    new ColumnStyle(SizeType.Absolute, 120),
                    new ColumnStyle(SizeType.Absolute, 220)
                }
            };
            gbPortRange.Controls.Add(portRangeTable);

            lblMinPort = new Label { Text = "最小端口：", TextAlign = ContentAlignment.MiddleRight };
            portRangeTable.Controls.Add(lblMinPort, 0, 0);
            nudMinPort = new NumericUpDown { Minimum = 1024, Maximum = 65535, Dock = DockStyle.Fill };
            portRangeTable.Controls.Add(nudMinPort, 1, 0);

            lblMaxPort = new Label { Text = "最大端口：", TextAlign = ContentAlignment.MiddleRight };
            portRangeTable.Controls.Add(lblMaxPort, 2, 0);
            nudMaxPort = new NumericUpDown { Minimum = 1024, Maximum = 65535, Dock = DockStyle.Fill };
            portRangeTable.Controls.Add(nudMaxPort, 3, 0);

            lblPortCount = new Label { Text = "生成数量：", TextAlign = ContentAlignment.MiddleRight };
            portRangeTable.Controls.Add(lblPortCount, 4, 0);
            nudPortCount = new NumericUpDown { Minimum = 1, Maximum = 20, Value = 5, Dock = DockStyle.Fill };
            portRangeTable.Controls.Add(nudPortCount, 5, 0);

            btnGenPorts = new Button { Text = "生成端口-代理映射（默认HTTP）", Dock = DockStyle.Fill };
            portRangeTable.Controls.Add(btnGenPorts, 0, 1);
            portRangeTable.SetColumnSpan(btnGenPorts, 6);
            btnGenPorts.Click += BtnGenPorts_Click;

            // 2. 映射表格分组
            gbPortProxyMap = new GroupBox
            {
                Text = "端口-代理映射列表（支持HTTP/SOCKS5）",
                Location = new Point(20, 140),
                Size = new Size(720, 320)
            };
            this.Controls.Add(gbPortProxyMap);

            dgvPortProxy = new DataGridView
            {
                Location = new Point(10, 30),
                Size = new Size(700, 240),
                AllowUserToAddRows = false,
                EditMode = DataGridViewEditMode.EditOnEnter,
                SelectionMode = DataGridViewSelectionMode.FullRowSelect,
                ColumnHeadersDefaultCellStyle = new DataGridViewCellStyle { Alignment = DataGridViewContentAlignment.MiddleCenter },
                RowHeadersVisible = false,
                ReadOnly = false,
                AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.None
            };

            dgvPortProxy.EditingControlShowing += DgvPortProxy_EditingControlShowing;
            dgvPortProxy.CellFormatting += DgvPortProxy_CellFormatting;
            dgvPortProxy.CellValueChanged += DgvPortProxy_CellValueChanged;
            dgvPortProxy.CellEndEdit += DgvPortProxy_CellEndEdit;

            // 表格列
            var portColumn = new DataGridViewTextBoxColumn
            {
                Name = COL_PORT,
                HeaderText = "本地监听端口",
                DataPropertyName = COL_PORT,
                ReadOnly = true,
                Width = 100
            };

            var proxyTypeColumn = new DataGridViewComboBoxColumn
            {
                Name = COL_PROXY_TYPE,
                HeaderText = "代理类型",
                DataPropertyName = COL_PROXY_TYPE,
                Width = 100,
                DisplayStyle = DataGridViewComboBoxDisplayStyle.DropDownButton,
                ReadOnly = false,
                DefaultCellStyle = new DataGridViewCellStyle { Alignment = DataGridViewContentAlignment.MiddleCenter }
            };
            proxyTypeColumn.Items.AddRange("HTTP", "SOCKS5");
            proxyTypeColumn.DefaultCellStyle.NullValue = "HTTP";

            var proxyIpColumn = new DataGridViewTextBoxColumn
            {
                Name = COL_PROXY_IP,
                HeaderText = "代理服务器IP",
                DataPropertyName = COL_PROXY_IP,
                Width = 120,
                ReadOnly = false
            };

            var proxyPortColumn = new DataGridViewTextBoxColumn
            {
                Name = COL_PROXY_PORT,
                HeaderText = "代理服务器端口",
                DataPropertyName = COL_PROXY_PORT,
                Width = 120,
                ReadOnly = false,
                DefaultCellStyle = new DataGridViewCellStyle { Alignment = DataGridViewContentAlignment.MiddleCenter }
            };

            var needAuthColumn = new DataGridViewCheckBoxColumn
            {
                Name = COL_NEED_AUTH,
                HeaderText = "需要认证",
                DataPropertyName = COL_NEED_AUTH,
                Width = 80,
                TrueValue = true,
                FalseValue = false,
                ReadOnly = false,
                DefaultCellStyle = new DataGridViewCellStyle { Alignment = DataGridViewContentAlignment.MiddleCenter }
            };

            var usernameColumn = new DataGridViewTextBoxColumn
            {
                Name = COL_USERNAME,
                HeaderText = "认证用户名",
                DataPropertyName = COL_USERNAME,
                Width = 100,
                ReadOnly = false
            };

            var passwordColumn = new DataGridViewTextBoxColumn
            {
                Name = COL_PASSWORD,
                HeaderText = "认证密码",
                DataPropertyName = COL_PASSWORD,
                Width = 100,
                ReadOnly = false
            };

            dgvPortProxy.Columns.AddRange(new DataGridViewColumn[]
            {
                portColumn, proxyTypeColumn, proxyIpColumn, proxyPortColumn,
                needAuthColumn, usernameColumn, passwordColumn
            });

            gbPortProxyMap.Controls.Add(dgvPortProxy);

            btnAddProxy = new Button { Text = "添加单个映射", Location = new Point(10, 275), Size = new Size(120, 30) };
            btnAddProxy.Click += BtnAddProxy_Click;
            gbPortProxyMap.Controls.Add(btnAddProxy);

            btnDeleteProxy = new Button { Text = "删除选中映射", Location = new Point(140, 275), Size = new Size(120, 30) };
            btnDeleteProxy.Click += BtnDeleteProxy_Click;
            gbPortProxyMap.Controls.Add(btnDeleteProxy);

            btnSaveMap = new Button { Text = "保存映射配置", Location = new Point(580, 275), Size = new Size(120, 30) };
            btnSaveMap.Click += BtnSaveMap_Click;
            gbPortProxyMap.Controls.Add(btnSaveMap);

            // 3. 流量统计分组
            var trafficGroup = new GroupBox
            {
                Text = "实时流量统计（每秒更新）",
                Location = new Point(20, 480),
                Size = new Size(720, 40),
                BackColor = Color.LightBlue
            };
            this.Controls.Add(trafficGroup);

            var trafficTable = new TableLayoutPanel
            {
                Dock = DockStyle.Fill,
                ColumnCount = 3,
                ColumnStyles = {
                    new ColumnStyle(SizeType.Percent, 33.33f),
                    new ColumnStyle(SizeType.Percent, 33.33f),
                    new ColumnStyle(SizeType.Percent, 33.34f)
                }
            };
            trafficGroup.Controls.Add(trafficTable);

            lblReceived = new Label
            {
                Text = "接收流量：0 B",
                TextAlign = ContentAlignment.MiddleCenter,
                Font = new Font("Consolas", 10),
                ForeColor = Color.Green
            };
            trafficTable.Controls.Add(lblReceived, 0, 0);

            lblSent = new Label
            {
                Text = "发送流量：0 B",
                TextAlign = ContentAlignment.MiddleCenter,
                Font = new Font("Consolas", 10),
                ForeColor = Color.Orange
            };
            trafficTable.Controls.Add(lblSent, 1, 0);

            lblTotal = new Label
            {
                Text = "总流量：0 B",
                TextAlign = ContentAlignment.MiddleCenter,
                Font = new Font("Consolas", 10, FontStyle.Bold),
                ForeColor = Color.Red
            };
            trafficTable.Controls.Add(lblTotal, 2, 0);

            // 4. 监听IP与启动控制
            lblLocalIp = new Label { Text = "监听IP：", Location = new Point(20, 530), Size = new Size(80, 25) };
            this.Controls.Add(lblLocalIp);

            cboLocalIp = new ComboBox { Location = new Point(110, 530), Size = new Size(180, 25), DropDownStyle = ComboBoxStyle.DropDownList };
            this.Controls.Add(cboLocalIp);

            btnStart = new Button { Text = "启动服务", Location = new Point(310, 530), Size = new Size(120, 30), BackColor = Color.LightGreen };
            btnStart.Click += BtnStart_Click;
            this.Controls.Add(btnStart);

            btnStop = new Button { Text = "停止服务", Location = new Point(450, 530), Size = new Size(120, 30), BackColor = Color.LightCoral, Enabled = false };
            btnStop.Click += BtnStop_Click;
            this.Controls.Add(btnStop);

            // 5. 日志级别配置
            lblLogLevel = new Label { Text = "日志级别：", Location = new Point(20, 560), Size = new Size(80, 25) };
            this.Controls.Add(lblLogLevel);

            cboLogLevel = new ComboBox { Location = new Point(110, 560), Size = new Size(180, 25), DropDownStyle = ComboBoxStyle.DropDownList };
            cboLogLevel.Items.AddRange(Enum.GetNames(typeof(LogLevel)));
            this.Controls.Add(cboLogLevel);

            // 6. 日志框
            rtLog = new RichTextBox
            {
                Location = new Point(20, 610),
                Size = new Size(720, 130),
                ReadOnly = true,
                ScrollBars = RichTextBoxScrollBars.Vertical,
                Font = new Font("Consolas", 9),
                BackColor = Color.Black,
                ForeColor = Color.LightGray
            };
            this.Controls.Add(rtLog);
            Console.SetOut(new ControlWriter(rtLog));

            // 7. 流量更新Timer
            var trafficTimer = new System.Windows.Forms.Timer { Interval = 1000 };
            btnStart.Click += (s, e) =>
            {
                trafficTimer.Start();
                Log("[流量统计] 开始监控流量");
            };

            btnStop.Click += (s, e) =>
            {
                trafficTimer.Stop();
                lblReceived.Text = "接收流量：0 B";
                lblSent.Text = "发送流量：0 B";
                lblTotal.Text = "总流量：0 B";
                _proxyServer?.ResetTraffic();
            };

            trafficTimer.Tick += (s, e) =>
            {
                if (_proxyServer == null) return;

                this.Invoke(new Action(() =>
                {
                    long received = _proxyServer.TotalReceivedBytes;
                    long sent = _proxyServer.TotalSentBytes;
                    long total = received + sent;

                    lblReceived.Text = $"接收流量：{FormatBytes(received)}";
                    lblSent.Text = $"发送流量：{FormatBytes(sent)}";
                    lblTotal.Text = $"总流量：{FormatBytes(total)}";
                }));
            };
        }

        // 流量单位格式化
        private string FormatBytes(long bytes)
        {
            if (bytes < 1024) return $"{bytes} B";
            else if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F2} KB";
            else if (bytes < 1024 * 1024 * 1024) return $"{bytes / (1024.0 * 1024):F2} MB";
            else return $"{bytes / (1024.0 * 1024 * 1024):F2} GB";
        }

        // 单元格编辑结束
        private void DgvPortProxy_CellEndEdit(object sender, DataGridViewCellEventArgs e)
        {
            if (e.RowIndex < 0 || e.ColumnIndex < 0) return;

            var row = dgvPortProxy.Rows[e.RowIndex];
            var cell = row.Cells[e.ColumnIndex];

            // 代理类型变更时自动设置默认端口
            if (cell.ColumnIndex == dgvPortProxy.Columns[COL_PROXY_TYPE].Index)
            {
                string proxyType = cell.Value?.ToString() ?? "HTTP";
                var portCell = row.Cells[COL_PROXY_PORT];
                if (portCell.Value == null || string.IsNullOrEmpty(portCell.Value.ToString()) ||
                    portCell.Value.ToString() == "8080" || portCell.Value.ToString() == "1080")
                {
                    portCell.Value = proxyType switch
                    {
                        "HTTP" => 8080,
                        "SOCKS5" => 1080,
                        _ => 8080
                    };
                    Log($"[自动配置] 代理类型切换为 {proxyType}，默认端口设置为 {portCell.Value}");
                }
            }
            // 端口校验
            else if (cell.ColumnIndex == dgvPortProxy.Columns[COL_PROXY_PORT].Index)
            {
                if (cell.Value != null && !int.TryParse(cell.Value.ToString(), out int port))
                {
                    string proxyType = row.Cells[COL_PROXY_TYPE].Value?.ToString() ?? "HTTP";
                    cell.Value = proxyType switch
                    {
                        "HTTP" => 8080,
                        "SOCKS5" => 1080,
                        _ => 8080
                    };
                    Log($"⚠️  端口格式错误，已重置为 {proxyType} 默认值 {cell.Value}");
                }
                else if (cell.Value == null || string.IsNullOrEmpty(cell.Value.ToString()))
                {
                    string proxyType = row.Cells[COL_PROXY_TYPE].Value?.ToString() ?? "HTTP";
                    cell.Value = proxyType switch
                    {
                        "HTTP" => 8080,
                        "SOCKS5" => 1080,
                        _ => 8080
                    };
                }
            }

            dgvPortProxy.InvalidateCell(cell);
        }

        // 单元格编辑样式
        private void DgvPortProxy_EditingControlShowing(object sender, DataGridViewEditingControlShowingEventArgs e)
        {
            if (e.Control is TextBox txt)
            {
                txt.ShortcutsEnabled = true;
                txt.AcceptsTab = false;

                if (dgvPortProxy.CurrentCell.ColumnIndex == dgvPortProxy.Columns[COL_PASSWORD].Index)
                {
                    txt.PasswordChar = '*';
                    txt.Text = dgvPortProxy.CurrentCell.Value?.ToString() ?? "";
                }
                else if (dgvPortProxy.CurrentCell.ColumnIndex == dgvPortProxy.Columns[COL_PROXY_PORT].Index)
                {
                    txt.KeyPress -= Txt_KeyPress;
                    txt.KeyPress += Txt_KeyPress;
                    txt.Text = dgvPortProxy.CurrentCell.Value?.ToString() ?? "";
                }
                else if (dgvPortProxy.CurrentCell.ColumnIndex == dgvPortProxy.Columns[COL_USERNAME].Index ||
                         dgvPortProxy.CurrentCell.ColumnIndex == dgvPortProxy.Columns[COL_PASSWORD].Index)
                {
                    var needAuth = (bool)(dgvPortProxy.CurrentRow.Cells[COL_NEED_AUTH].Value ?? false);
                    txt.ReadOnly = !needAuth;
                    txt.ForeColor = needAuth ? Color.Black : Color.Gray;
                    txt.Text = dgvPortProxy.CurrentCell.Value?.ToString() ?? "";
                }
                else
                {
                    txt.PasswordChar = '\0';
                    txt.Text = dgvPortProxy.CurrentCell.Value?.ToString() ?? "";
                }
            }
            else if (dgvPortProxy.CurrentCell.ColumnIndex == dgvPortProxy.Columns[COL_PROXY_TYPE].Index)
            {
                if (e.Control is ComboBox cbo)
                {
                    cbo.DropDownStyle = ComboBoxStyle.DropDownList;
                    var cellValue = dgvPortProxy.CurrentCell.Value?.ToString() ?? "HTTP";
                    if (cbo.Items.Contains(cellValue)) cbo.SelectedItem = cellValue;
                    else cbo.SelectedIndex = 0;
                }
            }
        }

        // 端口输入限制
        private void Txt_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (!char.IsDigit(e.KeyChar) && e.KeyChar != (char)Keys.Back && e.KeyChar != (char)Keys.Delete)
            {
                e.Handled = true;
            }
        }

        // 单元格格式化
        private void DgvPortProxy_CellFormatting(object sender, DataGridViewCellFormattingEventArgs e)
        {
            if (e.ColumnIndex == dgvPortProxy.Columns[COL_PASSWORD].Index && e.RowIndex >= 0)
            {
                var cellValue = e.Value?.ToString() ?? "";
                if (!string.IsNullOrEmpty(cellValue))
                {
                    e.Value = new string('*', cellValue.Length);
                    e.FormattingApplied = true;
                }
            }
            else if (e.ColumnIndex == dgvPortProxy.Columns[COL_PROXY_PORT].Index && e.RowIndex >= 0)
            {
                if (e.Value != null && int.TryParse(e.Value.ToString(), out int port))
                {
                    e.Value = port.ToString();
                    e.FormattingApplied = true;
                }
                else
                {
                    string proxyType = dgvPortProxy.Rows[e.RowIndex].Cells[COL_PROXY_TYPE].Value?.ToString() ?? "HTTP";
                    e.Value = proxyType switch
                    {
                        "HTTP" => "8080",
                        "SOCKS5" => "1080",
                        _ => "8080"
                    };
                    e.FormattingApplied = true;
                }
            }
            else if (e.ColumnIndex == dgvPortProxy.Columns[COL_PROXY_TYPE].Index && e.RowIndex >= 0)
            {
                e.Value = e.Value ?? "HTTP";
                e.FormattingApplied = true;
            }
        }

        // 初始化控件值
        private void InitControlValues()
        {
            nudMinPort.Value = _config.MinRandomPort;
            nudMaxPort.Value = _config.MaxRandomPort;

            // 联动调整端口范围
            nudMinPort.ValueChanged += (s, e) =>
            {
                if (nudMinPort.Value > nudMaxPort.Value) nudMaxPort.Value = nudMinPort.Value;
            };
            nudMaxPort.ValueChanged += (s, e) =>
            {
                if (nudMaxPort.Value < nudMinPort.Value) nudMinPort.Value = nudMaxPort.Value;
            };

            // 初始化日志级别
            cboLogLevel.SelectedItem = _config.LogLevel.ToString();
            cboLogLevel.SelectedIndexChanged += (s, e) =>
            {
                if (Enum.TryParse<LogLevel>(cboLogLevel.SelectedItem.ToString(), out var level))
                {
                    _config.LogLevel = level;
                    _config.Save();
                    Log($"✅ 日志级别已设置为：{level}");
                }
            };

            LoadGridData();
        }

        // 加载本地IP
        private void LoadLocalIps()
        {
            cboLocalIp.Items.Clear();
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    cboLocalIp.Items.Add(ip.ToString());
                    if (ip.ToString() == _config.LocalIp || _config.LocalIp == "0.0.0.0")
                    {
                        cboLocalIp.SelectedItem = ip.ToString();
                    }
                }
            }
            if (cboLocalIp.SelectedItem == null && cboLocalIp.Items.Count > 0)
            {
                cboLocalIp.SelectedIndex = 0;
            }
        }

        // 加载表格数据
        private void LoadGridData()
        {
            dgvPortProxy.Rows.Clear();
            foreach (var (port, proxy) in _config.PortProxyMap)
            {
                var rowIndex = dgvPortProxy.Rows.Add();
                var row = dgvPortProxy.Rows[rowIndex];
                row.Cells[COL_PORT].Value = port;
                row.Cells[COL_PROXY_TYPE].Value = string.IsNullOrEmpty(proxy.ProxyType) ? "HTTP" : proxy.ProxyType;
                row.Cells[COL_PROXY_IP].Value = proxy.ProxyIp;
                row.Cells[COL_PROXY_PORT].Value = proxy.ProxyPort <= 0 ?
                    (row.Cells[COL_PROXY_TYPE].Value.ToString() switch
                    {
                        "HTTP" => 8080,
                        "SOCKS5" => 1080,
                        _ => 8080
                    }) : proxy.ProxyPort;
                row.Cells[COL_NEED_AUTH].Value = proxy.NeedAuth;
                row.Cells[COL_USERNAME].Value = proxy.ProxyUsername;
                row.Cells[COL_PASSWORD].Value = proxy.ProxyPassword;

                row.Cells[COL_USERNAME].ReadOnly = !proxy.NeedAuth;
                row.Cells[COL_PASSWORD].ReadOnly = !proxy.NeedAuth;
                row.Cells[COL_USERNAME].Style.ForeColor = proxy.NeedAuth ? Color.Black : Color.Gray;
                row.Cells[COL_PASSWORD].Style.ForeColor = proxy.NeedAuth ? Color.Black : Color.Gray;
            }
        }

        // 生成端口-代理映射
        private void BtnGenPorts_Click(object sender, EventArgs e)
        {
            try
            {
                int targetCount = (int)nudPortCount.Value;
                var proxyServer = new ProxyServerCore(_config);
                var newPorts = new List<int>();
                var random = new Random();
                int attempts = 0;

                while (newPorts.Count < targetCount && attempts < 1000)
                {
                    int port = random.Next((int)nudMinPort.Value, (int)nudMaxPort.Value + 1);
                    if (proxyServer.IsPortAvailable(port, _config))
                    {
                        newPorts.Add(port);
                        _config.PortProxyMap[port] = new ProxyItem
                        {
                            ProxyType = "HTTP",
                            ProxyIp = "",
                            ProxyPort = 8080
                        };
                    }
                    attempts++;
                }

                if (newPorts.Count < targetCount)
                {
                    Log($"❌ 生成失败：仅找到 {newPorts.Count} 个可用端口（目标 {targetCount} 个）");
                    return;
                }

                LoadGridData();
                _config.Save();
                Log($"✅ 生成成功！新增HTTP代理端口：{string.Join(", ", newPorts)}（默认端口8080）");
            }
            catch (Exception ex)
            {
                Log($"❌ 生成映射失败：{ex.Message}");
            }
        }

        // 添加单个映射
        private void BtnAddProxy_Click(object sender, EventArgs e)
        {
            try
            {
                var proxyServer = new ProxyServerCore(_config);
                int newPort = -1;
                for (int port = (int)nudMinPort.Value; port <= (int)nudMaxPort.Value; port++)
                {
                    if (proxyServer.IsPortAvailable(port, _config))
                    {
                        newPort = port;
                        break;
                    }
                }

                if (newPort == -1)
                {
                    Log("❌ 未找到可用端口");
                    return;
                }

                _config.PortProxyMap[newPort] = new ProxyItem
                {
                    ProxyType = "HTTP",
                    ProxyIp = "",
                    ProxyPort = 8080
                };

                LoadGridData();
                _config.Save();
                Log($"✅ 新增映射：本地端口 {newPort}（默认HTTP代理）");
            }
            catch (Exception ex)
            {
                Log($"❌ 新增映射失败：{ex.Message}");
            }
        }

        // 删除选中映射
        private void BtnDeleteProxy_Click(object sender, EventArgs e)
        {
            if (dgvPortProxy.SelectedRows.Count == 0)
            {
                Log("❌ 请选中要删除的映射行");
                return;
            }

            var selectedRow = dgvPortProxy.SelectedRows[0];
            int port = (int)selectedRow.Cells[COL_PORT].Value;
            string proxyType = selectedRow.Cells[COL_PROXY_TYPE].Value?.ToString() ?? "HTTP";

            _config.PortProxyMap.Remove(port);
            LoadGridData();
            _config.Save();
            Log($"✅ 删除映射：本地端口 {port}（{proxyType}代理）");
        }

        // 保存映射配置
        private void BtnSaveMap_Click(object sender, EventArgs e)
        {
            try
            {
                if (dgvPortProxy.IsCurrentCellInEditMode) dgvPortProxy.EndEdit();

                foreach (DataGridViewRow row in dgvPortProxy.Rows)
                {
                    int localPort = (int)row.Cells[COL_PORT].Value;
                    string proxyType = row.Cells[COL_PROXY_TYPE].Value?.ToString() ?? "HTTP";
                    string proxyIp = row.Cells[COL_PROXY_IP].Value?.ToString()?.Trim() ?? "";
                    string proxyPortStr = row.Cells[COL_PROXY_PORT].Value?.ToString() ?? "";

                    if (string.IsNullOrEmpty(proxyIp))
                    {
                        Log($"❌ 本地端口 {localPort}（{proxyType}）：代理IP不能为空");
                        return;
                    }

                    if (!int.TryParse(proxyPortStr, out int proxyPort) || proxyPort < 1 || proxyPort > 65535)
                    {
                        Log($"❌ 本地端口 {localPort}（{proxyType}）：代理端口格式错误");
                        return;
                    }

                    bool needAuth = (bool)(row.Cells[COL_NEED_AUTH].Value ?? false);
                    string username = row.Cells[COL_USERNAME].Value?.ToString()?.Trim() ?? "";
                    if (needAuth && string.IsNullOrEmpty(username))
                    {
                        Log($"❌ 本地端口 {localPort}（{proxyType}）：启用认证后用户名不能为空");
                        return;
                    }

                    if (_config.PortProxyMap.ContainsKey(localPort))
                    {
                        var proxyItem = _config.PortProxyMap[localPort];
                        proxyItem.ProxyType = proxyType;
                        proxyItem.ProxyIp = proxyIp;
                        proxyItem.ProxyPort = proxyPort;
                        proxyItem.NeedAuth = needAuth;
                        proxyItem.ProxyUsername = username;
                        proxyItem.ProxyPassword = row.Cells[COL_PASSWORD].Value?.ToString() ?? "";
                    }
                }

                _config.Save();
                Log("✅ 配置保存成功！");
            }
            catch (Exception ex)
            {
                Log($"❌ 保存配置失败：{ex.Message}");
            }
        }

        // 启动服务
        private async void BtnStart_Click(object sender, EventArgs e)
        {
            if (_config.PortProxyMap.Count == 0)
            {
                Log("❌ 请先添加端口-代理映射");
                return;
            }

            // 先保存配置
            BtnSaveMap_Click(sender, e);

            _config.LocalIp = cboLocalIp.SelectedItem?.ToString() ?? "0.0.0.0";
            _proxyServer = new ProxyServerCore(_config);
            _serverTask = _proxyServer.StartAsync();

            // 禁用配置编辑控件
            btnStart.Enabled = false;
            btnStop.Enabled = true;
            btnGenPorts.Enabled = false;
            btnAddProxy.Enabled = false;
            btnDeleteProxy.Enabled = false;
            btnSaveMap.Enabled = false;
            dgvPortProxy.ReadOnly = true;
            nudMinPort.Enabled = false;
            nudMaxPort.Enabled = false;
            nudPortCount.Enabled = false;
            cboLocalIp.Enabled = false;
            cboLogLevel.Enabled = false;
        }

        // 停止服务
        private void BtnStop_Click(object sender, EventArgs e)
        {
            _proxyServer?.Stop();
            // 启用配置编辑控件
            btnStart.Enabled = true;
            btnStop.Enabled = false;
            btnGenPorts.Enabled = true;
            btnAddProxy.Enabled = true;
            btnDeleteProxy.Enabled = true;
            btnSaveMap.Enabled = true;
            dgvPortProxy.ReadOnly = false;
            nudMinPort.Enabled = true;
            nudMaxPort.Enabled = true;
            nudPortCount.Enabled = true;
            cboLocalIp.Enabled = true;
            cboLogLevel.Enabled = true;
            Log("🛑 服务已停止");
        }

        // 认证开关变化事件
        private void DgvPortProxy_CellValueChanged(object sender, DataGridViewCellEventArgs e)
        {
            if (e.ColumnIndex == dgvPortProxy.Columns[COL_NEED_AUTH].Index && e.RowIndex >= 0)
            {
                var row = dgvPortProxy.Rows[e.RowIndex];
                bool needAuth = (bool)(row.Cells[COL_NEED_AUTH].Value ?? false);
                row.Cells[COL_USERNAME].ReadOnly = !needAuth;
                row.Cells[COL_PASSWORD].ReadOnly = !needAuth;
                row.Cells[COL_USERNAME].Style.ForeColor = needAuth ? Color.Black : Color.Gray;
                row.Cells[COL_PASSWORD].Style.ForeColor = needAuth ? Color.Black : Color.Gray;

                dgvPortProxy.InvalidateRow(e.RowIndex);
            }
        }

        // 日志输出
        private void Log(string message)
        {
            if (rtLog.InvokeRequired)
            {
                rtLog.Invoke(new Action(() => Log(message)));
                return;
            }
            rtLog.AppendText($"[{DateTime.Now:HH:mm:ss}] {message}\n");
            rtLog.ScrollToCaret();
        }

        // 日志重定向类
        public class ControlWriter : TextWriter
        {
            private readonly RichTextBox _control;
            public override Encoding Encoding => Encoding.UTF8;

            public ControlWriter(RichTextBox control) => _control = control;

            public override void WriteLine(string? value)
            {
                if (_control.IsDisposed) return;
                if (_control.InvokeRequired)
                {
                    _control.Invoke(new Action(() => WriteLine(value)));
                    return;
                }
                // 过滤调试日志（仅在日志级别为Debug时显示）
                var config = ProxyConfig.Load();
                if (config.LogLevel == LogLevel.Debug || !value.Contains("[Debug]"))
                {
                    _control.AppendText(value + "\n");
                    _control.ScrollToCaret();
                }
            }

            public override void Write(string? value) => WriteLine(value);
            public override void WriteLine() => WriteLine("");
            public override void Write(char value) => Write(value.ToString());
        }

        // 窗口关闭事件
        private void ServerMainForm_FormClosing(object? sender, FormClosingEventArgs e)
        {
            _proxyServer?.Stop();
            if (_serverTask != null && !_serverTask.IsCompleted)
            {
                _serverTask.Wait(1000);
            }
        }

        // 程序入口
        [STAThread]
        public static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new ServerMainForm());
        }
    }
}