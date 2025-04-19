package com.taosdata.jdbc.ws;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;
import com.google.common.base.Strings;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.Utils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandshaker;
import io.netty.handler.codec.http.websocketx.extensions.compression.PerMessageDeflateClientExtensionHandshaker;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WSClient implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(WSClient.class);
    ThreadPoolExecutor executor;
    Transport transport;

    public final String serverUri;
    private final Channel channel;


    static {
        Utils.initEventLoopGroup();
        //ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        Runtime.getRuntime().addShutdownHook(new Thread(Utils::finalizeEventLoopGroup));
    }

    /**
     * create websocket connection client
     *
     * @param serverUri connection url
     */
    public WSClient(URI serverUri, Transport transport, ConnectionParam connectionParam) throws SQLException {
        this.transport = transport;
        this.serverUri = serverUri.toString();
        executor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                r -> {
                    Thread t = new Thread(r);
                    t.setName("parse-message-" + t.getId());
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());


        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        if (connectionParam.isEnableCompression()) {
            headers.add(
                    "Sec-WebSocket-Extensions",
                    "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
            );
        }

        // 自定义压缩参数
        WebSocketClientExtensionHandshaker deflateHandshaker = new PerMessageDeflateClientExtensionHandshaker(
                6, false,
                15,       // clientMaxWindowSize (2^15 = 32KB)
                true,     // clientNoContextTakeover
                true    // serverNoContextTakeover

        );

        deflateHandshaker.newRequestData();

        // Connect with V13 (RFC 6455 aka HyBi-17). You can change it to V08 or V00.
        // If you change it to V00, ping is not supported and remember to change
        // HttpResponseDecoder to WebSocketHttpResponseDecoder in the pipeline.
        final WebSocketClientHandler handler =
                new WebSocketClientHandler(
                        WebSocketClientHandshakerFactory.newHandshaker(
                                serverUri, WebSocketVersion.V13, null, true, headers),
                        connectionParam.getTextMessageHandler(),
                        connectionParam.getBinaryMessageHandler());

        Bootstrap b = new Bootstrap();
        b.group(Utils.getEventLoopGroup())
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionParam.getConnectTimeout())
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws SSLException {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LoggingHandler(LogLevel.DEBUG));  // 关键点
                        if (connectionParam.isUseSsl()) {
                            SslContext sslCtx = SslContextBuilder.forClient()
                                    .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
                            p.addLast(sslCtx.newHandler(ch.alloc(), serverUri.getHost(), serverUri.getPort()));
                        }

                        if (connectionParam.isEnableCompression()){
                            p.addLast(
                                    new HttpClientCodec(),
                                    new HttpObjectAggregator(8192),
                                    WebSocketClientCompressionHandler.INSTANCE,
                                    handler);
                        } else {
                            p.addLast(
                                    new HttpClientCodec(),
                                    new HttpObjectAggregator(8192),
                                    handler);
                        }


//                        // 自定义压缩参数
//                        WebSocketClientExtensionHandshaker handshaker = new PerMessageDeflateClientExtensionHandshaker(6, false,
//                                15,  // clientWindowSize
//                                true, // clientNoContext
//                                true  // serverNoContext
//                        );
//
//                        // 创建压缩处理器
//                        WebSocketClientCompressionHandler compressionHandler = new WebSocketClientCompressionHandler(handshaker);
//                        p.addLast(compressionHandler);


                        p.addLast(new WebSocketFrameAggregator(100 * 1024 * 1024)); // 100MB
                    }
                });

        ChannelFuture connectFuture = b.connect(serverUri.getHost(), serverUri.getPort());
        connectFuture.syncUninterruptibly();


        if (!connectFuture.isSuccess()) {
            Throwable cause = connectFuture.cause();
            if (cause instanceof ConnectTimeoutException) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT);
            } else {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, cause.getMessage());
            }
        }

        // 关键：等待 WebSocket 握手完成
        ChannelFuture handshakeFuture = handler.getHandshakeFuture();
        handshakeFuture.syncUninterruptibly(); // 阻塞等待握手结果

        if (!handshakeFuture.isSuccess()) {
            // 握手失败（如服务器返回 404、协议版本不匹配）
            Throwable cause = handshakeFuture.cause();
            throw new IllegalStateException("WebSocket handshake failed: " + cause.getMessage());
        }

        channel = connectFuture.channel();
    }

    public boolean isOpen() {
        return channel.isActive();
    }

    public boolean isClosed() {
        return !isOpen();
    }
    @Override
    public void close() {
        if (channel != null && channel.isOpen()) {
            int statusCode = 1000;
            String reason = "Normal close";
            CloseWebSocketFrame closeFrame = new CloseWebSocketFrame(statusCode, reason);
            ChannelFuture writeFuture = channel.writeAndFlush(closeFrame);
            writeFuture.syncUninterruptibly();

            ChannelFuture closeFuture = channel.close();
            closeFuture.syncUninterruptibly();
            if (closeFuture.isSuccess()) {
                log.debug("WebSocket 连接已成功关闭");
            } else {
                log.error("关闭 WebSocket 连接时出错: " + closeFuture.cause());
            }
        }
    }
   public boolean reconnectBlockingWithoutRetry() throws InterruptedException {
//        return super.reconnectBlocking();

       return false;
    }

    public void send(String strData) {
        if (!channel.isActive()) {
            throw new WebsocketNotConnectedException();
        }
        // 通过 EventLoop 异步执行（确保在通道所属线程）
        channel.eventLoop().execute(() -> {
            channel.writeAndFlush(new TextWebSocketFrame(strData));
        });
    }

    public void send(ByteBuf binData) {
        if (!channel.isActive()) {
            ReferenceCountUtil.release(binData);
            throw new WebsocketNotConnectedException();
        }
        channel.writeAndFlush(new BinaryWebSocketFrame(binData));
    }
    public void closeBlocking() {
        this.close();
    }
    public static WSClient getInstance(ConnectionParam params, WSFunction function, Transport transport) throws SQLException {
        if (Strings.isNullOrEmpty(function.getFunction())) {
            throw new SQLException("websocket url error");
        }
        String protocol = "ws";
        if (params.isUseSsl()) {
            protocol = "wss";
        }
        String port = "";
        if (null != params.getPort()) {
            port = ":" + params.getPort();
        }

        String wsFunction = "/ws";
        if (function.equals(WSFunction.TMQ)){
            wsFunction = "/rest/tmq";
        }
        String loginUrl = protocol + "://" + params.getHost() + port + wsFunction;

        if (null != params.getCloudToken()) {
            loginUrl = loginUrl + "?token=" + params.getCloudToken();
        }

        URI urlPath;
        try {
            urlPath = new URI(loginUrl);
        } catch (URISyntaxException e) {
            throw new SQLException("Websocket url parse error: " + loginUrl, e);
        }
        return new WSClient(urlPath, transport, params);
    }
    public static WSClient getSlaveInstance(ConnectionParam params, WSFunction function, Transport transport) throws SQLException {
        if (StringUtils.isEmpty(params.getSlaveClusterHost()) || StringUtils.isEmpty(params.getSlaveClusterHost())){
            return null;
        }

        if (Strings.isNullOrEmpty(function.getFunction())) {
            throw new SQLException("websocket url error");
        }
        String protocol = "ws";
        if (params.isUseSsl()) {
            protocol = "wss";
        }
        String port = ":" + params.getSlaveClusterPort();

        String wsFunction = "/ws";

        if (!function.equals(WSFunction.WS)){
            throw new SQLException("slave cluster is not supported!");
        }

        String loginUrl = protocol + "://" + params.getSlaveClusterHost() + port + wsFunction;

        URI urlPath;
        try {
            urlPath = new URI(loginUrl);
        } catch (URISyntaxException e) {
            throw new SQLException("Slave cluster websocket url parse error: " + loginUrl, e);
        }
        return new WSClient(urlPath, transport, params);
    }
}
