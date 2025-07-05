package com.taosdata.jdbc.ws;

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
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandler;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandshaker;
import io.netty.handler.codec.http.websocketx.extensions.compression.DeflateFrameClientExtensionHandshaker;
import io.netty.handler.codec.http.websocketx.extensions.compression.PerMessageDeflateClientExtensionHandshaker;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;

public class WSClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WSClient.class);
    Transport transport;

    public final URI serverUri;

    private final String host;
    private final int port;

    private Channel channel;
    private final ConnectionParam connectionParam;

    static {
        Utils.initEventLoopGroup();
    }
    /**
     * create websocket connection client
     *
     * @param serverUri connection url
     */
    public WSClient(URI serverUri, Transport transport, ConnectionParam connectionParam) {
        this.transport = transport;
        this.serverUri = serverUri;
        this.connectionParam = connectionParam;
        this.channel = null;

        String scheme = serverUri.getScheme() == null ? "ws" : serverUri.getScheme();
        host = serverUri.getHost() == null ? "127.0.0.1" : serverUri.getHost();
        if (serverUri.getPort() == -1) {
            if ("ws".equalsIgnoreCase(scheme)) {
                port = 80;
            } else {
                port = 443;
            }
        } else {
            port = serverUri.getPort();
        }
    }

    private Channel getChannel() throws SQLException {
        Bootstrap b = new Bootstrap();
        b.group(Utils.getEventLoopGroup())
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionParam.getConnectTimeout())
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws SSLException {
                        ChannelPipeline p = ch.pipeline();

                        if (connectionParam.isUseSsl()) {
                            if (connectionParam.isDisableSslCertValidation()){
                                SslContext sslCtx = SslContextBuilder.forClient()
                                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                                        .build();
                                p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                            } else {
                                SslContext sslCtx = SslContextBuilder.forClient().build();
                                p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                            }
                        }

                        // for debug
                        //p.addLast(new LoggingHandler(LogLevel.DEBUG));

                        p.addLast(new HttpClientCodec());
                        p.addLast(new HttpObjectAggregator(8192));

                        // use custom websocket client handshaker to avoid mask encode
                        WebSocketClientHandshaker handshaker = new CustomWebSocketClientHandshaker(serverUri,
                                WebSocketVersion.V13,
                                null,
                                true,
                                new DefaultHttpHeaders(),
                                100 * 1024 * 1024,
                                true,
                                false,
                                -1L);
                        p.addLast(new WebSocketHandshakeHandler(handshaker));

                        if (connectionParam.isEnableCompression()) {
                            WebSocketClientExtensionHandshaker deflateHandshaker = new PerMessageDeflateClientExtensionHandshaker(
                                    6, false,
                                    15,       // clientMaxWindowSize (2^15 = 32KB)
                                    true,     // clientNoContextTakeover
                                    true    // serverNoContextTakeover

                            );

                            WebSocketClientExtensionHandler extensionHandler = new WebSocketClientExtensionHandler(deflateHandshaker,  new DeflateFrameClientExtensionHandshaker(false),
                                    new DeflateFrameClientExtensionHandshaker(true));
                            p.addLast(extensionHandler);
                        }

                        p.addLast(new WebSocketFrameAggregator(100 * 1024 * 1024)); // max 100MB

                        // Connect with V13 (RFC 6455 aka HyBi-17). You can change it to V08 or V00.
                        // If you change it to V00, ping is not supported and remember to change
                        // HttpResponseDecoder to WebSocketHttpResponseDecoder in the pipeline.
                        final WebSocketClientHandler handler =
                                new WebSocketClientHandler(connectionParam.getTextMessageHandler(),
                                        connectionParam.getBinaryMessageHandler());
                        p.addLast(handler);
                    }
                });

        ChannelFuture connectFuture = b.connect(host, port);

        Channel tmpChn = null;
        try {
            if (!connectFuture.awaitUninterruptibly(connectionParam.getConnectTimeout())) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT);
           }

            if (!connectFuture.isSuccess()) {
                Throwable cause = connectFuture.cause();
                if (cause instanceof ConnectTimeoutException) {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT);
                }
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, cause.getMessage());
            }

            tmpChn = connectFuture.channel();

            WebSocketHandshakeHandler wsHandler = tmpChn.pipeline().get(WebSocketHandshakeHandler.class);
            ChannelFuture handshakeFuture = wsHandler.handshakeFuture();

            if (!handshakeFuture.awaitUninterruptibly(connectionParam.getConnectTimeout())) {
                tmpChn.close().syncUninterruptibly();
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT, "Handshake timed out");
            }

            if (!handshakeFuture.isSuccess()) {
                tmpChn.close().syncUninterruptibly();
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "Handshake failed: " + handshakeFuture.cause().getMessage());
            }
            tmpChn.pipeline().remove(wsHandler);
            return tmpChn;
        } catch (TimeoutException e) {
            if (tmpChn != null) {
                tmpChn.close().syncUninterruptibly();
            }
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT, e.getMessage());
        }
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
            channel.attr(WebSocketClientHandler.LOCAL_INITIATED_CLOSE).set(true);

            writeFuture.syncUninterruptibly();

            ChannelFuture closeFuture = channel.close();
            closeFuture.syncUninterruptibly();
            if (closeFuture.isSuccess()) {
                log.debug("WebSocket connection closed successfully");
            } else {
                log.error("WebSocket connection closed error", closeFuture.cause());
            }
        }
    }
   public boolean reconnectBlockingWithoutRetry() {
       return connectBlocking();
    }

    public boolean connectBlocking(){
        if (channel != null && channel.isActive()){
            return true;
        }
        if (channel != null){
            channel.close().syncUninterruptibly();
        }

        try {
            channel = getChannel();
            return true;
        } catch (SQLException e){
            return false;
        }
    }

    public boolean reconnectBlocking(){
        return connectBlocking();
    }

    public void send(String strData) {
        if (!channel.isActive()) {
            throw new WebsocketNotConnectedException();
        }
        channel.eventLoop().execute(() -> {
            channel.writeAndFlush(new TextWebSocketFrame(strData));
        });
    }

    public void send(ByteBuf binData) {
        if (!channel.isActive()) {
            Utils.releaseByteBuf(binData);
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
