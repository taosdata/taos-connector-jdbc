package com.taosdata.jdbc.ws;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketHandshakeHandler extends ChannelInboundHandlerAdapter {
    private final Logger log = LoggerFactory.getLogger(WebSocketHandshakeHandler.class);

    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;
    public WebSocketHandshakeHandler(WebSocketClientHandshaker handshaker) {
        this.handshaker = handshaker;
        this.handshakeFuture = null;
    }

    // 新增方法：暴露握手 Future
    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        // 创建握手 Future
        handshakeFuture = ctx.newPromise();
    }
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // 发起握手
        handshaker.handshake(ctx.channel());
        ctx.fireChannelActive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!handshaker.isHandshakeComplete()) {
            try {
                // 完成握手
                handshaker.finishHandshake(ctx.channel(), (FullHttpResponse) msg);
                // 标记握手完成
                handshakeFuture.setSuccess(); // 关键代码

                // 移除握手处理器（可选）
                ctx.pipeline().remove(this);
            } catch (WebSocketHandshakeException e) {
                log.error("握手失败: ", e);
                handshakeFuture.setFailure(e); // 标记失败
                ctx.close();
            }
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause); // 异常时标记失败
        }
        ctx.close();
    }
}