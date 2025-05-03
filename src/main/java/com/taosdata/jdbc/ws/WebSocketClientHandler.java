package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
    private final Logger log = LoggerFactory.getLogger(WebSocketClientHandler.class);
    private final Consumer<String> textMessageHandler;
    private final Consumer<ByteBuf> binaryMessageHandler;


    public WebSocketClientHandler(Consumer<String> textMessageHandler,
                                  Consumer<ByteBuf> binaryMessageHandler) {
        this.textMessageHandler = textMessageHandler;
        this.binaryMessageHandler = binaryMessageHandler;
    }
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.trace("WebSocket Client disconnected!");
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        Channel ch = ctx.channel();
        if (msg instanceof FullHttpResponse) {
//            FullHttpResponse response = (FullHttpResponse) msg;
//            String content = response.content().toString(CharsetUtil.UTF_8);
//            System.err.println("收到意外 HTTP 响应: " + response.status() + " | Content: " + content);
//            throw new IllegalStateException(
//                    "Unexpected FullHttpResponse (getStatus=" + response.status() +
//                            ", content=" + response.content().toString(CharsetUtil.UTF_8) + ')');
//            super.channelRead(ctx, msg);
            return;
        }

        WebSocketFrame frame = (WebSocketFrame) msg;
        if (frame instanceof PingWebSocketFrame) {
            // Handle ping frames
            PongWebSocketFrame pongFrame = new PongWebSocketFrame(frame.content().retain());
            ctx.writeAndFlush(pongFrame);
        } else if (frame instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
            textMessageHandler.accept(textFrame.text());
        } else if (frame instanceof BinaryWebSocketFrame) {
            BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
            binaryMessageHandler.accept(binaryFrame.content());
        } else if (frame instanceof PongWebSocketFrame) {
            System.out.println("WebSocket Client received pong");
        } else if (frame instanceof CloseWebSocketFrame) {
            // do nothing, wait next send to retry.
//            if (remote){
//                log.error("disconnect uri: {},  code : {} , reason: {}, remote: {}", StringUtils.getBasicUrl(serverUri), code, reason, remote);
//            }else{
//                log.debug("disconnect uri: {},  code : {} , reason: {}, remote: {}", StringUtils.getBasicUrl(serverUri), code, reason, remote);
//            }
            ch.close();
        }
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("exception caught", cause);
        ctx.close();
    }
}