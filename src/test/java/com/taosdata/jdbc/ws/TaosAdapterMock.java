package com.taosdata.jdbc.ws;

// WebSocketServerExample.java

import org.java_websocket.WebSocket;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.handshake.ServerHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaosAdapterMock extends WebSocketServer {

    volatile boolean isReady = false;

    // Target WebSocket server URI

    // Mapping between client connections and their corresponding target connections
    private final Map<WebSocket, TargetWebSocketClient> clientMap = new ConcurrentHashMap<>();

    /**
     * Constructor to initialize the proxy server.
     *
     * @param port            The port number on which the proxy server will listen.
     */
    public TaosAdapterMock(int port) {
        super(new InetSocketAddress(port));
    }

    /**
     * Called when a new client connection is opened.
     *
     * @param conn      The WebSocket connection object of the client.
     * @param handshake The handshake data sent by the client.
     */
    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("New connection from: " + conn.getRemoteSocketAddress());

        // Initialize and connect to the target server for this client
        TargetWebSocketClient targetClient = null;
        try {
            targetClient = new TargetWebSocketClient(conn, new URI("ws://localhost:6041/ws"));
            targetClient.connectBlocking();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // Map the client connection to the target connection
        clientMap.put(conn, targetClient);
    }

    /**
     * Called when a client connection is closed.
     *
     * @param conn   The WebSocket connection object of the client.
     * @param code   The status code indicating why the connection was closed.
     * @param reason The reason for the closure.
     * @param remote Indicates whether the connection was closed by the remote host.
     */
    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        System.out.println("Connection closed from: " + conn.getRemoteSocketAddress() + ", Reason: " + reason);

        // Retrieve and close the corresponding target connection
        TargetWebSocketClient targetClient = clientMap.remove(conn);
        if (targetClient != null) {
            try {
                targetClient.closeConnection();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Called when a message is received from a client.
     *
     * @param conn    The WebSocket connection object of the client.
     * @param message The message received from the client.
     */
    @Override
    public void onMessage(WebSocket conn, String message) {
        System.out.println("Received message from " + conn.getRemoteSocketAddress() + ": " + message);

        // Forward the message to the target server
        TargetWebSocketClient targetClient = clientMap.get(conn);
        if (targetClient != null){
            targetClient.send(message);
        }
    }

    @Override
    public void onMessage(WebSocket conn, ByteBuffer message) {
        System.out.println("Received message from " + conn.getRemoteSocketAddress() + ": " + message);

        // Forward the message to the target server
        TargetWebSocketClient targetClient = clientMap.get(conn);
        if (targetClient != null){
            targetClient.send(message);
        }
    }

    /**
     * Called when an error occurs on a client connection.
     *
     * @param conn The WebSocket connection object of the client. It may be null if the error is not related to a specific connection.
     * @param ex   The exception that was thrown.
     */
    @Override
    public void onError(WebSocket conn, Exception ex) {
        if (conn != null) {
            System.err.println("An error occurred on connection " + conn.getRemoteSocketAddress() + ":" + ex);
        } else {
            System.err.println("A server error occurred: " + ex);
        }
        ex.printStackTrace();

        // Optionally, handle specific errors or perform cleanup
    }

    /**
     * Called when the WebSocket server starts successfully.
     */
    @Override
    public void onStart() {
        System.out.println("WebSocket Proxy Server started successfully!");
        isReady = true;
    }

    /**
     * Gracefully shuts down the WebSocket proxy server.
     *
     * @throws InterruptedException If the current thread is interrupted while waiting.
     */
    public void stopServer() throws InterruptedException {
        System.out.println("Shutting down WebSocket Proxy Server...");
        // Close all target connections
        for (TargetWebSocketClient targetClient : clientMap.values()) {
            targetClient.closeConnection();
        }
        clientMap.clear();

        // Stop the server with a 1-second timeout
        this.stop(10000, "Server stopped");
        System.out.println("WebSocket Proxy Server stopped.");
    }


    private class TargetWebSocketClient extends WebSocketClient {

        private final WebSocket sourceConnection;

        /**
         * Constructor to initialize the target WebSocket client.
         *
         * @param sourceConnection The source client connection that initiated the proxy.
         * @param serverUri        The URI of the target WebSocket server.
         */
        public TargetWebSocketClient(WebSocket sourceConnection, URI serverUri) {
            super(serverUri);
            this.sourceConnection = sourceConnection;
        }

        /**
         * Called when the connection to the target server is opened.
         *
         * @param handshakedata The handshake data sent by the server.
         */
        @Override
        public void onOpen(ServerHandshake handshakedata) {
            System.out.println("Connected to target server: " + getURI());
            // Optionally, send an initial message to the target server
            // send("Hello from Proxy Server!");
        }


        /**
         * Called when a message is received from the target server.
         *
         * @param message The message received from the target server.
         */
        @Override
        public void onMessage(String message) {
            System.out.println("Received message from target server: " + message);
            // Forward the message back to the source client
            sourceConnection.send(message);
        }

        @Override
        public void onMessage(ByteBuffer bytes) {
            sourceConnection.send(bytes);
        }

        /**
         * Called when the connection to the target server is closed.
         *
         * @param code   The status code indicating why the connection was closed.
         * @param reason The reason for the closure.
         * @param remote Indicates whether the connection was closed by the remote host.
         */
        @Override
        public void onClose(int code, String reason, boolean remote) {
            System.out.println("Connection to target server closed: " + reason);
            // Notify the source client about the closure
            sourceConnection.close(1000, "Target server closed the connection.");
        }

        /**
         * Called when an error occurs on the target WebSocket connection.
         *
         * @param ex The exception that was thrown.
         */
        @Override
        public void onError(Exception ex) {
            System.err.println("An error occurred with the target connection: " + ex.getMessage());
            ex.printStackTrace();
            // Optionally, notify the source client about the error
            sourceConnection.send("Error: " + ex.getMessage());
        }

        /**
         * Closes the connection to the target server.
         */
        public void closeConnection() throws InterruptedException {
            if (isOpen()) {
                sourceConnection.close();
                closeBlocking();
            }
        }
    }

    public boolean isReady() {
        return isReady;
    }
}