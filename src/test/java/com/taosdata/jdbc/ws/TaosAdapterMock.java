package com.taosdata.jdbc.ws;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class TaosAdapterMock {
    private final String targetHost;
    private final int targetPort;
    private final int listenPort;
    private volatile boolean isRunning = false;
    private ServerSocket serverSocket;
    private ExecutorService executor;

    // 客户端连接记录
    private final ConcurrentHashMap<Socket, Boolean> activeConnections = new ConcurrentHashMap<>();

    public TaosAdapterMock(int listenPort) {
        this.targetHost = "localhost";
        this.targetPort = 6041;
        this.listenPort = listenPort;
    }

    public synchronized void start() throws IOException {
        if (isRunning) {
            throw new IllegalStateException("Service is already running");
        }

        serverSocket = new ServerSocket(listenPort);
        executor = Executors.newCachedThreadPool();
        isRunning = true;

        executor.execute(() -> {
            System.out.printf("Forward service started on port %d -> %s:%d%n",
                    listenPort, targetHost, targetPort);

            while (isRunning) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    activeConnections.put(clientSocket, true);
                    executor.execute(new ClientHandler(clientSocket));
                } catch (SocketException e) {
                    // 正常停止时产生的异常
                } catch (IOException e) {
                    if (isRunning) {
                        System.err.println("Accept failed: " + e.getMessage());
                    }
                }
            }
        });
    }

    public synchronized void stop() {
        if (!isRunning) return;
        isRunning = false;

        try {
            // 关闭监听Socket
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e.getMessage());
        }

        // 关闭所有客户端连接
        activeConnections.keySet().forEach(this::closeSocket);
        activeConnections.clear();

        // 关闭线程池
        if (executor != null) {
            executor.shutdownNow();
        }

        System.out.println("Forward service stopped");
    }

    private class ClientHandler implements Runnable {
        private final Socket clientSocket;

        ClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try (Socket targetSocket = new Socket(targetHost, targetPort);
                 InputStream clientInput = clientSocket.getInputStream();
                 OutputStream clientOutput = clientSocket.getOutputStream();
                 InputStream targetInput = targetSocket.getInputStream();
                 OutputStream targetOutput = targetSocket.getOutputStream()) {

                // 启动双向转发
                Future<?> clientToTarget = executor.submit(
                        () -> forwardData(clientInput, targetOutput, "client->target")
                );

                Future<?> targetToClient = executor.submit(
                        () -> forwardData(targetInput, clientOutput, "target->client")
                );

                // 等待任意一个转发完成
                clientToTarget.get();
                targetToClient.get();

            } catch (Exception e) {
                if (isRunning) {
                    System.err.println("Connection error: " + e.getMessage());
                }
            } finally {
                closeSocket(clientSocket);
                activeConnections.remove(clientSocket);
            }
        }

        private void forwardData(InputStream input, OutputStream output, String direction) {
            byte[] buffer = new byte[4096];
            int bytesRead;

            try {
                while ((bytesRead = input.read(buffer)) != -1) {
                    output.write(buffer, 0, bytesRead);
                    output.flush();
                }
            } catch (IOException e) {
                if (!isRunning) return; // 正常停止时不打印错误
                System.err.printf("[%s] Forward error: %s%n", direction, e.getMessage());
            }
        }
    }

    private void closeSocket(Socket socket) {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing socket: " + e.getMessage());
        }
    }
}
    