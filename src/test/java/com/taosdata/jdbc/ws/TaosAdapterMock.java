package com.taosdata.jdbc.ws;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TaosAdapterMock {
    private final String targetHost;
    private final int targetPort;
    private final int listenPort;
    private volatile boolean isRunning = false;
    private ServerSocket serverSocket;
    private ExecutorService executor;
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
                    // exception in normal stop
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
            // close server socket
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e.getMessage());
        }

        // close all active connections
        activeConnections.keySet().forEach(this::closeSocket);
        activeConnections.clear();

        // close executor service
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

                // start two threads to forward data
                Future<?> clientToTarget = executor.submit(
                        () -> forwardData(clientInput, targetOutput, "client->target")
                );

                Future<?> targetToClient = executor.submit(
                        () -> forwardData(targetInput, clientOutput, "target->client")
                );

                // wait for both threads to finish
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
                if (!isRunning) return; // ignore if service is stopped
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
    