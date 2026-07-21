package com.p2p;

import com.p2p.core.PeerInfo;
import com.p2p.sharer.SharerNode;

public class Main {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 5000;

    public static void main(String[] args) {
        if (args.length > 0 && ("--help".equals(args[0]) || "-h".equals(args[0]))) {
            printUsage();
            return;
        }

        String peerId = args.length > 0 ? args[0] : "Peer1";
        String host = args.length > 1 ? args[1] : DEFAULT_HOST;
        int port = args.length > 2 ? parsePort(args[2], DEFAULT_PORT) : DEFAULT_PORT;

        String joinHost = null;
        int joinPort = -1;
        if (args.length >= 5) {
            joinHost = args[3];
            joinPort = parsePort(args[4], -1);
        }

        PeerInfo info = new PeerInfo(peerId, host, port);
        SharerNode node = new SharerNode(10, info);

        System.out.println("Peer started at " + info.getHost() + ":" + info.getPort());
        node.startWebServer();
        System.out.println("Web status page: http://localhost:" + (info.getPort() + 100));

        if (joinHost != null && joinPort > 0) {
            final String finalJoinHost = joinHost;
            final int finalJoinPort = joinPort;
            System.out.println("Attempting to join peer at " + finalJoinHost + ":" + finalJoinPort);
            Thread joinThread = new Thread(() -> {
                try {
                    for (int attempt = 0; attempt < 10; attempt++) {
                        if (node.buildPeers(finalJoinHost, finalJoinPort, 1)) {
                            System.out.println("Joined peer at " + finalJoinHost + ":" + finalJoinPort);
                            break;
                        }
                        Thread.sleep(500);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            joinThread.setDaemon(true);
            joinThread.start();
        }

        node.mainLoop();
    }

    private static int parsePort(String value, int fallback) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            System.out.println("Invalid port: " + value + ". Using default " + fallback);
            return fallback;
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java -cp out com.p2p.Main <peerId> <host> <port> [joinHost joinPort]");
        System.out.println("Example:");
        System.out.println("  java -cp out com.p2p.Main Peer1 localhost 5000");
        System.out.println("  java -cp out com.p2p.Main Peer2 localhost 5001 localhost 5000");
    }
}