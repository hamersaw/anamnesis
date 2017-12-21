package com.bushpath.anamnesis.ipc.rpc;

import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;

import com.bushpath.anamnesis.ipc.rpc.packet_handler.PacketHandler;
import com.bushpath.anamnesis.ipc.rpc.packet_handler.RpcPacketHandler;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class RpcServer extends Thread {
    private ServerSocket serverSocket;
    private ExecutorService executorService;
    private RpcPacketHandler rpcPacketHandler;
    private Map<Integer, PacketHandler> packetHandlers;

    public RpcServer(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
        this.executorService = Executors.newFixedThreadPool(4);
        this.rpcPacketHandler = new RpcPacketHandler();
        this.packetHandlers = new HashMap<>();
    }

    public void addRpcProtocol(String className, Object protocol) {
        this.rpcPacketHandler.addProtocol(className, protocol);
    }

    public void addPacketHandler(PacketHandler packetHandler) {
        this.packetHandlers.put(packetHandler.getCallId(), packetHandler);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = this.serverSocket.accept();

                // add SocketHandler to threadpool
                Runnable socketHandler = new SocketHandler(socket);
                this.executorService.execute(socketHandler);
            } catch(Exception e) {
                System.err.println("failed to accept server connection: " + e);
            }
        }
    }
    
    private void handlePacket(DataInputStream in, DataOutputStream out)
            throws Exception {
        // read total packet length
        int packetLen = in.readInt();

        // parse rpc header
        RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto =
            RpcHeaderProtos.RpcRequestHeaderProto.parseDelimitedFrom(in);

        int callId = rpcRequestHeaderProto.getCallId();
        if (callId >= 0) { // rpc request
            this.rpcPacketHandler.handle(in, out, rpcRequestHeaderProto);
        } else { // should be handled by a registered packet handler
            if (!this.packetHandlers.containsKey(callId)) {
                throw new Exception("packet type with callId '" + callId 
                    + "' not supported");
            }

            this.packetHandlers.get(callId).handle(in, out, rpcRequestHeaderProto);
        }
    }

    private class SocketHandler implements Runnable {
        private Socket socket;

        public SocketHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                DataOutputStream out =
                    new DataOutputStream(this.socket.getOutputStream());
                DataInputStream in = new DataInputStream(this.socket.getInputStream());
 
                // read connection header
                byte[] connectionHeaderBuf = new byte[7];
                in.readFully(connectionHeaderBuf);

                // TODO validate connection header (hrpc, verison, auth_protocol)
 
                while (true) {
                    handlePacket(in, out);
                }
            } catch(EOFException e) {
            } catch(Exception e) {
                e.printStackTrace();
                System.err.println("failed to read rpc request: " + e);
            } finally {
                try {
                    this.socket.close();
                } catch(IOException e) {
                    System.err.println("failed to close socket: " + e);
                }
            }
        }
    }
}
