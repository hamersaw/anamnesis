package com.bushpath.anamnesis.rpc;

import com.google.protobuf.Message;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class RpcServer extends Thread {
    private ServerSocket serverSocket;
    private Map<String,RpcHandler> rpcHandlers;

    public RpcServer(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
        this.rpcHandlers = new HashMap<>();
    }

    public void registerRpcHandler(String className, RpcHandler rpcHandler) {
        this.rpcHandlers.put(className, rpcHandler);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = this.serverSocket.accept();

                // TODO add SocketHandler to threadpool
                SocketHandler socketHandler = new SocketHandler(socket);
                socketHandler.run();
            } catch(Exception e) {
                System.err.println("failed to accept server connection: " + e);
            }
        }
    }

    private byte[] readBuffer(DataInputStream in) throws Exception {
        int length = (int) in.readByte();
        byte[] buffer = new byte[length];
        in.readFully(buffer);
        return buffer;
    }

    private void handlePacket(DataInputStream in, DataOutputStream out)
            throws Exception {
        // read total packet length
        int packetLen = in.readInt();

        // parse rpc header
        RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto =
            RpcHeaderProtos.RpcRequestHeaderProto.parseFrom(readBuffer(in));

        // parse request
        switch (rpcRequestHeaderProto.getCallId()) {
        case -3:
            IpcConnectionContextProtos.IpcConnectionContextProto context =
                IpcConnectionContextProtos.IpcConnectionContextProto
                    .parseFrom(readBuffer(in));

            // TODO - update i.getUserInfo().getEffectiveUser()
            // and i.getProtocol()
            break;
        default:
            ProtobufRpcEngineProtos.RequestHeaderProto requestHeaderProto =
                ProtobufRpcEngineProtos.RequestHeaderProto.parseFrom(readBuffer(in));

            // build response - set to 'SUCCESS' and change on failure
            RpcHeaderProtos.RpcResponseHeaderProto.Builder respBuilder =
                RpcHeaderProtos.RpcResponseHeaderProto.newBuilder()
                    .setStatus(RpcStatusProto.SUCCESS)
                    .setCallId(rpcRequestHeaderProto.getCallId())
                    .setClientId(rpcRequestHeaderProto.getClientId());

            // retreive rpc arguments
            String methodName = requestHeaderProto.getMethodName();
            String declaringClassProtocolName =
                requestHeaderProto.getDeclaringClassProtocolName();

            System.out.println(declaringClassProtocolName + " : " + methodName);

            Message message = null;
            if (!rpcHandlers.containsKey(declaringClassProtocolName)) {
                // error -> protocol does not exist
                respBuilder.setStatus(RpcStatusProto.ERROR);
                respBuilder.setErrorMsg("protocol '" + declaringClassProtocolName 
                    + "' does not exist");
                respBuilder.setErrorDetail(RpcErrorCodeProto.ERROR_NO_SUCH_PROTOCOL);
            } else {
                RpcHandler handler = rpcHandlers.get(declaringClassProtocolName);
                if (!handler.containsMethod(methodName)) {
                    // error -> method does not exist
                    respBuilder.setStatus(RpcStatusProto.ERROR);
                    respBuilder.setErrorMsg("method '" + methodName + "' does not exist");
                    respBuilder.setErrorDetail(RpcErrorCodeProto.ERROR_NO_SUCH_PROTOCOL);
                } else {
                    // use rpc handler to execute method
                    try {
                        message = handler.handle(methodName, readBuffer(in));
                    } catch(Exception e) {
                        respBuilder.setStatus(RpcStatusProto.ERROR);
                        respBuilder.setExceptionClassName(e.getClass().toString());
                        respBuilder.setErrorMsg(e.getMessage());
                        respBuilder.setErrorDetail(RpcErrorCodeProto.ERROR_RPC_SERVER);
                    }
                }
            }

            // send response
            RpcHeaderProtos.RpcResponseHeaderProto resp = respBuilder.build();
            int length = (1 + resp.getSerializedSize()) +
                (message == null ? 0 : 1 + message.getSerializedSize());
            out.writeInt(length);
            out.writeByte((byte) resp.getSerializedSize());
            resp.writeTo(out);
            if (message != null) {
                out.writeByte((byte)message.getSerializedSize());
                message.writeTo(out);
            }

            out.flush();
            break;
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
                DataInputStream in = new DataInputStream(this.socket.getInputStream());
                DataOutputStream out =
                    new DataOutputStream(this.socket.getOutputStream());
 
                // read connection header
                byte[] connectionHeaderBuf = new byte[7];
                in.readFully(connectionHeaderBuf);

                // TODO validate connection header (hrpc, verison, auth_protocol)
 
                while (true) {
                    handlePacket(in, out);
                }
            } catch(EOFException e) {
            } catch(Exception e) {
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
