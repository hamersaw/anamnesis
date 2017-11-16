package com.bushpath.anamnesis.rpc;

import com.google.protobuf.Message;
import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class RpcServer extends Thread {
    private ServerSocket serverSocket;
    private ExecutorService executorService;
    private Map<String, Object> rpcHandlers;

    public RpcServer(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
        this.executorService = Executors.newFixedThreadPool(4);
        this.rpcHandlers = new HashMap<>();
    }

    public void registerRpcHandler(String className, Object rpcHandler) {
        this.rpcHandlers.put(className, rpcHandler);
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
            RpcHeaderProtos.RpcRequestHeaderProto.parseDelimitedFrom(in);

        // parse request
        switch (rpcRequestHeaderProto.getCallId()) {
        case -3:
            IpcConnectionContextProtos.IpcConnectionContextProto context =
                IpcConnectionContextProtos.IpcConnectionContextProto
                    .parseDelimitedFrom(in);

            // TODO - update i.getUserInfo().getEffectiveUser()
            // and i.getProtocol()
            break;
        case -33:
            // handle SASL RPC request
            RpcHeaderProtos.RpcSaslProto rpcSaslProto =
                RpcHeaderProtos.RpcSaslProto.parseDelimitedFrom(in);

            switch (rpcSaslProto.getState()) {
            case NEGOTIATE:
                // send automatic SUCCESS - mean simple server on HDFS side
                RpcHeaderProtos.RpcResponseHeaderProto rpcResponse =
                    RpcHeaderProtos.RpcResponseHeaderProto.newBuilder()
                        .setStatus(RpcStatusProto.SUCCESS)
                        .setCallId(rpcRequestHeaderProto.getCallId())
                        .setClientId(rpcRequestHeaderProto.getClientId())
                        .build();

                RpcHeaderProtos.RpcSaslProto message =
                    RpcHeaderProtos.RpcSaslProto.newBuilder()
                        .setState(RpcHeaderProtos.RpcSaslProto.SaslState.SUCCESS)
                        .build();

                // TODO - fix this with sending response to general rpc request
                int respSize = rpcResponse.getSerializedSize();
                int messageSize = message.getSerializedSize();

                int length = CodedOutputStream.computeRawVarint32Size(respSize) + respSize
                    + CodedOutputStream.computeRawVarint32Size(messageSize) + messageSize;
                out.writeInt(length);
                rpcResponse.writeDelimitedTo(out);
                message.writeDelimitedTo(out);

                out.flush();
                break;
            default:
                System.out.println("TODO - handle sasl " + rpcSaslProto.getState());
                break;
            }

            break;
        default:
            ProtobufRpcEngineProtos.RequestHeaderProto requestHeaderProto =
                ProtobufRpcEngineProtos.RequestHeaderProto.parseDelimitedFrom(in);

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
                Object rpcHandler = rpcHandlers.get(declaringClassProtocolName);

                // check if handler contains method
                Method method = null;
                for (Method m: rpcHandler.getClass().getMethods()) {
                    if (m.getName().equals(methodName)) {
                        method = m;
                        break;
                    }
                }

                if (method == null) {
                    // error -> method does not exist
                    respBuilder.setStatus(RpcStatusProto.ERROR);
                    respBuilder.setErrorMsg("method '" + methodName + "' does not exist");
                    respBuilder.setErrorDetail(RpcErrorCodeProto.ERROR_NO_SUCH_PROTOCOL);
                } else {
                    // use rpc handler to execute method
                    try {
                        //message = (Message) method.invoke(rpcHandler, readBuffer(in));
                        message = (Message) method.invoke(rpcHandler, in);
                    } catch(Exception e) {
                        respBuilder.setStatus(RpcStatusProto.ERROR);
                        respBuilder.setExceptionClassName(e.getClass().toString());
                        respBuilder.setErrorMsg(e.toString());
                        respBuilder.setErrorDetail(RpcErrorCodeProto.ERROR_RPC_SERVER);
                    }
                }
            }

            RpcHeaderProtos.RpcResponseHeaderProto resp = respBuilder.build();

            // send response
            int respSize = resp.getSerializedSize();
            int messageSize = message == null ? 0 : message.getSerializedSize();

            int length = CodedOutputStream.computeRawVarint32Size(respSize) + respSize
                + (message == null ? 0 :
                (CodedOutputStream.computeRawVarint32Size(messageSize) + messageSize));

            out.writeInt(length);
            resp.writeDelimitedTo(out);
            if (message != null) {
                message.writeDelimitedTo(out);
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
