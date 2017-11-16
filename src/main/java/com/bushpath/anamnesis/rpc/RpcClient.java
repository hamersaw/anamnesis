package com.bushpath.anamnesis.rpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Random;

public class RpcClient {
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private String user, protocol;
    private int callId;
    private byte[] clientId;

    public RpcClient(String host, int port, String user, String protocol) 
            throws Exception {
        // connect to host
        this.socket = new Socket(host, port);
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
        this.user = user;
        this.protocol = protocol;
        this.callId = 0;
        this.clientId = new byte[4];

        // initialize clientId
        Random random = new Random(System.currentTimeMillis());
        random.nextBytes(this.clientId);

        // write connection Header
        out.write("hrpc".getBytes());
        out.write((byte)9);
        out.write((byte)0);
        out.write((byte)0);

        // initialize RpcRequeestHeaderProto
        RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto =
            buildRpcRequestHeaderProto(
                RpcHeaderProtos.RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET,
                -3, this.clientId);

        // initialize IpcConnectionContextProto
        IpcConnectionContextProtos.UserInformationProto userInformationProto =
            IpcConnectionContextProtos.UserInformationProto.newBuilder()
                .setEffectiveUser(user)
                .build();
        
        IpcConnectionContextProtos.IpcConnectionContextProto ipcConnectionContextProto =
            IpcConnectionContextProtos.IpcConnectionContextProto.newBuilder()
                .setUserInfo(userInformationProto)
                .setProtocol(protocol)
                .build();
        
        // write to output stream
        out.writeInt(1 + rpcRequestHeaderProto.getSerializedSize()
            + 1 + ipcConnectionContextProto.getSerializedSize());
        out.writeByte((byte) rpcRequestHeaderProto.getSerializedSize());
        rpcRequestHeaderProto.writeTo(out);
        out.writeByte((byte) ipcConnectionContextProto.getSerializedSize());
        ipcConnectionContextProto.writeTo(out);
        out.flush();
    }

    public byte[] send(String methodName, Message req)
            throws Exception {
        if (this.socket == null) {
            throw new Exception("rpc client has been closed");
        }

        // initialize RpcRequestHeaderProto
        RpcHeaderProtos.RpcRequestHeaderProto rpcRequestHeaderProto =
            buildRpcRequestHeaderProto(
                RpcHeaderProtos.RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET,
                this.callId, this.clientId);
        this.callId += 1;
 
        // initialize RequestHeaderProto
        ProtobufRpcEngineProtos.RequestHeaderProto requestHeaderProto =
            ProtobufRpcEngineProtos.RequestHeaderProto.newBuilder()
                .setMethodName(methodName)
                .setDeclaringClassProtocolName(this.protocol)
                .setClientProtocolVersion(0) // TODO - fix this
                .build();

        // write to output stream
        int rpcRequestHeaderSize = rpcRequestHeaderProto.getSerializedSize();
        int requestHeaderSize = requestHeaderProto.getSerializedSize();
        int reqSize = req.getSerializedSize();
        int length = CodedOutputStream.computeRawVarint32Size(rpcRequestHeaderSize)
            + rpcRequestHeaderSize 
            + CodedOutputStream.computeRawVarint32Size(requestHeaderSize) 
            + requestHeaderSize 
            + CodedOutputStream.computeRawVarint32Size(reqSize) + reqSize;

        out.writeInt(length);
        rpcRequestHeaderProto.writeDelimitedTo(out);
        requestHeaderProto.writeDelimitedTo(out);
        req.writeDelimitedTo(out);

        // read response
        int packetLength = in.readInt();
        RpcHeaderProtos.RpcResponseHeaderProto rpcResponseHeaderProto =
            RpcHeaderProtos.RpcResponseHeaderProto.parseDelimitedFrom(in);
        // TODO - handle response

        // TODO - return the data input stream (could fix size issues)
        int respLength = (int) in.readByte();
        byte[] respBuf = new byte[respLength];
        in.readFully(respBuf);
        return respBuf;
    }

    private RpcHeaderProtos.RpcRequestHeaderProto buildRpcRequestHeaderProto(
            RpcHeaderProtos.RpcRequestHeaderProto.OperationProto rpcOp,
            int callId, byte[] clientId) {

        return RpcHeaderProtos.RpcRequestHeaderProto.newBuilder()
            .setRpcKind(RpcHeaderProtos.RpcKindProto.RPC_PROTOCOL_BUFFER)
            .setRpcOp(rpcOp)
            .setCallId(callId)
            .setClientId(ByteString.copyFrom(clientId))
            .build();
    }

    public void close() throws IOException {
        this.in.close();
        this.out.close();
        this.socket.close();
        this.socket = null;
    }
}
