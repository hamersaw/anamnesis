package com.bushpath.anamnesis.rpc;

import com.google.protobuf.Message;

public interface RpcHandler {
    public abstract Message handle(String method, byte[] message) throws Exception;
}
