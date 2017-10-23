package com.bushpath.anamnesis.client;

public class Location {
    private String ipAddr;
    private int port;

    public Location(String ipAddr, int port) {
        this.ipAddr = ipAddr;
        this.port = port;
    }

    public String getIpAddr() {
        return this.ipAddr;
    }

    public int getPort() {
        return port;
    }
}
