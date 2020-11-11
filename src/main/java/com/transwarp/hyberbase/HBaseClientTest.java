package com.transwarp.hyberbase;

import org.apache.thrift.TException;

import javax.security.sasl.SaslException;

public class HBaseClientTest {
    public static void main(String[] args) throws SaslException, TException {
        HBaseClient client = new HBaseClient("172.18.8.27", 30391);
        client.openTransport();
        client.listTables();
    }
}
