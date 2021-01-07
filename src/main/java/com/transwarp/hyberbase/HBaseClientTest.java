package com.transwarp.hyberbase;

import org.apache.thrift.TException;

import javax.security.sasl.SaslException;

public class HBaseClientTest {
    public static void main(String[] args) throws SaslException, TException {
        HBaseClient client = new HBaseClient("31.0.141.193", 32672);
        client.openTransport();
        client.listTables();
    }
}
