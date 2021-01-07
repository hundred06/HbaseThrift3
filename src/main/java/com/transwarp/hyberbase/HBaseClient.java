package com.transwarp.hyberbase;

import org.apache.hadoop.hbase.thrift.generated.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.security.sasl.SaslException;
import java.nio.ByteBuffer;
import java.util.*;

public class HBaseClient {
    public static Hbase.Client  hbaseClient;
    private String hbaseAddr = "";
    private Integer hbasePort = 0;
    private TTransport socket = null;
    private TProtocol protocol = null;
    protected static final String CHAR_SET = "UTF-8";
    public HBaseClient(String address, Integer port) throws SaslException {
        hbaseAddr = address;
        hbasePort = port;
        socket = new TSocket(hbaseAddr, hbasePort);
//        TFramedTransport transport = new TFramedTransport(socket);
        protocol = new TBinaryProtocol(socket, true, true);
        hbaseClient = new Hbase.Client(protocol);
    }
    public void listTables() throws TException {
        for(ByteBuffer tableByteBuffer : hbaseClient.getTableNames()){
            System.out.println(new String(toBytes(tableByteBuffer)));
        }
    }


    public void updateRow1(String  table,String rowKey, List<Map<String, String>> columns, List<Map<String, byte []>> data) throws TException {
        boolean writeToWal = false;
        List<Mutation> mutations = new ArrayList<Mutation>();
        if(columns != null){
            for(Map< String, String> element: columns){
                for(Map.Entry < String, String> entry: element.entrySet()){
                    mutations.add(new Mutation(false, getByteBuffer(entry.getKey()), getByteBuffer(entry.getValue()), writeToWal));
                }
            }
        }
        if(data != null){
            for (Map<String, byte []> element : data){
                for(Map.Entry<String, byte []> entry : element.entrySet()) {
                    mutations.add(new Mutation(false, getByteBuffer(entry.getKey()), ByteBuffer.wrap(entry.getValue()), writeToWal));
                }
            }
        }
        ByteBuffer tableName = getByteBuffer(table);
        ByteBuffer row = getByteBuffer(rowKey);
        hbaseClient.mutateRow(tableName, row, mutations, null);

    }


    public void updateRow(String  table,String rowKey, List<Map<String, byte []>> data) throws TException {
        boolean writeToWal = false;
//        Map<String, String> attributes = new HashMap<String, String>();
        List<Mutation> mutations = new ArrayList<Mutation>();
        for (Map<String, byte []> element : data){
            for(Map.Entry<String, byte []> entry : element.entrySet()) {
                mutations.add(new Mutation(false, getByteBuffer(entry.getKey()), ByteBuffer.wrap(entry.getValue()), writeToWal));
            }
        }

        ByteBuffer tableName = getByteBuffer(table);
        ByteBuffer row = getByteBuffer(rowKey);
        hbaseClient.mutateRow(tableName, row, mutations, null);

    }
    public static void dropTable(String tableName) throws TException {
        hbaseClient.disableTable(getByteBuffer(tableName));
        hbaseClient.deleteTable(getByteBuffer(tableName));
    }
    public void query(String table, List<String> rowKeys) throws TException {
        ByteBuffer tableName = getByteBuffer(table);
        List<ByteBuffer> rows = new ArrayList<ByteBuffer> ();
        for(String key: rowKeys){
            ByteBuffer row = getByteBuffer(key);
            rows.add(row);
        }
        List<TRowResult> scanResults = hbaseClient.getRowsWithColumns(tableName, rows, new ArrayList<ByteBuffer>(0), null);
//        System.out.println(scanResults.size());
        if(scanResults != null && !scanResults.isEmpty()) {
            for (TRowResult rslt : scanResults) {
                iterateResults(rslt);
            }
        }
    }
    public void deleteRow(String table, String rowKey) throws TException {
        ByteBuffer tableName = getByteBuffer(table);
        ByteBuffer row = getByteBuffer(rowKey);
        hbaseClient.deleteAllRow(tableName, row, getAttributesMap(new HashMap<String, String>()));
    }
    public static void createTable(String tableName, List<String> rowFamilies) throws Exception {

        ByteBuffer tableNameByte = getByteBuffer(tableName);

        List<ColumnDescriptor> columnFamilies = new ArrayList<ColumnDescriptor>();

        for (String f : rowFamilies){
            ColumnDescriptor cd = new ColumnDescriptor();
            cd.setName(getByteBuffer(f));
            columnFamilies.add(cd);
//            System.out.println(new String(f));
        }
//        ColumnDescriptor cd1 = new ColumnDescriptor();
//        cd1.setName(getByteBuffer(f1));
//
//        ColumnDescriptor cd2 = new ColumnDescriptor();
//        cd2.setName(getByteBuffer(f2));
//
//        ColumnDescriptor cd3 = new ColumnDescriptor();
//        cd3.setName(getByteBuffer(f3));
//
//        columnFamilies.add(cd1);
//        columnFamilies.add(cd2);
//        columnFamilies.add(cd3);

        hbaseClient.createTable(tableNameByte,columnFamilies);
        System.out.println(hbaseClient.toString());
    }
    public void scanData(String tableName) throws TException{
        String startRow = "";
        int rowCnt = 1000;

        List<String> columns = new ArrayList<String>(0);
        Map<String, String> attributesTest = new HashMap<String, String>();
        int scannerID = scannerOpen(tableName, startRow, columns, attributesTest);
        try {
            List<TRowResult> scanResults = scannerGetList(scannerID, rowCnt);
            while (scanResults != null && !scanResults.isEmpty()) {
                for (TRowResult rslt : scanResults) {
                    iterateResults(rslt);
                }
                scanResults = scannerGetList(scannerID, rowCnt);
            }
        } finally {
            scannerClose(scannerID);
        }
    }
    public void iterateResults(TRowResult result) {
        Iterator<Map.Entry<ByteBuffer, TCell>> iter = result.columns.entrySet().iterator();
        System.out.println("RowKey:" + new String(result.getRow()));
        while (iter.hasNext()) {
            Map.Entry<ByteBuffer, TCell> entry = iter.next();
            if(entry.getKey().equals(getByteBuffer("f3:IMAGE"))){
                FileUtil.byte2File(entry.getValue().getValue(), "C:\\Users\\jinyupeng\\Desktop\\HbaseThrift1\\files", "b.jpg");
            }else if(entry.getKey().equals(getByteBuffer("f2:ZP"))){

            }
            else{
                System.out.println("\tCol=" + new String(toBytes(entry.getKey())) + ", Value=" + new String(entry.getValue().getValue()));
            }
        }
    }
    public void deleteCells(String table, String rowKey, List<String> columns) throws TException {
        boolean writeToWal = false;
        List<Mutation> mutations = new ArrayList<Mutation>();
        for (String column : columns) {
            mutations.add(new Mutation(false, getByteBuffer(column), null, writeToWal));
        }
        ByteBuffer tableName = getByteBuffer(table);
        ByteBuffer row = getByteBuffer(rowKey);
        hbaseClient.mutateRow(tableName, row, mutations, getAttributesMap(new HashMap<String, String>()));
    }
    public List<TRowResult> scannerGetList(int id, int nbRows)throws TException {
        return hbaseClient.scannerGetList(id, nbRows);
    }
    public int scannerOpen(String table, String startRow, List<String> columns, Map<String, String> attributes) throws TException {
        ByteBuffer tableName = getByteBuffer(table);
        List<ByteBuffer> blist = getColumnsByte(columns);
        Map<ByteBuffer, ByteBuffer> wrappedAttributes = getAttributesMap(attributes);
        return hbaseClient.scannerOpen(tableName, getByteBuffer(startRow), blist, wrappedAttributes);
    }
    public void scannerClose(int id) throws TException {
        hbaseClient.scannerClose(id);
    }
    public List<ByteBuffer> getColumnsByte(List<String> columns) {
        List<ByteBuffer> blist = new ArrayList<ByteBuffer>();
        for(String column : columns) {
            blist.add(getByteBuffer(column));
        }
        return blist;
    }
    public static Map<ByteBuffer, ByteBuffer> getAttributesMap(Map<String, String> attributes) {
        Map<ByteBuffer, ByteBuffer> attributesMap = null;
        if(attributes != null && !attributes.isEmpty()) {
            attributesMap = new HashMap<ByteBuffer, ByteBuffer>();
            for(Map.Entry<String, String> entry : attributes.entrySet()) {
                attributesMap.put(getByteBuffer(entry.getKey()), getByteBuffer(entry.getValue()));
            }
        }
        return attributesMap;
    }
    public byte[] toBytes(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.limit()];
        for (int i = 0; i < buffer.limit(); i++) {
            bytes[i] = buffer.get();
        }
        return bytes;
    }
    public ByteBuffer encodeValue(byte[] value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(value.length);
        byteBuffer.clear();
        byteBuffer.get(value, 0, value.length);
        return byteBuffer;
    }
    public void openTransport() throws TTransportException {
        if (socket != null) {
            socket.open();
        }
    }
    public void closeTransport() throws TTransportException {
        if (socket != null) {
            socket.close();
        }
    }
    public static ByteBuffer getByteBuffer(String str) {
        return ByteBuffer.wrap(str.getBytes());
    }
}
