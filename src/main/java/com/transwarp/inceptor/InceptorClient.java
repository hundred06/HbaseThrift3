package com.transwarp.inceptor;
import	java.util.ArrayList;
import java.sql.*;
import	java.util.HashMap;

public class InceptorClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public InceptorClient(){
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void updateV3(String jdbcURL, String user, String password, HashMap oneRow, String sql, String rowKey) throws SQLException{
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            if(user == ""){
                conn = DriverManager.getConnection(jdbcURL);
            }else{
                conn = DriverManager.getConnection(jdbcURL, user, password);
            }
            pstmt = conn.prepareStatement(sql);
            pstmt.setBytes(1, (byte[]) oneRow.get("img"));
            pstmt.setString(2, (String) oneRow.get(rowKey));
            pstmt.executeUpdate();
//            pstmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }
            if (conn != null) {
                conn.close();
            }

        }
    }


    public void updateV2(String jdbcURL, String user, String password, ArrayList<HashMap> data, String sql, String rowKey) throws SQLException{
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            if(user == ""){
                conn = DriverManager.getConnection(jdbcURL);
            }else{
                conn = DriverManager.getConnection(jdbcURL, user, password);
            }
            pstmt = conn.prepareStatement(sql);
            System.out.println(sql);
            for(int i = 0; i < data.size(); i++){
                HashMap oneRow = data.get(i);
                pstmt.setBytes(1, (byte[]) oneRow.get("img"));
                pstmt.setString(2, (String) oneRow.get(rowKey));
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (pstmt != null) {
                pstmt.close();
            }
            if (conn != null) {
                conn.close();
            }

        }
    }

    public boolean update(String jdbcURL, String user, String password, ArrayList<HashMap> data, String sql, String[] columns) throws SQLException{
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        ArrayList totalData = new ArrayList();
        Boolean flag = false;
                try {
            if(user == ""){
                conn = DriverManager.getConnection(jdbcURL);
            }else{
                conn = DriverManager.getConnection(jdbcURL, user, password);
            }
            pstmt = conn.prepareStatement(sql);
            for(int i = 0; i < data.size(); i++){
                HashMap oneRow = data.get(i);
                pstmt.setString(1, (String) oneRow.get("taskid"));
                pstmt.setString(2, (String) oneRow.get("infosourcename"));
                pstmt.setString(3, (String) oneRow.get("infotypename"));
                pstmt.setString(4, (String) oneRow.get("img_name"));
                pstmt.setObject(5, oneRow.get("synctime"));
                pstmt.setObject(6, oneRow.get("img_id"));
                pstmt.setString(7, (String) oneRow.get("uimg_src"));
                pstmt.setString(8, (String) oneRow.get("uimg_name"));
                pstmt.setObject(9, oneRow.get("usynctime"));
                pstmt.setBytes(10, (byte[]) oneRow.get("img"));
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            int res = pstmt.executeUpdate();
            if(res > 0){
                flag = true;
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (pstmt != null) {
                pstmt.close();
            }
            if (conn != null) {
                conn.close();
            }

        }
        return flag;
    }

    public ArrayList<HashMap <String, String>> getData(String jdbcURL, String user, String password, String sql) throws SQLException {
        //Hive2 JDBC URL with LDAP

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        ArrayList totalData = new ArrayList();
        try {
            if(user == ""){
                conn = DriverManager.getConnection(jdbcURL);
            }else{
                conn = DriverManager.getConnection(jdbcURL, user, password);
            }

            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            int size = rsmd.getColumnCount();
            while (rs.next()) {
//                StringBuffer value = new StringBuffer();
                HashMap map = new HashMap();
                for (int i = 0; i < size; i++) {
//                    value.append(rs.getString(i + 1)).append("\t");
//                    System.out.println(rs.getDate(2));
//                    rsmd.getColumnName(i + 1);
                    map.put(rsmd.getColumnName(i+1), rs.getString(i + 1));
                }
//                System.out.println(value.toString());
                totalData.add(map);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }

        }
        return totalData;
    }
}
