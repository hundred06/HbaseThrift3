package com.transwarp.inceptor;

import org.apache.hadoop.hive.common.type.HiveDate;

import java.sql.*;
public class Test{
    //Hive2 Driver class name
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        //Hive2 JDBC URL with LDAP
        String jdbcURL = "jdbc:hive2://31.0.37.142:32101/guomai_xh_jiaoyu;guardianToken=14mViv7KU02gwxDBl8ts-LDPCG4W.TDH";

        String user = "bigdata ";
        String password = "bigdata@123";
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try{
            conn = DriverManager.getConnection(jdbcURL);
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select * from test1");
            ResultSetMetaData rsmd = rs.getMetaData();
            int size = rsmd.getColumnCount();
            while(rs.next()) {
                StringBuffer value = new StringBuffer();
                for(int i = 0; i < size; i++) {
                    value.append(rs.getString(i+1)).append("\t");
                    System.out.println(rs.getDate(2));
                }
                System.out.println(value.toString());
            }
        }catch (Exception e){

        }finally {
            if(rs != null){
                rs.close();
            }
            if(stmt != null){
                stmt.close();
            }
            if(conn != null){
                conn.close();
            }

        }

    }
//    public void query(String querySql){
//
//    }
//    public void insert(){
//        String sql="Insert into stu_info(id,name,sex,age,weight,hight) values(?,?,?,?,?,?)";
//        PreparedStatement pstmt=dbConn.prepareStatement(sql);
//        request.setCharacterEncoding("UTF-8");//设置字符编码，避免出现乱码
//        int id=Integer.parseInt(request.getParameter("id"));
//        String name=request.getParameter("name");
//        String sex=request.getParameter("sex");
//        int age=Integer.parseInt(request.getParameter("age"));
//        float weight=Float.parseFloat(request.getParameter("weight"));
//        float hight=Float.parseFloat(request.getParameter("hight"));
//
//        pstmt.setInt(1,id);//这里不能有空格，虽然这里并不会报错，但运行会出现错误500提示。
//        pstmt.setString(2,name);
//        pstmt.setString(3,sex);
//        pstmt.setInt(4,age);
//        pstmt.setFloat(5,weight);
//        pstmt.setFloat(6,hight);
//        int n=pstmt.executeUpdate();
//    }
}
