package com.lsp.spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Top5BySparkSQL {
	public static void main(String[] args) {
		String sql="select name from people whre age=?";
		Connection conn=null;
		ResultSet resultSet=null;
		
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			
			conn=DriverManager.getConnection("jdbc:hive2://master:10001/hive?"
					+ "hive.server2.transport.mode=http;hive.server2.thrift.http.path=clientservice",
					"root","");
			
			PreparedStatement ps=conn.prepareStatement(sql);
			ps.setInt(1, 30);
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
