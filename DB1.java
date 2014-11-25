//STEP 1. Import required packages
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.sql.*;

public class DBdemo  {
   // JDBC driver name and database URL
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://54.86.203.127/TEST";

   //  Database credentials
   static final String USER = "bill";
   static final String PASS = "passpass";
   
   
   public static void create_table()
   {
	   Connection conn = null;
	   Statement stmt = null;
	   
	   try
	   {
		   Class.forName("com.mysql.jdbc.Driver");
		   conn = DriverManager.getConnection(DB_URL,USER,PASS);
		   System.out.println("Connecting to database...");
		   conn = DriverManager.getConnection(DB_URL,USER,PASS);
		   stmt = conn.createStatement();
		   String sql = "CREATE TABLE augurdata" +
                   "(movie_name VARCHAR(255),"+
				   "twitter_score INTEGER, "+
                   "RT_audience_cmt INTEGER, "+
				   "RT_critic_cmt INTEGER, "+
                   "RT_audience_score INTEGER,"+
				   "RT_critic_score INTEGER,"+
                   "YT_cmt_score INTEGER,"+
				   "YT_views INTEGER,"+
                   "YT_likes INTEGER,"+
				   "YT_dislikes INTEGER,"+
                   "img_path VARCHAR(255),"+
				   "collection BIGINT,"+
                   "pred_collection BIGINT,"+
                   " PRIMARY KEY ( movie_name ))"; 
		   stmt.executeUpdate(sql);
		   System.out.println("Created table in given database.");
	   }
	   catch(SQLException se){
		      //Handle errors for JDBC
		      se.printStackTrace();
		   }catch(Exception e){
		      //Handle errors for Class.forName
		      e.printStackTrace();
		   }finally
		   {
		      //finally block used to close resources
		      try{
		         if(stmt!=null)
		            stmt.close();
		      }catch(SQLException se2)
		      {
		      }// nothing we can do
		      try
		      {
		         if(conn!=null)
		            conn.close();
		      }catch(SQLException se)
		      {
		         se.printStackTrace();
		      }//end finally try
		   }//end try
		   System.out.println("Goodbye!");
   }
   
   public static void  load_entry(movie_data movie)
   {
	   Connection conn = null;
	   Statement stmt = null;
	   
	   try
	   {
		   Class.forName("com.mysql.jdbc.Driver");
		   conn = DriverManager.getConnection(DB_URL,USER,PASS);
		   System.out.println("Connecting to database...");
		   conn = DriverManager.getConnection(DB_URL,USER,PASS);
		   PreparedStatement preparedStatement = null;
		   String sql = "INSERT INTO augurdata " +
				   		"(movie_name, twitter_score, RT_audience_cmt,  RT_critic_cmt,RT_audience_score,RT_critic_score,"
				   		+ "YT_cmt_score,YT_views,YT_likes,YT_dislikes,collection) VALUES"
				   		+ "(?,?,?,?,?,?,?,?,?,?,?)"; 
		   preparedStatement = conn.prepareStatement(sql);
		   preparedStatement.setString(1, movie.movie_name);
		   preparedStatement.setInt(2, movie.twitter_score);
		   preparedStatement.setInt(3, movie.RT_audience_cmt);
		   preparedStatement.setInt(4, movie.RT_critic_cmt);
		   preparedStatement.setInt(5, movie.RT_audience_score);
		   preparedStatement.setInt(6, movie.RT_critic_score);
		   preparedStatement.setInt(7, movie.YT_cmt_score);
		   preparedStatement.setInt(8, movie.YT_views);
		   preparedStatement.setInt(9, movie.YT_likes);
		   preparedStatement.setInt(10, movie.YT_dislikes);
		   preparedStatement.setLong(11, movie.collections);
		   preparedStatement.executeUpdate();
		   System.out.println("Record is inserted into table.");
	   }
	   catch(SQLException se){
		      //Handle errors for JDBC
		      se.printStackTrace();
		   }catch(Exception e){
		      //Handle errors for Class.forName
		      e.printStackTrace();
		   }finally
		   {
		      //finally block used to close resources
		      try{
		         if(stmt!=null)
		            stmt.close();
		      }catch(SQLException se2)
		      {
		      }// nothing we can do
		      try
		      {
		         if(conn!=null)
		            conn.close();
		      }catch(SQLException se)
		      {
		         se.printStackTrace();
		      }//end finally try
		   }//end try
		   System.out.println("Goodbye!");
   }
   
   public static void load_file2DB(String path)
   {
	   movie_data temp = new movie_data();
	   try{
		    System.out.println("start inside load_file2DB");
	        String line=null;
	        FileReader fr = new FileReader(path);
	        BufferedReader br = new BufferedReader(fr);
	        //&& (line.isEmpty() || line.trim().equals("") || line.trim().equals("\n"))
	        while ((line = br.readLine()) != null ) {
	            String[] splited = line.split("\t");
	            temp.movie_name=splited[0];
	            temp.twitter_score=Integer.parseInt(splited[1]);
	            temp.RT_audience_cmt = Integer.parseInt(splited[2]);
	            temp.RT_critic_cmt = Integer.parseInt(splited[3]);
	            temp.RT_audience_score = Integer.parseInt(splited[4]);
	            temp.RT_critic_score = Integer.parseInt(splited[5]);
	            temp.YT_cmt_score = Integer.parseInt(splited[6]);
	            temp.YT_views = Integer.parseInt(splited[7]);
	            temp.YT_likes = Integer.parseInt(splited[8]);
	            temp.YT_dislikes = Integer.parseInt(splited[9]); 
	            temp.collections = Long.parseLong(splited[10]);
	            load_entry(temp);
	            System.out.println("inside load_file2DB");
	        }
	        br.close();

	    }
	    catch (IOException e) 
	    {
	    	System.out.println("There was a problem: " + e);
    	}
	
   }
   
   public static void read_folder(String path) throws Exception
   {
	   File folder = new File(path);
	   File[] files = folder.listFiles();
	   String file_path;
	   if (files==null)
		   {
		   		System.out.println("null in directory");
		   }
	   else
	   {
		   for (int i = 0;i<files.length;i++)
		   {
			   file_path = files[i].getAbsolutePath();
			   System.out.println(file_path);
			   load_file2DB(file_path);
			   
		   }
	   }
   }
   
   public  static void main(String[] args) throws Exception
   {
	   String option1 = args[0];
	   
	   if (option1.equals("create") )
		   create_table();
	   else if (option1.equals("load"))
	   {
		   System.out.println("load");
		   read_folder(args[1]);
		   System.out.println("load done "+ args[1]);
	   }
		   
}//end main
}//end FirstExample
