import java.net.URI;

import java.io.BufferedReader;

import java.io.IOException;

import java.io.InputStreamReader;

import java.text.SimpleDateFormat;

import java.util.Date;

import java.util.List;

import java.util.ArrayList;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.HColumnDescriptor;

import org.apache.hadoop.hbase.HConstants;

import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.Admin;

import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;



public class save3{



    private static final String TABLE_NAME = "save3";

    private static final String CF_DEFAULT = "CF";

    private static final String uri="hdfs://localhost:9000/user/hadoop/input/a11.txt";

	private static Connection conn = null;



    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {

        if (admin.tableExists(table.getTableName())) {

            admin.disableTable(table.getTableName());

            admin.deleteTable(table.getTableName());

        }

        admin.createTable(table);

    }



    public static void createSchemaTables(Configuration config) throws IOException {

        try (Connection connection = ConnectionFactory.createConnection(config);

             Admin admin = connection.getAdmin()) {



            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

            table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));



            System.out.print("Creating table. ");

            createOrOverwrite(admin, table);

            System.out.println(" Done.");

        }

    }



	public static void AddData(Configuration config) throws IOException {



        TableName tableName = TableName.valueOf(TABLE_NAME);

		Table table = conn.getTable(tableName);

//		Instantiating Put class

//		accepts a row name.

		FileSystem fileSystem=FileSystem.get(URI.create(uri), config);

		FSDataInputStream in=fileSystem.open(new Path(uri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));	

//		FileStatus fileStatus=fileSystem.getFileStatus(new Path(uri));

        String line;
        String date = "begin";
            
        List<Put> puts = new ArrayList<Put>();

		while((line=br.readLine())!=null){

            String[] contents = line.split(",");
            String str = stampToDate(contents[0]);
            if(date!=str.substring(0,str.indexOf(":"))){
                date = str.substring(0,str.indexOf(":"));
            }
            
            for(int i = 1;i<contents.length;i++){
            
            	Put p = new Put(Bytes.toBytes("c"+i+date));	
		String columnName = str.substring(str.indexOf(":")+1,str.length());

		p.addColumn(Bytes.toBytes(CF_DEFAULT),Bytes.toBytes(columnName),Bytes.toBytes(contents[i]));
            	puts.add(p);	
            }
	
	 

            if(puts.size()>=8000){
                table.put(puts);
                puts.clear();
                System.out.println("insert to table");
            }

	}

        if(puts.size()!=0){
            table.put(puts);
            puts.clear();
        }
//		in.read(4096, buffer, 0, 1024);

//		IOUtils.copyBytes(in, System.out, 4096, false);

		br.close();

		IOUtils.closeStream(in);

//		Put p = new Put(Bytes.toBytes("row1")); 



		  // adding values using add() method

		  // accepts column family name, qualifier/row name ,value

//		p.addColumn(Bytes.toBytes(CF_DEFAULT),Bytes.toBytes("name"),Bytes.toBytes("lalala"));



//		p.addColumn(Bytes.toBytes(CF_DEFAULT),Bytes.toBytes("city"),Bytes.toBytes("hyderabad"));

		  

		  // Saving the put Instance to the Table.

//		table.put(p);

		System.out.println("data inserted");

		  

		  // closing Table

		table.close();

    }
    public static void testTime(Configuration config) throws IOException{
        //TableName tableName = TableName.valueOf(TABLE_NAME);

		//Table table = conn.getTable(tableName);

//		Instantiating Put class

//		accepts a row name.

		FileSystem fileSystem=FileSystem.get(URI.create(uri), config);

		FSDataInputStream in=fileSystem.open(new Path(uri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));	

//		FileStatus fileStatus=fileSystem.getFileStatus(new Path(uri));

        String line;
        String hour;
        int k = 0;
        while((line = br.readLine())!=null){
            String[] contents = line.split(",");
            System.out.println(stampToDate(contents[0]));
            hour = stampToDate(contents[0]).substring(stampToDate(contents[0]).indexOf(" ")+1,stampToDate(contents[0]).indexOf(":"));
            System.out.println(hour);
            k++;
            if(k==10)break;
        }
        br.close();

		IOUtils.closeStream(in);
    }

/* 
     * 将时间戳转换为时间
     */
    public static String stampToDate(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }

    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();

        //Add any necessary configuration files (hbase-site.xml, core-site.xml)

        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));

        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));

        try {

            conn = ConnectionFactory.createConnection(config);

        } catch (IOException e) {

            e.printStackTrace();

        } 

        createSchemaTables(config);

        AddData(config);

        //modifySchema(config);

        //testTime(config);

    }

}
