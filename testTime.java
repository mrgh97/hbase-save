import java.net.URI;

import java.io.BufferedReader;

import java.io.IOException;

import java.io.InputStreamReader;

import java.util.List;

import java.util.ArrayList;

import java.util.Scanner;

import java.util.Date;

import java.text.SimpleDateFormat;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.hbase.Cell;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.HColumnDescriptor;

import org.apache.hadoop.hbase.HConstants;

import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.Result;

import org.apache.hadoop.hbase.client.ResultScanner;

import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.Admin;

import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

 
/**
 * @author SinWang
 *
 */
public class CallClass {
	
	
	public static void main(String[] args) throws IOException{
        Configuration config = HBaseConfiguration.create();
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        try {
            conn = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Scanner s = new Scanner(System.in);
        String name = s.nextLine();
        Table table = conn.getTable(TableName.valueOf(name));

		double CORR = 0.0;
		List<String> xList = new ArrayList<String>();
		List<String> yList = new ArrayList<String>();
        System.out.println("批量计算Pearson相关系数");
        
        Scan scan = new Scan();
        ResultScanner rscan = table.getScanner(scan);
//		String filePath = ".\\例11.6.xls";
//	    FileInputStream stream = new FileInputStream(filePath);
//	    HSSFWorkbook workbook = new HSSFWorkbook(stream);//读取现有的Excel
//	    HSSFSheet sheet= workbook.getSheet("Sheet3");//得到指定名称的Sheet
	//HSSFRow Row=null;
	// HSSFCell Cell=null;
	
	for (Result rs : rscan)
	{
        System.out.println("Row:" + Bytes.toString(rs.getRow()));
        Cell[] cells = rs.rawCells();
        int count = 0;
        for (Cell cell : cells)
        {
            // System.out.print(cell.getCellType());
            //如果是第一列就把它放到xlist，如果是第二列就把它放到ylist
            if(count==0){
                //Get the value of the cell as a number.      return double
                String x1=Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.print(x1+"\t");
                //String x1=Double.toString(x);
                xList.add(x1);
                count++;
            }else if(count==1){
                //Get the value of the cell as a number.      return double
                String y1=Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.print(y+"");
                //String y1=Double.toString(y);
                yList.add(y1);
                count++;
            }
            else{
                break;
            }
        }
 	System.out.println();
	}
		
 
		
		NumeratorCalculate nc = new NumeratorCalculate(xList,yList);
		double numerator = nc.calcuteNumerator();
		DenominatorCalculate dc = new DenominatorCalculate();
		double denominator = dc.calculateDenominator(xList, yList);
		CORR = numerator/denominator;
		System.out.println("运算结果是：");
		System.out.printf("CORR = "+CORR);
	}
}
 

