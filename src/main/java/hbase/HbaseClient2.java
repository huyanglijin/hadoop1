package hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseClient2 {
    public static void main(String[] args)throws Exception {
        boolean lijin = exits("lijin");
    }
    //全局的连接对象
    public static Connection connection;
    //静态代码块
    static {
        //获取conf
        HBaseConfiguration conf = new HBaseConfiguration();
        //设置参数
        conf.set("hbase.zookeeper.quorum","192.168.220.110");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        //获取连接对象
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //判断表是否存在
    public static boolean exits(String table)throws Exception{
        //总指挥的对象
        Admin admin = connection.getAdmin();
        //判断
        boolean b = admin.tableExists(TableName.valueOf(table));
        return b;
    }
    //创建表
    public static void createtable(String table,String...ji1)throws Exception{
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor();
        for (String s : ji1) {
            hTableDescriptor.addFamily(new HColumnDescriptor(s));
        }
        //创建
        admin.createTable(hTableDescriptor);
        //创建成功
        System.out.println("创建成功");
    }
    //删除表
    public static void deltable(String table)throws Exception{
        Admin admin = connection.getAdmin();
        //
        admin.disableTable(TableName.valueOf(table));
        if (exits(table)){
            admin.deleteTable(TableName.valueOf(table));
            System.out.println("删除成功");
        }
    }
    //添加参数
    public static void addtable(String table,String rowkey,String columnfamily,String column,String value)throws Exception{
        //首先拿到table表
        Table table1 = connection.getTable(TableName.valueOf(table));
        //new put对象
        Put put = new Put(Bytes.toBytes(rowkey));
        //接下来添加了
        put.add(Bytes.toBytes(columnfamily),Bytes.toBytes(column),Bytes.toBytes(value));
        //接下来告知上边，我有值了
        table1.put(put);
        System.out.println("添加成功");
    }

        //scan
        public static void show(String table)throws Exception{
            Table table1 = connection.getTable(TableName.valueOf(table));
            Scan scan = new Scan();
            ResultScanner scanner = table1.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.println(cell.getFamilyArray().toString());
                    System.out.println(cell.getRow().toString());
                    System.out.println(cell.getQualifierArray().toString());
                    System.out.println(cell.getValue().toString());
                }
            }
        }

        //get
    public static void get(String table,String rowkey)throws Exception{
        Table table1 = connection.getTable(TableName.valueOf(table));
        Get get = new Get(Bytes.toBytes(table));
        Result result = table1.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(cell.getQualifierArray().toString());
            System.out.println(cell.getRow().toString());
            System.out.println(cell.getQualifierArray().toString());
            System.out.println(cell.getValue().toString());
        }
    }
}
