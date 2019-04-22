package day02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;


public class Hdfs2 {

    FileSystem fileSystem = null;

    @Before
    public void init()throws Exception{
        // 配置信息 core-site.xml hdfs-site.xml等 没有加的时候就是默认参数
        Configuration conf = new Configuration();
        // 手动设置
        // conf.set(name, value);
        // 获取文件系统
        try {
            fileSystem  = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf,"root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    // 上传 下载 删除 改名 移动 创建文件夹 查看文件信息等
    /**
     * 上传
     * @throws Exception
     */
    @Test
    public void testUpload()throws Exception{
        fileSystem.copyFromLocalFile(new Path("F://大数据.txt"),new Path("/"));
    }

    /**
     * 下载
     */
    @Test
    public void testDownLoad()throws Exception{
        fileSystem.copyToLocalFile(false,new Path("/write.txt"),new Path("F://"),true);
    }

    /**
     * 删除文件
     */
    @Test
    public void testdel()throws Exception{
        fileSystem.delete(new Path("/xx"),true);
    }



    /**
     * 移动和更改名字
     */
    @Test
    public void testrename()throws Exception{
        fileSystem.rename(new Path("/hdfs/autossh.txt"),new Path("/sfdh/autosh.txt"));
    }



    /**
     * 创建目录
     */
    @Test
    public void testmkdir()throws Exception{
        fileSystem.mkdirs(new Path("/xx/yy/zz"));
    }



    /**
     * 文件的详细信息
     */
    @Test
    public void testListDir() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"),true);
        while(listFiles.hasNext()){
            //文件状态   文件的大小 ，存储副本数量 路径
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("path:"+fileStatus.getPath());
            System.out.println("len:"+fileStatus.getLen());
            System.out.println("blockSize:"+fileStatus.getBlockSize());
            System.out.println("replication:"+fileStatus.getReplication());
            //文件的块
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation:blockLocations){
                System.out.println(blockLocation);
            }
            System.out.println("-------------------");
        }
    }


    /**
     * 查看文件夹和文件
     */
    @Test
    public void testListDir2() throws IOException {
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/write.txt"));
        for (FileStatus fileStatus : listStatus){
            if (fileStatus.isDirectory()){
                System.out.println("这是一个目录");
            }
            if (fileStatus.isFile()){
                System.out.println("这是一个文件");
            }
            //文件状态  文件的大小，存储的副本数量，路径
            System.out.println("path:"+fileStatus.getPath());
            System.out.println("len:"+fileStatus.getLen());
            System.out.println("blockSize:"+fileStatus.getBlockSize());
            System.out.println("replication:"+fileStatus.getReplication());
        }
    }


    //使用流数据形式，读本地文件上传到hdfs上 ，和 下载
    @Test
    public void readFile() throws IOException {
        //输入流
        FSDataInputStream open = fileSystem.open(new Path("/write.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(open));
        String line = null;
        while ((line = br.readLine())!=null){
            System.out.println(line);
        }
    }

    /**
     * 写文件
     */
    @Test
    public void writeFile() throws IOException {
        FSDataOutputStream create = fileSystem.create(new Path("/write.txt"));
        create.write("nihao\n".getBytes());
        create.flush();
    }
}
