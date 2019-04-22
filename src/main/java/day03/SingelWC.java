package day03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;

public class SingelWC {
    public static void main(String[] args) throws Exception {

        //new hashmap
        HashMap<String,Integer> map = new HashMap<String,Integer>();

        //获取连接
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);

        //读取数据
        FSDataInputStream open = fileSystem.open(new Path("F:\\write1.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(open));

        //map处理数据（可以，value）
        String line = null;
        //一行一行读取数据
        while ((line = br.readLine())!=null){
            String[] spints = line.split(" ");
            for (String word : spints) {
                Integer count = map.getOrDefault(word,0);
                count++;
                map.put(word,count);
            }
        }

        //给reduce
        //给一个写出去的路径
        FSDataOutputStream out = fileSystem.create(new Path("F:\\a1.txt"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        //遍历map
        Set<Map.Entry<String,Integer>> entries = map.entrySet();
        List<Map.Entry<String,Integer>> list = new ArrayList<Map.Entry<String, Integer>>(entries);
        list.sort(new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o2.getValue()-o1.getValue();
            }
        });
        for (Map.Entry<String,Integer> entry:list){
            bw.write(entry.getKey()+"="+entry.getValue()+"\t\n");
            bw.newLine();
        }

        //关流
        br.close();
        bw.close();
    }
}
