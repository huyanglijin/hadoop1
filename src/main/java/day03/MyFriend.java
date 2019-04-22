package day03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyFriend {
    public static class MapTask extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //先切分  ，以：
            String[] split = value.toString().split(":");
            String user=split[0];
            String slists = split[1];
            //切分
            String[] friends = slists.split(",");
            for (String friend:friends) {
                context.write(new Text(friend),new Text(user));
            }
        }
    }



    //reduce端
    public static class ReduceTask  extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //用来放拼接字符串的
            StringBuffer sb = new StringBuffer();
            boolean flag = true;
            for (Text value:values) {
                if (flag){
                    sb.append(value);
                    flag = false;
                }else {
                    sb.append(",").append(value);
                }
            }
            //写出去
            context.write(key,new Text(sb.toString()));
        }
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            //设置类型
            job.setMapperClass(MyFriend.MapTask.class);
            job.setReducerClass(MyFriend.ReduceTask.class);
            job.setJarByClass(MyFriend.class);

            //设置输出参数
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //设置出入输出路径
            FileInputFormat.addInputPath(job,new Path("F:\\friend.txt"));
            FileOutputFormat.setOutputPath(job,new Path("F:\\hellocount"));
            boolean completion = job.waitForCompletion(true);
            System.out.println(completion?"老铁没毛病":"出bug，赶紧修改");
        }
    }
}
