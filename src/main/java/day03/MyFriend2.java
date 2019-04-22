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
import java.util.Arrays;

public class MyFriend2 {
    public static class MapTask extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //A:B,C,D,F,E,O
            String[] split = value.toString().split("\t");
            String val = split[0];
            String users = split[1];
            String[] zuhe = users.split(",");
            Arrays.sort(zuhe);
            for (int i = 0; i < zuhe.length-1; i++) {
                for (int j = i+1; j < zuhe.length; j++) {
                    String line = zuhe[i]+"-"+zuhe[j];
                    context.write(new Text(line),new Text(val));
                }
            }
        }
    }

    public static class ReduceTash extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            boolean flag = true;
            for (Text value : values) {
                if (flag) {
                    sb.append(value);
                    flag = false;
                }else {
                    sb.append(",").append(value);
                }
            }
            context.write(key,new Text(sb.toString()));
        }
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            //设置类型
            job.setMapperClass(MyFriend2.MapTask.class);
            job.setReducerClass(MyFriend2.ReduceTash.class);
            job.setJarByClass(MyFriend2.class);

            //设置输出参数
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //设置出入输出路径
            FileInputFormat.addInputPath(job,new Path("F:\\F:\\hellocount/part-r-00000"));
            FileOutputFormat.setOutputPath(job,new Path("F:\\hellocount2"));
            boolean completion = job.waitForCompletion(true);
            System.out.println(completion?"老铁没毛病":"出bug，赶紧修改");
        }
    }
}
