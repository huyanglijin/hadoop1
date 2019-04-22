package day03;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class Phone {
    public static class MapTask extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

                String[] split = value.toString().split("\t");
                if (split.length>=7){
                String prefix = split[0].substring(0,3);
                String sf = split[1];
                String isp = split[3];
                context.write(new Text(prefix+"\t"+sf), new Text(isp));
            }

        }
    }

    public static class ReduceTask extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(key, text);
                break;
            }
        }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            //设置类型
            job.setMapperClass(Phone.MapTask.class);
            job.setReducerClass(Phone.ReduceTask.class);
            job.setJarByClass(Phone.class);

            //设置输出参数
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //如果有相同的文件就删除
            File file = new File("F:\\helloPhone");
            if (file.exists()){
                FileUtils.deleteDirectory(file);
            }

            //设置出入输出路径
            FileInputFormat.addInputPath(job,new Path("F:\\Phone.txt"));
            FileOutputFormat.setOutputPath(job,new Path("F:\\helloPhone"));
            boolean completion = job.waitForCompletion(true);
            System.out.println(completion?"老铁没毛病":"出bug，赶紧修改");
        }
    }
}
