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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SingelWC2 {
    public static class MapTask extends Mapper<LongWritable,Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //把json数据转化为正常数据
            ObjectMapper mapper = new ObjectMapper();
            //利用mapper对象去转化json
            MovieBean movieBean = null;
            try {
                movieBean = mapper.readValue(value.toString(),MovieBean.class);
                context.write(new Text(movieBean.getMovie()),new IntWritable(movieBean.getRate()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static class Reduced extends Reducer<Text,IntWritable,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            Double sum = 0.0;
            for (IntWritable a:values) {
                count++;
                sum+=a.get();
            }
            //Double sums
            Double v = 1.0f*sum / count;
            context.write(new Text(key),new Text(String.valueOf(v)));
        }
        public static void main(String[] args) throws Exception{
            //File file = new File("F:\\phone.txt");
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            //设置类型
            job.setMapperClass(SingelWC2.MapTask.class);
            job.setReducerClass(SingelWC2.Reduced.class);
            job.setJarByClass(SingelWC2.class);

            //设置输出参数
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //设置出入输出路径
            FileInputFormat.addInputPath(job,new Path("F:\\movie.json"));
            FileOutputFormat.setOutputPath(job,new Path("F:\\hellocount3"));
            boolean completion = job.waitForCompletion(true);
            System.out.println(completion?"老铁没毛病":"出bug，赶紧修改");
        }
    }


}
