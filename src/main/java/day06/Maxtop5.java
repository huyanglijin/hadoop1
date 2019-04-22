package day06;

import day03.MovieBean;
import day03.MyFriend;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import java.util.Comparator;
import java.util.TreeSet;

public class Maxtop5 {
    public static class MapTask extends Mapper<LongWritable, Text,Text, MovieBean>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //把json数据转化为正常数据（JavaBean）
            ObjectMapper om = new ObjectMapper();
            //用mapper对象转
            MovieBean movieBean = null;
            try {
                movieBean = om.readValue(value.toString(),MovieBean.class);
                context.write(new Text(movieBean.getMovie()),movieBean);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static class ReduceTask extends Reducer<Text,MovieBean,MovieBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<MovieBean> values, Context context) throws IOException, InterruptedException {
            /*ArrayList<MovieBean> list = new ArrayList<>();
            for (MovieBean value : values) {
                list.add(value);
            }
            list.sort(new Comparator<MovieBean>() {
                @Override
                public int compare(MovieBean o1, MovieBean o2) {

                    return o2.getRate()-o1.getRate();
                }
            });
            //求前五
            for (int i = 0; i < 5; i++) {
                context.write(list.get(i),NullWritable.get());
            }*/

            TreeSet<MovieBean> set = new TreeSet<>(new ComparableComparator());
            for (MovieBean value : values) {
                if (set.size()<=5){
                    set.add(value);
                }else {
                    MovieBean last = set.last();
                    if (last.getRate()<value.getRate()){
                        set.remove(last);
                        set.add(value);
                    }
                }
            }
            for (MovieBean movieBean : set) {
                context.write(movieBean,NullWritable.get());
            }
        }
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            //设置类型
            job.setMapperClass(Maxtop5.MapTask.class);
            job.setReducerClass(Maxtop5.ReduceTask.class);
            job.setJarByClass(Maxtop5.class);

            //设置输出参数
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(MovieBean.class);
            job.setOutputKeyClass(MovieBean.class);
            job.setOutputValueClass(NullWritable.class);

            File file = new File("F:\\Maxtop5");
            if (file.exists()){
                FileUtils.deleteDirectory(file);
            }
            //设置出入输出路径
            FileInputFormat.addInputPath(job,new Path("F:\\movie.json"));
            FileOutputFormat.setOutputPath(job,new Path("F:\\Maxtop5"));
            boolean completion = job.waitForCompletion(true);
            System.out.println(completion?"老铁没毛病":"出bug，赶紧修改");
        }
    }

}
