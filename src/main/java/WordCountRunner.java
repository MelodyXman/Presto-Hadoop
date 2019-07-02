

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountRunner {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();

        //override the map() function
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //拿到一行数据，转换为String
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, new IntWritable(1));
            }

        }
    }


    public static class MyReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        //Override reduce() function
        //框架每传递进来一个<k,v>, reduce（）方法被调用一次
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    //WordCountRunner 作为一个混做那个的主类， 定义了mapper的类，reducer的类，要输入的数据在哪里，输出的数据存在哪里，描述成一个job对象
    public static void main(String[] args) throws Exception {
        //把描述好的job提交给集群去运行
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCountRunner.class);

        //指定这个job所在的jar包
        job.setJar("/Users/xmango/Desktop/Tools/hadoop/wordCount.jar");
        job.setJarByClass(WordCountRunner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定mapper类的输出key，value的数据类型
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //指定reducer类的输出key，value的数据类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //指定要处理的数据所存在的位置
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定处理后输出的文件的位置
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //向yarn集群提交这个job
        boolean status = job.waitForCompletion(true);
        if (status) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}