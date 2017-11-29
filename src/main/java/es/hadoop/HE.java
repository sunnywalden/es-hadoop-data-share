package es.hadoop;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import com.hadoop.mapreduce.LzoTextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HE {

    public static class HeMapper extends Mapper<Object, Text, NullWritable, Text> {
        private static final Logger LOG = LoggerFactory.getLogger(HeMapper.class);

        @Override
        public void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            byte[] source = value.toString().trim().getBytes();
            Text jsonDoc = new Text(source);
            context.write(NullWritable.get(), jsonDoc);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length!=3){
            System.out.println("Usage: MaxTemperature <es.nodes> <es.resource> <input path>");
            System.exit(-1);
        }

        long start_time = System.currentTimeMillis();

        Configuration conf = new Configuration();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("es.input.json", "yes");
//        conf.set("es.nodes", args[0]); //106.14.240.250:9200
//        conf.set("es.resource", args[1]);  //logstash-demo-2017.11.27/logs
        conf.set("es.nodes", args[0]); //106.14.240.250:9200
        conf.set("es.resource", args[1]);  //logstash-demo-2017.11.27/logs

        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");


        Job job = Job.getInstance(conf,"hadoop to elasticsearch");
        job.setMapperClass(HeMapper.class);

//        job.setInputFormatClass(LzoTextInputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置输入路径
        FileInputFormat.setInputPaths(job, new Path(args[2])); //hdfs://47.100.76.107:9000/es_output/logstash-new-2017.11.27

        job.waitForCompletion(true);

        System.out.println(System.currentTimeMillis()-start_time);
    }
}
