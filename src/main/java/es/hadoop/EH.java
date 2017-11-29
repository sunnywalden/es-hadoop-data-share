package es.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.elasticsearch.hadoop.mr.EsInputFormat;

import com.hadoop.compression.lzo.DistributedLzoIndexer;



public class EH {

    public static class EhMap extends Mapper<Writable, Writable, Text, Text> {
        @Override
        public void map(Writable key, Writable value, Mapper<Writable, Writable, Text, Text>.Context context)
                throws IOException, InterruptedException {
            context.write(new Text(key.toString()), new Text(value.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length!=4){
            System.out.println("Usage: MaxTemperature <es.nodes> <es.resource> <es.query> <output path>");
            System.exit(-1);
        }

        long start_time = System.currentTimeMillis();

        Configuration conf = new Configuration();
        conf.set("es.nodes", args[0]);
//        conf.set("es.nodes", "106.14.240.250:9200");
//        conf.set("es.resource", "logstash-2017.11.27/logs");
        conf.set("es.resource", args[1]);
        conf.set("es.output.json", "true");
        conf.set("es.query", "?q="+args[2]);//userid:0f91c56bcf834e4f9e9ad97ee701813d");

//        conf.set("io.compression.codecs","org.apache.hadoop.io.compress.GzipCodec, org.apache.hadoop.io.compress.DefaultCodec, com.hadoop.compression.lzo.LzopCodec");
//        conf.setBoolean("mapred.output.compress",true);
//        conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");


        conf.set("mapred.compress.map.output","true");
        conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");


        Job job = Job.getInstance(conf, "elasticsearch to hadoop");

        // 指定自定义的Mapper阶段的任务处理类

        job.setMapperClass(EhMap.class);
        job.setNumReduceTasks(0);
        // 设置map输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置输入格式
        job.setInputFormatClass(EsInputFormat.class);
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
//        FileOutputFormat.setOutputPath(job, new Path("hdfs://47.100.76.107:9000/es_output/logstash-new-2017.11.27"));
        // 运行MR程序
        job.waitForCompletion(true);


        // 上面的语句执行完成后，会生成最后的输出文件，需要在此基础上添加lzo的索引
        // 使用lzo分布式索引生成器
//        DistributedLzoIndexer lzoIndexer = new DistributedLzoIndexer();
//        lzoIndexer.setConf(conf);
//        lzoIndexer.run(new String[]{args[3]});
//        lzoIndexer.run(new String[]{"hdfs://47.100.76.107:9000/es_output/logstash-new-2017.11.27"});

        System.out.println(System.currentTimeMillis()-start_time);
    }
}



