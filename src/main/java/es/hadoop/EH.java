package es.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class EH {
    private static Logger LOG = LoggerFactory.getLogger(EH.class);

    public static class EhMap extends Mapper<Writable, Writable, NullWritable, Text> {
        @Override
        public void map(Writable key , Writable value , Mapper<Writable, Writable, NullWritable, Text>.Context context)
                throws IOException, InterruptedException {
            context.write(NullWritable.get() , new Text(value.toString()));
        }
    }

    public static void main(String[] args) {
        try {
            if (args.length != 4) {
                System.out.println("Usage: MaxTemperature <es.nodes> <es.resource> <es.query> <output path>");
                System.exit(-1);
            }
            long start_time = System.currentTimeMillis();


            Configuration conf = new Configuration();



            conf.set("es.nodes" , args[0]);

            conf.set("es.resource" , args[1]);
            conf.set("es.output.json" , "true");
            conf.set("es.query" , "?q=" + args[2]);
            conf.set("mapred.compress.map.output" , "true");
            conf.set("mapred.map.output.compression.codec" , "com.hadoop.compression.lzo.LzoCodec");

//        conf.set("io.compression.codecs","org.apache.hadoop.io.compress.GzipCodec, org.apache.hadoop.io.compress.DefaultCodec, com.hadoop.compression.lzo.LzopCodec");
//        conf.setBoolean("mapred.output.compress",true);
//        conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");


            Job job = Job.getInstance(conf , "elasticsearch to hadoop");
//            job.setJarByClass(EH.class);

            // 指定自定义的Mapper阶段的任务处理类

            job.setMapperClass(EhMap.class);

            // 设置输入格式
            job.setInputFormatClass(EsInputFormat.class);

            // 设置map输出格式
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);


            // 设置输出路径
            FileOutputFormat.setOutputPath(job , new Path(args[3]));
//        FileOutputFormat.setOutputPath(job, new Path("hdfs://47.100.76.107:9000/es_output/logstash-new-2017.11.27"));
            // 运行MR程序
            System.out.println(job.waitForCompletion(true));


            // 上面的语句执行完成后，会生成最后的输出文件，需要在此基础上添加lzo的索引
            // 使用lzo分布式索引生成器
//        DistributedLzoIndexer lzoIndexer = new DistributedLzoIndexer();
//        lzoIndexer.setConf(conf);
//        lzoIndexer.run(new String[]{args[3]});


            System.out.println(System.currentTimeMillis() - start_time);

        } catch (Exception e) {
            LOG.error(e.getMessage() , e);
        }
    }
}



