# es-hadoop-data-share based on hadoop 2.7.4 


## describe

    ELK version: 5.6.0

    ES-Hadoop version: 5.6.0
    
    MR version: 2



app name: es-hadoop-data-share

function describe: share data between es and hadoop base on ES-Hadoop


## deploy

way to deploy:

download the sources and import to your IDEA or use git clone to get it 

biuld the project 

please upload lib directory and jar file name hadoop-1.2.6.jar in target directory to your work dir

fell free to enjoy with it


## usage

usage example:

### ES2Hadoop

1st,wrting data that searched from ES to Hadoop 
    
        hadoop jar hadoop-1.2.6.jar 106.14.240.250:9200 es.hadoop.EH logstash-11011555/logs "*"  
        
        hdfs://47.100.76.107:9000/es_output/12011725
        
### Hadoop2ES

    2rd,wrting data that read from Hadoop to ES
    
        hadoop jar  hadoop-1.2.6.jar es.hadoop.HE 106.14.240.250:9200 logstash-11011555/logs  
        
        hdfs://47.100.76.107:9000/es_output/12011144  
