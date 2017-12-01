# es-hadoop-data-share based on hadoop 2.7.4 
env describe:
ELK version: 5.6.0
ES-Hadoop version: 5.6.0
MR version: 2


app name: es-hadoop-data-share

function describe: share data between es and hadoop base on ES-Hadoop

something important:
    there's still a bug I couldn't fix is that you can't give class name as arg to tell the app whether using EH class towrite date from ES to Hadoop or wrte Hadoop data to ES using HE class, so you should alter the pom.xml to choose the class to use. 

way to deploy:

download the sources and import to your IDEA or use git clone to get it 
biuld the project 
please upload lib directory and jar file name hadoop-1.2.6.jar in target directory to your work dir
fell free to enjoy with it

usage example:
    1st,wrting data that searched from ES to Hadoop  
        hadoop-1.2.6.jar 106.14.240.250:9200 logstash-11011555/logs "*"   hdfs://47.100.76.107:9000/es_output/12011725
    2rd,wrting data that read from Hadoop to ES
        hadoop jar  share/hadoop/mapreduce/hadoop-1.2.6.jar 106.14.240.250:9200 logstash-11011555/logs   hdfs://47.100.76.107:9000/es_output/12011144  
