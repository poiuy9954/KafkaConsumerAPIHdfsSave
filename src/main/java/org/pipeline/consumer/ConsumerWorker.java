package org.pipeline.consumer;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ConsumerWorker implements Runnable {

    private static Map<Integer, List<String>> bufferString = new ConcurrentHashMap<>();
    private static Map<Integer,Long> currentFileOffset = new ConcurrentHashMap<>();
    private final static int FLUSH_RECORD_COUNT = 10;
    private Properties properties;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    public ConsumerWorker(Properties properties,String TOPIC_NAME, int number) {
        log.info("Generete Consumer Worker");
        this.properties = properties;
        this.topic = TOPIC_NAME;
        this.threadName = "ConsumerWorker-Thread-" + number;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(threadName);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    addHdfsFileBuffer(record);
                }
                saveBufferToHdfsFile(consumer.assignment());
            }
        } catch (WakeupException e) {
            log.warn("Wakeup Exception");
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }finally {
            consumer.close();
        }
    }

    private void saveBufferToHdfsFile(Set<TopicPartition> assignment) {
        assignment.forEach(partition -> {checkFlushCount(partition.partition());});
    }

    private void checkFlushCount(int partitionNo) {
        if(bufferString.get(partitionNo) != null) {
            if(bufferString.get(partitionNo).size() > FLUSH_RECORD_COUNT - 1){
                save(partitionNo);
            }
        }
    }

    private void save(int partitionNo) {
        if(bufferString.get(partitionNo).size()>0){
            try {
//              color-{파티션no}-{offset}.log
                String fileName = "/data/color-"+partitionNo+"-"+currentFileOffset.get(partitionNo)+".log";
                Configuration conf = new Configuration();
                conf.set("fs.defaultFs","hdfs://localhost:9000/");
                FileSystem hdfsFileSystem = FileSystem.get(conf);
                FSDataOutputStream fileOutputStream = hdfsFileSystem.create(new Path(fileName));
                fileOutputStream.writeBytes(StringUtils.join(bufferString.get(partitionNo), "\n"));
                fileOutputStream.close();

                bufferString.put(partitionNo,new ArrayList<>());
            }catch (Exception e) {
                log.error(e.getMessage(),e);
            }
        }
    }

    private void addHdfsFileBuffer(ConsumerRecord<String, String> record) {
        List<String> buffer = bufferString.getOrDefault(record.partition(), new ArrayList<>());
        buffer.add(record.value());
        bufferString.put(record.partition(), buffer);
        
        if(buffer.size() == 1){
            currentFileOffset.put(record.partition(), record.offset());
        }
    }

    public void stopAndWakeup(){
        log.info("stopAndWakeup");
        consumer.wakeup();
        saveRemainBufferToHdfsFile();
    }

    private void saveRemainBufferToHdfsFile() {
        bufferString.forEach((partition, v) -> this.save(partition));
    }
}
