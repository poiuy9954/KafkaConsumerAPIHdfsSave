package org.pipeline;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

//@Slf4j
public class TestClass {

    public static void main(String[] args) throws IOException {

        Path path = new Path("test.log");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs","hdfs://localhost:9000");
        FileSystem hdfsFileSystem = FileSystem.get(conf);
        FSDataOutputStream fsDataOutputStream = hdfsFileSystem.create(path);
        fsDataOutputStream.writeBytes("asdasdasda");
        fsDataOutputStream.close();


    }
}
