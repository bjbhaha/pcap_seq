package io.ushabti;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SequenceFileReader {
    private Configuration configuration;
    private FileSystem fileSystem;
    private Path path;

    private IntWritable key = new IntWritable();
    private BytesWritable value = new BytesWritable();

    private SequenceFile.Reader reader = null;
    public SequenceFileReader(String sequenceFile,String compressionCodec) throws IOException, ClassNotFoundException{
        configuration = new Configuration();
        fileSystem = FileSystem.get(URI.create(sequenceFile), configuration);
        path = new Path(sequenceFile);


        if (compressionCodec.equalsIgnoreCase("org.apache.hadoop.io.compress.BZip2Codec")||compressionCodec.equalsIgnoreCase("org.apache.hadoop.io.compress.GzipCodec")) {
            Class<?> codecClass = Class.forName(compressionCodec);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration);
            reader = new SequenceFile.Reader(fileSystem, path,configuration);

        }
        else
            reader = new SequenceFile.Reader(fileSystem, path,configuration);
    }
    public void read() throws IOException{
        List<Object> sampleValues = new ArrayList<Object>();
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), fileSystem.getConf());
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), fileSystem.getConf());
        int count = 0;
        String keyName = "Key";
        String valueName = "Value";
        while (reader.next(key, value)) {
            sampleValues.add("{\"" + keyName + "\": \"" + key + "\", \"" + valueName + "\": \"" + value + "\"}");
            count++;
        }
        Iterator iterable=sampleValues.iterator();
        while(iterable.hasNext()){
            System.out.println(iterable.next()+"\n");

        }
    }
    public void close(){
        IOUtils.closeStream(reader);
    }
}
