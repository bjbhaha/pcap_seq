package io.ushabti;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
public class SequenceFileWriter {
	
	private Configuration configuration;
	private FileSystem fileSystem;
	private Path path;
	
	//private BytesWritable key = new BytesWritable();
	private IntWritable key = new IntWritable();
	private BytesWritable value = new BytesWritable();
	
	private SequenceFile.Writer writer = null;
	
	public SequenceFileWriter(String sequenceFile, String compressionCodec) throws IOException, ClassNotFoundException{
		configuration = new Configuration();
		fileSystem = FileSystem.get(URI.create(sequenceFile), configuration);
		path = new Path(sequenceFile);
		

		if (compressionCodec.equalsIgnoreCase("org.apache.hadoop.io.compress.BZip2Codec")||compressionCodec.equalsIgnoreCase("org.apache.hadoop.io.compress.GzipCodec")) {
			Class<?> codecClass = Class.forName(compressionCodec);
			CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration);
			//writer = SequenceFile.createWriter(fileSystem, configuration, path,key.getClass(), value.getClass(), org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK, codec);

			//edit by bjb
//			OutputStream outputStream = new FileOutputStream(args[1]);
//			BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
//			Option[] options = new Option[] {
//					SequenceFile.Writer.keyClass(keyClass),
//					SequenceFile.Writer.valueClass(valClass),
//					SequenceFile.Writer.compression(compress),
//					SequenceFile.Writer.blockCompress(blockCompress),
//					SequenceFile.Writer.codec(codec),
//					SequenceFile.Writer.metadata(metadata)
//			};
//			boolean compress = true;
//			boolean blockCompress = false; // 是否以块压缩方式进行压缩
//			// 创建 SequenceFile 的 Metadata 对象，可以用于存储一些元数据信息
//			SequenceFile.Metadata metadata = new SequenceFile.Metadata();
//			// 调用 createWriter 方法创建 SequenceFile 的写入器
//			writer = SequenceFile.createWriter(configuration, bufferedOutputStream, key.getClass(), value.getClass(), compress, blockCompress, codec, metadata);
			//writer = SequenceFile.createWriter(configuration,,bufferedOutputStream, key.getClass(), value.getClass());
			// 创建输出流

			JobConf conf = new JobConf();
			conf.setJobName("SequenceFileWriterExample");

			// 设置 SequenceFile 的 Key 和 Value 类型
			conf.setOutputKeyClass(key.getClass());
			conf.setOutputValueClass(value.getClass());//

			// 设置输出格式为 SequenceFile
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			OutputStream outputStream = new FileOutputStream(sequenceFile);
			BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

			// 创建 SequenceFile 的写入器
			writer = new SequenceFile.Writer(FileSystem.get(conf), conf, new Path(sequenceFile), key.getClass(), value.getClass());
		}
		else
			writer = SequenceFile.createWriter(fileSystem, configuration, path,key.getClass(), value.getClass());	
	}
	public static byte[] toLH(int n) {
		byte[] b = new byte[4];
		b[0] = (byte) (n & 0xff);
		b[1] = (byte) (n >> 8 & 0xff);
		b[2] = (byte) (n >> 16 & 0xff);
		b[3] = (byte) (n >> 24 & 0xff);
		return b;
	}
	public void write(int pcapkey, byte[] pcapValue) throws IOException{
		byte[] pcapkeyByte=toLH(pcapkey);
		//key.set(pcapkeyByte,0,pcapkeyByte.length);
		key.set(pcapkey);
		value.set(pcapValue, 0, pcapValue.length);
		//value=pcapValue;
		//System.out.println("!");
		writer.append(key, value);
	}
	
	public void close(){
		IOUtils.closeStream(writer);
	}
}

