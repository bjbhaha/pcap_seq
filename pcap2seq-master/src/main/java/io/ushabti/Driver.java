package io.ushabti;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class Driver {

    public static void main(String[] args) throws Exception {

    	if(args.length != 3){
    		System.out.println("Usage : input_pcap_file output_sequence_file [none, gzip, bzip]");
    		return;
    	}
		//args=new String[]{"/home/bjbhaha/Desktop/music.pcap","/home/bjbhaha/Desktop/music.seq"};
    	PcapReader testReader = new PcapReader(args[0]);
    	SequenceFileWriter testWriter = new SequenceFileWriter(args[1], args[2]);

    	System.out.println("Converting pcap file to Hadoop sequence file ...");

    	while (testReader.getPacketTimeStamp() != -1){
			System.out.println(testReader.getTimeStamp());
    		testWriter.write(testReader.getTimeStamp(), testReader.getPacket());
    	}

    	testWriter.close();
    	testReader.close();
//
    	System.out.println("Converted " + testReader.getTotalPackets() + " packets.");
    	System.out.println("Read a total of " + testReader.getTotalBytes() + " bytes.");
    }
//	public static void main(String[] args) throws Exception {
//
//		if(args.length != 3){
//			System.out.println("Usage : input_pcap_file output_sequence_file [none, gzip, bzip]");
//			return;
//		}
//
//		//PcapReader testReader = new PcapReader(args[0]);
//		SequenceFileReader SeqReader = new SequenceFileReader(args[1], args[2]);
//
//		System.out.println("Read Hadoop sequence file ...");
//
//		SeqReader.read();
////		Configuration config = new Configuration();
////		FileSystem fs  = FileSystem.get(config);
////		Path path = new Path("/home/bjbhaha/Desktop/file1.seq");
//
////		while (testReader.getPacketTimeStamp() != -1){
////			Writable key,value=null;
////			SeqReader.read(value);
////		}
//
//
//		//testReader.close();
//		SeqReader.close();
//
//		//System.out.println("Converted " + testReader.getTotalPackets() + " packets.");
//		//System.out.println("Read a total of " + testReader.getTotalBytes() + " bytes.");
//	}
} 