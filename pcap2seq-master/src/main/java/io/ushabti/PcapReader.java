package io.ushabti;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class PcapReader {
	
	public static final long pcapIdenticalHeader = 0xA1B2C3D4;
	public static final long pcapSwappedHeader = 0xD4C3B2A1;
	public static final short pcapMagicNumberHeaderSize = 4;
	public static final short pcapHeaderSize = 24;
	public static final short pcapPacketTimeStampSize = 4;
	public static final short pcapPacketNanoTimeStampSize = 4;
	public static final short pcapPacketLengthSize = 4;
	public static final short pcapOriginalPacketLengthSize = 4;
	
	private FileInputStream pcapByteInputStream;
	private BufferedInputStream pcapByteBufferedInputStream;
	private ByteOrder pcapFileByteOrder;
	private int currentPacketLength;
	private int timeStamp;
	private long totalPackets;
	private long totalBytes;

	private byte[] pcapHeader = new byte[16];
	public PcapReader(String pcapfile) throws MalformedFileException, IOException{
		this.pcapByteInputStream = new FileInputStream(new File(pcapfile));
		this.pcapByteBufferedInputStream  = new BufferedInputStream(this.pcapByteInputStream);
		verifyPcapFile();
	}
	
	public void verifyPcapFile() throws MalformedFileException, IOException {
		
		byte[] magicNumberBuffer = new byte[4];
		ByteBuffer magicNumberByteBuffer = ByteBuffer.wrap(magicNumberBuffer);
		// read the magic number
		//this.pcapByteInputStream.read(magicNumberBuffer, 0, pcapMagicNumberHeaderSize);
		this.pcapByteBufferedInputStream.read(magicNumberBuffer, 0, pcapMagicNumberHeaderSize);
		int magicNumber = magicNumberByteBuffer.getInt();
		
		// ByteBuffer uses BIG_ENDIAN by default
//		if(magicNumber == pcapIdenticalHeader){
//			System.out.println("PCAP FILE FORMAT : IDENTICAL");
//			pcapFileByteOrder = ByteOrder.BIG_ENDIAN;
//		}
//		else if(magicNumber == pcapSwappedHeader) {
//			System.out.println("PCAP FILE FORMAT : SWAPPED");
//			pcapFileByteOrder = ByteOrder.LITTLE_ENDIAN;
//		}
//		else
//			throw new MalformedFileException("MALFORMED PCAP FILE : BAD MAGIC NUMBER");
		
		// skip to the first packet header
		this.pcapByteBufferedInputStream.skip(pcapHeaderSize - pcapMagicNumberHeaderSize);
		
		//Set Total number of packets to 0
		this.setTotalPackets(0);
		
		//Set Total bytes to 0
		this.setTotalBytes(0);
	}
	
	public void close() throws IOException{
		this.pcapByteBufferedInputStream.close();
	}

	public int getHeaderTime() throws IOException {

		byte[] readBuffer = new byte[4];
		ByteBuffer readByteBuffer = ByteBuffer.wrap(readBuffer);
		readByteBuffer.order(pcapFileByteOrder);
		if(this.pcapByteBufferedInputStream.read(pcapHeader, 0, 16) != -1){
			System.arraycopy(pcapHeader,0,readBuffer,0,4);
			readByteBuffer.rewind();
			this.timeStamp = readByteBuffer.getInt();
			System.arraycopy(pcapHeader,12,readBuffer,0,4);
			readByteBuffer.rewind();
			currentPacketLength = readByteBuffer.getInt();
		}else
			return -1;

		return 0;
	}
	public int getPacketTimeStamp() throws IOException {
		
		byte[] readBuffer = new byte[4];
		ByteBuffer readByteBuffer = ByteBuffer.wrap(readBuffer);
		readByteBuffer.order(pcapFileByteOrder);
		
		// Read TimeStamp from packet header
		if(this.pcapByteBufferedInputStream.read(readBuffer, 0, pcapPacketTimeStampSize) != -1){
			readByteBuffer.rewind();
			this.timeStamp = readByteBuffer.getInt();
			// Skip to Packet Length
			if(this.pcapByteBufferedInputStream.skip(pcapPacketNanoTimeStampSize) != -1) {
				// Read Packet Length
				if(this.pcapByteBufferedInputStream.read(readBuffer, 0, pcapPacketLengthSize) != -1){
					readByteBuffer.rewind();
					// set current packet length
					currentPacketLength = readByteBuffer.getInt();
					// Skip to the next packet
					if(this.pcapByteBufferedInputStream.skip(pcapOriginalPacketLengthSize) == -1)
						return -1;
				}
				else
					return -1;
			}
			else 
				return -1;
		}
		else
			return -1;
		
		return 0;
	}
	
	public byte[] getPacket() throws IOException{
		
		byte[] readBuffer = new byte[currentPacketLength];
		
		// We'll Assume that after each header there's a packet; TODO : add checks
		// returned byte[] is in the original byte order
		this.pcapByteBufferedInputStream.read(readBuffer, 0, currentPacketLength);
		this.setTotalPackets(this.getTotalPackets() + 1);
		this.setTotalBytes(this.getTotalBytes() + currentPacketLength);
		
		return readBuffer;
	}
	
	public int getTimeStamp(){
		return timeStamp;
	}

	public int getCurrentPacketLength(){
		return currentPacketLength;
	}
	public byte[] getPcapHeader(){
		return pcapHeader;
	}
	public long getTotalPackets() {
		return totalPackets;
	}

	public void setTotalPackets(long totalPackets) {
		this.totalPackets = totalPackets;
	}

	public long getTotalBytes() {
		return totalBytes;
	}

	public void setTotalBytes(long totalBytes) {
		this.totalBytes = totalBytes;
	}

}
