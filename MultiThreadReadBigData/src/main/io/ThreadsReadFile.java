package main.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Vector;
import java.util.HashMap;
import java.util.Scanner;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import main.io.BitMap;

// cd workspace\code\java\MultiThreadReadBigData\src
// javac -d bin -cp . main\io\ThreadsReadFile.java
// java -classpath d:\workspace\code\java\MultiThreadReadBigData\bin main.io.ThreadsReadFile

public class ThreadsReadFile {
	private BitMap bm = new BitMap();
	private ThreadReader[] threads = new ThreadReader[MAX_THREAD_NUM];
	private static final byte MAX_THREAD_NUM = 4;
	private HashMap<Byte, HashMap<String, Long>> threadReadRange = new HashMap<Byte, HashMap<String, Long>>();

	private String fileToRead;
	private RandomAccessFile raf;
	private File handle = null;
	private long fileLength = 0;
	private long threadAvgSize = 0;

	private Vector<Boolean> finishedTh = new Vector<Boolean>();

	public static void main(String[] args) {
		try {
			// 1、分割线程所需读的文件区域
			var reader = new ThreadsReadFile(
					"D:\\workspace\\code\\php\\test\\smalldata.txt",
					1024 * 1024 * 10, "UTF-8");
			reader.fileLenghtSplit((byte) 0, 0);
			System.out.println(reader.threadReadRange.toString());

			// 2、读取文件并写进bitmap
			var start = System.currentTimeMillis();
			System.out.println("start to set bitmap at" + start);

			reader.threadsToSetBitMap();
			while (reader.finishedTh.size() != MAX_THREAD_NUM) {
			}
			var end = System.currentTimeMillis();
			System.out.println("end to set bitmap at" + end + "spended time"
					+ (end - start));

			 System.out.println(
			 "distinct number count:" + reader.bm.countDistinctNum());

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ThreadsReadFile(String fileName, int bufferSize, String charset)
			throws IOException, FileNotFoundException {
		this.fileToRead = fileName;
		this.handle = new File(fileName);
		this.fileLength = this.handle.length();
		this.threadAvgSize = (long) Math
				.floor(this.fileLength / MAX_THREAD_NUM);
		System.out.println("file size is: " + this.fileLength + "bytes");

		this.raf = new RandomAccessFile(fileName, "r");

	}

	private void fileLenghtSplit(byte threadIndex, long startPos)
			throws IOException {
		if (startPos >= fileLength) {
			return;
		}

		var endPos = startPos + threadAvgSize;
		if (endPos >= fileLength) {

			putThreadReadRange(threadIndex, startPos, fileLength - 1);
			return;
		}
		endPos = endPos >= this.fileLength ? fileLength - 1 : endPos;

		this.raf.seek(endPos);
		byte tmp = this.raf.readByte();
		while (tmp != '\r' && tmp != '\n') {
			++endPos;
			this.raf.seek(endPos);
			tmp = (byte) this.raf.read();
		}
		putThreadReadRange(threadIndex, startPos, endPos);
		fileLenghtSplit(++threadIndex, endPos + 1);
		this.raf.close();
	}

	private void putThreadReadRange(byte index, long start, long end) {
		var hash = new HashMap<String, Long>();
		hash.put("start", Long.valueOf(start));
		hash.put("end", end);
		threadReadRange.put(Byte.valueOf(index), hash);
	}

	private void threadsToSetBitMap() {

		for (byte i = 0; i < MAX_THREAD_NUM; ++i) {
			threads[i] = new ThreadsReadFile.ThreadReader(i);
			threads[i].start();
		}
	}

	private void writeBitMap() {
		try {
			var file = new File(
					"D:\\workspace\\code\\php\\test\\bitmapdata.txt");
			if (!file.exists()) {
				file.createNewFile();
			}

			var out = new FileOutputStream(file, false);
			var buffOut = new BufferedOutputStream(out, 1024 * 4);

			var length = bm.bitArr.length;
			for (int i = 0; i < length; ++i) {
				buffOut.write(bm.bitArr[i]);
			}
			buffOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private class ThreadReader implements Runnable {
		private byte index;
		private String name;
		private Thread th;

		private ThreadReader() {
		}

		private ThreadReader(byte threadIndex) {
			this.name = "thread" + threadIndex;
			this.index = threadIndex;
		}

		public void start() {
			this.th = new Thread(this, this.name);
			this.th.start();
		}

		@Override
		public void run() {
			try {
				// 1、Scanner
				/*
				 * var console = new Scanner(new File(fileToRead));
				 * while(console.hasNextLine()) {
				 * bm.setBit(Integer.parseInt(console.nextLine().trim())); }
				 * console.close();
				 */

				// 2、RandomAccessFile
				/*
				 * var raf = new RandomAccessFile(fileToRead, "r");
				 * 
				 * raf.seek(threadReadRange.get(this.index).get("start")); var
				 * tmp = raf.readLine(); while (tmp != null &&
				 * raf.getFilePointer() <= threadReadRange
				 * .get(this.index).get("end")) {
				 * bm.setBit(Integer.parseInt(tmp.trim())); tmp =
				 * raf.readLine(); } raf.close();
				 */

				// 3、BufferedFileInputStream + MappedByteBuffer

				var length = threadReadRange.get(this.index).get("end")
						- threadReadRange.get(this.index).get("start") + 1;
				var reader = new MappedByteBufferReader(fileToRead,
						threadReadRange.get(this.index).get("start"), length);
				String tmpstr = null;
				while (reader.hasRemaining()) {
					tmpstr = reader.readLine();
					 bm.setBit(Integer.valueOf(tmpstr.trim()));
				}
				reader.close();
				
				finishedTh.add(true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
