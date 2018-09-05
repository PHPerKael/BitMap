package main.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Vector;
import java.util.HashMap;
import java.io.BufferedOutputStream;
import main.io.BitMap;

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
			// 1、seperate thread's reading line range
			var reader = new ThreadsReadFile(
					"D:\\workspace\\code\\php\\test\\smalldata.txt",
					1024 * 1024 * 10, "UTF-8");
			reader.fileLenghtSplit((byte) 0, 0);
			System.out.println(reader.threadReadRange.toString());

			// 2、read file and set bit
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
