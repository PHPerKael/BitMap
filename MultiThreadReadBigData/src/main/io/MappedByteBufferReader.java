package main.io;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedByteBufferReader {
	private MappedByteBuffer buffer = null;
	private FileInputStream fins = null;
	private FileChannel chl = null;
	private long size;
	public MappedByteBufferReader(String filename, long start, long size)
			throws IOException {
		this.fins = new FileInputStream(filename);
		this.chl = fins.getChannel();
		this.size = size;
		this.buffer = chl.map(FileChannel.MapMode.READ_ONLY, start, size);
		// this.buffer.limit(4 * 1024 * 1024);
	}

	public String readLine() {
		var bb = ByteBuffer.allocate(1024);
		byte tmpb;
		while (hasRemaining()) {
			tmpb = buffer.get();
			if (tmpb != 13) {
				bb.put(tmpb);
			} else {
				break;
			}
		}
		return new String(bb.array());
	}

	public boolean hasRemaining() {
		return buffer.hasRemaining();
	}

	public int remaining() {
		return buffer.remaining();
	}

	public void close() throws IOException {
		this.fins.close();
		this.chl.close();
		this.buffer.clear();

	}
}
