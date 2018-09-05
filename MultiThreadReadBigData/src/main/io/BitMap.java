package main.io;

public class BitMap {
	public byte[] bitArr;// bitÊý×é
	private static final byte mask = 3;// ÒÆÎ»Êý
	private static final int maxNum = (1 << mask) - 1;
	private long count = 0;

	BitMap() {
		bitArr = new byte[1 << (Integer.SIZE - mask)];
	}

	public void setBit(int num) {
		var val = bitArr[num >> mask];
		var bit = num & maxNum;
		if (val >= 0 && bit == maxNum) {
			bitArr[num >> mask] = (byte) ~val;
		} else if (val < 0 && bit != maxNum) {
			bitArr[num >> mask] = (byte) ~(~val | (1 << bit));
		} else if (val >= 0 && bit != maxNum) {
			bitArr[num >> mask] |= (1 << bit);
		}
	}

	public byte getBit(int num) {
		var val = bitArr[num >> mask];
		var bit = num & maxNum;
		if (bit == maxNum) {
			return bitArr[num >> mask] < 0 ? (byte) 1 : (byte) 0;
		} else if (val < 0 && bit != maxNum) {
			return (byte) (~bitArr[num >> mask] & (1 << (bit)));
		} else {
			return (byte) (bitArr[num >> mask] & (1 << (bit)));
		}
	}
	
	public void delBit(int num) {
		var val = bitArr[num >> mask];
		var bit = num & maxNum;
		if (bit == maxNum) {
			bitArr[num >> mask] = (byte) ~val;
		} else if (val < 0 && bit != maxNum) {
			bitArr[num >> mask] = (byte) ~(~bitArr[num >> mask] ^ (1 << (bit)));
		} else {
			bitArr[num >> mask] = (byte) (bitArr[num >> mask] ^ (1 << (bit)));
		}
	}
	
	public long countDistinctNum() {
		var length = bitArr.length;
		for (int i = 0; i < length; ++i) {
			if (bitArr[i] >= 0) {
				count += Integer.bitCount(bitArr[i]);
			}else {
				count += Integer.bitCount(~bitArr[i]) + 1;
			}
		}
		return count;
	}
}
