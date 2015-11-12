package com.training.avro;

import java.io.*;
import java.util.Set;
import java.lang.Integer;

import org.apache.hadoop.io.*;

/**
* To check the original class check here:
* @see: https://gist.github.com/rishav-rohit/9224985
*/
public class IntPair implements WritableComparable<IntPair>{
	private IntWritable first;
	private IntWritable second;

	public IntPair() {
		set(new IntWritable(), new IntWritable());
	}

	public IntPair(Integer first, Integer second) {
		set(new IntWritable(first), new IntWritable(second));
	}

	public void set(IntWritable first, IntWritable second) {
		this.first = first;
		this.second = second;
	}

	public IntWritable getFirst() {
		return first;
	}

	public Integer getFirstInt() {
		return new Integer(first.toString());
	}

	public Integer getSecondInt() {
		return new Integer(second.toString());
	}
	public IntWritable getSecond() {
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
	first.write(out);
	second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
	first.readFields(in);
	second.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof IntPair) {
			IntPair tp = (IntPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public int compareTo(IntPair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}
}