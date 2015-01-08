package ch.hesso.master.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class StackoverflowTags implements
		WritableComparable<StackoverflowTags>, Iterable<String>,
		Comparable<StackoverflowTags> {
	
	private List<String> tags;

	public StackoverflowTags() {
		reset();
	}

	public StackoverflowTags(StackoverflowTags other) {
		this();
		tags.addAll(other.tags);
	}

	public void reset() {
		if (tags == null) {
			this.tags = new ArrayList<String>();
		} else {
			tags.clear();
		}
	}

	public void readFields(DataInput in) throws IOException {
			int nbTags = in.readInt();
			tags = new ArrayList<String>(nbTags);
			for (int i = 0; i < nbTags; ++i) {
				tags.add(in.readUTF());
			}
	}

	public void write(DataOutput out) throws IOException {
		
			out.writeInt(tags.size());
			for (String tag : tags) {
				out.writeUTF(tag);
			}
	}

	/**
	 * Returns true iff <code>other</code> is a {@link StackoverflowTags} with
	 * the same value.
	 */
	public boolean equals(Object other) {
		if (!(other instanceof StackoverflowTags))
			return false;
		return tags.equals(((StackoverflowTags)other).tags);
	}

	public int hashCode() {
		return tags.hashCode();
	}

	public int compareTo(StackoverflowTags other) {
		if (this == other) {
			return 0;
		}
		return Integer.compare(this.tags.size(), other.tags.size());
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("StackoverflowTags [tags=");
		builder.append(tags);
		builder.append("]");
		return builder.toString();
	}

	public Iterator<String> iterator() {
		return tags.iterator();
	}
	
	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public void setTags(String tags) {
		this.tags.addAll(Arrays.asList(tags.substring(1, tags.length() - 1).split("><")));
		System.out.println(this.tags);
	}

	public void addTag(String tag) {
		tags.add(tag);
	}
	
}