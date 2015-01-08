package ch.hesso.master.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

public class StackoverflowTags implements WritableComparable<StackoverflowTags>, Comparable<StackoverflowTags> {

	private String[] tags;

	public StackoverflowTags() {
		reset();
	}

	public StackoverflowTags(StackoverflowTags other) {
		this();
		tags = Arrays.copyOf(other.tags, other.tags.length);
	}

	public void reset() {
		this.tags = null;
	}

	public void readFields(DataInput in) throws IOException {
		int nbTags = in.readInt();
		tags = new String[nbTags];
		for (int i = 0; i < nbTags; ++i) {
			tags[i] = in.readUTF();
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(tags.length);
		for (int i = 0; i < tags.length; ++i) {
			out.writeUTF(tags[i]);
		}
	}

	/**
	 * Returns true iff <code>other</code> is a {@link StackoverflowTags} with
	 * the same value.
	 */
	public boolean equals(Object other) {
		if (!(other instanceof StackoverflowTags))
			return false;
		return Arrays.equals(tags, ((StackoverflowTags) other).tags);
	}

	public int hashCode() {
		return tags.hashCode();
	}

	public int compareTo(StackoverflowTags other) {
		if (this == other) {
			return 0;
		}
		return (this.tags.length < other.tags.length) ? -1 : ((this.tags.length == other.tags.length) ? 0 : 1);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("StackoverflowTags [tags=");
		builder.append(tags);
		builder.append("]");
		return builder.toString();
	}

	public String[] getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags.substring(1, tags.length() - 1).split("><");
	}
}
