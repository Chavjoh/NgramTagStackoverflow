package ch.hesso.master;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

public class ArrayListWritable<E extends WritableComparable> extends ArrayList<E> implements WritableComparable {

	private static final long serialVersionUID = 1L;

	public ArrayListWritable() {
		super();
	}
	
	public ArrayListWritable(ArrayList<E> array) {
		super(array);
	}
	
	public void readFields(DataInput in) throws IOException {
		this.clear();

		int numFields = in.readInt();
		if (numFields == 0)
			return;
		
		String className = in.readUTF();
		E obj;
		
		try {
			@SuppressWarnings("unchecked")
			Class<E> c = (Class<E>) Class.forName(className);
			
			for (int i = 0; i < numFields; i++) {
				obj = (E) c.newInstance();
				obj.readFields(in);
				this.add(obj);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.size());
		
		if (size() == 0)
			return;
		
		E obj = get(0);

		out.writeUTF(obj.getClass().getCanonicalName());

		for (int i = 0; i < size(); i++) {
			obj = get(i);
			obj.write(out);
		}
	}
	
	public String toString() {
		return super.toString();
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	public int compareTo(Object obj) {
		ArrayListWritable<?> that = (ArrayListWritable<?>) obj;

		// iterate through the fields
		for (int i = 0; i < this.size(); i++) {
			// sort shorter list first
			if (i >= that.size())
				return 1;

			@SuppressWarnings("unchecked")
			Comparable<Object> thisField = this.get(i);
			@SuppressWarnings("unchecked")
			Comparable<Object> thatField = that.get(i);

			if (thisField.equals(thatField)) {
				// if we're down to the last field, sort shorter list first
				if (i == this.size() - 1) {
					if (this.size() > that.size())
						return 1;

					if (this.size() < that.size())
						return -1;
				}
				// otherwise, move to next field
			} else {
				return thisField.compareTo(thatField);
			}
		}

		return 0;
	}
}
