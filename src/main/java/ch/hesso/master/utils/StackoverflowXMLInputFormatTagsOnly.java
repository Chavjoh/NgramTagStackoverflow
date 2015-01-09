package ch.hesso.master.utils;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//
//	To create sub dataset:
//	head -202 Posts.xml > Posts_200.xml && tail -1 Posts.xml >> Posts_200.xml
//
public class StackoverflowXMLInputFormatTagsOnly extends
		FileInputFormat<NullWritable, StackoverflowTags> {

	@Override
	public RecordReader<NullWritable, StackoverflowTags> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		return new StackoverflowXMLRecordReader();
	}

	private static class StackoverflowXMLRecordReader extends
			RecordReader<NullWritable, StackoverflowTags> {

		private final byte[] startTag = "<row".getBytes();
		private final byte[] endTag = "/>".getBytes();
		private final byte keyValueSeparator = (byte) '=';
		private final byte space = (byte) ' ';
		private final byte quote = (byte) '"';

		private final byte[] tagsKey = "Tags".getBytes();

		private long start;
		private long end;
		private FSDataInputStream fsin;
		private DataOutputBuffer buffer = new DataOutputBuffer();

		private NullWritable key;
		private StackoverflowTags value;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) split;
			// open the file and seek to the start of the split
			start = fileSplit.getStart();
			end = start + fileSplit.getLength();
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(context.getConfiguration());
			fsin = fs.open(fileSplit.getPath());
			fsin.seek(start);
		}
		
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (key == null) {
				key = NullWritable.get();
			}
			if (value == null) {
				value = new StackoverflowTags();
			} else {
				value.reset();
			}
			while (readUntilMatch(startTag, false)) {
				buffer.reset();
				while (readNextKey()) {
					byte[] xmlKey = Arrays.copyOf(buffer.getData(),
							buffer.getLength());
					buffer.reset();
					readNextValue();
					if (Arrays.equals(xmlKey, tagsKey)) {
						byte[] xmlValue = Arrays.copyOf(buffer.getData(),
								buffer.getLength());
						value.setTags(getString(xmlValue));
						return true;
					}
					buffer.reset();
				}
			}

			return false;
		}

		private String getString(byte[] bytes) {
			return StringEscapeUtils.unescapeXml(new String(bytes));
//			return new String(bytes);
		}

		private boolean readNextKey() throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// end of file:
				if (b == -1) {
					return false;
				}
				if (b == keyValueSeparator) {
					return true;
				}
				// see if we've passed the stop point:
				if (fsin.getPos() >= end) {
					return false;
				}
				// check if end tag
				if (b == endTag[i]) {
					i++;
					if (i >= endTag.length) {
						return false;
					}
				} else {
					i = 0;
				}
				// save to buffer:
				if (b != space) {
					buffer.write(b);
				}
			}
		}

		private boolean readNextValue() throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// end of file:
				if (b == -1) {
					return false;
				}

				// check if quote and if it's the second one
				if (b == quote) {
					if (++i == 2) {
						return true;
					}
				} else { // save to buffer:
					buffer.write(b);
				}
				// TODO: Check if we can delete the instruction below
				// see if we've passed the stop point:
				if (fsin.getPos() >= end) {
					return false;
				}
			}
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock)
				throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// end of file:
				if (b == -1) {
					return false;
				}
				// save to buffer:
				if (withinBlock) {
					buffer.write(b);
				}

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length) {
						return true;
					}
				} else {
					i = 0;
				}
				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && fsin.getPos() >= end) {
					return false;
				}
			}
		}

		@Override
		public NullWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public StackoverflowTags getCurrentValue() throws IOException,
				InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (fsin.getPos() - start) / (float) (end - start);
		}

		@Override
		public void close() throws IOException {
			fsin.close();
		}

	}
}
