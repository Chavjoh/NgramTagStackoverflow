package ch.hesso.master.utils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//
//	To create sub dataset:
//	head -202 Posts.xml > Posts_200.xml && tail -1 Posts.xml >> Posts_200.xml
//
public class StackoverflowXMLInputFormat extends
		FileInputFormat<LongWritable, StackoverflowPost> {

	private final static DateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss");

	@Override
	public RecordReader<LongWritable, StackoverflowPost> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		return new StackoverflowXMLRecordReader();
	}

	private static class StackoverflowXMLRecordReader extends
			RecordReader<LongWritable, StackoverflowPost> {

		private final byte[] startTag = "<row".getBytes();
		private final byte[] endTag = "/>".getBytes();
		private final byte keyValueSeparator = (byte) '=';
		private final byte space = (byte) ' ';
		private final byte quote = (byte) '"';

		private final byte[] idKey = "Id".getBytes();
		private final byte[] questionKey = "PostTypeId".getBytes();
		private final byte[] acceptedAnswerIdKey = "AcceptedAnswerId".getBytes();
		private final byte[] parentIDKey = "ParentId".getBytes();
		private final byte[] creationDateKey = "CreationDate".getBytes();
		private final byte[] scoreKey = "Score".getBytes();
		private final byte[] viewCountKey = "ViewCount".getBytes();
		private final byte[] bodyKey = "Body".getBytes();
		private final byte[] ownerUserIdKey = "OwnerUserId".getBytes();
		private final byte[] lastEditorUserIdKey = "LastEditorUserId".getBytes();
		private final byte[] lastEditorDisplayNameKey = "LastEditorDisplayName".getBytes();
		private final byte[] lastEditDateKey = "LastEditDate".getBytes();
		private final byte[] lastActivityDateKey = "LastActivityDate".getBytes();
		private final byte[] titleKey = "Title".getBytes();
		private final byte[] tagsKey = "Tags".getBytes();
		private final byte[] answerCountKey = "AnswerCount".getBytes();
		private final byte[] commentCountKey = "CommentCount".getBytes();
		private final byte[] favoriteCountKey = "FavoriteCount".getBytes();
		private final byte[] closedDateKey = "ClosedDate".getBytes();
		private final byte[] ownerDisplayNameKey = "OwnerDisplayName".getBytes();
		private final byte[] communityOwnedDateKey = "CommunityOwnedDate".getBytes();

		private long start;
		private long end;
		private FSDataInputStream fsin;
		private DataOutputBuffer buffer = new DataOutputBuffer();

		private LongWritable key;
		private StackoverflowPost value;

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
				key = new LongWritable();
			}
			if (value == null) {
				value = new StackoverflowPost(); 
			} else {
				value.reset();
			}
			boolean newPost = readUntilMatch(startTag, false); // Find the post
																// start
			if (!newPost) {
				return false;
			}
			buffer.reset();
			while (readNextKey()) {
				byte[] xmlKey = Arrays.copyOf(buffer.getData(),
						buffer.getLength());
				buffer.reset();
				readNextValue();
				byte[] xmlValue = Arrays.copyOf(buffer.getData(),
						buffer.getLength());
				if (Arrays.equals(xmlKey, idKey)) {
					value.setId(getInt(xmlValue));
				} else if (Arrays.equals(xmlKey, questionKey)) {
					value.setQuestion(getInt(xmlValue) == 1); // 1 is question,
																// 2 answer
				} else if (Arrays.equals(xmlKey, acceptedAnswerIdKey)) {
					value.setAcceptedAnswerId(getInt(xmlValue));
				} else if (Arrays.equals(xmlKey, parentIDKey)) {
					value.setParentID(getInt(xmlValue));
				} else if (Arrays.equals(xmlKey, creationDateKey)) {
					value.setCreationDate(getDate(xmlValue));
				} else if (Arrays.equals(xmlKey, scoreKey)) {
					value.setScore(getInt(xmlValue));
				} else if (Arrays.equals(xmlKey, viewCountKey)) {
					value.setViewCount(getInt(xmlValue));
				} else if (Arrays.equals(xmlKey, bodyKey)) {
					value.setBody(getString(xmlValue));
				} else if (Arrays.equals(xmlKey, ownerUserIdKey)) {
					value.setOwnerUserId(getInt(xmlValue));
				} else if (Arrays.equals(xmlKey, lastEditorUserIdKey)) {
					value.setLastEditorUserId(getInt(xmlValue));
				} else if (Arrays.equals(xmlKey, lastEditorDisplayNameKey)) {
					value.setLastEditorDisplayName(getString(xmlValue));
				} else if (Arrays.equals(xmlKey, lastEditDateKey)) {
					value.setLastEditDate(getDate(xmlValue));
				} else if (Arrays.equals(xmlKey, lastActivityDateKey)) {
					value.setLastActivityDate(getDate(xmlValue));
				} else if (Arrays.equals(xmlKey, titleKey)) {
					value.setTitle(getString(xmlValue));
				} else if (Arrays.equals(xmlKey, tagsKey)) {
					value.setTags(getString(xmlValue));
				} else if (Arrays.equals(xmlKey, answerCountKey)) {
					value.setAnswerCount(getInt(xmlValue));
				} else if (Arrays.equals(xmlKey, commentCountKey)) {
					value.setCommentCount(getInt(xmlValue));
				} else if (Arrays.equals(xmlKey, favoriteCountKey)) {
					value.setFavoriteCount(getInt(xmlValue));
				} else if (Arrays.equals(xmlKey, closedDateKey)) {
					value.setClosedDate(getDate(xmlValue));
				} else if (Arrays.equals(xmlKey, ownerDisplayNameKey)) {
					value.setOwnerDisplayName(getString(xmlValue));
				} else if (Arrays.equals(xmlKey, communityOwnedDateKey)) {
					value.setCommunityOwnedDate(getDate(xmlValue));
				} else {
					System.out.println(getString(xmlKey));
					System.out.println(getString(xmlValue));
				}
				buffer.reset();
			}

			return true;
		}

		private int getInt(byte[] bytes) {
			final int OFFSET = '0';
			int theInt = 0;

			for (int i = 0; i < bytes.length; ++i) {
				theInt *= 10;
				theInt += bytes[i] - OFFSET;
			}
			return theInt;
			// return ByteBuffer.wrap(bytes).getInt();
		}

		private String getString(byte[] bytes) {
			return StringEscapeUtils.unescapeXml(new String(bytes));
//			return new String(bytes);
		}

		private Date getDate(byte[] bytes) {
			try {
				return dateFormat.parse(getString(bytes));
			} catch (ParseException e) {
				e.printStackTrace();
				return new Date();
			}
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
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public StackoverflowPost getCurrentValue() throws IOException,
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
