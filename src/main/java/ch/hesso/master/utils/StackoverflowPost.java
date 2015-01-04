package ch.hesso.master.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class StackoverflowPost implements WritableComparable<StackoverflowPost>, Iterable<String>, Comparable<StackoverflowPost> {
	
	private static final double EPSILON = 1e-8;

	private int id;
	private boolean question;
	private int acceptedAnswerId; // Only for question post, if one
	private int parentID; // Only for non-question post
	private Date creationDate;
	private int score;
	private int viewCount; // Only for question post
	private String body;
	private int ownerUserId;
	private int lastEditorUserId;
	private String lastEditorDisplayName;
	private Date lastEditDate;
	private Date lastActivityDate;
	private String title; // Only for question post
	private List<String> tags; // Only for question post
	private int answerCount; // Only for question post
	private int commentCount;
	private int favoriteCount;
	 
	public StackoverflowPost(boolean question) {
		this.question = question;
		if (question) {
			this.tags = new ArrayList<String>();
		}
	}

	public StackoverflowPost(StackoverflowPost other) {
		this(other.question);
		id = other.id;
		creationDate = other.creationDate;
		score = other.score;
		body = other.body;
		ownerUserId = other.ownerUserId;
		lastEditorUserId = other.lastEditorUserId;
		lastEditorDisplayName = other.lastEditorDisplayName;
		lastEditDate = other.lastEditDate;
		lastActivityDate = other.lastActivityDate;
		commentCount = other.commentCount;
		favoriteCount = other.favoriteCount;
		if (question) {
			acceptedAnswerId = other.acceptedAnswerId;
			viewCount = other.viewCount;
			title = other.title;
			tags.addAll(other.tags);
			answerCount = other.answerCount;
		}
		else {
			parentID = other.parentID;
		}
	}

	public void readFields(DataInput in) throws IOException {
		question = in.readBoolean();
		id = in.readInt();
		creationDate = new Date(in.readLong());
		score = in.readInt();
		body = in.readUTF();
		ownerUserId = in.readInt();
		lastEditorUserId = in.readInt();
		lastEditorDisplayName = in.readUTF();
		lastEditDate = new Date(in.readLong());
		lastActivityDate = new Date(in.readLong());
		commentCount = in.readInt();
		favoriteCount = in.readInt();
		if (question) {
			acceptedAnswerId = in.readInt();
			viewCount = in.readInt();
			title = in.readUTF();
			int nbTags = in.readInt();
			tags = new ArrayList<String>(nbTags);
			for (int i = 0; i < nbTags; ++i) {
				tags.add(in.readUTF());
			}
			answerCount = in.readInt();
		}
		else {
			parentID = in.readInt();
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(question);
		out.writeInt(id);
		out.writeLong(creationDate.getTime());
		out.writeInt(score);
		out.writeUTF(body);
		out.writeInt(ownerUserId);
		out.writeInt(lastEditorUserId);
		out.writeUTF(lastEditorDisplayName);
		out.writeLong(lastEditDate.getTime());
		out.writeLong(lastActivityDate.getTime());
		out.writeInt(commentCount);
		out.writeInt(favoriteCount);
		if (question) {
			out.writeInt(acceptedAnswerId);
			out.writeInt(viewCount);
			out.writeUTF(title);
			out.writeInt(tags.size());
			for (String tag : tags) {
				out.writeUTF(tag);
			}
			out.writeInt(answerCount);
		}
		else {
			out.writeInt(parentID);
		}
	}

	/** Returns true iff <code>other</code> is a {@link StackoverflowPost} with the same value. */
	public boolean equals(Object other) {
		if (!(other instanceof StackoverflowPost))
			return false;
		return this.id == ((StackoverflowPost)other).id;
	}

	public int hashCode() {
		return id;
	}

	@Override
	public int compareTo(StackoverflowPost o) {
		return Integer.compare(id, o.id);
	}
	
	@Override
	public String toString() {
		return "StackoverflowPost [id=" + id + ", question=" + question
				+ ", acceptedAnswerId=" + acceptedAnswerId + ", parentID="
				+ parentID + ", creationDate=" + creationDate + ", score="
				+ score + ", viewCount=" + viewCount + ", body=" + body
				+ ", ownerUserId=" + ownerUserId + ", lastEditorUserId="
				+ lastEditorUserId + ", lastEditorDisplayName="
				+ lastEditorDisplayName + ", lastEditDate=" + lastEditDate
				+ ", lastActivityDate=" + lastActivityDate + ", title=" + title
				+ ", tags=" + tags + ", answerCount=" + answerCount
				+ ", commentCount=" + commentCount + ", favoriteCount="
				+ favoriteCount + "]";
	}

	@Override
	public Iterator<String> iterator() {
		return tags.iterator();
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public boolean isQuestion() {
		return question;
	}

	public void setQuestion(boolean question) {
		this.question = question;
	}

	public int getAcceptedAnswerId() {
		return acceptedAnswerId;
	}

	public void setAcceptedAnswerId(int acceptedAnswerId) {
		this.acceptedAnswerId = acceptedAnswerId;
	}

	public int getParentID() {
		return parentID;
	}

	public void setParentID(int parentID) {
		this.parentID = parentID;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

	public int getScore() {
		return score;
	}

	public void setScore(int score) {
		this.score = score;
	}

	public int getViewCount() {
		return viewCount;
	}

	public void setViewCount(int viewCount) {
		this.viewCount = viewCount;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public int getOwnerUserId() {
		return ownerUserId;
	}

	public void setOwnerUserId(int ownerUserId) {
		this.ownerUserId = ownerUserId;
	}

	public int getLastEditorUserId() {
		return lastEditorUserId;
	}

	public void setLastEditorUserId(int lastEditorUserId) {
		this.lastEditorUserId = lastEditorUserId;
	}

	public String getLastEditorDisplayName() {
		return lastEditorDisplayName;
	}

	public void setLastEditorDisplayName(String lastEditorDisplayName) {
		this.lastEditorDisplayName = lastEditorDisplayName;
	}

	public Date getLastEditDate() {
		return lastEditDate;
	}

	public void setLastEditDate(Date lastEditDate) {
		this.lastEditDate = lastEditDate;
	}

	public Date getLastActivityDate() {
		return lastActivityDate;
	}

	public void setLastActivityDate(Date lastActivityDate) {
		this.lastActivityDate = lastActivityDate;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}
	
	public void addTag(String tag) {
		tags.add(tag);
	}

	public int getAnswerCount() {
		return answerCount;
	}

	public void setAnswerCount(int answerCount) {
		this.answerCount = answerCount;
	}

	public int getCommentCount() {
		return commentCount;
	}

	public void setCommentCount(int commentCount) {
		this.commentCount = commentCount;
	}

	public int getFavoriteCount() {
		return favoriteCount;
	}

	public void setFavoriteCount(int favoriteCount) {
		this.favoriteCount = favoriteCount;
	}
}