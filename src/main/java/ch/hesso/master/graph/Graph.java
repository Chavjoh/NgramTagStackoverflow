package ch.hesso.master.graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ch.hesso.master.utils.Utils;

public class Graph {
	
	static class Tag {
		
		private String name;
		private Integer occurence;

		public Tag(String name) {
			setName(name);
		}
		
		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Integer getOccurence() {
			return occurence;
		}

		public void setOccurence(Integer occurence) {
			this.occurence = occurence;
		}

		@Override
		public boolean equals(Object object) {
			if (object == this) { return true; }
	 		if (object == null || object.getClass() != this.getClass()) { return false; }
	 
	 		Tag tag = (Tag)object;
	 		return this.name.equals(tag.getName());
		}

		@Override
		public int hashCode() {
			return this.name.hashCode();
		}

		@Override
		public String toString() {
			return this.name;
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("Usage: Graph <list_tag_file> <bigram_file>");
			System.exit(0);
		}
		
		File listTagPath = new File(args[0]);
		File bigramPath = new File(args[1]);
		
		Map<Tag, List<Tag>> mapTag = new HashMap<Tag, List<Tag>>();
		
		BufferedReader br;
		String line;
		
		/**
		 * LOAD LIST TAG
		 */
		
		br = new BufferedReader(new FileReader(listTagPath));
		while ((line = br.readLine()) != null) {
		   String[] data = Utils.words(line);
		   mapTag.put(new Tag(data[0].trim()), new ArrayList<Tag>());
		   //System.out.println(data[0]);
		}
		br.close();
		
		/**
		 * LOAD LINKS
		 */
		
		br = new BufferedReader(new FileReader(bigramPath));
		while ((line = br.readLine()) != null) {
		   String[] data = line.replace("[", "").replace("}", "").split("\\]\t\\{");
		   Tag mainTag = new Tag(data[0].trim());
		   //System.out.println(mainTag);
		   List<Tag> listAssociatedTag = mapTag.get(mainTag);
		   
		   String[] listTag = data[1].split(", ");
		   for (int i = 0; i < listTag.length; i++) {
			   //System.out.println(listTag[i].split("=")[0]);
			   Tag tag = new Tag(listTag[i].split("=")[0].trim());
			   listAssociatedTag.add(tag);
		   }
		   //System.out.println(data[0] + " -> " + data[1]);
		}
		br.close();
		
		for (Entry<Tag, List<Tag>> entry:mapTag.entrySet()) {
			System.out.println(entry.getKey() + " ==> " + entry.getValue().toString());
		}
	}

}
