package ch.hesso.master.graph;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Paint;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.geom.Ellipse2D;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.JFrame;

import org.apache.commons.collections15.Factory;
import org.apache.commons.collections15.Transformer;

import ch.hesso.master.utils.Utils;
import edu.uci.ics.jung.algorithms.layout.KKLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.SparseGraph;
import edu.uci.ics.jung.visualization.BasicVisualizationServer;
import edu.uci.ics.jung.visualization.Layer;
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
import edu.uci.ics.jung.visualization.renderers.Renderer.VertexLabel.Position;

public class GraphView {
	
	Graph<String, String> g;
	Map<Tag, List<Tag>> mapTag;
	
	static class Tag {
		
		private String name;
		private Integer occurence;

		public Tag(String name) {
			setName(name);
		}
		
		public Tag(String name, Integer occurence) {
			setName(name);
			setOccurence(occurence);
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
	
	public GraphView(File listTagPath, File bigramPath) throws IOException {		
		mapTag = new HashMap<Tag, List<Tag>>();
		
		BufferedReader br;
		String line;
		
		/**
		 * LOAD LIST TAG
		 */
		
		BufferedReader in = new BufferedReader(new InputStreamReader(new ReverseLineInputStream(listTagPath)));

	    int indexTag = 0;
	    while (indexTag < 10) {
	      line = in.readLine();

	      if (line == null) {
	        break;
	      }

	      String[] data = Utils.words(line);
	      Tag tag = new Tag(data[0], Integer.parseInt(data[1]));
	      mapTag.put(tag, new ArrayList<Tag>());

	      indexTag++;
	      System.out.println(tag.getName() + " -> " + tag.getOccurence());
	    }
	    
	    in.close();
		
		/**
		 * LOAD LINKS
		 */
		
		br = new BufferedReader(new FileReader(bigramPath));
		while ((line = br.readLine()) != null) {
		   String[] data = line.replace("[", "").replace("}", "").split("\\]\t\\{");
		   Tag mainTag = new Tag(data[0].trim());
		   //System.out.println(mainTag);
		   List<Tag> listAssociatedTag = mapTag.get(mainTag);
		   
		   if (listAssociatedTag != null) {
			   String[] listTag = data[1].split(", ");
			   for (int i = 0; i < listTag.length; i++) {
				   //System.out.println(listTag[i].split("=")[0]);
				   Tag tag = new Tag(listTag[i].split("=")[0].trim());
				   listAssociatedTag.add(tag);
			   }
			   //System.out.println(data[0] + " -> " + data[1]);
		   }
		}
		br.close();
		
		Factory<String> edgeFactory = new Factory<String>() {
			int i = 0;
			
			public String create() {
				return Integer.toString(++i);
			}
		};
			
		// Graph<V, E> where V is the type of the vertices and E is the type of
		// the edges
		// Note showing the use of a SparseGraph rather than a SparseMultigraph
		g = new SparseGraph<String, String>();
		
		for (Entry<Tag, List<Tag>> entry:mapTag.entrySet()) {
			//System.out.println(entry.getKey() + " ==> " + entry.getValue().toString());
			g.addVertex(entry.getKey().toString());
			
			for (Tag currentTag:entry.getValue()) {
				g.addEdge(edgeFactory.create(), entry.getKey().toString(), currentTag.getName());
			}
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("Usage: GraphView <list_tag_file> <bigram_file>");
			System.exit(0);
		}
		
		File listTagPath = new File(args[0]);
		File bigramPath = new File(args[1]);
		
		Dimension dimension = new Dimension(1000, 1000);
		
		final GraphView sgv = new GraphView(listTagPath, bigramPath); // This builds the graph
		// Layout<V, E>, BasicVisualizationServer<V,E>
		Layout<String, String> layout = new KKLayout<String, String>(sgv.g);
		layout.setSize(dimension);
		BasicVisualizationServer<String, String> vv = new BasicVisualizationServer<String, String>(layout);
		vv.setPreferredSize(dimension);
		
		// Transformer maps the vertex number to a vertex property
        Transformer<String, Paint> vertexColor = new Transformer<String, Paint>() {
            public Paint transform(String i) {
                //if(i.equals("1")) return Color.GREEN;
                return Color.WHITE;
            }
        };
        
        Transformer<String, Shape> vertexSize = new Transformer<String, Shape>(){
            public Shape transform(String i){
            	float scale = 1;
            	float total = 1;
            	for (Entry<Tag, List<Tag>> entry:sgv.mapTag.entrySet()) {
            		total += entry.getKey().getOccurence();
            		if (entry.getKey().getName().equals(i)) {
            			scale += entry.getKey().getOccurence();
            		}
            	}
            	scale = scale / total;
            	
                Ellipse2D circle = new Ellipse2D.Double(scale * -60, scale * -60, scale * 120, scale * 120);
                // in this case, the vertex is twice as large
                return AffineTransform.getScaleInstance(2, 2).createTransformedShape(circle);
                //else return circle;
            }
        };
        vv.getRenderContext().setVertexFillPaintTransformer(vertexColor);
        vv.getRenderContext().setVertexShapeTransformer(vertexSize);

        vv.getRenderContext().getMultiLayerTransformer().getTransformer(Layer.LAYOUT).setScale(1.2, 1.2, vv.getCenter());
        
        //VertexShapeSizeAspect<String, Number> vssa = new VertexShapeSizeAspect<String,Number>();
        //vv.getRenderContext().setVertexShapeTransformer(vssa);

		vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller<String>());
		//vv.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller());
		vv.getRenderer().getVertexLabelRenderer().setPosition(Position.N);
		
		JFrame frame = new JFrame("Graph View");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(new BorderLayout());
		frame.getContentPane().add(vv, BorderLayout.CENTER);
		frame.pack();
		frame.setVisible(true); 
	}
	/*
	private final static class VertexShapeSizeAspect<V, E> extends
			AbstractVertexShapeTransformer<V> implements Transformer<V, Shape> {

		public VertexShapeSizeAspect() {
			setSizeTransformer(new Transformer<V, Integer>() {
				public Integer transform(V v) {
					return 50;
				}
			});
			setAspectRatioTransformer(new Transformer<V, Float>() {
				public Float transform(V v) {
					return 1.0f;
				}
			});
		}

		public Shape transform(V v) {
			return factory.getEllipse(v);
		}
	}
	*/
}
