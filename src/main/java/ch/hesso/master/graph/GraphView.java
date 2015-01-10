package ch.hesso.master.graph;

import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Paint;
import java.awt.Shape;
import java.awt.Stroke;
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
import java.util.TreeMap;

import javax.swing.JFrame;

import org.apache.commons.collections15.Factory;
import org.apache.commons.collections15.Transformer;

import ch.hesso.master.utils.Utils;
import edu.uci.ics.jung.algorithms.layout.KKLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.SparseGraph;
import edu.uci.ics.jung.graph.util.EdgeType;
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
		  
	   int currentTotal;
	   
	   // Used to get the best probability tag after current
	   TreeMap<Integer, String> nextTagOccurence = new TreeMap<Integer, String>();
		
		br = new BufferedReader(new FileReader(bigramPath));
		while ((line = br.readLine()) != null) {
		   String[] data = line.replace("[", "").replace("}", "").split("\\]\t\\{");
		   Tag mainTag = new Tag(data[0].trim());
		   //System.out.println(mainTag);
		   List<Tag> listAssociatedTag = mapTag.get(mainTag);
		   
		   currentTotal = 0;
		   nextTagOccurence.clear();
		   
		   if (listAssociatedTag != null) {
			   String[] listTag = data[1].split(", ");
			   for (int i = 0; i < listTag.length; i++) {
				   String[] dataTag = listTag[i].split("=");
				   Tag tag = new Tag(dataTag[0], Integer.parseInt(dataTag[1]));
				   listAssociatedTag.add(tag);
				   
				   
				   if (nextTagOccurence.get(tag.getOccurence()) != null) {
					   nextTagOccurence.put(tag.getOccurence(), tag.getName() + ", " + nextTagOccurence.get(tag.getOccurence()));
				   } else {
					   nextTagOccurence.put(tag.getOccurence(), tag.getName());
				   }
				   currentTotal += tag.getOccurence();
				   //System.out.println(mainTag.getName() + " -> " + tag.getName() + " = " + tag.getOccurence());
			   }
			   //System.out.println(data[0] + " -> " + data[1]);
		   }
		   
		   if (listAssociatedTag != null) {
			   System.out.println(mainTag.toString() + "(" + currentTotal + ")" + nextTagOccurence.toString());

				ArrayList<Integer> keys = new ArrayList<Integer>(nextTagOccurence.keySet());
				for (int i = keys.size() - 1; i >= 0; i--) {
					System.out.println(" - (" + ((float)keys.get(i) / currentTotal) * 100 + "%) " + nextTagOccurence.get(keys.get(i)));
				}
		   }
		}
		br.close();
		
		Factory<String> edgeFactory = new Factory<String>() {
			int i = 0;
			
			public String create() {
				return Integer.toString(++i);
			}
		};
		
		g = new SparseGraph<String, String>();
		
		for (Entry<Tag, List<Tag>> entry:mapTag.entrySet()) {
			g.addVertex(entry.getKey().toString());
			
			for (Tag currentTag:entry.getValue()) {
				if (mapTag.get(currentTag) != null) {
					g.addEdge(edgeFactory.create(), entry.getKey().toString(), currentTag.getName(), EdgeType.DIRECTED);
				}
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
		
		/**
		 * BUILD THE GRAPH
		 */
		final GraphView sgv = new GraphView(listTagPath, bigramPath);
		Layout<String, String> layout = new KKLayout<String, String>(sgv.g);
		layout.setSize(dimension);
		BasicVisualizationServer<String, String> vv = new BasicVisualizationServer<String, String>(layout);
		vv.setPreferredSize(dimension);
		
        Transformer<String, Paint> vertexColor = new Transformer<String, Paint>() {
            public Paint transform(String i) {
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
                return AffineTransform.getScaleInstance(2, 2).createTransformedShape(circle);
            }
        };
        vv.getRenderContext().setVertexFillPaintTransformer(vertexColor);
        vv.getRenderContext().setVertexShapeTransformer(vertexSize);
        
        Transformer<String, Paint> edgePaint = new Transformer<String, Paint>() {
            public Paint transform(String s) {
                return Color.BLACK;
            }
        };

        Transformer<String, Stroke> edgeStroke = new Transformer<String, Stroke>() {
            public Stroke transform(String s) {
                return new BasicStroke(1.0f /*, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.0f, dash, 0.0f */);
            }
        };
        
        vv.getRenderContext().setEdgeDrawPaintTransformer(edgePaint);
        vv.getRenderContext().setEdgeStrokeTransformer(edgeStroke);

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
}
