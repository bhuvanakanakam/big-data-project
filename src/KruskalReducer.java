import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.HashMap;
import java.util.Map;


public class KruskalReducer extends Reducer<Text, Text, Text, NullWritable> {

    private static class Edge implements Comparable<Edge> {
        private String source;
        private String destination;
        private int weight;
        
        public Edge(String source, String destination, int weight) {
            this.source = source;
            this.destination = destination;
            this.weight = weight;
        }
        
        public String getSource() {
            return source;
        }
        
        public String getDestination() {
            return destination;
        }
        
        public int getWeight() {
            return weight;
        }
        
        @Override
        public int compareTo(Edge other) {
            return Integer.compare(this.weight, other.weight);
        }
    }

    private static class UnionFind {
        private final Map<String, String> parent;
        
        public UnionFind() {
            parent = new HashMap<>();
        }
        
        public String find(String vertex) {
            if (!parent.containsKey(vertex)) {
                parent.put(vertex, vertex);
                return vertex;
            }

            if (!parent.get(vertex).equals(vertex)) {
                parent.put(vertex, find(parent.get(vertex)));
            }

            return parent.get(vertex);
        }

        public void union(String vertex1, String vertex2) {
            String root1 = find(vertex1);
            String root2 = find(vertex2);

            if (!root1.equals(root2)) {
                parent.put(root1, root2);
            }
        }

        public boolean isConnected(String vertex1, String vertex2) {
            return find(vertex1).equals(find(vertex2));
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Edge> edges = new ArrayList<>();
        
        // Parse and add edges to the list
        for (Text value : values) {
            String[] parts = value.toString().split("-");
            String source = parts[0];
            String destination = parts[1];
            int weight = Integer.parseInt(key.toString());
            edges.add(new Edge(source, destination, weight));
        }
        
        // Sort edges by weight
        Collections.sort(edges);
        
        // Initialize union-find data structure
        UnionFind unionFind = new UnionFind();
        
        // Iterate over sorted edges
        for (Edge edge : edges) {
            String source = edge.getSource();
            String destination = edge.getDestination();
            
            // Check if adding this edge creates a cycle
            if (!unionFind.isConnected(source, destination)) {
                // If not, add the edge to the minimum spanning tree
                context.write(new Text(source + "-" + destination), NullWritable.get());
                unionFind.union(source, destination);
            }
        }
    }
}
