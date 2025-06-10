package com.graphlog.core;

import lombok.Getter;

import java.util.*;

public class Graph {

    @Getter
    private int numVertices;
    private List<List<Integer>> adj;
    private int capacity;

    private int totalEdges = 0;

    public Graph(int initialCapacity) {
        this.capacity = Math.max(initialCapacity, 16);
        this.numVertices = 0;
        this.adj = new ArrayList<>(this.capacity);

        for (int i = 0; i < this.capacity; i++) {
            adj.add(new ArrayList<>());
        }
    }

    /**
     * Copy constructor for creating temporary graphs (used in cycle detection)
     */
    public Graph(Graph other) {
        this.numVertices = other.numVertices;
        this.capacity = other.capacity;
        this.totalEdges = other.totalEdges;
        this.adj = new ArrayList<>(this.capacity);

        // Deep copy adjacency lists
        for (int i = 0; i < other.adj.size(); i++) {
            this.adj.add(new ArrayList<>(other.adj.get(i)));
        }

        // Fill remaining capacity if needed
        for (int i = other.adj.size(); i < this.capacity; i++) {
            this.adj.add(new ArrayList<>());
        }
    }

    public int addNode() {
        if (numVertices == capacity) {
            growCapacity();
        }
        return numVertices++;
    }

    private void growCapacity() {
        int newCapacity = capacity * 2;
        List<List<Integer>> newAdj = new ArrayList<>(newCapacity);
        newAdj.addAll(adj);

        for (int i = capacity; i < newCapacity; i++) {
            newAdj.add(new ArrayList<>());
        }

        adj = newAdj;
        capacity = newCapacity;
    }


    public void addDirectedEdge(int uEffect, int vCause) {
        validateVertex(uEffect);
        validateVertex(vCause);

        if (!adj.get(uEffect).contains(vCause)) {
            adj.get(uEffect).add(vCause);
            totalEdges++;
        }
    }

    private void validateVertex(int v) {
        if (v < 0 || v >= numVertices) {
            throw new IllegalArgumentException("Vertex " + v + " is out of bounds [0, " + (numVertices - 1) + "]");
        }
    }

    private boolean isCyclicUtil(int u, boolean[] visited, boolean[] recursionStack) {
        if (recursionStack[u]) {
            return true;
        }
        if (visited[u]) {
            return false;
        }

        visited[u] = true;
        recursionStack[u] = true;

        for (int neighbour : adj.get(u)) {
            if (isCyclicUtil(neighbour, visited, recursionStack)) {
                return true;
            }
        }
        recursionStack[u] = false;
        return false;
    }

    public boolean hasCycle() {

        if (numVertices == 0) {
            return false;
        }

        boolean[] visited = new boolean[numVertices];
        boolean[] recursionStack = new boolean[numVertices];

        for (int i =0; i<numVertices; i++) {
            if (!visited[i] && isCyclicUtil(i, visited, recursionStack)) {
                return true;
            }
        }
        return false;
    }

    private void topologicalSortUtil(int u, boolean[] visited, Stack<Integer> stack) {
        visited[u] = true;
        for (int neighbour : adj.get(u)) {
            if (!visited[neighbour]) {
                topologicalSortUtil(neighbour, visited, stack);
            }
        }
        stack.push(u);
    }

    public List<Integer> topologicalSort() {
        if (hasCycle()) {
            throw new IllegalStateException("Graph has a cycle, cannot topologically sort");
        }

        boolean[] visited = new boolean[numVertices];
        Stack<Integer> stack = new Stack<>();

        for (int i=0; i<numVertices; i++) {
            if (!visited[i]) {
                topologicalSortUtil(i, visited, stack);
            }
        }

        LinkedList<Integer> result = new LinkedList<>();
        while (!stack.isEmpty()) {
            result.add(stack.pop());
        }
        return result;
    }

    public List<Integer> getNeighbours(int u) {
        validateVertex(u);
        return new ArrayList<>(adj.get(u));
    }

    /**
     * Get all vertices reachable from a given starting vertex
     * Used for finding causal ancestry
     */
    public Set<Integer> getReachableVertices(int startVertex) {
        validateVertex(startVertex);

        Set<Integer> reachable = new HashSet<>();
        Deque<Integer> stack = new ArrayDeque<>();

        stack.push(startVertex);
        reachable.add(startVertex);

        while (!stack.isEmpty()) {
            int current = stack.pop();

            for (int neighbor : adj.get(current)) {
                if (!reachable.contains(neighbor)) {
                    reachable.add(neighbor);
                    stack.push(neighbor);
                }
            }
        }
        return reachable;
    }

    public String getGraphStats() {
        return String.format("Graph[vertices=%d, edges=%d, capacity=%d, density=%.3f]",
                numVertices, totalEdges, capacity,
                numVertices > 0 ? (double) totalEdges / (numVertices * numVertices) : 0.0);
    }

    @Override
    public String toString() {
        return getGraphStats();
    }

}
