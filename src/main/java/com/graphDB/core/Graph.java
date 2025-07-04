package com.graphDB.core;

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
    @Deprecated
    public Graph(Graph other) {
        this.numVertices = other.numVertices;
        this.capacity = other.capacity;
        this.totalEdges = other.totalEdges;
        this.adj = new ArrayList<>(this.capacity);

        // Deep copy adjacency lists
        for (int i = 0; i < other.adj.size(); i++) {
            this.adj.add(new ArrayList<>(other.adj.get(i)));
        }

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

    private boolean isCyclicUtilWithHypotheticalEdges(int u, boolean[] visited, boolean[] recursionStack, Map<Integer, List<Integer>> hypotheticalNewEdges) {

        if (recursionStack[u]) {
            return true;
        }
        if (visited[u]) {
            return false;
        }

        visited[u] = true;
        recursionStack[u] = true;

        if (u < numVertices) {
            for (int neighbour : adj.get(u)) {
                if (isCyclicUtilWithHypotheticalEdges(neighbour, visited, recursionStack, hypotheticalNewEdges)) {
                    return true;
                }
            }
        }

        if (hypotheticalNewEdges.containsKey(u)) {
            for (int neighbour : hypotheticalNewEdges.get(u)) {
                if (isCyclicUtilWithHypotheticalEdges(neighbour, visited, recursionStack, hypotheticalNewEdges)) {
                    return true;
                }
            }
        }

        recursionStack[u] = false;
        return false;
    }

    public boolean hasCycleWithProposedAdditions(int proposedNodeId, Map<Integer, List<Integer>> hypotheticalNewEdges) {
        if (hypotheticalNewEdges.isEmpty()) {
            return hasCycle();
        }

        int maxVertexId = Math.max(proposedNodeId, numVertices - 1);
        for (Map.Entry<Integer, List<Integer>> entry : hypotheticalNewEdges.entrySet()) {
            maxVertexId = Math.max(maxVertexId, entry.getKey());
            for (int target : entry.getValue()) {
                maxVertexId = Math.max(maxVertexId, target);
            }
        }

        // Create arrays sized to accommodate the hypothetical vertex
        boolean[] visited = new boolean[maxVertexId + 1];
        boolean[] recursionStack = new boolean[maxVertexId + 1];

        if (isCyclicUtilWithHypotheticalEdges(proposedNodeId, visited, recursionStack, hypotheticalNewEdges)) {
            return true;
        }

        // Also check all existing vertices (in case the hypothetical edges create cycles elsewhere)
        for (int i = 0; i < numVertices; i++) {
            if (!visited[i] && isCyclicUtilWithHypotheticalEdges(i, visited, recursionStack, hypotheticalNewEdges)) {
                return true;
            }
        }

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

        double densityValue = 0.0;
        if (this.numVertices > 1) {
            densityValue = (double) this.totalEdges / ((double) this.numVertices * (double) (this.numVertices - 1));
        } else if (this.numVertices == 1 && this.totalEdges > 0) {
            densityValue = (double) this.totalEdges;
        }
        return String.format("Graph[vertices=%d, edges=%d, capacity=%d, density=%.3f]",
                numVertices, totalEdges, capacity, densityValue);
    }

    public void setNumVerticesAndEnsureCapacity(int count) {
        this.numVertices = count;
        while (this.capacity < count) {
            growCapacity();
        }

        while (this.adj.size() < this.capacity) {
            this.adj.add(new ArrayList<>());
        }

        while (this.adj.size() < count) {
            this.adj.add(new ArrayList<>());
        }
    }

    public void syncVertices(int expectedNumVertices) {
        this.numVertices = 0;
        for (int i =0; i< expectedNumVertices; i++) {
            addNode();
        }
    }

    public void ensureNodeExistsUpTo(int nodeId) {
        while (nodeId >= this.capacity) {
            growCapacity();
        }
        while (nodeId >= this.adj.size()) {
            this.adj.add(new ArrayList<>());
        }

        if (nodeId >= this.numVertices) {
            this.numVertices = nodeId + 1;
        }
    }

    public void clearGraph() {
        this.numVertices = 0;
        this.totalEdges = 0;
        this.adj.clear();

        for (int i=0; i< this.capacity; i++) {
            this.adj.add(new ArrayList<>());
        }
    }

    public void clearEdges() {
        this.totalEdges = 0;
        for (List<Integer> adjList : this.adj) {
            adjList.clear();
        }
    }

    @Override
    public String toString() {
        return getGraphStats();
    }

}
