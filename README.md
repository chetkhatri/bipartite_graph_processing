# bipartite_graph_processing
Large-Scale Graph Processing: Compares GraphLab and Iterative MapReduce for Parallel Adaboost on a Bipartite Graph

This project compares the Twister iterative MapReduce framework against the vertex-oriented GraphLab framework for processing a bipartite graph

MapReduce is a Big Data programming model that performs computation in a single pass over the data.  A few frameworks have been developed that extend the MapReduce model to include loop-awareness, so users may program iterative algorithms.  These iterative MapReduce frameworks include Twister, HaLoop, and iMapReduce.  This project uses Twister.

MapReduce is generally not a good programming model for performing graph computations for many of the same reasons large-scale graph computation is difficult, namely the inherent interdependencies in the graph data structure.  

Recently, vertex-oriented graph processsing frameworks have been introduced that are designed for executing iterative graph algorithms on large-scale, distributed graphs.  Users adopt a vertex-centric programming model that increases locality and reduces inter-node communication in a distributed memory environment.  Exemplary frameworks include Giraph (Pregel) and GraphLab.  For a comprehensive survey of vertex-oriented frameworks, see: http://www3.nd.edu/~rmccune/papers/Think_Like_a_Vertex_MWM.pdf

Despite the advantages of the vertex-oriented programming model over the MapReduce model, the latter is commonly used.

Recently, Google introduced the Sibyl framework (https://users.soe.ucsc.edu/~niejiazhong/slides/chandra.pdf) for large-scale machine learning.  The presentation includes a Parallelized AdaBoost algorithm that is computed using iterative MapReduce by converting the data into a bipartite graph (slide 10-11).

This project implements a vertex-oriented parallel AdaBoost algorithm, and then demonstrates the viability of the vertex-oriented model for large-scale distributed graphs by comparing runtime against parallel AdaBoost implemented for Twister.