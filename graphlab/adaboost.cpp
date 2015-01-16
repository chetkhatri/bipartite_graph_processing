/**
 * Copyright (c) 2009 Carnegie Mellon University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */

//#include <vector>
//#include <string>
//#include <fstream>
#include <math.h>
#include <iomanip>
//#include <tuple>

//#include <graphlab/agents/agent_container.hpp>

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_stl.hpp>
#include <boost/unordered_set.hpp>

#include <graphlab.hpp>

#include <graphlab/util/stl_util.hpp>


#include <graphlab/macros_def.hpp>


int num_features;
int num_instances;
int vert_id_index;
double eta;

struct vert_data {
    int label;
    bool instance; //true if "instance" vertice, false if "feature" vertice
    double qval; //qval is the q for instance vertices, BUT ALSO the newly computed edge weights for feature verts
    int counter;
    double pos_cor;
    double neg_cor;
    
    vert_data():label(-2),instance(false), qval(0.0), counter(0){ }
    explicit vert_data(std::string node_type, int _label):label(_label),instance(true),qval(0.0),counter(0),pos_cor(0.0),neg_cor(0.0){}
    explicit vert_data(std::string node_type):label(0),instance(false),qval(0.0),counter(0){}
    
    void save(graphlab::oarchive& oarc) const {
        oarc << label << instance << qval << counter << pos_cor << neg_cor;
        //if(instance==false)
        //    std::cout << std::setprecision(9) << qval << std::endl;
        
    }
    void load(graphlab::iarchive& iarc) {
        iarc >> label >> instance >> qval >> counter >> pos_cor >> neg_cor;
        //if(instance==false)
        //    std::cout << std::setprecision(9) << qval << std::endl;
    }
};

struct edge_data : public graphlab::IS_POD_TYPE {
    double weight;
    edge_data():weight(0.0) {}
};

typedef graphlab::distributed_graph<vert_data, edge_data> graph_type;

//weights initialized to 1 / #times feature is used
void init_edge_weight(graph_type::edge_type& edge) {
    edge.data().weight = 1.0/( double(edge.source().num_out_edges()) );
    //std::cout << "init weight: " << edge.data().weight << " " << edge.source().num_out_edges() << std::endl;
    edge.source().data().qval = edge.data().weight;
    //std::cout << "init qval: " << edge.source().data().qval << std::endl << std::endl;
}

//writes the weight of the edges (the edge weights are written as the qval of feature vertices)
//(if something happens with the qval, could potentially look into the save_edge func at bottom
class edge_weight_writer {
public:
    std::string save_vertex(graph_type::vertex_type v) {
        std::stringstream strm;
        if(v.data().instance==true)
        strm << v.id() << "\t" << v.data().qval << "\n"; //<< v.data().label << "\t" << v.num_in_edges() << "," << v.num_out_edges();
        //#}
        /*if(v.data().instance==false) {
            strm << v.id() << "\t" << std::setprecision(9) << v.data().qval << "\t" << std::setprecision(9) << v.data().pos_cor << "\t" << std::setprecision(9) << v.data().neg_cor << "\n";
        }*/
        return strm.str();
    }
    std::string save_edge(graph_type::edge_type e) { return ""; }
};

//writes the output of the instance vertices -- not enabled by default
class instance_qval_writer {
public:
    std::string save_vertex(graph_type::vertex_type v) {
        std::stringstream strm;
        if(v.data().instance==true) {
            strm << v.id() << "\t"  << v.data().qval << "\n";
        }
        return strm.str();
    }
    std::string save_edge(graph_type::edge_type e) { return "";}
};

/*parses feature adjacency list of form:
    #num_instances num_features
    0 feat_i feat_i+1
    1 feat_i+8 feat_i+x etc
 */

/*
 a parser for when the input format is an edgelist
 e.g.  vert_group_a_id vert_group_b_id
 */
bool edgelist_parser(graph_type & graph,
                     const std::string& filename,
                     const std::string& textline) {
    std::stringstream strm(textline);
    
}

/* for adjacency lists, so each line is:
    group_a_vert_id group_b_vert_id1 group_b_vert_id2 group_b_vert_id3
 */
bool feature_adj_list_parser(graph_type & graph,
                      const std::string& filename,
                      const std::string& textline) {
    //first line starts with '#' and gives number of instances and featurse
    std::stringstream strm(textline);
    if(textline[0]=='#') {
        std::string blank; //capture '#'
        strm >> blank;
        strm >> num_instances;
        strm >> num_features;
        //feature are indexed at the number of instances (num_instances indexed at 0)
        for(int i=0;i<num_features;i++) {
            graphlab::vertex_id_type fid = i+num_instances;
            graph.add_vertex(fid,vert_data("feature"));
        }
    }
    else {
        int label;
        graphlab::vertex_id_type vid;
        strm >> vid;
        strm >> label;
        graph.add_vertex(vid, vert_data("instance", label));
        int feature_val;
        while(1) {
            strm >> feature_val;
            if(strm.fail()) {
                return true;
            }
            graphlab::vertex_id_type fid;
            fid = (feature_val-1) + num_instances; //feature vals indexed at num_instances, listed values start at 1
            graph.add_edge(fid, vid, edge_data());
        }
    }
}

bool feature_matrix_parser(graph_type & graph,
                          const std::string& filename,
                          const std::string& textline) {
    //on the first pass, count the number of features
    //check if first pass by checking global 'num_features' variable
    if(num_features==-1) {
        num_features=1;
        
        std::stringstream strm(textline);
        int label;
        strm >> label;
        
        graphlab::vertex_id_type vid = vert_id_index;
        graph.add_vertex(vid, vert_data("instance", label));
        
        graphlab::vertex_id_type fid = num_features;
        int feature_val;
        while(1) {
            strm>>feature_val;
            if(strm.fail()) {
                vert_id_index=num_features;
                return true;
            }
            fid = num_features;
            graph.add_vertex(fid, vert_data("feature"));
            if(feature_val==1){
                graph.add_edge(fid, vid, edge_data());
                //graph.add_edge(fid, vid);
            }
            num_features++;
        }
        
    }
    else {
        std::stringstream strm(textline);
        int label;
        strm >> label;
        graphlab::vertex_id_type vid = vert_id_index;
        graph.add_vertex(vid, vert_data("instance", label));
        
        graphlab::vertex_id_type fid;
        int feature_index=1;
        int feature_val;
        while(1) {
            strm>>feature_val;
            if(strm.fail()) {
                vert_id_index++;
                return true;
            }
            if(feature_val==1) {
                fid = feature_index;
                graph.add_edge(fid,vid, edge_data());
                //graph.add_edge(fid,vid);
            }
            feature_index++;
        }
    }
}

/* 
 the vertex program to be executed depends on whether a vertex is an "instance" or a "feature"
 if an instance, then weights from all in edges are applied to the q-function
 if a feature, then the weights are applied to one of two sums, depending on the target "instance" vertex q value
 
 for this logic to be contained in a single gather function, which returns the same value regardless of vertex, then we must implement a special data type than can account for both circumstances
 
 */

struct boost_struct : graphlab::IS_POD_TYPE {
    double weight_sum;
    double pos_cor;
    double neg_cor;
    
    boost_struct() {}
    boost_struct(double ws, double pc, double nc):weight_sum(ws),pos_cor(pc),neg_cor(nc){}
    boost_struct& operator+=(const boost_struct& other) {
        weight_sum+=other.weight_sum;
        pos_cor+=other.pos_cor;
        neg_cor+=other.neg_cor;
        return *this;
    }
};

class adaboost :
public graphlab::ivertex_program<graph_type, boost_struct>,
public graphlab::IS_POD_TYPE {
public:
    
    edge_dir_type gather_edges(icontext_type& context,
                               const vertex_type& vertex) const {
        if (vertex.data().instance){
            return graphlab::IN_EDGES;
        }
        else if (vertex.data().instance==false) {
            return graphlab::OUT_EDGES;
        }
    }
    // for each in-edge get the id of the target vertex
    // assume this is vertex global ids
    boost_struct gather(icontext_type& context, const vertex_type& vertex,
                  edge_type& edge) const {
        if (vertex.data().instance)
            return (boost_struct(edge.data().weight,0.0,0.0));
        else if (vertex.data().instance==false) {
            if (edge.target().data().label == 1)
                return (boost_struct(0.0,edge.target().data().qval,0.0) );
            else if (edge.target().data().label == -1)
                return (boost_struct( 0.0,0.0,edge.target().data().qval) );
        }
    }
    
    // Use the total rank of adjacent pages to update this page
    void apply(icontext_type& context, vertex_type& vertex,
               const gather_type& total) {
        if (vertex.data().instance) {
            vertex.data().qval = 1/(1+exp(vertex.data().label * total.weight_sum));
        }
        else if (vertex.data().instance==false) {
            double pc=total.pos_cor;
            double nc=total.neg_cor;
            if (nc<.0000000001) {
                nc=.0000000001;
                //std::cout << "zero neg_cor " << vertex.id() << ": "<<vertex.data().qval<<", " << total.pos_cor << ", " << total.neg_cor<< std:: endl;
            }
            if (pc<.0000000001) {
                pc=.0000000001;
                //std::cout << "zero neg_cor " << vertex.id() << ": "<<vertex.data().qval<<", " << total.pos_cor << ", " << total.neg_cor<< std:: endl;
            }
            
            vertex.data().qval = eta*log10(pc/nc);
            vertex.data().pos_cor = pc;
            vertex.data().neg_cor = nc;
        }
        ++vertex.data().counter;
    }
    
    // No scatter needed. Return NO_EDGES
    edge_dir_type scatter_edges(icontext_type& context,
                                const vertex_type& vertex) const {
        if (vertex.data().instance)
            return graphlab::IN_EDGES;
        else if (vertex.data().instance==false)
            return graphlab::OUT_EDGES;
    }
                        
    void scatter(icontext_type& context, const vertex_type& vertex,
                edge_type& edge) const {
        if (vertex.data().instance) {
            context.signal(edge.source());
        }
        else if (vertex.data().instance==false) {
            edge.data().weight+=vertex.data().qval;
            if(vertex.data().counter < 2 ) {
                context.signal(edge.target());
            }
        }
    }
};

bool has_in_edges(const graph_type::vertex_type& vertex) {
    return vertex.num_in_edges() > 0;
}

bool has_out_edges(const graph_type::vertex_type& vertex) {
    return vertex.num_out_edges() > 0;
}

int main(int argc, char** argv) {
    // Initialize control plain using mpi
    graphlab::mpi_tools::init(argc, argv);
    graphlab::distributed_control dc;
    
    graph_type graph(dc);
    
    num_features=-1;
    vert_id_index=0;
    eta=.5;
    
    graph.load("input_data/x24data_adjlist.txt", feature_adj_list_parser);
    graph.finalize();
    graph.transform_edges(init_edge_weight);
    
    graphlab::omni_engine<adaboost> engine(dc, graph, "sync");
    graphlab::vertex_set instance_verts = graph.select(has_in_edges);
    //std::cout << "num instance verts: " << graph.vertex_set_size(instance_verts) << std::endl;
    engine.signal_vset(instance_verts);
    //graphlab::vertex_set feature_verts = graph.select(has_out_edges);
    //engine.signal_vset(feature_verts);
    engine.start();
    
    graph.save("weight_out",
               edge_weight_writer(),
               //instance_qval_writer(),
               false, // set to true if each output file is to be gzipped
               true, // whether vertices are saved
               false); // whether edges are saved
    
    graphlab::mpi_tools::finalize();
    return EXIT_SUCCESS;
} // End of main
