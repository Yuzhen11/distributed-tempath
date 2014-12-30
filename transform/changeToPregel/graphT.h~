#ifndef GRAPHT_H_HHWU
#define GRAPHT_H_HHWU

#include "graph.h"
#include "Timer.h"

#include <vector>
#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>

using namespace std;
const int infinity = 1e9;

class GraphT
{
public:
    GraphT(){}
    GraphT(const Graph& g);
    void add_edge(int u, int v, int w);
    void make_unique(vector<int> & data);
    int getVertexID(int u, int offset, int startOrArrival);
    
    void initial_query(const char* filePath); // query file
    void initial_query();
    void initial_ds();
    void initial_ds_f();
    
    void run_earliest_arrival(); 
    void earliest_arrival(int source);
    void run_fastest(); 
    void fastest(int source);
    
    // for testing the correctness
    void run_earliest_arrival(const char* filePath); // output the result
    void earliest_arrival(int source, FILE * file); 
    void run_fastest(const char* filePath);
    void fastest(int source, FILE * file);
    
    void print_result(const int source, const vector<int>& t_time, FILE * file);
    void print_avg_time();
    void print_avg_time(const char* filePath1, const char* filePath2);
    void show_statistics(const char* filePath1, const char* filePath2);
    
    
    bool reachability(int from, int to, int t1, int t2);
    int getFrom(int from, int t1, int t2);
    int getTo(int to, int t1, int t2);
    int output(string s);
public:
    int V, E, gv;
    vector< vector< pair<int,int> > > adj;
    vector< vector<int> > startT, arrivalT;
    vector< int > curSize, sumSize;
    vector< int > timeId, originalId;
    vector< int > sources; 
    int t_start, t_end;
    double time_sum;
    vector <int> a_time, f_time;
    queue <int> myqueue;
    vector <bool> mark_in_Q; // whether the vertex is in queue
    vector <bool> visited; // Whether the vertex has been visited 
    
    int originalV;
};


int GraphT::output(string s)
{


	fstream out;
	string sdata = s + "T.txt";
	cout << "writing to " << sdata << " ..." << endl;
	out.open(sdata.c_str(), ios::out);
	
	// for TF
	
	//out << V << "\t" << E << endl;
	for (int i = 0; i < V; ++ i)
	{
		//from originalID timestamp  num_of_neighbors n1.t n1.w... toOriginalId
		out << i << " " << originalId[i] << " " << timeId[i] << " " << adj[i].size() << " ";
		for (int j = 0; j < adj[i].size(); ++ j)
		{
			out << adj[i][j].first << " " << adj[i][j].second << " ";	
		}
		out << originalId[i] + V;
		out << endl;
	}
	//virtual node
	for (int i = 0; i < gv; ++ i)
	{
		out << i+V << " " << i << " " << -1 << " " << 0 << " " << 0 << endl;
	}
	
	out.close();

}

void GraphT::make_unique(vector<int> & data)
{
    sort(data.begin(),data.end());
    data.erase( unique(data.begin(),data.end()),data.end());
}


int GraphT::getVertexID(int u, int offset, int startOrArrival)
{
    //  0 arrival 1 start
    
    int base = u == 0 ? 0 : sumSize[u - 1];
    const vector<int> & arrival = arrivalT[u];
    const vector<int> & start = startT[u];
 
    if(startOrArrival == 0)
    {
        return base + offset;
    }
    else if(startOrArrival == 1)
    {
        return base +  arrival.size() + offset;
    }

}

void GraphT::add_edge(int u,int v,int w)
{
    E ++;
    adj[u].push_back( std::make_pair(v,w) );
}

GraphT::GraphT(const Graph& g)
{
    startT = vector< vector<int> >(g.V, vector<int>() );
    arrivalT = vector< vector<int> >(g.V, vector<int>());
    curSize = vector<int>(g.V, 0);
    sumSize = vector<int>(g.V, 0);

    for(int i = 0 ;i < g.V ; i ++)
    {
        for(int j = 0 ;j < g.adj[i].size() ; j ++)
        {
            int u = i;
            int v = g.adj[i][j].v;
            int w = g.adj[i][j].w;
            for(int k = 0 ; k < g.adj[i][j].interval.size(); k ++)
            {
                int t = g.adj[i][j].interval[k];
                // u -> v at t

                startT[u].push_back(t);
                arrivalT[v].push_back(t + w);
            }
        }
    }

    for(int i = 0 ;i < g.V ;i ++)
    {
        make_unique(startT[i]);
        make_unique(arrivalT[i]);
        int cur = startT[i].size() + arrivalT[i].size();
        curSize[i] = cur;
        sumSize[i] = cur + (i > 0 ? sumSize[i-1] : 0);
    }
    
    for(int i=0; i< g.V; i++){
		for(int j=0; j<arrivalT[i].size(); j++){
			timeId.push_back(arrivalT[i][j]);
		}
		
		for(int j=0; j<startT[i].size(); j++){
			timeId.push_back(startT[i][j]);
		}
	}
    
    V =  sumSize.back();
    gv = g.V;
    //distance.resize(gv);
    adj.resize(V);
    originalId.resize(V);
    //dis.resize(V);
    visited.resize(V);
    E = 0;
    
    //Mapping the new vertex id to original vertex id
    
    for(int i=0; i< g.V; i++){
    	for(int j = 0 ; j < arrivalT[i].size(); j ++)
        {
            int u = getVertexID(i, j, 0);
            originalId[u]=i;
        }
        
        for(int j = 0 ; j < startT[i].size() ; j ++)
        {
            int u = getVertexID(i, j , 1);
            originalId[u]=i;
        }
    }
    
    
    
    // constructing edges;

    
    for(int i = 0 ;i < g.V; i ++)
    {
        // link T1
        for(int j = 1 ; j < arrivalT[i].size(); j ++)
        {
            int u21 = getVertexID(i, j - 1, 0);
            int u22 = getVertexID(i, j, 0);
            int w = arrivalT[i][j] - arrivalT[i][j-1];

            add_edge(u21,u22,0);
        }
        
        // link T2
        for(int j = 1 ; j < startT[i].size() ; j ++)
        {
            int u11 = getVertexID(i, j-1, 1);
            int u12 = getVertexID(i, j , 1);
            int w = startT[i][j] - startT[i][j-1];

            add_edge(u11,u12,0);
        }

        //link T1 to T2;

        int last = -1;        
        for(int j = int(arrivalT[i].size()) - 1; j >= 0 ; j --)
        {
            int u21 = getVertexID(i, j, 0);
            vector<int>::iterator it = lower_bound( startT[i].begin(),startT[i].end(), arrivalT[i][j] );
            if( it != startT[i].end() )
            {
                int offset = it - startT[i].begin();
                if(offset == last)
                    continue;
                last = offset;
                int u1 = getVertexID(i , offset , 1);
                int w  =  startT[i][offset] -  arrivalT[i][j];
                add_edge(u21, u1, 0);
            }
        }
        
        // add edge from temporal graph
        for(int j = 0 ;j < g.adj[i].size() ; j ++)
        {
            for(int k = 0 ;k < g.adj[i][j].interval.size() ; k ++)
            {
                int u = i, v = g.adj[i][j].v, w = g.adj[i][j].w, t = g.adj[i][j].interval[k];

                int offset_u = lower_bound( startT[u].begin(), startT[u].end(), t  ) - startT[u].begin();
                int offset_v = lower_bound( arrivalT[v].begin(), arrivalT[v].end(), t + w ) - arrivalT[v].begin();
                int uT = getVertexID(u, offset_u, 1);
                int vT = getVertexID(v, offset_v, 0);

                add_edge(uT,vT, w);

            }
        }
    }

}



#endif
