#include "basic/pregel-dev.h"
#include "utils/type.h"
#include <sstream>
#include <set>
#include <map>
#include <algorithm>
#include <iostream>
using namespace std;

const int inf = 1e9;
int src = 0;
int tstart = 0;
int tend = inf;


struct vertexValue
{

    int originalID;
    int timestamp;
    vector<pair<int,int> > neighbors;
    
    int toOriginalId;
};

ibinstream & operator<<(ibinstream & m, const vertexValue & v){

    m << v.originalID;
    m << v.timestamp;
    m << v.neighbors;
    m << v.toOriginalId;
    return m;
}

obinstream & operator>>(obinstream & m, vertexValue & v){

    m >> v.originalID;
    m >> v.timestamp;
    m >> v.neighbors;
    m >> v.toOriginalId;
    return m;
}
//vertex
class pVertex : public Vertex<VertexID, vertexValue, int>{

public:
    int arrivalTime;
    int start;
    void broadcast(int startTime)
    {
    	for (int i = 0; i < value().neighbors.size(); ++ i) 
           send_message(value().neighbors[i].first, startTime);
    }
    virtual void compute(MessageContainer& messages)
    {
    //phase 1
    if (global_phase_num == 1) {
	if (step_num() == 1)
	{
	    start = -1;
	    if (value().originalID == src) 
	    {
	    	start = value().timestamp;
	   	broadcast(start);
	    }
	   
	}
	else
	{
	    int maxi = -1;
	    for (int i = 0; i < messages.size(); ++ i)
	    {
	    	if (messages[i] > maxi) maxi = messages[i];
	    }
	    if (maxi > start)
	    {
	    	start = maxi;
	    	broadcast(start);
	    }
	}
	vote_to_halt();
    }
    //phase 2
    else if (global_phase_num == 2)
    {
        if (step_num() == 1 && value().timestamp >= 0 && start != -1)
        send_message(value().toOriginalId, -(value().timestamp-start) );
        
        if (step_num() == 2 && value().timestamp < 0)
        {
       	    int maxi = -inf;
            for (int i = 0; i < messages.size(); ++ i) 
            {
            	if (messages[i] > maxi) maxi = messages[i];
            }
            maxi = -maxi;
            arrivalTime = maxi;
        }
        vote_to_halt();
    }
    
    
    }

};

class pathWorker : public Worker<pVertex>{
    char buf[1000];

public:
    virtual pVertex* toVertex(char * line)
    {
        pVertex* v = new pVertex;
        istringstream ssin(line);
        int from, o_id, timestamp, num_of_neighbors, to, w;
        //format: from originalID timestamp num_of_neighbors n1.id n1.w ... toOriginalId;
        //timestamp == -1 means virtual node 
        ssin >> from >> o_id >> timestamp >> num_of_neighbors;
        
        v->id = from;
        v->value().originalID = o_id;
        v->value().timestamp = timestamp;
        v->value().neighbors.resize(num_of_neighbors);
        for (int i = 0; i < num_of_neighbors; ++ i)
        {
            ssin >> to >> w;
            v->value().neighbors[i] = make_pair(to, w);
        }
        ssin >> v->value().toOriginalId;
        return v;
        //cout << "load done" << endl;
    }
    virtual void toline(pVertex* v, BufferedWriter& writer)
    {
    	if (v->value().timestamp < 0)
    	{
        sprintf(buf, "%d %d\n", v->value().originalID, v->arrivalTime);
        writer.write(buf);
        }	
    }

};

class pathCombiner : public Combiner<int>
{
public:
    virtual void combine(int & old, const int & new_msg)
    {
	if (old < new_msg) old = new_msg;
    }
};

void pregel_path(int srcID, string in_path, string out_path)
{
    src = srcID;

    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    pathWorker worker;
    pathCombiner combiner;
    worker.setCombiner(&combiner); //use combiner
    worker.run(param, 2);
}
