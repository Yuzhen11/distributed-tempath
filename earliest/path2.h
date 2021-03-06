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
    //int arrivalTime;
    //int num_of_neighbors;
    vector<vector<pair<int,int> > > v_time; //(t,w)
    vector<int> neighbors;
    //vector<int> pn;
};

ibinstream & operator<<(ibinstream & m, const vertexValue & v){
    //m << v.arrivalTime;
    //m << v.num_of_neighbors;
    m << v.v_time;
    m << v.neighbors;
    //m << v.pn;
    return m;
}

obinstream & operator>>(obinstream & m, vertexValue & v){
    //m >> v.arrivalTime;
    //m >> v.num_of_neighbors;
    m >> v.v_time;
    m >> v.neighbors;
    //m >> v.pn;
    return m;
}
//vertex
class pVertex : public Vertex<VertexID, vertexValue, int>{



public:
vector<int> pn;
int arrivalTime;
    void broadcast(int startTime)
    {
        for (int i = 0; i < value().v_time.size(); ++ i)
        {
            vector<pair<int, int> >:: iterator it; 
            pair<int,int> tmp(startTime, 0);
            vector<pair<int, int> >:: iterator vend = value().v_time[i].begin()+pn[i];
            it = lower_bound(value().v_time[i].begin(), vend, tmp);
            if (it != vend)
            {
                send_message(value().neighbors[i], (*it).first+(*it).second);
                pn[i] = it-value().v_time[i].begin();
                it++;
            }

        }
    }

    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1)
        {
            pn.resize(value().v_time.size());
            for (int i = 0; i < value().v_time.size(); ++ i) 
            {
            	pn[i] = value().v_time[i].size();
            }
            
            
            if (id == src) 
            {
                arrivalTime = tstart;
                broadcast(tstart);
            }
            else
            {
                arrivalTime = inf;
            }
        }
        else
        {
            int mini = inf;
            for (int i = 0; i < messages.size(); ++ i)
            {
               if (messages[i] < mini) mini = messages[i];
            }
            if (mini < arrivalTime)
            {
                arrivalTime = mini;
                broadcast(mini);
            }
        }
        vote_to_halt();
    }

};

class pathWorker : public Worker<pVertex>{
    char buf[1000];

public:
    virtual pVertex* toVertex(char * line)
    {
        pVertex* v = new pVertex;
        istringstream ssin(line);
        int from, num_of_neighbors, to, num_of_t, w, t;
        ssin >> from >> num_of_neighbors;
        
        v->id = from;
        //v->value().num_of_neighbors = num_of_neighbors;
        v->value().v_time.resize(num_of_neighbors);
        v->value().neighbors.resize(num_of_neighbors);
        //v->value().pn.resize(num_of_neighbors);
        for (int i = 0; i < num_of_neighbors; ++ i)
        {
            //cout << i << endl;
            ssin >> to >> num_of_t >> w;
            v->value().neighbors[i] = to;
            //v->value().pn[i] = num_of_t;
            for (int j = 0; j < num_of_t; ++ j)
            {
                ssin >> t;
                v->value().v_time[i].push_back(make_pair(t, w));
            }

        }
        return v;
        //cout << "load done" << endl;
    }
    virtual void toline(pVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d %d\n", v->id, v->arrivalTime);
        writer.write(buf);
    }

};

class pathCombiner : public Combiner<int>
{
public:
    virtual void combine(int & old, const int & new_msg)
    {
	if (old > new_msg) old = new_msg;
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
    worker.run(param);
}
