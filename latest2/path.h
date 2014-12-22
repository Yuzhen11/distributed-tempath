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
int tstart = inf;
int tend = inf;


struct twv
{
    int t, w, v;
    twv(){}
    twv(int t, int w, int v): t(t),w(w),v(v){}
};
bool cmp(twv t1, twv t2)
{
    return t1.t < t2.t;
}
struct vertexValue
{
    int arrivalTime;
    vector<twv> neighbors;
    int pn;
};
ibinstream & operator<<(ibinstream & m, const twv & v) 
{m << v.t << v.w << v.v; return m;}
ibinstream & operator<<(ibinstream & m, const vertexValue & v){
    m << v.arrivalTime;
    m << v.neighbors;
    m << v.pn;
    return m;
}
obinstream & operator>>(obinstream & m, twv & v) 
{m >> v.t >> v.w >> v.v; return m;}
obinstream & operator>>(obinstream & m, vertexValue & v){
    m >> v.arrivalTime;
    m >> v.neighbors;
    m >> v.pn;
    return m;
}
//vertex
class pVertex : public Vertex<VertexID, vertexValue, int>{

public:

    void broadcast(int startTime)
    {
    	/*
        for (; value().pn >= 0 && value().neighbors[value().pn].t >= startTime; --value().pn)
        {
            send_message(value().neighbors[value().pn].v, value().neighbors[value().pn].t+value().neighbors[value().pn].w);
        }
        */
        for (; value().pn < value().neighbors.size() && value().neighbors[value().pn].t <= startTime; ++ value().pn)
        {
            send_message(value().neighbors[value().pn].v, value().neighbors[value().pn].t - value().neighbors[value().pn].w);
        }
    }

    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1)
        {
            if (id == src) 
            {
                value().arrivalTime = tstart;
                broadcast(tstart);
            }
            else
            {
                value().arrivalTime = -1;
            }
        }
        else
        {
            int maxi = -1;
            for (int i = 0; i < messages.size(); ++ i)
            {
               if (messages[i] > maxi) maxi = messages[i];
            }
            if (maxi > value().arrivalTime)
            {
                value().arrivalTime = maxi;
                broadcast(maxi);
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
        for (int i = 0; i < num_of_neighbors; ++ i)
        {
            //cout << i << endl;
            ssin >> to >> num_of_t >> w;
            for (int j = 0; j < num_of_t; ++ j)
            {
                ssin >> t;
                v->value().neighbors.push_back(twv(t+w, w, to));
            }
        }
        v->value().pn = 0;
        sort(v->value().neighbors.begin(), v->value().neighbors.end(), cmp);
        return v;
        //cout << "load done" << endl;
    }
    virtual void toline(pVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d %d\n", v->id, v->value().arrivalTime);
        writer.write(buf);
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
    worker.run(param);
}
