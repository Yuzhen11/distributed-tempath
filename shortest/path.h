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

struct myCmp
{
    bool operator() (const pair<int,int>& p1, const pair<int,int>& p2) const
    {
    	return p1.first > p2.first;
    }
};
bool myCmp2(const pair<int,int>& p1, const pair<int,int>& p2)
{
	return p1.first < p2.first;
}
struct vertexValue
{
    int arrivalTime;
    int num_of_neighbors;
    vector<vector<pair<int,int> > > v_time; //(t,w)
    vector<int> neighbors;
    vector<int> pn;
    
    
};

ibinstream & operator<<(ibinstream & m, const pair<int,int> p) 
{m << p.first << p.second; return m;}
ibinstream & operator<<(ibinstream & m, const vertexValue & v){
    m << v.arrivalTime;
    m << v.num_of_neighbors;
    m << v.v_time;
    m << v.neighbors;
    m << v.pn;
    return m;
}
obinstream & operator>>(obinstream & m, pair<int,int> & p)
{m >> p.first >> p.second; return m;}
obinstream & operator>>(obinstream & m, vertexValue & v){
    m >> v.arrivalTime;
    m >> v.num_of_neighbors;
    m >> v.v_time;
    m >> v.neighbors;
    m >> v.pn;
    return m;
}
//vertex
class pVertex : public Vertex<VertexID, vertexValue,  pair<int, int> >{

public:

	set<pair<int, int>, myCmp > s;

    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1)
        {
            if (id == src) 
            {
                value().arrivalTime = 0;
                for (int i = 0; i < value().neighbors.size(); ++ i)
                {
                    vector<pair<int,int> >:: iterator it;
                    it = lower_bound(value().v_time[i].begin(), value().v_time[i].end(), make_pair(tstart,0), myCmp2);
                    if (it != value().v_time[i].end())
                    {
                    	send_message(value().neighbors[i], make_pair(it->second, it->first+it->second));
                    }
                }
            }
            else
            {
                value().arrivalTime = inf;
            }
        }
        else
        {
            for (int i = 0; i < messages.size(); ++ i)
            {
            	pair<int,int>& p = messages[i];
            	set<pair<int,int>, myCmp>::iterator it;
            	it = s.lower_bound(p);
            	if (it == s.end() || it->second > p.second)
            	{
            	    if (it == s.end())
            	    {           	    
            	    	s.insert(p);
            	    }
            	    else if (it->first == p.first)
            	    {
            	    	s.erase(it);
            	    	s.insert(p);
            	    }
            	    else if (it->first < p.first)
            	    {
            	    	s.insert(p);
            	    }
            	    
            	    //remove
            	    set<pair<int,int> >::iterator rp = s.lower_bound(p);
            	    set<pair<int,int> >::iterator tmp;
            	    if (rp != s.begin())
            	    {
            	    	rp--;
            	    	while(rp != s.begin() && rp -> second >= p.second)
            	    	{
            	    	    tmp = rp;
            	    	    rp--;
            	    	    s.erase(tmp); 
            	    	}
            	    	if (rp == s.begin())
            	    	{
            	    	    if (rp->second >= p.second) s.erase(rp);
            	    	}
            	    }
            	    
            	    if (p.first < value().arrivalTime)
            	    value().arrivalTime = p.first;
            	    
            	 
            	    
            	    //send
            	    for (int j = 0; j < value().neighbors.size(); ++ j)
            	    {
            	    	vector<pair<int,int> >::iterator it2;
            	    	it2 = lower_bound(value().v_time[j].begin(), value().v_time[j].end(), make_pair(p.second,0), myCmp2);
            	    	if (it2!=value().v_time[j].end())
            	    	send_message(value().neighbors[j], make_pair(p.first+it2->second, it2->first+it2->second));
            	    	
            	    }
            	}         
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
        v->value().num_of_neighbors = num_of_neighbors;
        v->value().v_time.resize(num_of_neighbors);
        v->value().neighbors.resize(num_of_neighbors);
        v->value().pn.resize(num_of_neighbors);
        for (int i = 0; i < num_of_neighbors; ++ i)
        {
            //cout << i << endl;
            ssin >> to >> num_of_t >> w;
            v->value().neighbors[i] = to;
            v->value().pn[i] = 0;
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
        sprintf(buf, "%d %d\n", v->id, v->value().arrivalTime);
        writer.write(buf);
    }

};
/*
class pathCombiner : public Combiner<int>
{
public:
    virtual void combine(int & old, const int & new_msg)
    {
	if (old > new_msg) old = new_msg;
    }
};
*/
void pregel_path(int srcID, string in_path, string out_path)
{
    src = srcID;

    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    pathWorker worker;
    //pathCombiner combiner;
    //worker.setCombiner(&combiner); //use combiner
    worker.run(param);
}
