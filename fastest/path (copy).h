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


struct twv
{
    int t, w, v;
    twv(){}
    twv(int t, int w, int v): t(t),w(w),v(v){}
};
bool cmp(const twv& t1, const twv& t2)
{
    return t1.t < t2.t;
}
struct myCmp
{
    bool operator() (const pair<int,int>& p1, const pair<int,int>& p2) const
    {
    	return p1.first < p2.first;
    }
};
struct vertexValue
{
    int arrivalTime;
    vector<twv> neighbors;
    int pn;
    
    set<pair<int, int>, myCmp > s;
};
template <class T, class compare>
ibinstream& operator<<(ibinstream& m, const set<T, compare>& v)
{
    m << v.size();
    for (typename set<T, compare>::const_iterator it = v.begin(); it != v.end(); ++it) {
        m << *it;
    }
    return m;
}
template <class T, class compare>
obinstream& operator>>(obinstream& m, set<T, compare>& v)
{
    size_t size;
    m >> size;
    for (size_t i = 0; i < size; i++) {
        T tmp;
        m >> tmp;
        v.insert(v.end(), tmp);
    }
    return m;
}
ibinstream & operator<<(ibinstream & m, const twv & v) 
{m << v.t << v.w << v.v; return m;}
ibinstream & operator<<(ibinstream & m, const pair<int,int> p) 
{m << p.first << p.second; return m;}
ibinstream & operator<<(ibinstream & m, const vertexValue & v){
    m << v.arrivalTime;
    m << v.neighbors;
    m << v.pn;
    m << v.s;
    return m;
}
obinstream & operator>>(obinstream & m, twv & v) 
{m >> v.t >> v.w >> v.v; return m;}
obinstream & operator>>(obinstream & m, pair<int,int> p)
{m >> p.first >> p.second; return m;}
obinstream & operator>>(obinstream & m, vertexValue & v){
    m >> v.arrivalTime;
    m >> v.neighbors;
    m >> v.pn;
    m >> v.s;
    return m;
}


//vertex
class pVertex : public Vertex<VertexID, vertexValue, pair<int, int> >{

public:

    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1)
        {
            if (id == src) 
            {
                value().arrivalTime = 0;
                for (int i = 0; i < value().neighbors.size(); ++ i)
                {
                    send_message(value().neighbors[i].v, make_pair(value().neighbors[i].t, value().neighbors[i].t+value().neighbors[i].w));
                }  
              
            }
            else
            {
                value().arrivalTime = inf;
            }
        }
        else
        {
            //cout << id << endl;
            for (int i = 0; i < messages.size(); ++ i)
            {
            	//cout << i << endl;
            	//cout << "s.size() " << value().s.size() << endl; 
            	
            	pair<int,int>& p = messages[i];
            	set<pair<int,int> >::iterator it;
            	it = value().s.lower_bound(p);
            	if (it == value().s.end() || it->second > p.second)
            	{
            	    //cout << "it->first " << it -> first << endl;
            	    //cout << "it->second " << it ->second << endl;
            	    //cout << "p.first " << p.first << endl;
            	    //cout << "p.second " << p.second << endl;
            	    if (it == value().s.end())
            	    {
            	    	vector<twv> :: iterator start, end, itSend;
            	    	start = lower_bound(value().neighbors.begin(), value().neighbors.end(), twv(p.second,0,0), cmp);
            	    	end = value().neighbors.end();
            	    	for (itSend = start; itSend != end; ++ itSend)
            	    	{
            	    	    send_message(itSend->v, make_pair(p.first, itSend->t+itSend->w));
            	    	}            	    
            	    	value().s.insert(p);
            	    }
            	    else if (it->first == p.first)
            	    {
            	    	vector<twv> :: iterator start, end, itSend;
            	    	start = lower_bound(value().neighbors.begin(), value().neighbors.end(), twv(p.second,0,0), cmp);
            	    	end = upper_bound(value().neighbors.begin(), value().neighbors.end(), twv(it->second,0,0), cmp);
            	    	for (itSend = start; itSend != end; ++ itSend)
            	    	{
            	    	    send_message(itSend->v, make_pair(p.first, itSend->t+itSend->w));
            	    	}
            	    	value().s.erase(it);
            	    	value().s.insert(p);
            	    }
            	    else
            	    {
            	    	vector<twv> :: iterator start, end, itSend;
            	    	start = lower_bound(value().neighbors.begin(), value().neighbors.end(), twv(p.second,0,0), cmp);
            	    	end = upper_bound(value().neighbors.begin(), value().neighbors.end(), twv(it->second,0,0), cmp);
            	    	for (itSend = start; itSend != end; ++ itSend)
            	    	{
            	    	    send_message(itSend->v, make_pair(p.first, itSend->t+itSend->w));
            	    	}
            	    	value().s.insert(p);
            	    }
            	    //remove backward
            	    set<pair<int,int> >::iterator rp = value().s.lower_bound(p);
            	    if (rp != value().s.begin())
            	    {
            	    	rp--;
            	    	while(rp != value().s.begin() && rp -> second >= p.second)
            	    	{
            	    	    value().s.erase(rp); rp--;
            	    	}
            	    	if (rp == value().s.begin())
            	    	{
            	    	    if (rp->second >= p.second) value().s.erase(rp);
            	    	}
            	    }
            	    
            	    if (p.second - p.first < value().arrivalTime)
            	    value().arrivalTime = p.second - p.first;
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
        for (int i = 0; i < num_of_neighbors; ++ i)
        {
            //cout << i << endl;
            ssin >> to >> num_of_t >> w;
            for (int j = 0; j < num_of_t; ++ j)
            {
                ssin >> t;
                v->value().neighbors.push_back(twv(t, w, to));
            }
        }
        v->value().pn = v->value().neighbors.size()-1;
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
