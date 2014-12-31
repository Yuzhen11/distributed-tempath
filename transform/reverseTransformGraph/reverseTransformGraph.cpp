#include <cstdio>
#include <vector>
#include <cstring>
#include <algorithm>
#include <set>
#include <queue>
#include <iostream>
#include <fstream>
#include <map>
using namespace std;
int n, m1, m2;
vector<vector<pair<int,int> > > adj;
vector<vector<int> > neighbors; //originalID timestamp toOriginalId
int main(int argc, char *argv[])
{
	fstream in;
	in.open(argv[1], ios::in);
	//from originalID timestamp  num_of_neighbors n1.t n1.w... toOriginalId
	int from, originalID, timestamp, num_of_neighbors, toOriginalId;
	
	while(in >> from)
	{
	    if (from >= neighbors.size())
	    {
		neighbors.resize(from+1);
		adj.resize(from+1);
	    }
	    in >> originalID >> timestamp >> num_of_neighbors;
	    neighbors[from].push_back(originalID);
	    neighbors[from].push_back(timestamp);
	    int t1, w1;
	    for (int i = 0; i < num_of_neighbors; ++ i)
	    {
	    	in >> t1 >> w1;
	    	if (t1 >= neighbors.size())
	    	{
	    	    neighbors.resize(t1+1);
	    	    adj.resize(t1+1);
	    	}
	    	adj[t1].push_back(make_pair(from,w1) );
	    }
	    in >> toOriginalId;
	    neighbors[from].push_back(toOriginalId);
	}
	in.close();
	
	cout << "load done." << endl;
	
	fstream out;
	string s = argv[1];
	s += "R";
	out.open(s.c_str(), ios::out);
	for (int i = 0; i < adj.size(); ++ i)
	{
		//from originalID timestamp  num_of_neighbors n1.t n1.w... toOriginalId
	    out << i << " " << neighbors[i][0] << " " << neighbors[i][1] << " " << adj[i].size() << " ";
	    for (int j = 0; j < adj[i].size(); ++ j)
	    {
	    	out << adj[i][j].first << " " << adj[i][j].second << " ";
	    }
	    out << neighbors[i][2] << endl;
	}
	
	out.close();
}
