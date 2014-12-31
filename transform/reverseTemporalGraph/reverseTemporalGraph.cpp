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
vector<vector<vector<int> > > adj;
vector<vector<int> > neighbors;
int main(int argc, char *argv[])
{
	fstream in;
	in.open(argv[1], ios::in);
	in >> n >> m1 >> m2;
	
	int u, num_of_v;
	int v, num_of_t, weight_of_v;
	int t;
	
	while(in >> u)
	{
		if (u >= adj.size() ) 
		{
			adj.resize(u+1);
			neighbors.resize(u+1);
		}
		
		in >> num_of_v;
		for (int i = 0; i < num_of_v; ++ i)
		{
			in >> v >> num_of_t >> weight_of_v;
			if (v >= adj.size() ) 
			{
				adj.resize(v+1);
				neighbors.resize(v+1);
			}
			
			neighbors[v].push_back(u);
			adj[v].resize(adj[v].size()+1);
			for (int j = 0; j < num_of_t; ++ j)
			{
				in >> t;
				adj[v][adj[v].size()-1].push_back(t);
			}
		}
	}
	in.close();
	
	cout << "load done." << endl;
	
	fstream out;
	string s = argv[1];
	s += "R";
	out.open(s.c_str(), ios::out);
	for (int i = 0; i < adj.size(); ++ i)
	{
		out << i << " " << adj[i].size() << " ";
		for (int j = 0; j < adj[i].size(); ++ j)
		{
			out << neighbors[i][j] << " " << adj[i][j].size() << " " << 1 << " ";
			for (int k = 0; k < adj[i][j].size(); ++ k)
			out << adj[i][j][k] << " ";
		}
		out << endl;
	}
	
	out.close();
}
