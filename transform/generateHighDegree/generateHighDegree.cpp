#include <iostream>
#include <fstream>
#include <algorithm>
#include <vector>
using namespace std;
struct Node
{	
    int d;
    int id;
};
vector<Node> d;
bool cmp(Node d1, Node d2)
{
     return d1.d > d2.d;
}
int main(int argc, char * argv[])
{
    fstream in;
    in.open(argv[1],ios::in);
    int n, m1, m2;
    in >> n >> m1 >> m2;
    d.resize(n);
    int u, num_of_v, v, num_of_t, weight, tmp;
    for (int i = 0; i < n; ++ i)
    {
    	int count1 = 0;
    	in >> u >> num_of_v;
    	for (int j = 0; j < num_of_v; ++ j)
    	{
    	    in >> v >> num_of_t >> weight;
    	    count1+=num_of_t;
    	    for (int k = 0; k < num_of_t; ++ k) in >> tmp;
    	}
    	d[i].d = count1;
    	d[i].id = u;
    }
    sort(d.begin(), d.end(), cmp);
    in.close();    
    
    fstream out;   
    string outputPath = argv[1];
    outputPath += 'H';
    out.open(outputPath.c_str(), ios::out);
    int line = min(10, n);
    for (int i = 0; i < line; ++ i)
    out << d[i].id << endl;
    out.close();

}
