#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include "graph.h"
#include "graphT.h"
using namespace std;

int main(int argc, char* argv[]){

	
	//char* option = argv[1];
	
	string s1 = argv[1];
	Graph g(argv[1]);
	GraphT gT(g);
	
	gT.output(s1);


	cout << "transform done! " << endl;
	
	return 0;
}



