#include "path.h"

string s1,s2;

int RUN_TIME;


void run(int srcID)
{
    
    pregel_path(srcID, s1.c_str(), s2.c_str());
    //worker_finalize();
}
/*
argv:
1 input path
2 output path
3 vertex number
4 highDegree vertexes file / random test number
*/
int main(int argc, char* argv[])
{
    if (argv[4][0] == '/') //high degree
    {
	    int srcID = 0;
	    
	    s1 = argv[1];
	    s2 = argv[2];
	    int totalNum = atoi(argv[3]);
	    string srcFile = argv[4];
	    double total_time = 0;
	    
	    hdfsFS fs = getHdfsFS();
	    BufferedReader reader(srcFile.c_str(), fs);
	    char* line = 0;
	    int tmp;
	    vector<int> src;
	    while ((line = reader.getLine()) != NULL) {
		 istringstream ssin(line);   
		 ssin >> tmp;
		 //cout << tmp << endl;
		 src.push_back(tmp);
	    }
	    hdfsDisconnect(fs);
	    
	    
	    init_workers(); //init
	    for (int i = 0; i < src.size(); ++ i)
	    {
	    	if (_my_rank == MASTER_RANK) 
	    	cout << "src = " << src[i] << endl;
	    	
	    	run(src[i]);
	    	
	    	if (_my_rank == MASTER_RANK) 
	    	{total_time += computeTime; cout << computeTime << endl;}
	    }
	    RUN_TIME = src.size();
	    worker_finalize(); //finalize
	    if (_my_rank == MASTER_RANK)
	    {
	    	cout << "---------------" << endl;
	    	cout << "TotalRunTime " << total_time << endl;
	    	cout << "RunTime " << RUN_TIME << endl;
	    	cout << "AverageRunTime " << total_time*1.0/RUN_TIME << endl; 
	    }
	    return 0;
	}
	else //random degree
	{
		int srcID = 0;
    
		s1 = argv[1];
		s2 = argv[2];
		int totalNum = atoi(argv[3]);
		RUN_TIME  = atoi(argv[4]);
		init_workers(); //init
		srand(0);
		double total_time = 0;
		
		int saveSrc = 0;
		for (int i = 0; i < RUN_TIME; ++ i)
		{
			srand(saveSrc);
			int randomNum = rand();
			int src = randomNum%totalNum;
			saveSrc = randomNum;
			
			if (_my_rank == MASTER_RANK) 
			cout << "src = " << src << endl;
			
			run(src);
			
			if (_my_rank == MASTER_RANK) 
			{total_time += computeTime; cout << computeTime << endl;}
		}
		worker_finalize(); //finalize
		
		if (_my_rank == MASTER_RANK)
		{
			cout << "---------------" << endl;
			cout << "TotalRunTime " << total_time << endl;
			cout << "RunTime " << RUN_TIME << endl;
			cout << "AverageRunTime " << total_time*1.0/RUN_TIME << endl; 
		}
		return 0;
	}
	
}


