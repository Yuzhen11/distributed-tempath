#include "path.h"

int main(int argc, char* argv[])
{
    int srcID = 0;
    
    
    init_workers();
    pregel_path(srcID, argv[1], argv[2]);
    worker_finalize();
    return 0;
}
