transform the original temporal graph into transform graph
pregel mode

Usage:
./tran toy

toy:
5 100 100
0 2 1 2 1 1 2 2 2 1 2 4
1 1 3 1 1 5
2 2 3 1 1 6 4 1 1 7
3 0
4 0

toyT.txt:
0 0 1 2 1 0 3 1 13
1 0 2 3 2 0 4 1 6 1 13
2 0 4 1 7 1 13
3 1 2 1 4 0 14
4 1 3 1 5 0 14
5 1 5 1 10 1 14
6 2 3 1 7 0 15
7 2 5 1 8 0 15
8 2 6 2 9 0 11 1 15
9 2 7 1 12 1 15
10 3 6 1 11 0 16
11 3 7 0 16
12 4 8 0 17
13 0 -1 0 0
14 1 -1 0 0
15 2 -1 0 0
16 3 -1 0 0
17 4 -1 0 0


from originalID timestamp  num_of_neighbors n1.t n1.w... toOriginalId
virtual node
IDnumber originalID -1 0 0
