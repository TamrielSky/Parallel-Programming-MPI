Parallel-Programming using MPI
========================

Used MPI for implementing Array Packing and Quick sort and ran it on a cluster of machines, varying data size and number of processors.


Parallel Array Packing for n labels

Considering an input dataset containing N labelled items:
We distribute the data evenly across different processors(depending on size of input data).

1. We initialize an array of size equal to the size of the input dataset with initial value 0 for all elements.
2. We pass the array among each processor and increment the count of each label inits corresponding array element. Once all the local data in a processor has been 
   traversed, the array is passed on to the next processor. 
3. This prefix sum operation on the array is continued till all data in all processors are traversed through. 
4. The last processor will hold an array with the count of all labels. The data is rearranged accordingly.



Parallel Quick Sort

We randomly choose a pivot from one of the processes and broadcast it to every processor
1. Divide the local list of each processor based on the pivot
Each processor in the lower half of the processor list sends its “upper list” to processor with rank=own rank+s/2 and keep the 
lower half of the data

2. Now, the upper-half processes have only values greater than the pivot, and the lower-half processes have only values smaller than the
   pivot

3. Processor divides into 2 groups and the algorithm runs recursively Using Quicksort – Parallel Implementation

4. After log P recursions, every process has an unsorted list of values completely disjoint from the values held by the other processes
5. The elements in each process will be sorted and that the highest element in the Pi will be lesser than the lowest element in Pi+1 

6. Each process can sort its list using sequential quicksort


