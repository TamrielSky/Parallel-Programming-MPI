#include<stdlib.h>
#include<stdio.h>
#include<time.h>
#include "mpi.h"





int* generateDataSet(int);
int partition(int*, int, int);
void swap(int*, int, int);
void quicksort(int*, int ,int);
void shuffle(int*, int);
int lessThan(int, int, int*, int*);
int greaterThan(int, int, int*, int*);
int SIZE, n;
int sendsize;
int * dataset, *sendset, *recvset;
void countNumbers(int*, int*); 


int main( argc, argv ) 


	int argc;
	char**argv;
{ int rank, size, *dataset, *bucketsize; 
	int len; 
	int  countZeros=0;
	int  countOnes=0;
	int partialcountZeros = 0;
	int partialcountOnes = 0;
	char procname[MPI_MAX_PROCESSOR_NAME]; 
	MPI_Status status;
	time_t t;


	MPI_Init(&argc, &argv);


	MPI_Comm_size( MPI_COMM_WORLD, &size );
	MPI_Comm_rank( MPI_COMM_WORLD, &rank );

	double start = MPI_Wtime();
	double end;

	int LIMIT = atoi(argv[1]);
	SIZE = atoi(argv[2]);
//	printf("\n the numbers from rank %d  are ", rank);

	n = size;
	int last = size -1;
	srand(rank + time(&t));
	dataset = generateDataSet(SIZE); 
	sendset = generateDataSet(SIZE);
	recvset = generateDataSet(SIZE);

	int actual =  SIZE/n;
	int i = 0; 
	for(i=0; i< SIZE/n; i++)
	{

		// srand(time(NULL));
		dataset[i] = rand() % LIMIT; 
//		printf("%d--\t", dataset[i]);
	}

	int sz = size;

	
	int recvsize = SIZE;
	int lastindex;

	int pivot = 0;
	while(sz >= 1)
	{


		int limit = sz*2;
		int num = 0;
		int range = 0;
		while(range< size)
		{
			range = limit * num; 
			num++;
			if((range <= rank) && (rank <(range+limit))) 
			{
				break; 
			}  

		}

		//		range = range*sz
		if(rank == 0 && sz == size)
		{
			int bcast = 0;
			for(bcast = 1; bcast< sz; bcast++)
			{ 
				pivot = dataset[0];
				MPI_Send(&pivot, 1, MPI_INT, bcast, 1000, MPI_COMM_WORLD);

			}
			lastindex = greaterThan(pivot, actual, dataset, sendset); 
		}   
		else if(sz < size)
		{
			//find whether it is a sender or a receiver

			if(rank >= (range +sz))
			{
				int sendto = rank - sz;
				int recvfrom = rank - sz;

                                //printf("\ngoing to send from rank %d", rank);
                                int testme = 0;
                                for(testme = 0; testme< sendsize; testme++)
                                { 
                                 // printf("%d\t", sendset[testme]);
                                }
				MPI_Send(sendset, sendsize, MPI_INT, sendto, 1000, MPI_COMM_WORLD);	

                                MPI_Recv(recvset, SIZE, MPI_INT, recvfrom, 1000, MPI_COMM_WORLD, &status);
    

				MPI_Get_count(&status, MPI_INT, &recvsize);

                                //printf("\nthe size of numbers recieved by rank %d is %d and revsize zeo is %d", rank, recvsize, recvset[0]); 
				int fillup;
                                if(recvset[0] != -1)
                                {      
				for(fillup =0; fillup< recvsize; fillup++)
				{
					dataset[lastindex] = recvset[fillup];
					lastindex++;
				}
                                }
				if(rank%sz == 0 && sz >1)
				{
                                        if(lastindex != 0)
					shuffle(dataset, lastindex);
					pivot = dataset[0];
					int bcast;

					for(bcast = (rank+1); bcast< (rank+sz); bcast++)
					{

						MPI_Send(&pivot, 1, MPI_INT, bcast, 1000, MPI_COMM_WORLD);
					}

                                        sendset = generateDataSet(SIZE);
                                        recvset = generateDataSet(SIZE);
					lastindex = greaterThan(pivot, lastindex, dataset, sendset);
				}
				else if(sz> 1)
				{

					int fromrecv = rank;
					while(fromrecv % sz != 0)
						fromrecv--;
					MPI_Recv(&pivot, 1, MPI_INT, fromrecv, 1000, MPI_COMM_WORLD, &status);


					int num1 = 0;
					int limit1 = sz;
					int range1 = 0;
					while(range1< size)
					{
						range1 = limit1 * num1; 
						num1++;
						if((range1 <= rank) && (rank <(range1+limit1))) 
						{
							break; 
						}  

					}
					if(rank >= (range1 +sz/2))
                                        {
                                                recvset = generateDataSet(SIZE);
                                                sendset = generateDataSet(SIZE);
						lastindex = lessThan(pivot, lastindex, dataset, sendset);
					}
                                        else
                                        {
                                                recvset = generateDataSet(SIZE);
                                                sendset = generateDataSet(SIZE);
						lastindex = greaterThan(pivot, lastindex, dataset, sendset);
                                        }
				}


			}
			else
			{

				int sendto = rank+sz;
				int recvfrom = rank+sz;
                                
			      
                           	
                                MPI_Recv(recvset, SIZE, MPI_INT, recvfrom, 1000, MPI_COMM_WORLD, &status);

                                int testme = 0; 
                                //printf("\ngoing to send from rank %d", rank); 
                                for(testme = 0; testme< sendsize; testme++)
                                {
                                  // printf("%d\t", sendset[testme]);
                                }
				MPI_Send(sendset, sendsize, MPI_INT, sendto, 1000, MPI_COMM_WORLD);
                                
                                MPI_Get_count(&status, MPI_INT, &recvsize);
                                
				int fillup;
                                
                                //printf("the size of numbers received from rank %d is %d", rank, recvsize);
                                if(recvset[0] != -1)
                                {
                                 
				for(fillup = 0; fillup< recvsize; fillup++)
				{
                                  //      printf("the last index is %d and value to insert is %d", lastindex, recvset[fillup]);  
					dataset[lastindex] = recvset[fillup]; 
					lastindex++;
				}
                                }
				if(rank >=sz && sz > 1)
				{
					if(rank%sz == 0)
					{
                                                if(lastindex !=0 )  
						shuffle(dataset, lastindex);
						pivot = dataset[0];
						int bcast;

						for(bcast = (rank +1); bcast< (rank+sz); bcast++)
						{

							MPI_Send(&pivot, 1, MPI_INT, bcast, 1000, MPI_COMM_WORLD);
						} 
                                                recvset = generateDataSet(SIZE);
                                                sendset = generateDataSet(SIZE);
						lastindex = greaterThan(pivot, lastindex, dataset, sendset);
					}
					else
					{

						int fromrecv = rank;
						while(fromrecv % sz != 0)
							fromrecv--;
						MPI_Recv(&pivot, 1, MPI_INT, fromrecv, 1000, MPI_COMM_WORLD, &status);



						int num1 = 0;
						int limit1 = sz;
						int range1 = 0;
						while(range1< size)
						{
							range1 = limit1 * num1; 
							num1++;
							if((range1 <= rank) && (rank <(range1+limit1))) 
							{
								break; 
							}  

						}
						if(rank >= (range1 +sz/2))
                                                {
                                                        recvset = generateDataSet(SIZE);
                                                        sendset = generateDataSet(SIZE);
							lastindex = lessThan(pivot, lastindex, dataset, sendset);
                                                }
						else
                                                {
                                                        recvset = generateDataSet(SIZE);
                                                        sendset = generateDataSet(SIZE);
							lastindex = greaterThan(pivot, lastindex, dataset, sendset);
                                                }

					}

				}
				else if(rank<sz && sz > 1)
				{
					if(rank % sz == 0)
					{
                                                if(lastindex != 0) 
						shuffle(dataset, lastindex);
						pivot = dataset[0];
						int bcast;

						for(bcast = (rank + 1); bcast< (rank+sz); bcast++)
						{

							MPI_Send(&pivot, 1, MPI_INT, bcast, 1000, MPI_COMM_WORLD);
						} 
                                                recvset = generateDataSet(SIZE);
                                                sendset = generateDataSet(SIZE);
						lastindex = greaterThan(pivot, lastindex, dataset, sendset);
					}
					else
					{

						int fromrecv = rank;
						while(fromrecv % sz != 0)
							fromrecv--;
						MPI_Recv(&pivot, 1, MPI_INT, fromrecv,1000, MPI_COMM_WORLD, &status);
					        int num1 = 0;
						int limit1 = sz;
						int range1 = 0;
						while(range1< size)
						{
							range1 = limit1 * num1; 
							num1++;
							if((range1 <= rank) && (rank <(range1+limit1))) 
							{
								break; 
							}  

						}
						if(rank >= (range1 +sz/2))
                                                {
                                                        recvset = generateDataSet(SIZE);
                                                        sendset = generateDataSet(SIZE);
							lastindex = lessThan(pivot, lastindex, dataset, sendset);
						}else{
                                                        recvset = generateDataSet(SIZE);
                                                        sendset = generateDataSet(SIZE);
							lastindex = greaterThan(pivot, lastindex, dataset, sendset);
                                                        
                                                }

					}

				}


			}

		}
		else
		{
			//printf("\nwaiting to receive"); 
			MPI_Recv(&pivot, 1, MPI_INT, 0, 1000, MPI_COMM_WORLD, &status);

			//printf("\nreceived pivot is %d and split size is %d", pivot, actual);
			if(rank < sz/2)
			{			lastindex = greaterThan(pivot, actual, dataset, sendset);	
			//	printf("last index from rank %d is %d ",rank, lastindex);
			}
			else
			{
				lastindex = lessThan(pivot, actual, dataset, sendset); 
			//	printf("lastindex from rank %d is %d", rank, lastindex);
			}

		}  
		sz = sz/2;

	}

	printf("\n the data from processor %d  with  no of elements %d :  ",rank, lastindex);

        if(lastindex>0)
           quicksort(dataset, 0, lastindex);
	int count = 0;

	for(count=0; count< lastindex; count++)
	{

		printf("%d(proc %d)\t", dataset[count], rank);

	}

}

int * generateDataSet(int size)
{
	int * ptr = (int*)malloc((size*sizeof(int)));
	return ptr;
}

int greaterThan(int pivot, int size, int* dataset, int* sendset)
{
	int i, j=0, k;
        
	for(i = 0; i< size; i++)
	{
		if(dataset[i]>pivot)
		{   
			sendset[j] = dataset[i];
			for(k=i; k< size-1; k++)
			{
				dataset[k] = dataset[k+1];
			}
			size--;
			j++;
                        i--; 
		}
	}
       if(j==0)
       {
        if(size ==0)
           dataset[0] = 0;
         sendset[0] = -1;
         sendsize = 1;
       } 
       else{
         if(size == 0)
            dataset[0] = 0;
         sendsize = j;
        }
	return (size);
}

void shuffle(int *array, int  n) {    
	struct timeval tv;
	gettimeofday(&tv, NULL);
	int usec = tv.tv_usec;
	srand48(usec);


	if (n > 1) {
		size_t i;
		for (i = n - 1; i > 0; i--) {
			size_t j = (unsigned int) (drand48()*(i+1));
			int t = array[j];
			array[j] = array[i];
			array[i] = t;
		}
	}
}

int lessThan(int pivot, int size, int* dataset, int* sendset)
{
	int i, j=0, k;
	for(i = 0; i< size; i++)
	{
          //printf("comparing %d and %d for size of %d", dataset[i], pivot, size);
		if(dataset[i]<=pivot)
		{
			sendset[j] = dataset[i];
			for(k=i; k< size -1; k++)
			{
				dataset[k] = dataset[k+1];
			}  
			size--;
			j++;
                        i--;
		}

	}
        if(j==0)
        {
           if(size ==0)
              dataset[0] = 0;
           sendset[0] = -1;
           sendsize = 1;
        }
        else {
           if(size == 0)
              dataset[0] = 0;            
           sendsize = j;
        }
         
	return (size);
}

int partition(int *data, int start, int end) 
{
  if (start >= end) return 0;

  int pivotValue = data[start];
  int low = start;
  int high = end - 1;
  while (low < high) {
    while (data[low] <= pivotValue && low < end) low++;
    while (data[high] > pivotValue && high > start) high--;
    if (low < high) swap(data, low, high);
  }
  swap(data, start, high);

  return high;
}

void quicksort(int *data, int start, int end) {
  if  (end-start+1 < 2) return;

  int pivot = partition(data, start, end);

  quicksort(data, start, pivot);
  quicksort(data, pivot+1, end);
}


void swap(int *data, int i, int j) {
  int temp = data[i];
  data[i] = data[j];
  data[j] = temp;
}



