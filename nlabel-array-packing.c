#include<stdlib.h>
#include<stdio.h>
#include<time.h>
#include "mpi.h"





int* generateDataSet(int);
int SIZE, n;
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
//printf("\n the limit is %d",LIMIT);

 n = size;
 int last = size -1;
	srand(rank + time(&t));
 dataset = generateDataSet(SIZE/n); 
 bucketsize = generateDataSet(LIMIT);
 int i = 0; 
 for(i=0; i< SIZE/n; i++)
 {

	// srand(time(NULL));
    dataset[i] = rand() % LIMIT; 
//    printf("%d", dataset[i]);
 }  

for(i=0; i<LIMIT; i++)
{
  bucketsize[i] = 0;
}

 countNumbers(bucketsize, dataset);
  
 
 if(rank == last)
 {
   int * finalbucket = generateDataSet(LIMIT);

   
   MPI_Recv(finalbucket,LIMIT, MPI_INT, rank-1, 0,MPI_COMM_WORLD, &status);
       

   int traverse = 0;

   for(traverse = 0; traverse < LIMIT; traverse++)
   {
      finalbucket[traverse] = finalbucket[traverse] + bucketsize[traverse];
      printf("the count for number of %d in bucket is %d\n",traverse, finalbucket[traverse]); 
   }  
   
   int numprocs = 0;
   while(numprocs < last)
   {
     MPI_Send(finalbucket, LIMIT, MPI_INT, numprocs, 999, MPI_COMM_WORLD);
       numprocs++;
    } 
      
   int lowerlimit = (rank*(SIZE/n))+1;

   int upperlimit = (rank+1)*(SIZE/n); 
  
   int elementcount;
   int countlimit;
   int dataconsumed = 0;
   int breakcondition=0;
   traverse = 0; 
   for(traverse = 0; traverse< LIMIT; traverse++)
   {
        elementcount = finalbucket[traverse];
        for(countlimit = 0; countlimit< elementcount; countlimit++)
        {
          dataconsumed = dataconsumed + 1;
          if(dataconsumed == (lowerlimit - 1))
          {
             breakcondition = 1;
             break; 
          }
        }
        if(breakcondition == 1)
          break;
   }
   
  countlimit = countlimit + 1;
  int index = 0; 
  while(countlimit< elementcount && index < (SIZE/n))
  {
    
    dataset[index] = traverse;   
    index++;
    countlimit++;
  } 
  traverse = traverse + 1;
  countlimit = 0;
  while(index < (SIZE/n))
  {   
    elementcount = finalbucket[traverse];
    if(countlimit < elementcount)      
    {  
       dataset[index] = traverse;
       countlimit++;
       index++;
    }
    else
    {
       traverse++;
       countlimit = 0;
    }
    
 
  }
   
  /* int zerosremain = finalprefixZeros - (lowerlimit-1);
   
   if(zerosremain > 0 && zerosremain < (SIZE/n))
   {
     int index = 0;
     for(index = 0; index< zerosremain; index++)
     {
       dataset[i] = 0;
     }
     while(index < (SIZE/n))
     {
      dataset[i] = 1;
      index++;
     }
   }
   else
   {
     int index = 0;
     while(index < (SIZE/n))
     {
       dataset[index] = 0;
       index++;
     }
   }    
   
    /*
     int localzeros = 0;

   MPI_Recv(&localzeros, 1, MPI_INT, 0, 1000, MPI_COMM_WORLD, &status);
  
   if(localzeros >SIZE/n)
   { 
      int i =0;
      for(i = 0; i<SIZE/n; i++)
      {
         dataset[i] = 0;
    
      }

   }   
     
    
   else if(localzeros = -10)
   {
      int i;
      for(i = 0;i< SIZE/n; i++)
      {
        //printf("adsasda");
        dataset[i] = 1;
      }
      
   }
   else
   {
     int i =0;
     for(i = 0; i< localzeros; i++)
     {
       dataset[i] = 0;

     }

     while(i< SIZE/n)
     {
        dataset[i] = 1;
     }
   }
    */
  //int nump;
  //for( nump = 0; nump< last; nump++)
  //{
    //int ack;
    
    //MPI_Recv(&ack, 1, MPI_INT, 0, 1111, MPI_COMM_WORLD, &status);
  //}
   //MPI_Recv(&ack, 1, MPI_INT, 1, 1111, MPI_COMM_WORLD, &status);
   //MPI_Recv(&ack, 1, MPI_INT, 2, 1111, MPI_COMM_WORLD, &status);
   double runningtime;
   end = MPI_Wtime();
   runningtime = end - start;
   

   FILE *fp = fopen("running.dat", "a+");
   fprintf(stderr,"\nthe running time is %f", runningtime);

   
   
   fprintf(fp, "%s","this is example");
     
  fclose(fp);
   getchar();
  
   //MPI_Finalize();
   //return 0;  
 }  
 else if(rank == 0)
 {
   MPI_Send(bucketsize, LIMIT, MPI_INT, 1, 0, MPI_COMM_WORLD);
   
   int * finalbucket = generateDataSet(LIMIT);
 
   MPI_Recv(finalbucket, LIMIT, MPI_INT, last, 999, MPI_COMM_WORLD, &status);
   int traverse = 0;
   int countlimit = 0;
   int index = 0;
   int elementcount = 0;
  while(index < (SIZE/n))
  {   
    elementcount = finalbucket[traverse];
    if(countlimit < elementcount)      
    {  
       dataset[index] = traverse;
       countlimit++;
       index++;
    }
    else
    {
       traverse++;
       countlimit = 0;
    }
    
 
  }
  /*
   int lowerlimit = (SIZE/n)*rank;
   int upperlimit = (SIZE/n) * (rank+1); 
   
   int index =0;
   
   if(totalzeros > upperlimit)
   { 
   for(index =0; i< upperlimit; index++)
   {
     
      dataset[index] = 0;
   } 
   }
   else
   {
     for(index = 0;index< totalzeros; index++)
     {
        dataset[index] = 0;
     }
     while(index< upperlimit)
     {
        dataset[index] = 1;
        index++;
     }
   }  
  
   
    
/*
   int GlobalZeros = totalzeros;
   int i=0;
   
   if(GlobalZeros > SIZE/n)
   {
     for(i = 0 ; i< SIZE/n; i++)
     {
       dataset[i] = 0;  
     }

     int numprocs = size;
     for(i = 1; i<numprocs; i++)
     {
       GlobalZeros = GlobalZeros - SIZE/n;
       if(GlobalZeros > 0)
       {
          MPI_Send(&GlobalZeros, 1, MPI_INT, i, 1000, MPI_COMM_WORLD);
       }
       else
       {
         int allone = -10;
         MPI_Send(&allone, 1, MPI_INT, i, 1000, MPI_COMM_WORLD);

       }
    
     }  
     
   }
   else
   {
     int i =0;
     for(i=0; i<GlobalZeros; i++)
     { 
       dataset[i] = 0; 
     }
     
     while(i<SIZE/n)
     {
       dataset[i] = 1;
     }

     int numprocs = size;
     for(i = 1; i< numprocs; i++)
     {
       int allone = -10;
       MPI_Send(&allone, 1, MPI_INT, i, 1000, MPI_COMM_WORLD);  
     }
   }
   i =0;
  //for(i=0; i< SIZE/n; i++) 
 //MPI_Finalize();
 //  
  */ // int ack = 1111;
   // MPI_Send(&ack, 1, MPI_INT, last, 1111, MPI_COMM_WORLD);
} 
 else
 {
  
   int * partialbucket = generateDataSet(LIMIT); 
   MPI_Recv(partialbucket,LIMIT, MPI_INT, rank-1, 0,MPI_COMM_WORLD, &status);
    
   int traverse = 0;

   for(traverse = 0; traverse < LIMIT; traverse++)
   {
     bucketsize[traverse] = partialbucket[traverse] + bucketsize[traverse];
   }
   
    
   MPI_Send(bucketsize, LIMIT, MPI_INT, rank+1, 0, MPI_COMM_WORLD);
  
   

   int * finalbucket = generateDataSet(LIMIT); 
   MPI_Recv(finalbucket, LIMIT, MPI_INT, last, 999, MPI_COMM_WORLD, &status);
   
      int lowerlimit = (rank*(SIZE/n))+1;

   int upperlimit = (rank+1)*(SIZE/n); 
  
   int elementcount;
   int countlimit;
   int dataconsumed = 0;
   int breakcondition=0; 
   for(traverse = 0; traverse< LIMIT; traverse++)
   {
        elementcount = finalbucket[traverse];
        for(countlimit = 0; countlimit< elementcount; countlimit++)
        {
          dataconsumed = dataconsumed + 1;
          if(dataconsumed == (lowerlimit - 1))
          {
             breakcondition = 1;
             break; 
          }
        }
        if(breakcondition == 1)
          break;
   }
   
  countlimit = countlimit + 1;
  int index = 0; 
  while(countlimit< elementcount && index< (SIZE/n))
  {
    
    dataset[index] = traverse;   
    index++;
    countlimit++;
  } 
  traverse = traverse + 1;
  countlimit = 0;
  while(index < (SIZE/n))
  {   
    elementcount = finalbucket[traverse];
    if(countlimit < elementcount)      
    {  
       dataset[index] = traverse;
       countlimit++;
       index++;
    }
    else
    {
       traverse++;
       countlimit = 0;
    }
    
 
  }
  /* 
   int lowerlimit = (rank*(SIZE/n))+1;

   int upperlimit = (rank+1)*(SIZE/n); 
   
   int zerosremain = totalzeros - (lowerlimit-1);
   
   if(zerosremain > 0 && zerosremain < (SIZE/n))
   {
     int index = 0;
     for(index = 0; index< zerosremain; index++)
     {
       dataset[i] = 0;
     }
     while(index < (SIZE/n))
     {
      dataset[i] = 1;
      index++;
     }
   }
   else
   {
     int index = 0;
     while(index < (SIZE/n))
     {
       dataset[index] = 0;
       index++;
     }
   }    
  /*
   if(localzeros >SIZE/n)
   { 
      int i =0;
      for(i = 0; i<SIZE/n; i++)
      {
         dataset[i] = 0;
    
      }  
   } 
   else if(localzeros == -10)
   {
      for(i = 0;i< SIZE/n; i++)
      {
        dataset[i] = 1;
      }
   }
   else
   {
     int i =0;
     for(i = 0; i< localzeros; i++)
     {
       dataset[i] = 0;
          
     }
    
     while(i< SIZE/n)
     {
        dataset[i] = 1;
     }
   }
    
//   int i =0;
  // printf("the dataset from processor %d is\n", rank);
   //for(i = 0; i< SIZE/n; i++)
  // {
  //   printf("%d", dataset[i]);
   //}   
   //
   //
   //MPI_Finalize();
 */    //int ack = 111;
     //MPI_Send(&ack, 1, MPI_INT, last, 1111, MPI_COMM_WORLD);
 }

printf("\n the dataset from processor %d is ", rank);
int finalcount = 0;
for(finalcount = 0; finalcount< (SIZE/n); finalcount++)
{
   printf("%d",dataset[finalcount]);  
  
} 
MPI_Finalize();

}

int * generateDataSet(int size)
{
  int * ptr = (int*)malloc((size*sizeof(int)) + 20);
  return ptr;
}

void countNumbers(int *bucketsize, int* dataset)
{
  int i = 0;
  int j = 0;

    for(i=0; i<SIZE/n; i++)
    {
      bucketsize[dataset[i]]++;
      
    }

   


}