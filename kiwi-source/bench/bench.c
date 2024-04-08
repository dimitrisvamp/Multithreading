#include "bench.h"
#include "pthread.h"
#include <string.h>
#include "../engine/db.h"
#include "../engine/variant.h"




#define DATAS ("testdb") //gia na mporesw na parw tin vasi 


/* INITIALIZE MUTEXES */
pthread_mutex_t write_lock = PTHREAD_MUTEX_INITIALIZER; 
							
pthread_mutex_t read_lock = PTHREAD_MUTEX_INITIALIZER;	

void _random_key(char *key,int length) {
	int i;
	char salt[36]= "abcdefghijklmnopqrstuvwxyz0123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}


/* 
*@name thread_func_write: function for printing stats for write function
*@param cost and count: costs and counts from _write_test
*@return : nothing
*/
void write_print_stats(double cost,long int count)
{
	printf(LINE);
	printf("|Total Random-Write 	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}

/* 
*@name thread_func_write: function for printing stats for read function
*@param cost and count:costs and counts and founds from _read_test
*@return : nothing
*/
void read_print_stats(double cost,long int count,int found)
{
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		(double)(cost / count),
		(double)(count / cost),
		cost);
}

/* 
*@name thread_func_write: function for printing stats for readwrite function
*@param cost and count:costs and counts from _write_test and _read_test
*@return : nothing
*/
void readwrite_print_stats(double writecost,long int writecount,double readcost,long int readcount,int found)
{
	printf(LINE);
	printf("|Total Random-Write 	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,writecount, (double)(writecost / writecount)
		,(double)(writecount / writecost)
		,writecost);	
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		readcount, found,
		(double)(readcost / readcount),
		(double)(readcount / readcost),
		readcost);
}

void _print_header(int count)
{
	double index_size = (double)((double)(KSIZE + 8 + 1) * count) / 1048576.0;
	double data_size = (double)((double)(VSIZE + 4) * count) / 1048576.0;

	printf("Keys:\t\t%d bytes each\n", 
			KSIZE);
	printf("Values: \t%d bytes each\n", 
			VSIZE);
	printf("Entries:\t%d\n", 
			count);
	printf("IndexSize:\t%.1f MB (estimated)\n",
			index_size);
	printf("DataSize:\t%.1f MB (estimated)\n",
			data_size);

	printf(LINE1);
}

void _print_environment()
{
	time_t now = time(NULL);

	printf("Date:\t\t%s", 
			(char*)ctime(&now));

	int num_cpus = 0;
	char cpu_type[256] = {0};
	char cache_size[256] = {0};

	FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
	if (cpuinfo) {
		char line[1024] = {0};
		while (fgets(line, sizeof(line), cpuinfo) != NULL) {
			const char* sep = strchr(line, ':');
			if (sep == NULL || strlen(sep) < 10)
				continue;

			char key[1024] = {0};
			char val[1024] = {0};
			strncpy(key, line, sep-1-line);
			strncpy(val, sep+1, strlen(sep)-1);
			if (strcmp("model name", key) == 0) {
				num_cpus++;
				strcpy(cpu_type, val);
			}
			else if (strcmp("cache size", key) == 0)
				strncpy(cache_size, val + 1, strlen(val) - 1);	
		}

		fclose(cpuinfo);
		printf("CPU:\t\t%d * %s", 
				num_cpus, 
				cpu_type);

		printf("CPUCache:\t%s\n", 
				cache_size);
	}
}

/*struct for having access in the data of threads*/
struct data{
	long int count;
	int r; 
} data;

 
/* 
*@name thread_func_write: function for passing data in the _write_test
*@param arg: pointer to a valid struct of thread's data
*@return : nothing
*/
void *threaded_func_write(void *arg){

	
	struct data *dt =(struct data *) arg;
        
	pthread_mutex_lock(&write_lock);        // blocking all threads inssertion out of the _write_test except from one 
	
	_write_test(dt->count,dt->r);

	pthread_mutex_unlock(&write_lock);	// unblocking
	return 0;
	
}

/* 
*@name thread_func_read: function for passing data in the _read_test
*@param arg: pointer to a valid struct of thread's data
*@return : nothing
*/
void *threaded_func_read(void *arg){
	
	
	struct data *dt =(struct data *) arg;
	
	pthread_mutex_lock(&read_lock);		// blocking all threads inssertion out of the _read_test except from one
	
	_read_test(dt->count,dt->r);

	pthread_mutex_unlock(&read_lock);	//unblocking
	return 0;
}



int main(int argc,char** argv)
{
	long int count;
	int i=0;
	int NUMTHRDS; // number of threads statement
	
	NUMTHRDS = atoi(argv[3]); 	// convert the number of threads from string to integer
	pthread_t threadID[NUMTHRDS];	// thread statement
	
	 
	long int divv=0;	// div statement
	int mod=0; 		//mod statement

	srand(time(NULL));
	if (argc < 4) {
		fprintf(stderr,"Usage: db-bench <write | read> <count>\n");
		exit(1);
	}

	
	
	if (strcmp(argv[1], "write") == 0) {
		int r = 0; 


		count = atoi(argv[2]);
		_print_header(count);
		_print_environment();

		divv = count/NUMTHRDS; // the standard number of data in each thread
		mod = count%NUMTHRDS; // the extra number of data to be classified in the threads
		printf("div = %ld",divv);

		if (argc == 5)
			r = 1;

		db_control(0); // open base
		
		/* thread creation */
		for(i=0;i<NUMTHRDS;i++){  
			if(mod > 0){									// when we have extra datas
				struct data data_threads;						// having access in the data of threads statement
				data_threads.count = divv +1; 						// number of data into thread
				data_threads.r = r; 							// r is the same 
				mod--; 									// decrease the number of extra datas
				pthread_create(&threadID[i],NULL,threaded_func_write, &data_threads);   // create the threadID for _write_test
		

			}
			else{										// when we dont have extra datas
				struct data data_threads;						// having access in the data of threads statement
				data_threads.count = divv; 						// number of data into thread
				data_threads.r = r;							// the same r
				pthread_create(&threadID[i],NULL,threaded_func_write, &data_threads);	// create the threadID for _write_test
	
			}
				
		}
		for(i=0;i<NUMTHRDS;i++){
			pthread_join(threadID[i],NULL);		// waiting until the creation of threads ends 
		}

		db_control(1); // close base
		write_print_stats(write_cost,count);		// printing the stats for write function

	} else if (strcmp(argv[1], "read") == 0) {
		int r = 0; 

		count = atoi(argv[2]);
		_print_header(count);
		_print_environment();

		divv = count/NUMTHRDS; // the standard number of data in each thread
		mod = count%NUMTHRDS; // the extra number of data to be classified in the threads
		

		if (argc == 5)
			r = 1;
		

		db_control(0); //open base 
		
		/* thread creation */
		for(i=0;i<NUMTHRDS;i++){  
			if(mod > 0){									// when we have extra datas
				struct data data_threads;						// having access in the data of threads statement
				data_threads.count = divv +1; 						// number of data into thread
				data_threads.r = r; 							// the same r 
				mod--; 									// decrease the number of extra datas
				pthread_create(&threadID[i],NULL,threaded_func_read,&data_threads);     // create the threadID for _read_test
				
			}
			else{										// when we dont have extra datas
				struct data data_threads;						// having access in the data of threads statement								
				data_threads.count = divv; 						// number of data into thread
				data_threads.r = r;							// the same r
				pthread_create(&threadID[i],NULL,threaded_func_read,&data_threads);	// create the threadID for _read_test
				
			}
				
		}
		for(i=0;i<NUMTHRDS;i++){
			pthread_join(threadID[i],NULL);		// waiting until the creation of threads ends 
											
		}
		db_control(1);	// close base
		read_print_stats(read_cost,count,bfound);	// printing the statds for read function
	} else if(strcmp(argv[1], "readwrite") == 0){
		int r = 0;
		
		count = atoi(argv[2]);
		_print_header(count);
		_print_environment();
		
		
		char *readwrite = (argv[4]);		
		int read_per = atoi(strtok(readwrite,"-"));		// spliting the numbers from 4th place of string from "-" and converting it from string to integer
		printf("read_per = %d \n",read_per);
		
		long int read_count_per = (read_per*count)/100;		// the percentace for read
		int read_NUMTHRDS = (NUMTHRDS * read_per)/100;		// the number of threads for read function 
		long int write_count_per = count - read_count_per;		// the percentace for write	
		int write_NUMTHRDS = NUMTHRDS - read_NUMTHRDS; 		// the number of threads for write function
		long int div_write = write_count_per/write_NUMTHRDS;		// the standard number of data for each thread for write function
		int mod_write = write_count_per%write_NUMTHRDS;		// the extra number of data for each thread for write function
		long int div_read = read_count_per/read_NUMTHRDS;		// the standard number of data for each thread for read function
		int mod_read = read_count_per%read_NUMTHRDS;		// the extra number of data for each thread for read function
		



		if (argc == 6)
			r = 1;
		
		db_control(0); // open base

		for(i=0;i<write_NUMTHRDS;i++){  
			if(mod_write > 0){
				struct data data_threads;	
				data_threads.count = div_write +1;
				data_threads.r = r;
				mod_write--;
				pthread_create(&threadID[i],NULL,threaded_func_write,&data_threads); 
				
			}
			else{
				struct data data_threads;	
				data_threads.count = div_write;
				data_threads.r = r;
				pthread_create(&threadID[i],NULL,threaded_func_write,&data_threads);
				
			}
				
		}
		
		
		for(i=write_NUMTHRDS;i<NUMTHRDS;i++){  
			if(mod_read > 0){
				struct data data_threads;	
				data_threads.count = div_read +1;
				data_threads.r = r;
				mod_read--;
				pthread_create(&threadID[i],NULL,threaded_func_read,&data_threads);  
				
			}
			else{
				struct data data_threads;	
				data_threads.count = div_read;
				data_threads.r = r;
				pthread_create(&threadID[i],NULL,threaded_func_read,&data_threads);
				
			}
			//pthread_create(&threadID[i],NULL,threaded_func_read,&data_threads);	
		}
		for(i=0;i<NUMTHRDS;i++){
			pthread_join(threadID[i],NULL); 
											
		}
		db_control(1); // close base  
		
		readwrite_print_stats(write_cost,div_write,read_cost,div_read,bfound);		// printing the stats for readwrite function

	}else {
		fprintf(stderr,"Usage: db-bench <write | read> <count> <random>\n");
		exit(1);
	}

	return 1;
}
