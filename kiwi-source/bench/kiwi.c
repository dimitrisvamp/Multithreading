#include <string.h>
#include "../engine/db.h"
#include "../engine/variant.h"
#include "bench.h"
#include <signal.h>

#define DATAS ("testdb")
DB* db;

pthread_mutex_t write_cost_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t read_cost_lock = PTHREAD_MUTEX_INITIALIZER;





#include <time.h>


/* control the base */
void db_control(int i){
	if(i==0){
		db = db_open(DATAS); 
	}else{
		db_close(db);
	}
}
void _write_test(long int count, int r)
{
	int i;
	double cost;
	long long start,end;
	Variant sk, sv;


	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	
	
	start = get_ustime_sec();

	for (i =0; i <count; i++) {

		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d adding %s\n", i, key);
		snprintf(val, VSIZE, "val-%d", i);

		sk.length = KSIZE;
		sk.mem = key;
		sv.length = VSIZE;
		sv.mem = val;

		db_add(db, &sk, &sv);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}
	
	
	
	end = get_ustime_sec();
	cost = end -start;
	pthread_mutex_lock(&write_cost_lock);     	// blocking all threads except from one
	write_cost = write_cost +cost;			// the thread that passes threw blocks writes its cost in write_cost
	pthread_mutex_unlock(&write_cost_lock);		// unblocking

	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}

void _read_test(long int count, int r)
{
	int i;
	int ret=0;
	int found = 0;
	double cost;
	long long start,end;
	Variant sk;
	Variant sv;
	char key[KSIZE + 1];
        srand(time(NULL));
	
       
	start = get_ustime_sec();
	for (i = 0; i < count; i++) {

		memset(key, 0, KSIZE + 1);
		
		/* if you want to test random write, use the following */
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d searching %s\n", i, key);
		sk.length = KSIZE;
		sk.mem = key;
                
		ret = db_get(db,&sk,&sv);
               
		if (ret) {
			//db_free_data(sv.mem); /*Htane se sxolia auto */
			found++;
		} else {
			INFO("not found key#%s", 
					sk.mem);
    		}
		
		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	
		
		
	}
	
	
	
	
	end = get_ustime_sec();
	cost = end - start;
	bfound=bfound+found;				//number of data that threads find
	pthread_mutex_lock(&read_cost_lock);		// blocking all threads except from one
	read_cost = read_cost +cost;			// the thread that passes threw blocks writes its cost in read_cost
	pthread_mutex_unlock(&read_cost_lock);		// unblocking
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		(double)(cost / count),
		(double)(count / cost),
		cost);
}
