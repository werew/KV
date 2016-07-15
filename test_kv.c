#include <stdlib.h>
#include <sys/time.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include "kv.h"
#include "common.h"

extern int errno;

#define MAX_VAL 4096
#define MAX_KEY 100

typedef enum { false, true} bool;


char* usage_string = "usage: %s [-h][-i hidx][-a first|worst|best][-s size] base\n";
char* help_string = NULL;


alloc_t allocation (const char *alloc){
	alloc_t a ;

	a = FIRST_FIT ;
	if (alloc == NULL)
		a = FIRST_FIT;
	else if (strcmp (alloc, "first") == 0)
		a = FIRST_FIT;
	else if (strcmp (alloc, "best") == 0)
		a = BEST_FIT;
	else if (strcmp (alloc, "worst") == 0)
		a = WORST_FIT;
	else {
		errno = EINVAL;
		raler (NULL, "allocation") ;
	}

	return a ;
}

void copy_kv_datum(kv_datum* dest, kv_datum* src){
	dest->len = src->len;
	memcpy(dest->ptr, src->ptr, src->len);
}

/**
 * @param prob The probability (include between 0 and 100)
 */
bool make_chance(int prob){ return rand() % 100 < prob; }

int test(KV* kv, len_t n){
	/* Values */
	char data[MAX_VAL];
	char vkey[MAX_KEY];

	kv_datum val, key;
	val.ptr = data;
	key.ptr = vkey;

	/* Used for suppression */
	char d_vkey[MAX_KEY];
	kv_datum d_key;
	d_key.ptr = d_vkey;
	bool d_valid = false; // true when d_key contains a stored (not yet removed) key

	/* Init random sequence */
     	struct timeval time; 
     	if (gettimeofday(&time,NULL) == -1) return -1;
	srand(time.tv_usec);

	len_t i;
	for (i = 0; i < n; i++){
		/* Generate key and val */
		rand_datum(&key, MAX_KEY);
		val.len = rand() % MAX_VAL;


		if (make_chance(50) && d_valid == true){
			if (kv_del(kv, &d_key) == -1) raler(kv,"kv_del");
			d_valid = false;
		}

		if ( kv_put(kv, &key, &val) == -1) raler(kv, "kv_put");

		if (make_chance(50) && d_valid == false) {
			copy_kv_datum(&d_key, &key);
			d_valid = true;
		} 
		
	}
	
	return 0;
}



int main(int argc, char* argv[]){

	int opt ;
	KV *kv ;

	/* Default values */
	int hidx = 0 ;
	char *alloc = NULL;
	len_t size_test = 10;

	alloc_t a;

	while ((opt = getopt (argc, argv, "ha:i:s:")) != -1) {
		switch (opt) {
			case 'h' :				/* help */
				usage (argv [0], 0) ;
				break ;
			case 'a' :				/* mode d'allocation */
				alloc = optarg ;
				break ;
	    		case 'i' :				/* index de la fct de hash */
				hidx = atoi(optarg) ;
				break ;
			case 's' :
				size_test = atoi(optarg);
				break;
	    		default :
				usage (argv [0], 1);
		}
	}
	
	if (argc - optind < 1) usage(argv[0], 1);
	
	a = allocation(alloc);

    	if ((kv = kv_open(argv [optind], "r+", hidx, a)) == NULL) 
		raler(kv, "kv_open");

	if (test(kv,size_test) == -1) raler(kv, "test");

	if (kv_close(kv) == -1) raler(kv, "kv_close");
	
	exit (0);
}
