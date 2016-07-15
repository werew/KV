#include <stdlib.h>
#include <sys/time.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include "kv.h"
#include "common.h"

extern int errno;

#define MAX_KEY 100

typedef enum { false, true} bool;

char* usage_string = "usage: %s [-h][-i hidx][-s size]\n";
char* help_string = "usage: %s [-h][-i hidx][-s size]\n";

len_t hash_function1(const kv_datum *key){
        len_t hash = 0;
        len_t i;
        for (i = 0; i < key->len; i++) {
                hash += ((unsigned char*) key->ptr)[i];
                hash %= 999983;
        }
        return hash;
}
/* XOR compression */
len_t hash_function2(const kv_datum *key){
        len_t hash = 0, k = 0;
        len_t i;
        for (i = 0; i < key->len; i++) {
                k = ((unsigned char*) key->ptr)[i];
                hash ^= k << (i % (sizeof (len_t) * CHAR_BIT));
                hash %= 999983;
        }
        return hash;
}

/* FNV-1a hash */
len_t hash_function3(const kv_datum *key){
        len_t hash = 2166136261;
        len_t i;
        for (i = 0; i < key->len; i++) {
                hash ^= ((char*) key->ptr)[i];
                hash *= 16777619;
                hash %= 999983;
        }
        return hash;
}

int generate_hash(int hidx, len_t n){
	/* Key */
	char vkey[MAX_KEY];
	kv_datum key;
	key.ptr = vkey;

	/* Init random sequence */
     	struct timeval time; 
     	if (gettimeofday(&time,NULL) == -1) return -1;
	srand(time.tv_usec);

	len_t i;
	for (i = 0; i < n; i++){
		/* Generate random key */
		rand_datum(&key, MAX_KEY);
		
		len_t hash;
		switch (hidx) {
			case 0:
			case 1: hash = hash_function1(&key);
				break;
			case 2: hash = hash_function2(&key);
				break;
			case 3: hash = hash_function3(&key);
				break;
			default: perror("Invalid hidx");
			 	 exit(1);
				
		};
		
		printf("%u\n",hash);
	}
	
	return 0;
}



int main(int argc, char* argv[]){
	/* Default values */
	int hidx = 0 ;
	len_t size_test = 10;


	int opt ;
	while ((opt = getopt (argc, argv, "hi:s:")) != -1) {
		switch (opt) {
			case 'h' :				/* help */
				usage (argv [0], 0) ;
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
	

	if (generate_hash(hidx,size_test) == -1) {
		perror("hash generation");
		exit(1);
	}

	exit (0);
}
