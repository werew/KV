#include <stdlib.h>
#include <sys/time.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include "kv.h"
#include "common.h"

extern int errno;

typedef enum { false, true} bool;

char* usage_string = NULL;
char* help_string = NULL;

int main(void){

	KV *kv ;
	kv_datum key; key.ptr = "My key1"; key.len = 8;
	kv_datum val; val.ptr = "My val1"; val.len = 8;


	/* Get and next from write only */
    	if ((kv = kv_open("MYDB", "w", 0, FIRST_FIT)) == NULL) raler(kv,"kv_open");

	
	if ( kv_put(kv, &key, &val) == -1) raler(kv,"kv_put");

	if ( kv_get(kv, &key, &val) == 0 || errno != EACCES) 
		raler(kv,"get_write_only");

	kv_start(kv);
	if ( kv_next(kv, &key, &val) >= 0 || errno != EACCES) 
		raler(kv,"get_write_only");
	
	if (kv_close(kv) == -1) raler(kv, "kv_close");

	/* Exact fit free space */
    	if ((kv = kv_open("MYDB", "r+", 0, FIRST_FIT)) == NULL) raler(kv,"kv_open");

	val.ptr = "My val2"; val.len = 8;
	key.ptr = "My key2";
	if ( kv_put(kv, &key, &val) == -1) raler(kv,"kv_put");
	
	val.ptr = "1234567";
	key.ptr = "My key1";
	if ( kv_put(kv, &key, &val) == -1) raler(kv,"kv_put");
	
	/* Allocate it for me... */
	val.ptr = NULL;
	if ( kv_get(kv, &key, &val) == -1) raler(kv,"kv_get");
	free(val.ptr);

	/* Put and get empty values */
	val.len = 0;
	if ( kv_put(kv, &key, &val) == -1) raler(kv,"kv_put");
	if ( kv_get(kv, &key, &val) == -1) raler(kv,"kv_get");

	/* All in the same block ... this will take time!
	   (this will cover the function extend_block_chain) */
	unsigned char kval[4] = "\x00\xff\x00\xff";
	key.ptr = kval;
	while(kval[0] < 10){
		do {
			if ( kv_put(kv, &key, &val) == -1) raler(kv,"kv_put");
			kval[2]++;
			kval[3]--;
		} while(kval[2] > 0);
		
		kval[0]++;
		kval[1]--;
	}


	/* Then create a big hole in .kv (this will cover the
	   memory reallocations when the dkv_cache gets shifted) */
 	kval[0]='\x00'; kval[1]='\xff'; kval[2]='\x00'; kval[3]='\xff';
	key.ptr = kval;
	while(kval[0] < 9) {
		do {
			if ( kv_del(kv, &key) == -1) raler(kv,"kv_del");
			kval[2]++;
			kval[3]--;
		} while(kval[2] > 0);
		
		kval[0]++;
		kval[1]--;
	}
	
	/* Use all the hash functions */
	if (kv_close(kv) == -1) raler(kv, "kv_close");

    	if ((kv = kv_open("MYDB", "w+", 1, FIRST_FIT)) == NULL) raler(kv,"kv_open");
	if ( kv_put(kv, &key, &val) == -1) raler(kv,"kv_put");
	if ( kv_get(kv, &key, &val) == -1) raler(kv,"kv_get");
	
    	if ((kv = kv_open("MYDB", "w+", 2, FIRST_FIT)) == NULL) raler(kv,"kv_open");
	if ( kv_put(kv, &key, &val) == -1) raler(kv,"kv_put");
	if ( kv_get(kv, &key, &val) == -1) raler(kv,"kv_get");

    	if ((kv = kv_open("MYDB", "w+", 3, FIRST_FIT)) == NULL) raler(kv,"kv_open");
	if ( kv_put(kv, &key, &val) == -1) raler(kv,"kv_put");
	if ( kv_get(kv, &key, &val) == -1) raler(kv,"kv_get");

	/* End test */
	if (kv_close(kv) == -1) raler(kv, "kv_close");

	exit (0);
}
