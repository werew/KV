#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "kv.h"
#include <stdio.h>

/**
 * This will tell the program to keep dkv sorted (compatible with test-130) 
 * You can comment it out for better performances.
 */
#define _SORT_DKV_

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ HEADERS  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

/* Size of the headers (bytes) 
 * ----------------------------------------------------------
 *   HEADER |  	CONTENT
 * ---------+------------------------------------------------
 * HSIZE_H  | Magic number + hash function identifier
 * HSIZE_KV | Magic number
 * HSIZE_BLK| Magic number + number allocated blocks
 * HSIZE_DKV| Magic number + numer of entries + offset end kv
 * ----------------------------------------------------------
 */
#define HSIZE_H	  (MGN_SIZE + sizeof (len_t))	
#define HSIZE_KV  (MGN_SIZE)			
#define HSIZE_BLK (MGN_SIZE + sizeof (len_t))	
#define HSIZE_DKV (MGN_SIZE + 2*sizeof (len_t))

/* Size of the biggest header */ 
#define MAX_HSIZE HSIZE_DKV //Update manually

/* Magic numbers */
#define MGN_H	0x68617368
#define MGN_KV	0x6b766462
#define MGN_BLK 0x626c6b76
#define MGN_DKV 0x646b766b

#define MGN_SIZE 4 /// Size of a magic number


/*~~~~~~~~~~~~~~~~~~~~~~~~~ FILE .BLK AND BLOCKS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

/**
 * The header of this file contains its magic number plus the number of
 * allocated blocks. Each block has an header of the size of an len_t. 
 * This header has the following form:
 *
 * +------------+------------------------------------------+
 * | 1 bit flag |     Total entries / Next block number    |
 * +------------+------------------------------------------+
 *
 * Where the first bit indicates if the block is full (1) or not (0).
 * If the block is not full the number after the one-bit flag indicates the 
 * number of entries stored inside the block, if the block is full if contains
 * the number of the next chained block (this is not an address but just the 
 * number, the address needs to be calculated taking in count SIZE_BLK and 
 * HSIZE_BLK).
 *
 * Below, the constants, macros and data structures relatives to this file:
 */


/* Size of a block */
#define SIZE_BLK 4096

/* Size of the header of each block */
#define SIZE_BLK_HEAD 4

/* Max blocks nb in the file .blk */
#define MAX_BLKS ((UNSIGNED_MAX(len_t) - HSIZE_BLK) / SIZE_BLK)  

/* Max entries (slots) for each block */
#define MAX_BLK_ENTR ((SIZE_BLK-SIZE_BLK_HEAD) / sizeof (len_t)) 


/* Struct used by the function read_blk to store the informations of a block */
typedef struct {
	len_t offset_nextblk; /// Next chained block
	len_t n_entries;      /// Number entries	
	len_t data[SIZE_BLK]; /// Entries
	} block;


/*
 * This data type is used by the function scan_blocks and provides a serie
 * of usefull information when searching a key into a concatenation of blocks
 */
typedef struct {
	len_t last_block;   /// Last block visited
	len_t slot_entry;   /// Slot refering to the entry
	len_t nblk_entries; /// Entries in last block
	len_t offset_kv;    /// Address to the stored data
	len_t free_slot;    /// First slot with value 0
	len_t free_block;   /// Block with free slot
	} scan_infos;




/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ FILE .DKV ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/


/* Check if a given mem_usage of a dkv_entry is pointing to used space */
#define DKV_IS_USED(mem_usage) ((mem_usage & FLAG_USED) == FLAG_USED)
/* Get the size (bytes) of the space pointed by a mem_usage of a dkv_entry*/
#define DKV_GET_SIZE(mem_usage) (mem_usage & (~FLAG_USED))

/* Every entry of the file .dkv has the following form */
typedef struct {
	len_t mem_usage; /// 1 bit flag (used/free) + size allocated space
	len_t offset;	 /// offset to the stored data in the file .kv
	} dkv_entry;

/* This struct provides a way to link a stored data to his
 * slot of the fil .dkv
 */
typedef struct {
	len_t offset_kv; /// offset to the data in the file .kv
	len_t dkv_slot;  /// number of the slot refering to that data
	} kv_stored;


/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ COMMON ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

/* Hash functions identifiers */
#define HASH_1 1
#define HASH_2 2
#define HASH_3 3

/* Minimum allocation/deallocation unit for a cache 
 * @note Currently the only cache implemented is the one refering to the
 * 	 entries of the file .dkv
 */
#define CACHE_PAGE 4096

/* This flag marks an address/reference of type len_t as used.
 * In this program it is employed to mark a dkv_entry or the header of a block.
 * @result First bit = 1, rest of the bits = 0 (0x800...000)
 */
#define FLAG_USED ((len_t) 1 << (sizeof (len_t)*CHAR_BIT-1)) //

/**
 * Slice a selection of bits (numeroted from 0 to n-1) 
 * @param: x Data from where to slice
 * @param: a,b Respectively ending and starting point (a>=b)
 */
#define BITSLICE(x, a, b) (((x) >> (b)) & ((1 << ((a)-(b)+1)) - 1))



/* Calculate max unsigned value of t */
#define UNSIGNED_MAX(t) (( (unsigned long long) 1 << \
			   (sizeof (t) * CHAR_BIT) ) - 1)



typedef enum { false, true} bool;



/**
 * This struct identifies an open database along with all the informations
 * necessary to perform operation on it.
 */
struct KV {
	/* File descriptors */
	int _fd_h;		/// File descriptor for the file .h
	int _fd_blk;		/// File descriptor for the file .blk
	int _fd_kv;		/// File descriptor for the file .kv
	int _fd_dkv;		/// File descriptor for the file .dkv

	/* Behaviour */
	int flags;		/// Opening flags
	bool write_only;	/// Read perissions
	alloc_t alloc;		/// Id of the allocation function
	len_t (*_hash_fun)(const kv_datum*); /// Pointer to the hash function

	/* Mem usage infos */	
	len_t nb_blocks;	/// Number allocated blocks on the file .blk
	len_t nb_dkv_entries;	/// Number of entries on the file .dkv	
	len_t end_kv;		/// Offset to the end of the file .kv

	/* Caches */
	len_t max_dkv_cache;	/// Amount of memory allocated for dkv_cache
	dkv_entry* dkv_cache;	/// Array containing the entries of .dkv

	/* Others */
	len_t next_entry;	/// Used by kv_next to return the correct value
};






/*~~~~~~~~~~~~~~~~~~~~~ INTERNAL FUNCTIONS PROTOTYPES ~~~~~~~~~~~~~~~~~~~~~~~~~*/

/*************** Opening and closure database ********************/

void initKV(KV *db);
int set_flags(KV* kv, const char *mode);
int openFilesKV(KV *db, const char *dbname, int flags);
int setHashFun(KV *db, int hidx);
int writeHeaders(KV *db, int hidx);
void infail_kvclose(KV *db);
int load_cache(KV* kv);
int useHeaders(KV *db);

/*************** Memory management ******************************/

/* Insertion into blocks (file .blk)*/
int insert_first_entry
(KV *kv, len_t hash, const kv_datum *key, const kv_datum *val);
int insert_to_chain 
(KV *kv, const kv_datum *key, const kv_datum *val, len_t offset_first_blk);

/* Insertion into .dkv */
int push_dkv_entry(KV *kv, const dkv_entry* dkv_content);
int use_dkv_slot(KV *kv, len_t offset, dkv_entry* new);
#ifdef _SORT_DKV_
int shift_dkv(KV *kv, len_t pos, int dir);
#endif

/* Store data into .kv */
int store_kv
(KV *kv, const kv_datum* key, const kv_datum* value, kv_stored* ref_kv);
int write_to_kv
(KV* kv, len_t offset, const kv_datum* key ,const kv_datum* value);

/* Blocks allocation (file .blk) */
len_t allocate_blk(KV *kv, len_t* block_number);
len_t extend_blocks_chain(KV *kv, len_t last_block);

/* Suppressions  */
int remove_data(KV *kv, len_t kv_offset);

/********************** Search/compute ******************************/

/* Find entries */
int scan_blocks
(KV *kv, len_t offset_blk, const kv_datum *key, scan_infos *infos);
len_t key_to_kv(KV* kv, const kv_datum *key, len_t* block_slot);
int dkv_find_contiguos(KV* kv,len_t offset_kv, len_t indexes[3], bool found[3]);

/* Free memory space */
int first_fit
(KV *kv, len_t size, dkv_entry* dkv_content ,len_t* dkv_entry_offset);
int worst_fit
(KV *kv, len_t size, dkv_entry* dkv_content ,len_t* dkv_entry_offset);
int best_fit
(KV *kv, len_t size, dkv_entry* dkv_content ,len_t* dkv_entry_offset);

/* Hash functions */
len_t hash_fun1(const kv_datum *key);
len_t hash_fun2(const kv_datum *key);
len_t hash_fun3(const kv_datum *key);

/********************** Others ******************************/

/* Blocks (file .blk) */
int read_blk(KV *kv, len_t blk_offset, block *blk) ;

/* kv_datum */
int fill_datum(int fd, len_t offset, len_t size, kv_datum *dat);
void init_datum(kv_datum *dat);
void drop_datum(kv_datum *dat);
inline int eq_datum(const kv_datum *a, const kv_datum *b);
int read_datum(KV *kv, len_t offset, kv_datum *dat);

/* Read/write at offset */
ssize_t read_at(int fd, len_t offset, void *buff, size_t count);
ssize_t safe_read_at(int fd, len_t offset, void *buff, size_t count);
ssize_t write_at(int fd, len_t offset, const void *buff, size_t count);
ssize_t safe_write_at(int fd, len_t offset, const void *buff, size_t count);




/*~~~~~~~~~~~~~~~~~~~~~ FUNCTIONS BODIES (part 1: API) ~~~~~~~~~~~~~~~~~~~~~~~*/


KV *kv_open (const char *dbname, const char *mode, int hidx, alloc_t alloc){
		
	KV *db;
	if ( ( db = malloc(sizeof(KV)) ) == NULL) return NULL;
	initKV(db);

	db->alloc = alloc; 

	if (  set_flags(db, mode)  == -1) goto error;

	if ( openFilesKV(db,dbname,db->flags) == -1 ) goto error;
				
	struct stat infos;
	if ( fstat(db->_fd_kv, &infos) == -1) goto error;

	if ( (db->flags & O_CREAT) == O_CREAT && infos.st_size == 0){
	
		if (setHashFun(db,hidx)    == -1 ||
		    writeHeaders(db, hidx) == -1
		) goto error;

	} else {

		if (useHeaders(db) == -1 ||
		    load_cache(db) == -1
		) goto error;
		
	}

	return db;		

error: 
	infail_kvclose(db);
	return NULL;
}



int kv_close (KV *kv) {

	if ( kv->flags != O_RDONLY){
		/* Sync file .blk */	
		if ( safe_write_at(kv->_fd_blk,MGN_SIZE,
			&kv->nb_blocks,sizeof (len_t)) == -1 ) return -1;
	
		/* Sync file .dkv */
		if ( safe_write_at(kv->_fd_dkv,MGN_SIZE,
			&kv->nb_dkv_entries,sizeof (len_t)) == -1 ) return -1;
	
		if ( safe_write_at(kv->_fd_dkv,MGN_SIZE + sizeof (len_t),
			&kv->end_kv,sizeof (len_t)) == -1 ) return -1;
	
		if ( safe_write_at(kv->_fd_dkv, HSIZE_DKV, kv->dkv_cache,
			kv->nb_dkv_entries * sizeof (dkv_entry)) == -1 ) return -1;

		if (ftruncate(kv->_fd_dkv, HSIZE_DKV + 
			kv->nb_dkv_entries * sizeof (dkv_entry)) == -1) return -1;
		
	}

	free(kv->dkv_cache);
	
	/* Close all open files */
	if( close(kv->_fd_h)   == -1 ||
	    close(kv->_fd_kv)  == -1 ||
	    close(kv->_fd_blk) == -1 ||
	    close(kv->_fd_dkv) == -1 ) return -1;

	
	free(kv);
	
	return 0;
	
}



int kv_put (KV *kv, const kv_datum *key, const kv_datum *val){

	len_t offset_blk;

	/* hash = offset of .h */	
	len_t hash = HSIZE_H + sizeof (len_t) * kv->_hash_fun(key);
	
	ssize_t nb = read_at(kv->_fd_h, hash, &offset_blk, sizeof offset_blk);
	switch (nb){
		case -1: return -1;

		case (sizeof offset_blk):
			 /* Add entry to chain of blocks */
			 if (offset_blk != 0) {
			 	if (insert_to_chain(kv, key, val, 
					offset_blk) == -1) return -1;
				break;
			 }
		case  0: /* Empty slot: first use of this hash */
			 if (insert_first_entry( kv, hash,
				key, val) == -1) return -1;
			 break;

		default: return -1;
	}
	
	return 0;	

}


int kv_get (KV *kv, const kv_datum *key, kv_datum *val){

	/* Do you have the permissions? */
	if (kv->write_only) {
		errno = EACCES;
		return -1;
	}

	/* Get offset on .kv */
	len_t key_offset;
	if ((key_offset = key_to_kv(kv, key, NULL)) == 0){
		return (errno == ENOENT)? 0 : -1;

	}

	/* Read size of the stored value */
	len_t val_offset = key_offset + key->len + sizeof (len_t);
	len_t val_size;
	if (safe_read_at(kv->_fd_kv, val_offset, &val_size, 
		sizeof val_size) == -1) return -1;

	/* Read data */
	if ( fill_datum(kv->_fd_kv, val_offset + sizeof (len_t),
		 val_size, val) == -1) return -1;

	return 1;

}


int kv_del (KV *kv, const kv_datum *key) {

	/* Get offset on .kv, and offset on .blk */
	len_t offset_kv, block_slot;
	if ((offset_kv = key_to_kv(kv, key, &block_slot)) == 0){
		return  -1;
	}
	
	/* Remove data on .kv */
	if (remove_data(kv, offset_kv) == -1) return -1;

	/* Remove reference on .blk */	
	len_t zero = 0;
	if (safe_write_at(kv->_fd_blk, block_slot, 
		&zero, sizeof zero) == -1) return -1;

	return 0;
}

void kv_start (KV *kv){ kv->next_entry = 0; }

int kv_next (KV *kv, kv_datum *key, kv_datum *val){

	/* Do you have the permissions? */
	if (kv->write_only) {
		errno = EACCES;
		return -1;
	}

	/* End of kv */	
	if (kv->next_entry >= kv->nb_dkv_entries) return 0;

	/* Skip empty blocks of memory */	
	while (DKV_IS_USED(kv->dkv_cache[kv->next_entry].mem_usage) == 0) {
		
		kv->next_entry++; 
		if (kv->next_entry >= kv->nb_dkv_entries) return 0;
	}
	

	/* Read total size of the stored key */
	len_t key_offset = kv->dkv_cache[kv->next_entry].offset;
	len_t key_size;
	if (safe_read_at(kv->_fd_kv, key_offset, &key_size, 
		sizeof key_size) == -1) return -1;

	/* Read total size of the stored value */
	len_t val_offset = key_offset + key_size + sizeof (len_t);
	len_t val_size;
	if (safe_read_at(kv->_fd_kv, val_offset, &val_size, 
		sizeof val_size) == -1) return -1;

	/* Read data */
	if ( fill_datum(kv->_fd_kv, key_offset + sizeof (len_t),
					 key_size, key) == -1 ||
	     fill_datum(kv->_fd_kv, val_offset + sizeof (len_t), 
					val_size, val)	== -1 
	   ) return -1;
	
	kv->next_entry++;

	return 1;
}

/*~~~~~~~~~~~~~~~~~~~~~ FUNCTIONS BODIES (part 2: internals) ~~~~~~~~~~~~~~~~~~~*/

/**
 * Translates a stored key into its offset on .kv.
 * @param kv Database
 * @param key Pointer to a struct containing the key to search
 * @param blocks If this pointer is not NULL it is filled with the offset of the
 *	  slot ( file .blk) who points to the key.
 * @return The offset of the key on the file .kv or 0 in case of error
 */
len_t key_to_kv(KV* kv, const kv_datum *key, len_t* block_slot){

	/* hash = offset of .h */	
	len_t hash = HSIZE_H + sizeof (len_t) * kv->_hash_fun(key);

	/* Read offset first block of the chain */	
	len_t offset_blk;
	ssize_t nb = read_at(kv->_fd_h, hash, &offset_blk, sizeof offset_blk);

	switch (nb){

		case  0: errno = ENOENT;
		case -1: return 0;

		default: if (nb < (ssize_t) sizeof offset_blk 
					|| offset_blk == 0 ){

				errno = ENOENT;
				return 0;
			 }
	}

	
	/* Find offset on .kv */	
	scan_infos infos;	
	if (scan_blocks(kv, offset_blk, key, &infos) == -1) return 0;

	if (infos.offset_kv == 0 ) {
		errno = ENOENT;
		return 0;
	}

	if (block_slot != NULL) *block_slot = infos.slot_entry;	
	return infos.offset_kv;
}


/**
 * Fill a given kv_datum with the data contained in the given file at the given
 * offset. If dat.ptr is NULL it is allocated, otherwise dat.len represent the
 * maximum of data to be read.
 * @param fd File descriptor
 * @param offset Offset to the value to read.
 * @param size Size of the data to read. This parameter is ignored if dat.ptr 
 *	  is not NULL, in this case dat.len is considered
 * @param dat Pointer to the kv_datum structure where to store the data
 * @return 0 in case of success, -1 otherwise
 */
int fill_datum(int fd, len_t offset, len_t size, kv_datum *dat){
	
	/* Empty value */
	if (size == 0) {
		dat->len = 0;
		return 0;
	}

	/* Allocate ptr if necessary, adapt the size otherwise */
	if (dat->ptr == NULL){

		if ((dat->ptr = malloc(size)) == NULL) return -1;

	} else {
		size  = (size > dat->len)? dat->len : size;
	}

	if (safe_read_at(fd, offset, dat->ptr, size) == -1) return -1;

	dat->len = size;

	return 0;
}


/**
 * Insert an entry using an empty .h slot
 * @param kv Database
 * @param hash Corresponding hash (= offset to the .h slot)
 * @param key,val Couple (key,value) to store
 * @return 0 in case of success, -1 otherwise
 */
int insert_first_entry
(KV *kv, len_t hash, const kv_datum *key, const kv_datum *val){
	
	/* Store the couple key-value */
	kv_stored ref_kv;
	if ( store_kv(kv, key, val, &ref_kv) == -1) return -1;

	/* Allocate a new empty block */
	len_t offset_blk = allocate_blk(kv, NULL);
	if (offset_blk == 0) goto err_kv_lost;

	/* Update hash table */
	if ( safe_write_at(kv->_fd_h, hash, &offset_blk,
		sizeof offset_blk) == -1) goto err_blk_lost; 

	/* Write the entry to the block */
	if ( safe_write_at(kv->_fd_blk, offset_blk + SIZE_BLK_HEAD, 
		&ref_kv.offset_kv, sizeof (len_t) ) == -1) goto err_kv_lost;

	len_t n_entries = 1;
	if ( safe_write_at(kv->_fd_blk, offset_blk , 
		&n_entries, sizeof (len_t) ) == -1) goto err_kv_lost;

	return 0;


err_blk_lost:
	//pop_block(KV *kv); // Further implementation
err_kv_lost:
	remove_data(kv, ref_kv.offset_kv);
	return -1;

}



/**
 * Insert an entry to a chain of blocks 
 * @param kv Database
 * @param key,val Couple key,value to store
 * @param offset_first_blk Offset to the first block of the chain
 * @return 0 in case of success, -1 otherwie
 * @warning: the insertion of values with identical keys result in the 
 * 	     suppression of the first inserted value. If an error occours
 *	     both values could be lost
 */
int insert_to_chain
(KV *kv, const kv_datum *key, const kv_datum *val, len_t offset_first_blk){
	
	// Offsets to the slot and block where to insert the entry
	len_t insertion_slot  = 0;  	
	len_t insertion_block = 0;
	
	// By default don't increment the num of entries of the insertion block
	bool increment_nblk_entries = false; 


	/* Step 1: Calculate insertion_block and insertion_slot */

	scan_infos infos;	
	if (scan_blocks(kv,offset_first_blk,key,&infos) == -1) return -1;

	/* Check if key exists */
	if (infos.slot_entry != 0) {
		/* Fill slot with 0 */
		len_t free_value = 0;
		if ( safe_write_at(kv->_fd_blk, infos.slot_entry, 
			&free_value, sizeof (len_t) ) == -1) return -1;

 		/* Remove old value */
		if ( remove_data(kv, infos.offset_kv) == -1) return -1;

		/* New free slot avalable */
		if ( infos.free_slot == 0) {
			infos.free_slot = infos.slot_entry;
			infos.free_block= infos.last_block;
		}
	}


	/* Check if there is any free slot avalable */		
	if (infos.free_slot != 0) {
		/* Free slot found */
		insertion_slot = infos.free_slot;
		insertion_block = infos.free_block;

	} else {
		/* Use the last block of the chain */
		if (infos.nblk_entries >= MAX_BLK_ENTR) {
			/* Last block is full */
			infos.last_block = extend_blocks_chain(kv, 
						infos.last_block);
			if (infos.last_block == 0) return -1;
			infos.nblk_entries = 0;
		}
	
		/* Append to the last block */
		insertion_block = infos.last_block;
		insertion_slot = infos.last_block + SIZE_BLK_HEAD 
				+ infos.nblk_entries * sizeof (len_t);

		increment_nblk_entries = true;
	}
		

	/* Step 2: Store the couple key-value */

	// Store data
	kv_stored ref_kv;
	if ( store_kv(kv, key, val, &ref_kv) == -1) return -1;

	// Write block entry
	if (safe_write_at(kv->_fd_blk, insertion_slot, 
		&ref_kv.offset_kv, sizeof (len_t)) == -1) goto err_kv_lost;

	// Increment the header of the block if necessary	
	if (increment_nblk_entries) {
		infos.nblk_entries++;
		if (safe_write_at(kv->_fd_blk, insertion_block,
			&infos.nblk_entries, sizeof (len_t)) == -1) {
			goto err_kv_lost;
		}
	}


	return 0;	

err_kv_lost:
	remove_data(kv, ref_kv.offset_kv);
	return -1;
	
}








/**
 * Traverse the chain of blocks searching for a key while collecting 
 * different informations depending on the given mode. Those informations
 * are stored into the fields of the given scan_infos structure.
 * @param: kv The database
 * @param: first_blk Offset (address) to the first block
 * @param: key The key to search
 * @param: infos The address to the struct of type scan_infos to be filled
 *
 * Here is a more detailed explaination of the fields of the struct scan_infos:
 *
 * +--------------+--------------------------------------------------+
 * |   FIELD      |         VALUE				     |
 * +--------------+--------------------------------------------------+
 * | slot_entry   | If the key exists it contains the offset to the  |
 * |              | slot refering the entry, 0 otherwise.	     |
 * +--------------+--------------------------------------------------+
 * | offset_kv    | If the key exists it contains the offset to the  |
 * |              | slot refering the stored data, 0 otherwise.      |
 * +--------------+--------------------------------------------------+
 * | free_slot    | The offset to the first free slot found, 0 if    |
 * |              | none has been found.		 	     |
 * +--------------+--------------------------------------------------+
 * | last_block   | If slot_entry == 0, it contains the offset to the|
 * |              | last allocated block of the chain, otherwise it  |
 * |              | contains the offset to the block where the entry |
 * |              | has been found (the last block visited by	     |
 * |              | by scan_infos).				     |
 * +--------------+--------------------------------------------------+
 * | free_block   | If free_slot != 0 it contains the offset to the  |
 * | 		  | block containing the free slot.		     |
 * +--------------+--------------------------------------------------+
 * | nblk_entries | Number of entries in last_block		     |
 * +--------------+--------------------------------------------------+
 * @return 0 in case of success, -1 otherwise
 */
int scan_blocks
(KV *kv, len_t offset_blk, const kv_datum *key, scan_infos *infos) {

	
	kv_datum current_entry;
	init_datum(&current_entry);

	#define EMPTY 0
	memset(infos, EMPTY, sizeof (scan_infos));
	
	block blk;
	len_t off_last_blk = 0;

	while ( offset_blk != EMPTY ){ // While there are blocks

		// Read block
		if ( read_blk(kv, offset_blk, &blk) == -1) goto error;
		// Scan block
		len_t i;
		for (i = 1; i <= blk.n_entries; i++) { // the entry 0 is the header
		
			len_t offset_slot = offset_blk + i*sizeof (len_t);
	
			if (blk.data[i] == EMPTY ) {
				/* Free slot */
				if (infos->free_slot == EMPTY) {
					infos->free_slot = offset_slot;
					infos->free_block = offset_blk;
				}
				continue;
			}
	
			/* Read stored key */
			if (read_datum(kv, blk.data[i], &current_entry) == -1)
				goto error;
	
			if (eq_datum(key,&current_entry)) {
				/* Key found */
				infos->slot_entry = offset_slot;
				infos->offset_kv = blk.data[i];
				infos->last_block = offset_blk;
				infos->nblk_entries  = blk.n_entries;
				drop_datum(&current_entry);
				return 0;
			}
		}
			
		off_last_blk = offset_blk;
		offset_blk = blk.offset_nextblk;
	}
	
	/* End of the chain (no entry has been found) */
	
	infos->last_block = off_last_blk;
	infos->nblk_entries  = blk.n_entries;
	drop_datum(&current_entry);

	return 0;

error:
	drop_datum(&current_entry);
	return -1;
}


/**
 * Read a whole block into a struct of type block
 * @param: kv Target database
 * @param: blk_offset Offset to the target block
 * @param: blk Address of the block struct where to store the data
 * @return: 0 in cae of success, -1 otherwise
 */
int read_blk(KV *kv, len_t blk_offset, block *blk) {

	ssize_t read_size;

	//Read block
	switch (read_size = read_at(kv->_fd_blk, blk_offset,
				 blk->data, SIZE_BLK) ) {

		case  0: errno = EINVAL;
		case -1: return -1;
		
		default: if (read_size < SIZE_BLK_HEAD){
			 	errno = EINVAL;
				return -1;
			}
	}

	/* Position first bit from the left */
	int first_bit = sizeof (len_t) * CHAR_BIT - 1;
	
	/* Check if the block is full */
	if (BITSLICE(blk->data[0], first_bit, first_bit) == 0) { 
		// Block not full
		blk->n_entries = blk->data[0];
		blk->offset_nextblk = 0;
	} else {
		// Block full
		blk->n_entries = MAX_BLK_ENTR; 
		blk->offset_nextblk = HSIZE_BLK + SIZE_BLK * 
				BITSLICE(blk->data[0],first_bit-1,0);
	}

	return 0;
}
	


/**
 * Allocates a new empty block
 * @param: kv Database
 * @param block_number A pointer where to store the number of the new allocated
 *	  block if it's not null, and if the function executed successfully
 * @return: The address of the new block or 0 in case of error
 */
len_t allocate_blk(KV *kv, len_t* block_number){

	/* Do not allocate more than allowed max of blocks */
	len_t n_blocks = kv->nb_blocks;
	if (n_blocks >= MAX_BLKS ) return 0;

	/* Write header new block */
	len_t blk_offset = HSIZE_BLK + n_blocks*SIZE_BLK; // Offset new block
	len_t blk_head = 0; 				  // Init header with 0
	if ( safe_write_at(kv->_fd_blk, blk_offset, 
		&blk_head, sizeof blk_head) == -1 ) return 0;

	if (block_number != NULL) *block_number = n_blocks;

	kv->nb_blocks = n_blocks + 1;
	
	return blk_offset;

}


	
/**
 * Extends a chain of blocks by adding a new block at the end of it
 * @param kv Database
 * @param last_block Offset to the last block of the chain
 * @return the offset of the new allocated block or 0 in case of error
 */
len_t extend_blocks_chain(KV *kv, len_t last_block){

	/* Allocate a new empty block */
	len_t block_number;
	len_t offset_blk = allocate_blk(kv, &block_number);
	if (offset_blk == 0) return 0;

	/* Update last_block header: [ 1 | block_number] */
	len_t header = FLAG_USED | block_number;
	if ( safe_write_at(kv->_fd_blk, last_block, &header,
		sizeof header) == -1) return 0;


	return offset_blk;

}



/**
 * Stores a couple (key,value) into the file .kv creating a reference into
 * the file .dkv
 * @param: kv, Database to use
 * @param: key, Key to save
 * @param: value, Value to save
 * @param: ref_kv, Pointer to a struct kv_stored where to write informations
 *	   about the stored couple.
 * @return: 0 in case of success, -1 otherwise
 */
int store_kv
(KV *kv, const kv_datum* key, const kv_datum* value, kv_stored* ref_kv){

	len_t size_entry = key->len + value->len + 2 * sizeof (len_t);
	len_t dkv_slot;
	dkv_entry free_dkv_slot;

	/* Find a free space in the file .kv*/
	int ret;
	switch (kv->alloc) {
		case FIRST_FIT: ret = first_fit(kv, size_entry,
					&free_dkv_slot, &dkv_slot);
				break;
		case WORST_FIT: ret = worst_fit(kv, size_entry, 
					&free_dkv_slot, &dkv_slot);
				break;
		case BEST_FIT:  ret = best_fit(kv, size_entry, 
					&free_dkv_slot, &dkv_slot);
				break;
		
		default: errno = EINVAL;
			 return -1;
	}

	if (ret == -1) return -1;

	dkv_entry new_dkv_entry = { FLAG_USED | size_entry, 
				    free_dkv_slot.offset 
				  };


	
	/* Note that the order of the 2 following actions is important
	   in order to avoid to create a reference to inexisting data in case
	   of error  */ 

	/* First: write data into the file .kv */
	if (write_to_kv(kv, new_dkv_entry.offset, key, value) == -1) return -1;
	
	/* Second: update file .dkv */
	if (ret == 0){
		
		if (push_dkv_entry(kv,&new_dkv_entry) == -1) return -1;
		kv->end_kv += size_entry;

	} else {
		if (use_dkv_slot(kv, dkv_slot, 
				&new_dkv_entry) == -1) return -1;
	}
	
	ref_kv->offset_kv = new_dkv_entry.offset;
	ref_kv->dkv_slot = dkv_slot;

	return 0;

}



/**
 * Writes a couple (key,value) to a given offset in the .kv file
 * @param kv Database
 * @param offset Offset to the space where to write the couple (key,value)
 * @param key,value Data to write
 * @return 0 in case of success -1 otherwise
 */
int write_to_kv
(KV* kv, len_t offset, const kv_datum* key ,const kv_datum* value){

	/* Use a unique array to avoid multiple writing steps */
	size_t total_size = key->len + value->len + 2*sizeof (len_t);
	char* data = malloc (total_size);
	if (data == NULL) return -1;

	/* Two references to the array previously allocated */
	char* data_key = data;
	char* data_value = data + sizeof (len_t) + key->len;

	/* Fill the array with the given values */
	(*(len_t*) data_key) = key->len;
	memcpy(data_key + sizeof (len_t), key->ptr, key->len);

	(*(len_t*) data_value) = value->len;
	memcpy(data_value + sizeof (len_t), value->ptr, value->len);

	/* Store the content of the array on .kv */
	if (safe_write_at(kv->_fd_kv ,offset ,data ,total_size ) == -1){
		free(data);
		return -1;
	}

	free(data);
	return 0;
}


/**
 * Push a new dkv_entry at the end of the file .dkv
 * @param kv Database to use
 * @param new_entry A pointer to the dkv_entry to push
 * @return 0 in case of success, -1 otherwise
 */
int push_dkv_entry(KV *kv, const dkv_entry* new_entry){

	/* Number of entries */
	len_t nb_entries = kv->nb_dkv_entries + 1;
	
	/* Allocate more memory if necessary */
	if (kv->max_dkv_cache < nb_entries * sizeof (dkv_entry)){
		dkv_entry* ptr = realloc ( kv->dkv_cache, 
			kv->max_dkv_cache + CACHE_PAGE);
		if (ptr == NULL) return -1;

		kv->dkv_cache = ptr;
		kv->max_dkv_cache += CACHE_PAGE;
	}

	/* Write new_entry at the bottom of the file */
	kv->dkv_cache[kv->nb_dkv_entries] = (*new_entry);

	/* Update the number of entries */
	kv->nb_dkv_entries = nb_entries;

	return 0;
}	

/**
 * Change te old content of a dkv slot with a new content. If the size of the
 * data pointed by new is smaller than the size of the data of the given 
 * dkv_slot then a new dvk entry pointing to the free remaining memory is 
 * generated.
 *
 * @param kv Database
 * @param dkv_slot Number of the slot to use (= index kv->dkv_cache).
 *	  The size of the space pointed by this slot must be smaller or equal
 *	  than the size pointed by the new entry.
 * @param new  A pointer to the new dkv_entry to store at offset
 * @return 0 in case of success, -1 otherwise
 */
int use_dkv_slot
(KV *kv, len_t dkv_slot, dkv_entry* new){

	/* Calculate remaining free space  */ 	
	dkv_entry old = kv->dkv_cache[dkv_slot];
	len_t size_new = DKV_GET_SIZE(new->mem_usage);

	dkv_entry new_free;
	new_free.mem_usage = DKV_GET_SIZE(old.mem_usage) - size_new;
	new_free.offset = old.offset + size_new;


	if (new_free.mem_usage > 0) {
		#ifdef _SORT_DKV_
		/* Keep dkv sorted */
		if (shift_dkv(kv, dkv_slot, 1) == -1) return -1;
		kv->dkv_cache[dkv_slot] = *new;
		kv->dkv_cache[dkv_slot+1] = new_free;
		#else
		/* There is remaining free space: insert the `new_free`
		   entry before `new` to improve time when searching. */
		kv->dkv_cache[dkv_slot] = new_free;
		if (push_dkv_entry(kv, new) == -1) {
			kv->dkv_cache[dkv_slot] = old; // Restore old
			return -1;
		}
		#endif

	} else if (new_free.mem_usage == 0) {

		kv->dkv_cache[dkv_slot] = (*new);

	} else {
		/* The old slot has too small size */
		errno = EINVAL; 
		return -1;
	};

	return 0;
}


/**
 * @param pos Position
 * @param dir Direction
 */
#ifdef _SORT_DKV_
int shift_dkv(KV *kv, len_t pos, int dir){
	
	/* Current size database */
	len_t size = kv->nb_dkv_entries * sizeof (dkv_entry);
	len_t new_size = size + dir * sizeof (dkv_entry);
	
	/* Allocate more memory if necessary */
	if (kv->max_dkv_cache < new_size){
		dkv_entry* ptr = realloc ( kv->dkv_cache, 
			kv->max_dkv_cache + CACHE_PAGE);
		if (ptr == NULL) return -1;

		kv->dkv_cache = ptr;
		kv->max_dkv_cache += CACHE_PAGE;
	}
	
	/* Update the number of entries */
	kv->nb_dkv_entries += dir;

	/* Shift block of memory */
	size_t size_shift = size - (pos * sizeof (dkv_entry));
	if (size_shift <= 0) return 0;
	memmove(&kv->dkv_cache[pos+dir], &kv->dkv_cache[pos], size_shift);


	return 0;
}	
#endif

/**
 * Search the first free block of memory big enough to contain `size` bytes
 * @param: kv Database
 * @param: size Length in byte of the space to search
 * @param: dkv_content Address to a struct dkv_entry that will be filled with
 *	   the content of the dkv_entry refering to the free space of memory.
 *	   If the free momory found is not referred by any dkv_entry 
 *	   ,theb this variable will contain a reference to the 
 *	   free space at the end of the file .kv.
 * @param: dkv_slot If the free memory is referred by an entry in the 
 *	   file dkv then the offset of that entry is stored here. In the other
 *	   cases the stored value is undefined.
 * @return: 0 if a dkv_entry has been found, 1 if the dkv_entry represent the
 *	    end of the file .kv, -1 in case of error.
 */
int first_fit(KV *kv, len_t size, dkv_entry* dkv_content ,len_t* dkv_slot){

	memset(dkv_content, 0, sizeof (dkv_entry));

	len_t size_free_space = 0;

	len_t i;
	for (i = 0; i < kv->nb_dkv_entries; i++){

		if (DKV_IS_USED(kv->dkv_cache[i].mem_usage)) continue;
		
		size_free_space = DKV_GET_SIZE(kv->dkv_cache[i].mem_usage);
		if (size <= size_free_space){
			/* Found */
			dkv_content->mem_usage = size_free_space;
			dkv_content->offset = kv->dkv_cache[i].offset;
			(*dkv_slot) = i;
			return 1;
		}
	}

	/* No free entries, try using remaining kv space */
	size_free_space = UNSIGNED_MAX(len_t) - kv->end_kv;
	if (size <= size_free_space) {
		dkv_content->mem_usage = size_free_space;
		dkv_content->offset = kv->end_kv;
		(*dkv_slot) = UNSIGNED_MAX(len_t);
		return 0;
	}

	return -1;
}


/**
 * Search the biggest free block of memory big enough to contain `size` bytes
 * @reference first_fit
 */
int worst_fit(KV *kv, len_t size, dkv_entry* dkv_content ,len_t* dkv_slot){

	memset(dkv_content, 0, sizeof (dkv_entry));

	len_t size_free_space = 0;
	len_t index_worst = 0;
	len_t current_free_space = 0;

	/* Search worst */
	len_t i;
	for (i = 0; i < kv->nb_dkv_entries; i++){

		if (DKV_IS_USED(kv->dkv_cache[i].mem_usage)) continue;
		
		current_free_space = DKV_GET_SIZE(kv->dkv_cache[i].mem_usage);

		if (current_free_space > size_free_space){
			index_worst = i;
			size_free_space = current_free_space;
		}

	}

	/* Use it if big enough */
	if (size <= size_free_space){
		dkv_content->mem_usage = size_free_space;
		dkv_content->offset = kv->dkv_cache[index_worst].offset;
		(*dkv_slot) = index_worst;
		return 1;
	}

	/* No free entries, try using remaining kv space */
	size_free_space = UNSIGNED_MAX(len_t) - kv->end_kv;
	if (size <= size_free_space) {
		dkv_content->mem_usage = size_free_space;
		dkv_content->offset = kv->end_kv;
		(*dkv_slot) = UNSIGNED_MAX(len_t);
		return 0;
	}

	return -1;
}

/**
 * Search the smallest free block of memory big enough to contain `size` bytes
 * @reference first_fit
 */
int best_fit(KV *kv, len_t size, dkv_entry* dkv_content ,len_t* dkv_slot){

	memset(dkv_content, 0, sizeof (dkv_entry));

	len_t size_free_space = UNSIGNED_MAX(len_t);
	len_t index_best = 0;
	len_t current_free_space = 0;
	bool found = false; 

	/* Search best */
	len_t i;
	for (i = 0; i < kv->nb_dkv_entries; i++){

		if (DKV_IS_USED(kv->dkv_cache[i].mem_usage)) continue;
		
		current_free_space = DKV_GET_SIZE(kv->dkv_cache[i].mem_usage);

		if (size <= current_free_space &&
		    current_free_space < size_free_space){

			index_best = i;
			size_free_space = current_free_space;
			found = true;
			if (size == size_free_space) break;
		}
	}

	if (found){
		dkv_content->mem_usage = size_free_space;
		dkv_content->offset = kv->dkv_cache[index_best].offset;
		(*dkv_slot) = index_best;
		return 1;
	}


	/* No free entries, try using remaining kv space */
	size_free_space = UNSIGNED_MAX(len_t) - kv->end_kv;
	if (size <= size_free_space) {
		dkv_content->mem_usage = size_free_space;
		dkv_content->offset = kv->end_kv;
		(*dkv_slot) = UNSIGNED_MAX(len_t);
		return 0;
	}

	return -1;
}


/**
 * Remove an entry from .dkv
 * @param kv Database
 * @param offset_kv Offset to the kv stored data
 * @return 0 in case of success, -1 otherwise
 */
int remove_data(KV *kv, len_t offset_kv){
	len_t indexes[3]; bool found[3];
	if (dkv_find_contiguos(kv, offset_kv, indexes, found) == -1) return -1;

		
	dkv_entry* target = &kv->dkv_cache[indexes[1]];

	target->mem_usage &= ~FLAG_USED; // Set free

	if (found[0] && !DKV_IS_USED(kv->dkv_cache[indexes[0]].mem_usage)) {
		
		target->offset = kv->dkv_cache[indexes[0]].offset;
		target->mem_usage += 
			DKV_GET_SIZE(kv->dkv_cache[indexes[0]].mem_usage);
		#ifdef _SORT_DKV_
		if (shift_dkv(kv,indexes[1],-1) == -1) return -1;
		indexes[1]--; indexes[2]--;
		#else
		kv->dkv_cache[indexes[0]] = 
			kv->dkv_cache[--kv->nb_dkv_entries];
		#endif

	}

	if (found[2] && !DKV_IS_USED(kv->dkv_cache[indexes[2]].mem_usage)) {

		target->mem_usage += 
			DKV_GET_SIZE(kv->dkv_cache[indexes[2]].mem_usage);
		#ifdef _SORT_DKV_
		if (shift_dkv(kv,indexes[2]+1,-1) == -1) return -1;
		#else
		kv->dkv_cache[indexes[2]] = 
			kv->dkv_cache[--kv->nb_dkv_entries];
		#endif

	}

	if (target->offset + target->mem_usage == kv->end_kv){

		if (ftruncate(kv->_fd_kv, target->offset) == -1) return -1;
		kv->end_kv = target->offset;
		*target = kv->dkv_cache[--kv->nb_dkv_entries];

		if (kv->nb_dkv_entries * sizeof (dkv_entry) 
			<= kv->max_dkv_cache - CACHE_PAGE  && 
		    kv->max_dkv_cache > CACHE_PAGE ){

			dkv_entry* tmp = realloc(kv->dkv_cache, 
					kv->max_dkv_cache-CACHE_PAGE);
			if (tmp == NULL) return -1;
			kv->dkv_cache = tmp;
			kv->max_dkv_cache -= CACHE_PAGE;
		}
		
	}
	
	return 0;
}


/**
 * Find a dkv slot and the slots adjacents to that slot (who points to adjacents
 * memory zones).
 */
int dkv_find_contiguos(KV* kv, len_t offset_kv, len_t indexes[3], bool found[3]){

	enum { prev = 0, target = 1, next = 2};

	found[0] = false; found[1] = false; found[2] = false; 

	len_t guess_offset_next = UNSIGNED_MAX(len_t);
	len_t offset_next;
	len_t c_offset, c_mem_usage; // Offset/mem_usage current entry

	len_t i;
	for (i = 0; i < kv->nb_dkv_entries; i++){

		c_offset = kv->dkv_cache[i].offset;
		c_mem_usage = kv->dkv_cache[i].mem_usage;

		if (c_offset == offset_kv){
			/* Entry found */
			indexes[target] = i;
			found[target] = true;
			break;

		} else if (c_offset > offset_kv && c_offset < guess_offset_next){

			guess_offset_next = c_offset;
			indexes[next] = i;

		} else if (c_offset + DKV_GET_SIZE(c_mem_usage) == offset_kv){

			indexes[prev] = i;
			found[prev] = true;
		}


	}


	if (found[target] == false) { 
		errno = ENOENT;
		return -1;
	}
	
	offset_next = offset_kv + DKV_GET_SIZE(c_mem_usage);

	if (offset_next == guess_offset_next) found[next] = true;	

	for (i++ ; i < kv->nb_dkv_entries; i++){
		
		if (found[prev] == true && found[next] == true) break;


		c_offset = kv->dkv_cache[i].offset;
		c_mem_usage = kv->dkv_cache[i].mem_usage;


		if (offset_next == c_offset){

			indexes[next] = i;
			found[next] = true;	

		} else if (c_offset + DKV_GET_SIZE(c_mem_usage) == offset_kv){

			indexes[prev] = i;
			found[prev] = true;
		}
			
	}

	return 0;
}









/**
 * Zero initialize a struct of type kv_datum
 * @param: dat Address to the data to initialize
 */
void init_datum(kv_datum *dat){
	memset(dat, 0, sizeof (kv_datum));
}


/**
 * Get rid of a struct of type kv_datum
 * @parem: dat Address of the struct to drop
 */
void drop_datum(kv_datum *dat){
	free(dat->ptr);
	init_datum(dat);
}


/**
 * Compare two kv_datum
 * @param: a,b The addresses of the elements to compare
 * @return: 1 if a == b , 0 otherwise
 */
inline int eq_datum(const kv_datum *a, const kv_datum *b){
	return a->len == b->len && (memcmp(a->ptr,b->ptr,a->len) == 0);
}

/** 
 * Read a stored entry in to a kv_datum 
 * @param: kv Database from where to read
 * @param: offset Offset to the target data 
 * @param: dat Address to the kv_datum where to store
 *	   the entry. It must have been initialized 
 * @return: 0 in case of success, -1 otherwise
 */
int read_datum(KV *kv, len_t offset, kv_datum *dat){ 

	if (safe_read_at(kv->_fd_kv, offset, &dat->len, sizeof (len_t)) == -1){
		return -1;
	}

	if (dat->len == 0) return 0;
	
	void *new_ptr;
	if ( (new_ptr = realloc(dat->ptr, dat->len)) == NULL) return -1;

	dat->ptr = new_ptr;

	if (safe_read_at(kv->_fd_kv, offset + sizeof (len_t), 
			 dat->ptr, dat->len) == -1 ) return -1;

	return 0;
} 








		
		


/* Read data from file at the given offset */
ssize_t read_at(int fd, len_t offset, void *buff, size_t count){

	if (lseek(fd, offset, SEEK_SET) == -1) return -1;

	return read(fd, buff, count);
}

/* Read data from file at the given offset. Returns an error if the size
 * of the data that has been read is different than count 
 */
ssize_t safe_read_at(int fd, len_t offset, void *buff, size_t count){
	ssize_t nb = read_at(fd, offset, buff, count);
	if (nb == -1 ) return -1;
	return ( (size_t) nb == count)? nb : -1;
}

/* @ref read_at */
ssize_t write_at(int fd, len_t offset, const void *buff, size_t count){
	
	if (lseek(fd, offset, SEEK_SET) == -1) return -1;
	return write(fd, buff, count);
}

/* @ref safe_read_at */
ssize_t safe_write_at(int fd, len_t offset, const void *buff, size_t count){
	ssize_t nb = write_at(fd, offset, buff, count);
	if (nb == -1 ) return -1;
	return ( (size_t) nb == count)? nb : -1;
}




/**
 * Assign hash fun to KV struct following the hidx
 */
int setHashFun(KV *db, int hidx){

	switch (hidx){ 
		case 0:
		case HASH_1:	db->_hash_fun = hash_fun1;
				break;
		case HASH_2:	db->_hash_fun = hash_fun2;
				break;
		case HASH_3:	db->_hash_fun = hash_fun3;
				break;
		default:	errno = EINVAL;
				return -1;
	}	
		
	return 0;
}


/**
 * Opens db files and assing fds to the KV struct
 */
int openFilesKV(KV *db, const char *dbname, int flags){
	/* Open database files */
	char *filename;
	mode_t permissions = 0666;
	size_t ll = strlen(dbname);

	if ( (filename = malloc( ll + 4)) == NULL) return -1;
	char *sffx = filename + ll;
	memcpy(filename, dbname, ll);
	
	
	sffx[0] = '.'; sffx[1] = 'h'; sffx[2] = 0; 			// ".h"
	db->_fd_h = open(filename, flags, permissions);

	sffx[1] = 'k'; sffx[2] = 'v'; sffx[3] = 0;			// ".kv"
	db->_fd_kv = open(filename, flags, permissions);

	sffx[1] = 'b'; sffx[2] = 'l'; sffx[3] = 'k'; sffx[4] = 0;	// ".blk"
	db->_fd_blk = open(filename, flags, permissions);
	
	sffx[1] = 'd'; sffx[2] = 'k'; sffx[3] = 'v'; sffx[4] = 0;	// ".dkv"
	db->_fd_dkv = open(filename, flags, permissions);
	
	if ( db->_fd_h   == -1 || db->_fd_kv  == -1 || 
	     db->_fd_blk == -1 || db->_fd_dkv == -1 ) {	
		free(filename);
		return -1;	// errno = [error on last failed open call]
	}
	
	free(filename);

	return 0;
}

/**
 * Writes headers into all the database related files
 * WARNING: this function must be consistent with the 
 *          definition  of the headers size (HSIZE_*)
 * @note: hidx must be a valid index
 */
int writeHeaders(KV *db, int hidx){

	char header[MAX_HSIZE];

	// File .h
	(*(len_t*) (&header[0])) = MGN_H;
	(*(len_t*) (&header[MGN_SIZE])) = (len_t) hidx;
	if (safe_write_at(db->_fd_h, 0, header, HSIZE_H) == -1) return -1;

	// File .blk
	(*(len_t*) (&header[0])) = MGN_BLK;
	(*(len_t*) (&header[MGN_SIZE])) =  0;
	if (safe_write_at(db->_fd_blk, 0, header, HSIZE_BLK) == -1) return -1;
	
	// File .kv
	(*(len_t*) (&header[0])) = MGN_KV;
	if (safe_write_at(db->_fd_kv, 0, header, HSIZE_KV) == -1) return -1;

	// File .dkv
	(*(len_t*) (&header[0])) = MGN_DKV;
	(*(len_t*) (&header[MGN_SIZE])) =  0; // n entries
	(*(len_t*) (&header[MGN_SIZE+sizeof (len_t)])) =  HSIZE_KV; // end kv
	if (safe_write_at(db->_fd_dkv, 0, header, HSIZE_DKV) == -1) return -1;


	return 0;
	
	
}

/**
 * Checks magic nb validity and sync db with headers infos
 * WARNING: same as "writeHeaders"
 */
int useHeaders(KV *db){
	/* Magic numbers */
	uint32_t mgn_h, mgn_kv, mgn_blk, mgn_dkv;
 	
	if ( safe_read_at(db->_fd_h,0,&mgn_h,MGN_SIZE)     == -1 ||
	     safe_read_at(db->_fd_kv,0,&mgn_kv,MGN_SIZE)   == -1 ||
	     safe_read_at(db->_fd_blk,0,&mgn_blk,MGN_SIZE) == -1 ||
	     safe_read_at(db->_fd_dkv,0,&mgn_dkv,MGN_SIZE) == -1 
	   ) return -1;

	if ( mgn_h   != MGN_H   || mgn_kv  != MGN_KV || 
	     mgn_blk != MGN_BLK || mgn_dkv != MGN_DKV  ){
		errno = EINVAL; 
		return -1;
	}

	// Hash function
	uint32_t hidx;
	if ( safe_read_at(db->_fd_h,MGN_SIZE,&hidx,4) == -1) return -1;
	if ( setHashFun(db,(int) hidx) == -1) return -1;

	// Nb blocks	
	if ( safe_read_at(db->_fd_blk,MGN_SIZE,&db->nb_blocks, 
		sizeof (len_t)) == -1) return -1;

	// Nb dkv entries
	if ( safe_read_at(db->_fd_dkv,MGN_SIZE,&db->nb_dkv_entries, 
		sizeof (len_t)) == -1) return -1;

	// Offset end kv
	if ( safe_read_at(db->_fd_dkv,MGN_SIZE + sizeof (len_t),
		&db->end_kv,sizeof (len_t)) == -1) return -1;

	return 0;
}

len_t hash_fun1(const kv_datum *key){
	len_t hash = 0;
	len_t i;
	for (i = 0; i < key->len; i++) {
		hash += ((unsigned char*) key->ptr)[i];
		hash %= 999983;
	}
	return hash;
}
/* XOR compression */
len_t hash_fun2(const kv_datum *key){
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
len_t hash_fun3(const kv_datum *key){
	len_t hash = 2166136261;
	len_t i;
	for (i = 0; i < key->len; i++) {
		hash ^= ((char*) key->ptr)[i];
		hash *= 16777619;
		hash %= 999983;
	}
	return hash;
}



/**
 * Closes all fds != -1 and free all the memory, errno keeps the pre-call value
 */
void infail_kvclose(KV *db){

	int _errbkp = errno;

	/* Close all open files */
	if (db->_fd_h != -1)   close(db->_fd_h);
	if (db->_fd_kv != -1)  close(db->_fd_kv);
	if (db->_fd_blk != -1) close(db->_fd_blk);
	if (db->_fd_dkv != -1) close(db->_fd_dkv);

	/* Free allocated memory */
	free(db);


	errno = _errbkp;
}

/**
 * Initializes a KV struct with NULL bytes, and fds = -1
 */
void initKV(KV *db){
	memset(db, 0, sizeof(KV));
	db->_fd_h   = -1;
	db->_fd_kv  = -1;
	db->_fd_blk = -1;
	db->_fd_dkv = -1;
	
	db->end_kv = HSIZE_KV;
}

int load_cache(KV* kv){
	/* DKV cache */
	len_t size_entries = kv->nb_dkv_entries * sizeof (dkv_entry);
	len_t size_cache = size_entries + CACHE_PAGE - 
		(size_entries % CACHE_PAGE); // Min cache pages needed

	if ((kv->dkv_cache = malloc(size_cache)) == NULL) return -1;

	kv->max_dkv_cache = size_cache;

	if (safe_read_at(kv->_fd_dkv, HSIZE_DKV, 
		kv->dkv_cache, size_entries) == -1 ) return -1;

	return 0;
}

int set_flags(KV *kv, const char *mode){
	int oflags, cflags; // opening flags, creation flags
	switch (*mode++){
		case 'r':
			oflags = O_RDONLY;
			cflags = 0;
			kv->write_only = false;
			break;
		case 'w':
		 	oflags = O_RDWR;
			cflags = O_CREAT | O_TRUNC;
			kv->write_only = true;
			break;
		default:
			errno = EINVAL;
			return -1;
	}
	if (*mode == '+'){
		oflags = O_RDWR;
		cflags |= O_CREAT; // "r+" must create if necessary
		kv->write_only = false;
	}

	kv->flags = oflags | cflags;	
	return 0;
}

