#include <stdint.h>		/* pour uint32_t */

/*
 * Définition de type incomplète : la struct KV est définie avec
 * l'implémentation des fonctions kv_*(), mais ça n'empêche pas
 * les programmes utilisateurs de ces fonctions de "transporter"
 * des pointeurs sur ces structures.
 */

typedef struct KV KV ;

/*
 * Avec des longueurs sur 32 bits, on peut aller jusqu'à 4 Go
 */

typedef uint32_t len_t ;

/*
 * Représente une donnée : soit une clef, soit une valeur
 */

struct kv_datum
{
    void  *ptr ;		/* la donnée elle-même */
    len_t len ;			/* sa taille */
} ;

typedef struct kv_datum kv_datum ;

/*
 * Les différents types d'allocation
 */

typedef enum { FIRST_FIT, WORST_FIT, BEST_FIT } alloc_t ;

/*
 * Définition de l'API de la bibliothèque kv
 */

KV *kv_open (const char *dbname, const char *mode, int hidx, alloc_t alloc) ;
int kv_close (KV *kv) ;
int kv_get (KV *kv, const kv_datum *key, kv_datum *val) ;
int kv_put (KV *kv, const kv_datum *key, const kv_datum *val) ;
int kv_del (KV *kv, const kv_datum *key) ;
void kv_start (KV *kv) ;
int kv_next (KV *kv, kv_datum *key, kv_datum *val) ;
