/*
 * Ajoute ou remplace un couple <clef, valeur>
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>

#include "kv.h"
#include "common.h"

char *usage_string = "usage: %s [-h][-i hidx][-a first|worst|best] base key [val]\n" ;

char *help_string = "\
Stocke un couple <clef, valeur>. Si la valeur n'est\n\
pas spécifiée comme un argument, elle est lue sur l'entrée standard.\n\
\n\
Les options sont :\n\
-h : à l'aide !\n\
-i : index de la fonction de hachage. L'index 0 existe toujours\n\
-a : algorithme d'allocation ('first' pour 'first fit', 'worst' ou 'best')\n\
" ;

/*
 * @brief Lire une valeur sur l'entrée standard
 *
 * Lit une valeur sur l'entrée standard et la stocke dans un kv_datum.
 * Tente de minimiser les accès à read :
 * - si on lit depuis un fichier, tenter de tout lire en une seule fois
 * - si on lit depuis autre chose (périphérique, tube), lire jusqu'à
 *   la fin du fichier
 *
 * @param val un kv_datum vide, qui sera rempli lors de la lecture
 */

#define	BLOCKSIZE	4096

void lire_val (kv_datum *val)
{
    struct stat stbuf ;
    ssize_t n ;
    size_t taille_buf, taille_lecture ;

    if (fstat (0, &stbuf) == -1)
	raler (NULL, "fstat") ;
    if (S_ISREG (stbuf.st_mode))
	taille_buf = taille_lecture = stbuf.st_size ;
    else
	taille_buf = taille_lecture = BLOCKSIZE ;
    val->len = 0 ;
    val->ptr = malloc (taille_buf) ;
    if (val->ptr == NULL)
	raler (NULL, "malloc") ;

    while ((n = read (0, ((uint8_t *)val->ptr) + val->len, taille_lecture)) > 0)
    {
	taille_lecture = BLOCKSIZE ;
	val->len += n ;
	if (val->len + taille_lecture > taille_buf)
	{
	    taille_buf += taille_lecture ;
	    val->ptr = realloc (val->ptr, taille_buf)  ;
	}
    }
    if (n == -1)
	raler (NULL, "read") ;
}


/*
 * @brief Reconnaître la méthode d'allocation fournie sous forme de chaîne
 *
 * Cette fonction renvoie l'alloc_t correspondant au char *
 *
 * @param alloc méthode d'allocation, sous forme de chaîne
 * @return méthode d'allocation reconnue
 */

alloc_t allocation (const char *alloc)
{
    alloc_t a ;

    a = FIRST_FIT ;
    if (alloc == NULL)
	a = FIRST_FIT ;
    else if (strcmp (alloc, "first") == 0)
	a = FIRST_FIT ;
    else if (strcmp (alloc, "best") == 0)
	a = BEST_FIT ;
    else if (strcmp (alloc, "worst") == 0)
	a = WORST_FIT ;
    else
    {
	errno = EINVAL ;
	raler (NULL, "allocation") ;
    }

    return a ;
}

/*
 * @brief Fonction principale
 */

int main (int argc, char *argv [])
{
    int opt ;
    KV *kv ;
    int hidx = 0 ;
    char *alloc = NULL ;
    alloc_t a ;
    kv_datum key, val ;

    while ((opt = getopt (argc, argv, "ha:i:")) != -1)
    {
	switch (opt)
	{
	    case 'h' :				/* help */
		usage (argv [0], 0) ;
		break ;
	    case 'a' :				/* mode d'allocation */
		alloc = optarg ;
		break ;
	    case 'i' :				/* index de la fct de hash */
		hidx = atoi (optarg) ;
		break ;
	    default :
		usage (argv [0], 1) ;
	}
    }

    switch (argc - optind)
    {
	case 2 :			/* base key */
	    lire_val (&val) ;
	    break ;

	case 3 :			/* base key val */
	    val.ptr = argv [optind + 2] ;
	    val.len = strlen (val.ptr) ;
	    break ;

	default :
	    usage (argv [0], 1) ;
	    break ;
    }

    a = allocation (alloc) ;

    if ((kv = kv_open (argv [optind], "r+", hidx, a)) == NULL)
	raler (kv, "kv_open") ;

    key.ptr = argv [optind + 1] ;
    key.len = strlen (key.ptr) ;

    if (kv_put (kv, &key, &val) == -1)
	raler (kv, "kv_put") ;

    if (kv_close (kv) == -1)
	raler (kv, "kv_close") ;

    exit (0) ;
}
