/*
 * Supprime une clef de la base
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

char *usage_string = "usage: %s [-h] base key\n" ;

char *help_string = "\
Supprime la clef indiquée de la base\n\
\n\
Les options sont :\n\
-h : à l'aide !\n\
";

/*
 * @brief Fonction principale
 */

int main (int argc, char *argv [])
{
    int opt ;
    KV *kv ;
    kv_datum key ;

    while ((opt = getopt (argc, argv, "h")) != -1)
    {
	switch (opt)
	{
	    case 'h':			/* help */
		usage (argv [0], 0) ;
		break ;
	    default :
		usage (argv [0], 1) ;
	}
    }

    if (optind != argc - 2)
	usage (argv [0], 1) ;

    key.ptr = argv [optind + 1] ;
    key.len = strlen (key.ptr) ;

    if ((kv = kv_open (argv [optind], "r+", 0, FIRST_FIT)) == NULL)
	raler (kv, "kv_open") ;

    if (kv_del (kv, &key) == -1)
	raler (kv, "kv_del") ;

    if (kv_close (kv) == -1)
	raler (kv, "kv_close") ;

    exit (0) ;
}
