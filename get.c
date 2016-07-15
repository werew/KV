/*
 * Lit un couple <clef, valeur> dans la base, ou tous les couples
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

#include "kv.h"
#include "common.h"

char *usage_string = "usage: %s [-h][-q] base [key ...]\n" ;

char *help_string = "\
Affiche une ou plusieurs clefs, avec deux modes possibles :\n\
- soit une ou plusieurs clefs sont spécifiées, et alors\n\
  seules ces clefs sont affichées\n\
- soit aucune clef n'est spécifiée, et toutes les clefs sont\n\
  alors affichées\n\
\n\
Les options sont :\n\
-h : à l'aide !\n\
-q : mode silencieux (quiet), a un sens différent suivant les deux modes :\n\
    - dans le mode 'une ou plusieurs clefs' : l'option -q indique\n\
      d'afficher seulement les valeurs (sinon, c'est 'clef: valeur')\n\
    - dans le mode 'toutes les clefs' : l'option -q indique\n\
      d'afficher seulement les clefs (sinon, c'est 'clef: valeur')\n\
";

/*
 * @brief Affiche une clef individuelle
 *
 * Affiche une clef et/ou une valeur, suivant si les paramètres
 * sont nuls ou non.
 *
 * @param key clef ou NULL
 * @param val valeur ou NULL
 */

void print_one (kv_datum *key, kv_datum *val)
{
    int neednl ;

    /*
     * On utilise write(2) plutôt que printf(3) ici car
     * les données (clef ou valeur) ne sont pas forcément
     * des chaînes de caractères, mais peuvent être des
     * données binaires contenant n'importe quel octet,
     * notamment \0.
     */

    neednl = 1 ;			/* par défaut, il faut le \n final */
    if (key != NULL)
    {
	write (1, key->ptr, key->len) ;
    }
    if (key != NULL && val != NULL)
	write (1, ": ", 2) ;
    if (val != NULL)
    {
	write (1, val->ptr, val->len) ;
	if (val->len == 0 || ((uint8_t *) val->ptr) [val->len - 1] == '\n')
	    neednl = 0 ;
    }
    if (neednl)
	write (1, "\n", 1) ;
}

/*
 * @brief Affiche toutes les clefs présentes dans la base
 *
 * @param kv descripteur d'accès à la base
 * @param quiet vrai si seules les clefs sont affichées (et non les valeurs)
 */

void print_all (KV *kv, int quiet)
{
    kv_datum key, val ;
    int r ;

    /*
     * La récupération des kv_datum fera le malloc nécessaire
     */

    key.ptr = NULL ;
    val.ptr = NULL ;

    /*
     * Parcourir tous les couples <clef, valeur>
     */

    kv_start (kv) ;
    while ((r = kv_next (kv, &key, &val)) == 1)
    {
	print_one (&key, quiet ? NULL : &val) ;

	free (key.ptr) ; key.ptr = NULL ;
	free (val.ptr) ; val.ptr = NULL ;
    }

    if (r == -1)
	raler (kv, "kv_next") ;
}

/*
 * @brief Fonction principale
 */

int main (int argc, char *argv [])
{
    int opt ;
    KV *kv ;
    int quiet = 0 ;
    int i ;
    int r ;

    while ((opt = getopt (argc, argv, "hq")) != -1)
    {
	switch (opt)
	{
	    case 'h' :			/* help */
		usage (argv [0], 0) ;
		break ;
	    case 'q' :			/* quiet */
		quiet = 1 ;
		break ;
	    default :
		usage (argv [0], 1) ;
	}
    }

    /*
     * Il faut au moins la base.
     */

    if (optind == argc)
	usage (argv [0], 1) ;

    if ((kv = kv_open (argv [optind], "r", 0, FIRST_FIT)) == NULL)
	raler (kv, "kv_open") ;

    r = 0 ;				/* code de retour */

    /*
     * Sélectionner l'un des deux modes
     */

    if (optind == argc - 1)
	print_all (kv, quiet) ;
    else
    {
	for (i = optind + 1 ; i < argc ; i++)
	{
	    kv_datum key, val ;
	    int g ;

	    key.ptr = argv [i] ;
	    key.len = strlen (key.ptr) ;
	    val.ptr = NULL ;
	    val.len = 0 ;

	    g = kv_get (kv, &key, &val) ;
	    switch (g)
	    {
		case -1 :
		    raler (kv, "kv_get") ;
		case 0 :
		    fprintf (stderr, "%s: non trouvé\n", argv [i]) ;
		    r = 1 ;
		    break ;
		default :
		    print_one (quiet ? NULL : &key, &val) ;
		    free (val.ptr) ;
	    }
	}
    }

    if (kv_close (kv) == -1)
	raler (kv, "kv_close") ;

    exit (r) ;
}
