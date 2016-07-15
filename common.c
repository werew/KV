/*
 * Fichier commun à plusieurs programmes exemples
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#include "kv.h"

extern char *usage_string ;
extern char *help_string ;

/*
 * @brief Affiche un message d'aide et sort
 *
 * Cette fonction affiche un message rappelant la syntaxe et sort.
 * Elle a deux modes, suivant le paramètre exitcode:
 * - soit c'est un code non nul, auquel cas cette fonction est
 *   appelée suite à une erreur de syntaxe : un message bref est
 *   alors affiché.
 * - soit c'est un code nul, auquel cas cette fonction est appelée
 *   suite à l'option "help" (-h par exemple) : un message long
 *   est utilisé.
 *
 * Le message est défini dans le programme sous forme de deux variables
 * (`usage_string` et `help_string`).
 *
 * @param argv0 le nom du programme
 * @param exitcode le code de retour à transmettre à exit()
 */

void usage (const char *argv0, int exitcode)
{
    const char *p = strrchr (argv0, '/') ;
    if (p == NULL)
	p = argv0 ;

    if (exitcode == 0)			/* help */
	fprintf (stderr, help_string, p) ;
    else				/* usage invalide */
	fprintf (stderr, usage_string, p) ;

    exit (exitcode) ;
}

/*
 * @brief Affiche un message d'erreur et sort
 *
 * Cette fonction affiche un message d'erreur en fonction de errno
 * et sort. Si un descripteur de base est ouvert, ferme la base.
 *
 * @param kv descripteur de base ouverte, ou NULL
 * @param msg message pour perror()
 */

void raler (KV *kv, const char *msg)
{
    perror (msg) ;
    if (kv != NULL)
	if (kv_close (kv) == -1)
	    perror ("kv_close") ;
    exit (1) ;
}

void rand_datum(kv_datum* dat, len_t max_len){

	dat->len = rand() % max_len + 1;
	
	len_t i; int rnd;
	for (i = 0; i < dat->len; i++){
		rnd = rand();
		((char*) dat->ptr)[i] = ((char*) &rnd)[i % sizeof (int)];
	}

}
