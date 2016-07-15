#!/bin/sh

#
# Quelques tests pour vérifier les options/paramètres
#

TEST=$(basename $0 .sh)-$$

DB=${TEST}-db
TMP=/tmp/$TEST
LOG=$TEST.log
V=${VALGRIND}			# mettre VALGRIND à "valgrind -q" pour activer

exec 2> $LOG
set -x

fail ()
{
    echo "==> Échec du test '$TEST' sur '$1'."
    echo "==> Log : '$LOG'."
    echo "==> DB : '$DB'."
    echo "==> Exit"
    exit 1
}

rm -f $DB.*

# base inexistante : doit générer une erreur
get $DB ma-clef					&& fail "get base inexistante"

# on stocke une entrée
$V put $DB ma-clef abc				|| fail "put ma-clef 1"

# on recommence en spécifiant un index de h
rm -f $DB.*
$V put -i 0 $DB ma-clef abc			|| fail "put -i 0 ma-clef"

# on recommence en spécifiant un index de h débile
rm -f $DB.*
put -i 9999 $DB ma-clef abc			&& fail "put -i 9999 ma-clef"

# allez, on revient à quelque chose de plus raisonnable
rm -f $DB.*
$V put $DB ma-clef abc				|| fail "put ma-clef 2"

# pas de paramètre : doit générer une erreur
get 						&& fail "get sans paramètre"

# récupération d'une clef inexistante
get $DB inconnu					&& fail "get inconnu"

# mode "toutes les clefs"
$V get -q $DB > /dev/null			|| fail "get all"

# vérifier le retour
test "$(get $DB ma-clef)" = "ma-clef: abc" 	|| fail "get ma-clef"

# récupération avec -q
test $(get -q $DB ma-clef) = abc		|| fail "get -q ma-clef"

# test option -h
$V get -h					|| fail "get -h"

# option inconnue
get -x $DB ma-clef				&& fail "get -x"

# test d'ouverture avec des fichiers avec le mauvais magic number
for SUFF in dkv h blk kv
do
    echo "mauvais magic" > $DB.$SUFF	
    get $DB ma-clef				&& fail "get magic $SUFF"
done

# supprimer le fichier temporaire en cas de sortie normale
rm -f $DB.*
