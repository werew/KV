#!/bin/sh

#
# Un test un peu gros
#

TEST=$(basename $0 .sh)-$$

DB=${TEST}-db
TMP=/tmp/$TEST
DIR=${DIR:-/usr/include}	# essayer avec /usr/bin pour voir...
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
rm -f $TMP.liste

# Stockage de "grosses" valeurs en "grosse" quantité
for F in $DIR/*
do
    if [ -f $F ]
    then
	echo $F
	$V put $DB $F < $F		 	|| fail "put $F"
    fi
done | sort > $TMP.liste

# Vérification de la liste
$V get -q $DB | sort | diff -q $TMP.liste -	|| fail "diff liste"
rm -f $TMP.liste

# Test des options
$V del -h					|| fail "del -h"
del -x						&& fail "del -x"
del $DB une-clef autre-chose			&& fail "del autre-chose"

# Tenter de détruire une clef inconnue
del $DB inconnue				&& fail "del inconnue"


# Vérification de chaque fichier individuel
for F in $(get -q $DB)
do
    $V get -q $DB $F | cmp -s $F		|| fail "cmp $F"
done

# Suppression des fichiers
for F in $(get -q $DB)
do
    $V del $DB $F 				|| fail "del $F"
done

# Vérification de la base : elle doit être vide
test "$(get -q $DB | wc -l)" = 0		|| fail "cmp vide"

# supprimer le fichier temporaire en cas de sortie normale
rm -f $DB.*
