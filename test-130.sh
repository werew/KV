#!/bin/sh

#
# Test des méthodes d'allocation
#

TEST=$(basename $0 .sh)-$$

DB=${TEST}-db
TMP=/tmp/$TEST
LOG=$TEST.log
V=${VALGRIND}			# mettre VALGRIND à "valgrind -q" pour activer

HIDX=0				# index de la clef de h par défaut

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

# insère la clef $1, avec la méthode $2 et une valeur de $3 octets
generer ()
{
    (echo "$*" ; dd if=/dev/zero count=1 bs=$3) | $V put -a $2 $DB $1
}

# teste l'ordre des clefs (on suppose que "get" renvoie les clefs
# dans l'ordre du fichier kv)
tester_ordre ()
{
    for k
    do
	echo $k
    done > $TMP.liste
    $V get -q $DB | cmp -s $TMP.liste
}

rm -f $DB.*

##############################################################################
# Crée une base pleine de trous
ESPACE=0
for i in $(seq 1 3)
do
    $V put $DB -i $HIDX a$i repere-$i		|| fail "put a$i"
    ESPACE=$((ESPACE + 1000))
    generer t$i first $ESPACE			|| fail "put t$i"
done
for i in $(seq 4 5)
do
    $V put $DB a$i repere-$i			|| fail "put a$i"
    ESPACE=$((ESPACE - 1000))
    generer t$i first $ESPACE			|| fail "put t$i"
done
put $DB a6 repere-6				|| fail "put a8"

for i in `seq 1 5`
do
    $V del $DB t$i
done

# À cet endroit, on a
#	<a1,repere1>
#	(trou de ~1000 octets)
#	<a2,repere-2>
#	(trou de ~2000)
#	<a3,repere-3>
#	(trou de ~3000)
#	<a4,repere-4>
#	(trou de ~2000)
#	<a5,repere-5>
#	(trou de ~1000)
#	<a6,repere-6>
# On vérifie quand même...
#

tester_ordre a1 a2 a3 a4 a5 a6			|| fail "ordre initial"

##############################################################################
# first fit

generer x1 first 500				|| fail "generer x1"
generer x2 first 1100				|| fail "generer x2"
generer x3 first 500		
tester_ordre a1 x1 a2 x2 x3 a3 a4 a5 a6		|| fail "ordre first"

for i in $(seq 1 3)
do
    $V del $DB x$i				|| fail "del x$i"
done
tester_ordre a1 a2 a3 a4 a5 a6			|| fail "ordre sans first"

##############################################################################
# worst fit

generer x4 worst 500				|| fail "generer x4"
generer x5 worst 2500				|| fail "generer x5"
generer x6 worst 500				|| fail "generer x6"
tester_ordre a1 a2 a3 x4 x6 a4 a5 a6 x5		|| fail "ordre worst"

for i in $(seq 4 6)
do
    $V del $DB x$i				|| fail "del x$i"
done
tester_ordre a1 a2 a3 a4 a5 a6			|| fail "ordre sans worst"

##############################################################################
# best fit

generer x7 best 1500				|| fail "generer x7"
generer x8 best 1500				|| fail "generer x8"
generer x9 best 500				|| fail "generer x9"
generer x10 best 500				|| fail "generer x10"
tester_ordre a1 x9 a2 x7 a3 a4 x8 a5 x10 a6	|| fail "ordre best"

for i in $(seq 7 10)
do
    $V del $DB x$i				|| fail "del x$i"
done
tester_ordre a1 a2 a3 a4 a5 a6			|| fail "ordre sans best"

# supprimer le fichier temporaire en cas de sortie normale
rm -f $DB.*

exit 0
