
#
# Une série de petits tests basiques
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

# insérer une clef
$V put $DB ma-clef abc				|| fail "put abc 1"
test $(get -q $DB ma-clef) = abc 		|| fail "get abc 1"

# changer la valeur de la clef
$V put $DB ma-clef une-autre			|| fail "put abc 2"
test $(get -q $DB ma-clef) = une-autre		|| fail "get abc 2"

# mettre le contenu d'un fichier
$V put $DB ma-clef < /usr/include/memory.h	|| fail "put < memory.h"

# mettre un contenu passé par un tube
cat /usr/include/memory.h | $V put $DB ma-clef	|| fail "put cat memory.h"

# test des options
$V put -h					|| fail "put -h"

# arguments invalides
put						&& fail "put"
put $DB						&& fail "put db"
put $DB clef val autre-chose			&& fail "put autre-chose"


# options d'allocation
for ALLOC in first best worst
do
    rm -f $DB.*
    $V put -a $ALLOC $DB ma-clef toto		|| fail "put -a $ALLOC"
done

# une option farfelue
rm -f $DB.*
put -a farfelue $DB ma-clef toto		&& fail "put -a farfelue"

# supprimer le fichier temporaire en cas de sortie normale
rm -f $DB.*
