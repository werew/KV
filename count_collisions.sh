#/bin/sh


TMP=/tmp/$( basename $0 ).$$
CNT=10000

main (){
	
	printf "\nCollisions for $CNT hash generated:\n\n\r"
	
	printf "HASH FUN 1: %5d\n" $(count_collisions $CNT 1)
	printf "HASH FUN 2: %5d\n" $(count_collisions $CNT 2)
	printf "HASH FUN 3: %5d\n" $(count_collisions $CNT 3)
	
	rm -f $TMP

}



count_collisions (){

	N_HASH=$1
	HASH_FUN=$2

	./hash_gen -s $N_HASH -i $HASH_FUN | sort -n > $TMP

	N_REPETED_LINES=$( uniq -d < $TMP | wc -l )
	N_REPETITIONS=$( uniq -D < $TMP | wc -l )
	N_COLLISIONS=$(( $N_REPETITIONS - $N_REPETED_LINES ))

	echo $N_COLLISIONS
}


main "$@"

