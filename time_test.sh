#!/bin/sh

CMD_TIME="/usr/bin/time"
FILE_TMP="/tmp/test-infos.$$"
BAR_LENGTH=30



if [ $# -lt 1 ]
then
	echo "usage: $0 <test size>" >&2
	exit 1

elif ! [ -x "$CMD_TIME" ]
then
	echo "Error: cannot locate \"$CMD_TIME\"" >&2
	exit 1
fi

TEST_SIZE=$1

main(){
	trap "rm -f $FILE_TMP" EXIT		
	cat << EOF

|******************************************************|
		     START TEST

EOF
	cat << EOF > $FILE_TMP
+------------------------------------------------------+
|                  TIME TEST RESULTS                   |
+------------------+-----------+-----------+-----------+
| TEST INFOS       |   REAL    |   USER    |   SYSTEM  |
+------------------+-----------+-----------+-----------+
EOF

#####################################################################
#			      TEST_LIST
#	HOWTOUSEIT:
#	Add a line for each speed test you would like to perform
#	The synthax of the line should look more or less like this:
#
#	test_performance <program> [extra infos] [end-of-test action]
#	
#####################################################################
	DB="DB.$$"
	CLEAN="rm ${DB}.h ${DB}.blk ${DB}.kv ${DB}.dkv"

	test_performance "./test_kv -s 50000 -a first $DB" "first" "$CLEAN"
	test_performance "./test_kv -s 50000 -a best $DB" "best" "$CLEAN"
	test_performance "./test_kv -s 50000 -a worst $DB" "worst" "$CLEAN"
	
	test_performance "./hash_gen -s 1000000 -i 1" "hash 1"
	test_performance "./hash_gen -s 1000000 -i 2" "hash 2"
	test_performance "./hash_gen -s 1000000 -i 3" "hash 3"

	cat "$FILE_TMP"	
}


test_performance(){
	PROG=$1
	INFOS=$2
	END_TEST=$3
	SUM_REAL=0
	SUM_USER=0
	SUM_SYST=0

	printf "Executing: \"$PROG\" - $INFOS \r\n\n"
	
	progress_bar 0 "Status: 0 of $TEST_SIZE";
	$PROG > /dev/null # drop first execution
	$END_TEST

	for COUNT in $(seq 1 $TEST_SIZE)
	do
		RESULT=$( ($CMD_TIME -f "%e %U %S" $PROG > /dev/null) 2>&1 )
		SUM_REAL=$( echo "$SUM_REAL + $(echo "$RESULT" | cut -d ' ' -f 1)" | bc )
		SUM_USER=$( echo "$SUM_USER + $(echo "$RESULT" | cut -d ' ' -f 2)" | bc )
		SUM_SYST=$( echo "$SUM_SYST + $(echo "$RESULT" | cut -d ' ' -f 3)" | bc )
		progress_bar $COUNT "Status: $COUNT of $TEST_SIZE"
		$END_TEST
	done
	printf "\n"

	# Update table
	printf "| %-16s | %9f | %9f | %9f |\n" "$INFOS" \
	$(echo "$SUM_REAL/$TEST_SIZE" | bc -l ) \
	$(echo "$SUM_USER/$TEST_SIZE" | bc -l ) \
	$(echo "$SUM_SYST/$TEST_SIZE" | bc -l )  >> $FILE_TMP
	echo   "+------------------+-----------+-----------+-----------+"  >> $FILE_TMP
}


progress_bar(){
	COUNT_DONE=$1

	if [ $COUNT_DONE -eq $TEST_SIZE ]
	then
		STATUS="Complete                               "
	else
		STATUS="$2"
	fi

	perl -e "use POSIX;
		 \$PROGRESS = floor( $COUNT_DONE/$TEST_SIZE * $BAR_LENGTH);
		 print \"\e[A\" . '[' . '=' x \$PROGRESS . 
		 ' ' x ($BAR_LENGTH-\$PROGRESS) . ']' . \" $STATUS \r\n\";"
}

main "$@" 
