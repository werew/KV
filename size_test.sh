#!/bin/sh

FILE_TMP="/tmp/test-infos.$$"
BAR_LENGTH=30



if [ $# -lt 1 ]
then
	echo "usage: $0 <test size>" >&2
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
+------------------------------------------------------------------+
|                   	  SIZE TEST RESULTS                        |
+------------------+-----------+-----------+-----------+-----------+
| TEST INFOS       |     H     |    BLK    |     KV    |    DKV    |
+------------------+-----------+-----------+-----------+-----------+
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
	test_size "./test_kv -s 50000 -a first $DB" "first"
	test_size "./test_kv -s 50000 -a best $DB" "best"
	test_size "./test_kv -s 50000 -a worst $DB" "worst"
	
	cat "$FILE_TMP"	
}


test_size(){
	PROG=$1
	INFOS=$2
	CLEAN="rm ${DB}.h ${DB}.blk ${DB}.kv ${DB}.dkv"
	SIZE_BLK=0
	SIZE_H=0
	SIZE_KV=0
	SIZE_DKV=0

	printf "Executing: \"$PROG\" - $INFOS \r\n\n"
	
	progress_bar 0 "Status: 0 of $TEST_SIZE";
	$PROG > /dev/null # drop first execution
	$CLEAN

	for COUNT in $(seq 1 $TEST_SIZE)
	do
		$PROG > /dev/null
	
		SIZE_H=$(( $SIZE_H + $( ls -l ${DB}.h | cut -d ' ' -f 5) ))
		SIZE_BLK=$(( $SIZE_BLK + $( ls -l ${DB}.blk | cut -d ' ' -f 5) ))
		SIZE_KV=$(( $SIZE_KV + $( ls -l ${DB}.kv | cut -d ' ' -f 5) ))
		SIZE_DKV=$(( $SIZE_DKV + $( ls -l ${DB}.dkv | cut -d ' ' -f 5) ))
		progress_bar $COUNT "Status: $COUNT of $TEST_SIZE"
		$CLEAN
	done
	printf "\n"

	# Update table
	printf "| %-16s | %9d | %9d | %9d | %9d |\n" "$INFOS" \
	$(( $SIZE_BLK / $TEST_SIZE )) \
	$(( $SIZE_H / $TEST_SIZE )) \
	$(( $SIZE_KV / $TEST_SIZE )) \
	$(( $SIZE_DKV / $TEST_SIZE ))  >> $FILE_TMP
	echo   "+------------------+-----------+-----------+-----------+-----------+"  >> $FILE_TMP
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
