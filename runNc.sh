if [ "$#" -eq 2 ]
then
	java -cp src/mapReduce.jar Main/Main $1 $2
elif [ "$#" -eq 4 ]
then
	java -cp src/mapReduce.jar Main/Main $1 $2 $3 $4
elif [ "$#" -eq 1 ]
then
	java -cp src/mapReduce.jar $1
else
	echo "Invalid number of arguments supplied- read the documentation for detail"
fi