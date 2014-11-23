make clean
bash bundle.sh

if [ "$#" -eq 1 ]
then
	java -cp src/mapReduce.jar Main/Main $1
elif [ "$#" -eq 4 ]
then
	java -cp src/mapReduce.jar Main/Main $1 $2 $3 $4
else
	echo "Invalid number of arguments supplied- read the documentation for detail"
fi
	