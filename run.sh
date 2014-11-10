make clean
bash bundle.sh

if [ "$#" -eq 0 ]
then
	echo "Running Master"
	java -cp src/rmi.jar RMIServer.ServerWrapper
elif [ "$#" -eq 3 ]
then
	echo "Running Client"
	java -cp src/rmi.jar $1 $2 $3
else
	echo "Invalid number of arguments supplied- read the documentation for detail"
fi
	