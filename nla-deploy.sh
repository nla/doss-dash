mvn package
mkdir -p $1/lib
cp target/*-jar-with-dependencies.jar $1/lib
