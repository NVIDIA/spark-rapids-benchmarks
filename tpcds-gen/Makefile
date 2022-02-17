
all: target/lib/dsdgen.jar target/tpcds-gen-1.0-SNAPSHOT.jar

target/tpcds-gen-1.0-SNAPSHOT.jar: $(shell find -name *.java) 
	mvn package

target/tpc-ds-tool.zip:
	mkdir -p target/
	cp ../tpc-ds-tool/tpc-ds-tool.zip target/tpc-ds-tool.zip

target/lib/dsdgen.jar: target/tools/dsdgen
	cd target/; mkdir -p lib/; ( jar cvf lib/dsdgen.jar tools/ || gjar cvf lib/dsdgen.jar tools/ )

target/tools/dsdgen: target/tpc-ds-tool.zip
	test -d target/tools/ || (cd target; unzip tpc-ds-tool.zip)
	test -d target/tools/ || (cd target; mv */tools tools)
	cd target/tools; make clean; make dsdgen

clean:
	mvn clean
