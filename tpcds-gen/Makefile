all: target/lib/dsdgen.jar target/tpcds-gen-1.0-SNAPSHOT.jar

target/tpcds-gen-1.0-SNAPSHOT.jar: $(shell find -name *.java) 
	mvn package

target/lib/dsdgen.jar: target/tools/dsdgen
	cd target/; mkdir -p lib/; ( jar cvf lib/dsdgen.jar tools/ || gjar cvf lib/dsdgen.jar tools/ )

target/tools/dsdgen: check-env
	mkdir -p target/
	cp patches/*.patch $(TPCDS_HOME)/
	# apply patches to both source code and templates
	cd $(TPCDS_HOME); cat *.patch | patch -p1
	test -d target/tools/ || (cd target; cp -r $(TPCDS_HOME)/tools tools)
	cd target/tools; make clean; make

check-env:
ifndef TPCDS_HOME
	$(error "TPCDS_HOME not defined, please set TPCDS_HOME environment variable to your TPCDS Tool directory")
endif

clean:
	mvn clean
