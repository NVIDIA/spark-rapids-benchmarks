#
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

all: check-tpch-env prepare-target copy-dbgen modify-makefile modify-tpc-h build-dbgen make-jar build-package

check-tpch-env:
ifndef TPCH_HOME
	$(error "TPCH_HOME not defined, please set TPCH_HOME environment variable to your TPCH Tool directory")
endif

prepare-target:
	rm -Rf target
	mkdir -p target/

copy-dbgen:
	#Copying all patches to the current query folder
	cp patches/template.patch "$(TPCH_HOME)/dbgen/queries"
	# This is required to ensure similar line ending semantics bw patch
	# file and the sql files
	cd "$(TPCH_HOME)/dbgen/queries"; dos2unix *.sql
	cd "$(TPCH_HOME)/dbgen/queries"; dos2unix *.patch
	# apply patches to both source code and templates
	cd "$(TPCH_HOME)/dbgen/queries" && cat *.patch | patch -p1
	cp -r "$(TPCH_HOME)/dbgen" target/

modify-makefile:
	# Create makefile from the template suit
	cp target/dbgen/makefile.suite target/dbgen/Makefile
	sed -i '103s/$$/ gcc/' target/dbgen/Makefile
	sed -i '109s/$$/ SPARK/' target/dbgen/Makefile
	sed -i '110s/$$/ LINUX/' target/dbgen/Makefile
	sed -i '111s/$$/ TPCH/' target/dbgen/Makefile
	sed -i '172i fprintf(ofp, "\\n-- Template file: %s\\n", qtag);' target/dbgen/qgen.c

modify-tpc-h:
	# Enter information for the SPARK replacement variables
	sed -i '115a\
		#ifdef SPARK\
		#define GEN_QUERY_PLAN  ""\
		#define START_TRAN      ""\
		#define END_TRAN        ""\
		#define SET_OUTPUT      ""\
		#define SET_ROWCOUNT    "LIMIT %d"\
		#define SET_DBASE       ""\
		#endif' target/dbgen/tpcd.h

build-dbgen:
	# Build it
	cd target/dbgen ; make clean; make 2>/dev/null

make-jar:
	cd target;  (jar cvf dbgen.jar dbgen/ || gjar cvf dbgen.jar dbgen/ )

build-package:
	mvn package
	