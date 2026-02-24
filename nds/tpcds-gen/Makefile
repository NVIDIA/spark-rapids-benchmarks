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

all: target/lib/dsdgen.jar target/tpcds-gen-1.0-SNAPSHOT.jar

target/tpcds-gen-1.0-SNAPSHOT.jar: $(shell find -name *.java)
ifdef MVN_REPO_URL
	mvn package -s ../cicd/settings.xml
else
	mvn package
endif

target/lib/dsdgen.jar: target/tools/dsdgen
	cd target/; mkdir -p lib/; ( jar cvf lib/dsdgen.jar tools/ || gjar cvf lib/dsdgen.jar tools/ )

target/tools/dsdgen: check-env
	mkdir -p target/
	cp patches/*.patch $(TPCDS_HOME)/
	# unapply previously applied patches if any, ignore errors
	-cd $(TPCDS_HOME); cat *.patch | patch -R -p1 -N
	# apply patches to both source code and templates
	cd $(TPCDS_HOME) && cat *.patch | patch -p1
	test -d target/tools/ || (cd target; cp -r $(TPCDS_HOME)/tools tools)
	cd target/tools; make clean; make

check-env:
ifndef TPCDS_HOME
	$(error "TPCDS_HOME not defined, please set TPCDS_HOME environment variable to your TPCDS Tool directory")
endif

clean:
	mvn clean
