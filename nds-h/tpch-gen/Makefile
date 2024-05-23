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

all: check-tpch-env prepare-target copy-dbgen modify-makefile build-dbgen

check-tpch-env:
ifndef TPCH_HOME
	$(error "TPCH_HOME not defined, please set TPCH_HOME environment variable to your TPCH Tool directory")
endif

prepare-target:
	rm -R target
	mkdir -p target/

copy-dbgen:
	cd target
	cp -r "$(TPCH_HOME)/dbgen" target/

modify-makefile:
	cp target/dbgen/makefile.suite target/dbgen/Makefile
	sed -i '103s/$$/ gcc/' target/dbgen/Makefile
	sed -i '109s/$$/ SQLSERVER/' target/dbgen/Makefile
	sed -i '110s/$$/ LINUX/' target/dbgen/Makefile
	sed -i '111s/$$/ TPCH/' target/dbgen/Makefile

build-dbgen:
	cd target/dbgen ; make clean; make 2>/dev/null