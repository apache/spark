#
# Copyright 2005 The Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
ifneq ($$(XBUILT_SOURCES),)
    XBUILT_SOURCES := $$(XBUILT_SOURCES) $$(XTARGET)
else
    XBUILT_SOURCES := $$(XTARGET)
endif

showvars:
	@echo BUILD_SOURCES = $(BUILT_SOURCES)
	@echo XBUILTSOURCES = $(XBUILT_SOURCES)
	@echo DEFS = $(DEFS)
	@echo CXXFLAGS = $(CXXFLAGS)
	@echo AM_CXXFLAGS = $(AM_CXXFLAGS)
	@echo CPPFLAGS = $(CPPFLAGS)
	@echo AM_CPPFLAGS = $(AM_CPPFLAGS)
	@echo LDFLAGS = $(LDFLAGS)
	@echo AM_LDFLAGS = $(AM_LDFLAGS)
	@echo LDADD = $(LDADD)
	@echo LIBS = $(LIBS)
	@echo EXTERNAL_LIBS = $(EXTERNAL_LIBS)
	@echo EXTERNAL_PATH = $(EXTERNAL_PATH)
	@echo MAKE = $(MAKE)
	@echo MAKE_FLAGS = $(MAKE_FLAGS)
	@echo AM_MAKEFLAGS = $(AM_MAKEFLAGS)
	@echo top_builddir = $(top_builddir)
	@echo top_srcdir = $(top_srcdir)
	@echo srcdir = $(srcdir)
	@echo PHPVAL = $(PHPVAL)
	@echo PHPCONFIGDIR  = $(PHPCONFIGDIR)
	@echo PHPCONFIGINCLUDEDIR = $(PHPCONFIGINCLUDEDIR)
	@echo PHPCONFIGINCLUDES  = $(PHPCONFIGINCLUDES)
	@echo PHPCONFIGLDFLAGS  = $(PHPCONFIGLDFLAGS)
	@echo PHPCONFIGLIBS  = $(PHPCONFIGLIBS)

clean-common:
	rm -rf gen-*
