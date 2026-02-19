# pg_mqtt_pub Makefile — uses PGXS build infrastructure

MODULE_big = pg_mqtt_pub
OBJS = src/pg_mqtt_pub.o src/pg_mqtt_pub_worker.o

EXTENSION = pg_mqtt_pub
DATA = sql/pg_mqtt_pub--1.0.sql

SHLIB_LINK = -lmosquitto

PG_CPPFLAGS = -I$(shell pkg-config --cflags-only-I libmosquitto 2>/dev/null | sed 's/-I//g')
PG_CFLAGS = -std=c23 -Wall -Wextra -Wno-unused-parameter -Wno-declaration-after-statement -Werror=implicit-function-declaration

REGRESS = pg_mqtt_pub_basic
REGRESS_OPTS = --temp-config=$(srcdir)/test/pg_mqtt_pub_test.conf

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: check-deps

check-deps:
	@echo "Checking dependencies..."
	@pkg-config --exists libmosquitto 2>/dev/null \
		|| (echo "ERROR: libmosquitto-dev not found. Install with:" \
		    && echo "  apt install libmosquitto-dev  (Debian/Ubuntu)" \
		    && echo "  dnf install mosquitto-devel   (RHEL/Fedora)" \
		    && exit 1)
	@echo "  libmosquitto: OK"
	@$(PG_CONFIG) --version >/dev/null 2>&1 \
		|| (echo "ERROR: pg_config not found" && exit 1)
	@echo "  PostgreSQL:   $$($(PG_CONFIG) --version)"
	@echo "All dependencies satisfied."
