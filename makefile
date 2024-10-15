TOOLCHAIN ?= nightly

all: build

build:
	cargo +$(TOOLCHAIN) build --all-features

test:
	cargo +$(TOOLCHAIN) test --all-features

doc: RUSTDOCFLAGS="-D warnings --cfg docsrs"
doc:
	cargo +$(TOOLCHAIN) doc -Z unstable-options -Z rustdoc-scrape-examples --all-features # --no-deps

clippy:
	cargo +$(TOOLCHAIN) clippy --all-features -- -D warnings

check: build test clippy doc
	cargo +$(TOOLCHAIN) fmt --all -- --check
	cargo +$(TOOLCHAIN) check
	cargo +$(TOOLCHAIN) check --no-default-features

check_extra:
	cargo +$(TOOLCHAIN) clippy --all-features -- -D warnings -W clippy::nursery -A clippy::significant_drop_tightening -A clippy::significant_drop_in_scrutinee

fmt:
	cargo +$(TOOLCHAIN) fmt

clean:
	cargo clean

.PHONY: all build test doc clippy check fmt clean
