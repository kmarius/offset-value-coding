# tested with:
# gcc 13.2.1
# cmake 3.28.3
# python 3.11.8
# python-seaborn 0.12.2

.phony: all experiments plots clean

all: plots

clean:
	rm -rf build build_stats img sort_order1.csv sort_order1_stats.csv sort_order2.csv sort_order2_stats.csv

# plots

plots: img/sort_order1.png img/sort_order2.png img/sort_order1_cc.png

img/sort_order1.png: sort_order1.py sort_order1.csv
	./sort_order1.py sort_order1.csv

img/sort_order1_cc.png: sort_order1.py sort_order1_stats.csv
	./sort_order1.py sort_order1_stats.csv

img/sort_order2.png: sort_order2.py sort_order2.csv
	./sort_order2.py sort_order2.csv

## Execution

experiments: sort_order1.csv sort_order1_stats.csv sort_order2.csv

# Note: Files created during execution are part of data generation, not the experiment
sort_order1.csv sort_order2.csv: build/paper2
	build/paper2

sort_order1_stats.csv: build_stats/paper2
	build_stats/paper2

## Build

build/paper2:
	mkdir -p build
	cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && make paper2 -j

build_stats/paper2:
	mkdir -p build_stats
	cd build_stats && cmake .. -DCMAKE_BUILD_TYPE=Release -DCOLLECT_STATS=1 && make paper2 -j
