#!/bin/bash
# Read C++ source from stdin, compile to .so, write to stdout.
# Args: [std] [opt]  — defaults: c++20, -O2
# Output on success: "SUCCESS\n" followed by raw .so bytes
# Output on failure: "ERROR\n" followed by GCC stderr

STD=${1:-c++20}
OPT=${2:--O2}

cat > /tmp/indicator.cpp
g++ -shared -fPIC -std="$STD" "$OPT" -o /tmp/indicator.so /tmp/indicator.cpp 2>/tmp/gcc_stderr.txt

if [ $? -eq 0 ]; then
    printf "SUCCESS\n"
    cat /tmp/indicator.so
else
    printf "ERROR\n"
    cat /tmp/gcc_stderr.txt
fi
