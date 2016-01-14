#ROOT=/usr
#ROOT=${HOME}
ROOT=../zookeeper-git/build/c/build/usr
INCDIR=${ROOT}/include/zookeeper
#LIBDIR=${ROOT}/lib64
LIBDIR=${ROOT}/lib
INC = -I${INCDIR}
LIB = -L${LIBDIR} -lzookeeper_mt
LIB += -lreadline
FLAGS = -fPIC -Wall -Werror -m64

zoosh: zoosh.cpp
	g++ $(FLAGS) $(INC) $(LIB) zoosh.cpp -o zoosh

run: zoosh
	LD_LIBRARY_PATH=${LIBDIR}:${LD_LIBRARY_PATH} ./zoosh

clean:
	rm -f zoosh
