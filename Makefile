
CC = g++ -O2 -Wno-deprecated -Wall -Wextra -pedantic

tag = -i

ifdef linux
tag = -n
endif

a1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o InternalDB.o HeapDB.o SortedDB.o StopWatch.o y.tab.o lex.yy.o a1.o
	$(CC) -o a1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o InternalDB.o HeapDB.o SortedDB.o StopWatch.o y.tab.o lex.yy.o a1.o -lfl -lpthread -lrt

a21.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o InternalDB.o HeapDB.o SortedDB.o Pipe.o y.tab.o lex.yy.o a21.o
	$(CC) -o a21.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o InternalDB.o HeapDB.o SortedDB.o Pipe.o y.tab.o lex.yy.o a21.o -lfl -lpthread -lrt

a22.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o InternalDB.o HeapDB.o SortedDB.o y.tab.o lex.yy.o a22.o
	$(CC) -o a22.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o InternalDB.o HeapDB.o SortedDB.o Pipe.o y.tab.o lex.yy.o a22.o -lfl -lpthread -lrt

a3test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o InternalDB.o HeapDB.o SortedDB.o RelOp.o SelectFile.o SelectPipe.o WriteOut.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a3test.o
	$(CC) -o a3test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o InternalDB.o HeapDB.o SortedDB.o RelOp.o SelectFile.o SelectPipe.o WriteOut.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a3test.o -lfl -lpthread -lrt

a3.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o InternalDB.o HeapDB.o SortedDB.o RelOp.o SelectFile.o SelectPipe.o WriteOut.o Project.o DuplicateRemoval.o Sum.o GroupBy.o Join.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a3.o
	$(CC) -o a3.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o InternalDB.o HeapDB.o SortedDB.o RelOp.o SelectFile.o SelectPipe.o WriteOut.o Project.o DuplicateRemoval.o Sum.o GroupBy.o Join.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a3.o -lfl -lpthread -lrt

a41.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o InternalDB.o HeapDB.o SortedDB.o Pipe.o Statistics.o RelationStatistics.o y.tab.o lex.yy.o a41.o
	$(CC) -o a41.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o InternalDB.o HeapDB.o SortedDB.o Pipe.o Statistics.o RelationStatistics.o y.tab.o lex.yy.o a41.o -lfl -lpthread -lrt

a41test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o InternalDB.o HeapDB.o SortedDB.o Pipe.o Statistics.o RelationStatistics.o y.tab.o lex.yy.o a41test.o
	$(CC) -o a41test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o InternalDB.o HeapDB.o SortedDB.o Pipe.o Statistics.o RelationStatistics.o y.tab.o lex.yy.o a41test.o -lfl -lpthread -lrt

main.out:   y.tab.o lex.yy.o main.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o InternalDB.o HeapDB.o SortedDB.o RelOp.o SelectFile.o SelectPipe.o WriteOut.o Project.o DuplicateRemoval.o Sum.o GroupBy.o Join.o Function.o Statistics.o RelationStatistics.o SelectQuery.o QueryTree.o QueryTreeNode.o
	$(CC) -o main.out y.tab.o lex.yy.o main.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o InternalDB.o HeapDB.o SortedDB.o RelOp.o SelectFile.o SelectPipe.o WriteOut.o Project.o DuplicateRemoval.o Sum.o GroupBy.o Join.o Function.o Statistics.o RelationStatistics.o SelectQuery.o QueryTree.o QueryTreeNode.o -lfl -lpthread -lrt

a1.o: a1.cc a1.h
	$(CC) -g -c a1.cc

a21.o: a21.cc a21.h
	$(CC) -g -c a21.cc

a22.o: a22.cc a22.h
	$(CC) -g -c a22.cc

a3test.o: a3test.cc a3test.h
	$(CC) -g -c a3test.cc

a3.o: a3.cc a3.h
	$(CC) -g -c a3.cc

a41.o: a41.cc
	$(CC) -g -c a41.cc

a41test.o: a41test.cc 
	$(CC) -g -c a41test.cc

Comparison.o: Comparison.cc Comparison.h
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc ComparisonEngine.h
	$(CC) -g -c ComparisonEngine.cc

Pipe.o: Pipe.cc Pipe.h
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc BigQ.h
	$(CC) -g -c BigQ.cc

DBFile.o: DBFile.cc DBFile.h
	$(CC) -g -c DBFile.cc

InternalDB.o: InternalDB.cc InternalDB.h
	$(CC) -g -c InternalDB.cc

HeapDB.o: HeapDB.cc HeapDB.h
	$(CC) -g -c HeapDB.cc

SortedDB.o: SortedDB.cc SortedDB.h
	$(CC) -g -c SortedDB.cc

StopWatch.o: StopWatch.cc StopWatch.h
	$(CC) -g -c StopWatch.cc

File.o: File.cc File.h
	$(CC) -g -c File.cc

Record.o: Record.cc Record.h
	$(CC) -g -c Record.cc

RelOp.o: RelOp.cc RelOp.h
	$(CC) -g -c RelOp.cc

SelectFile.o: SelectFile.cc SelectFile.h
	$(CC) -g -c SelectFile.cc

SelectPipe.o: SelectPipe.cc SelectPipe.h
	$(CC) -g -c SelectPipe.cc

WriteOut.o: WriteOut.cc WriteOut.h
	$(CC) -g -c WriteOut.cc

Project.o: Project.cc Project.h
	$(CC) -g -c Project.cc

DuplicateRemoval.o: DuplicateRemoval.cc DuplicateRemoval.h
	$(CC) -g -c DuplicateRemoval.cc

Sum.o: Sum.cc Sum.h
	$(CC) -g -c Sum.cc

GroupBy.o: GroupBy.cc GroupBy.h
	$(CC) -g -c GroupBy.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

Statistics.o: Statistics.cc Statistics.h
	$(CC) -g -c Statistics.cc

RelationStatistics.o: RelationStatistics.cc RelationStatistics.h
	$(CC) -g -c RelationStatistics.cc

SelectQuery.o: SelectQuery.cc
	$(CC) -g -c SelectQuery.cc

QueryTree.o: QueryTree.cc QueryTree.h
	$(CC) -g -c QueryTree.cc

QueryTreeNode.o: QueryTreeNode.cc QueryTreeNode.h
	$(CC) -g -c QueryTreeNode.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -g -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -g -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc -g -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc -g -c lex.yyfunc.c

main.o : main.cc
	$(CC) -g -c main.cc

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
