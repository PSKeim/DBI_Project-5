#ifndef JOIN_REL_OP_H
#define JOIN_REL_OP_H

#include "RelOp.h"
#include "Pipe.h"
#include "Record.h"
#include "Comparison.h"
#include "Schema.h"

#include <vector>

using std::vector;

class Join : public RelationalOp { 

public:

	Join();
	~Join();

	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe,
		  CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void SetSchemas(Schema *l, Schema *r);
	Schema *lSchema;
	Schema *rSchema;

private:

	static void* RunWorker (void *op);
	void DoWork ();
	void ExecuteBlockNestJoin();
	void ExecuteSortMergeJoin(OrderMaker *left, OrderMaker *right);
	void CreateAndInsertRecord(Record *left, Record *right);

	bool AdvanceJoinSet(vector<Record*> &buffer, Pipe *pipe,
			    Record &temp, OrderMaker *order, int side);

	bool AdvanceKeyPointer(Pipe *pipe, Record *temp, OrderMaker *order);

	Pipe *_inPipeL;
	Pipe *_inPipeR;
	Pipe *_outPipe;
	CNF *_selOp;
	Record *_literal;
	int numOutput;

};

#endif
