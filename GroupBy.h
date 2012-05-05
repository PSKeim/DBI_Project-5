#ifndef GROUPBY_REL_OP_H
#define GROUPBY_REL_OP_H

#include "RelOp.h"
#include "Pipe.h"
#include "Defs.h"
#include "Record.h"
#include "Function.h"
#include "Comparison.h"

class GroupBy : public RelationalOp {

public:

	GroupBy();
	~GroupBy();

	void Run (Pipe &inPipe, Pipe &outPipe,
		  OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

private:

	static void* RunWorker (void *op);
	void DoWork ();
	Type IncrementSum (Record *rec, int &isum, double &dsum);
	void CreateAndInsertRecord (Record *rec, Type type, int isum,
				    double dsum);

	Pipe *_inPipe;
	Pipe *_outPipe;
	OrderMaker *_groupAtts;
	Function *_computeMe;

};

#endif
