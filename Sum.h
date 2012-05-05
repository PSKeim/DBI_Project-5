#ifndef SUM_REL_OP_H
#define SUM_REL_OP_H

#include "RelOp.h"
#include "Pipe.h"
#include "Record.h"
#include "Function.h"
#include "Comparison.h"

class Sum : public RelationalOp { 

public:

	Sum();
	~Sum();

	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

private:

	static void* RunWorker (void *op);
	void DoWork ();

	Pipe *_inPipe;
	Pipe *_outPipe;
	Function *_computeMe;

};

#endif
