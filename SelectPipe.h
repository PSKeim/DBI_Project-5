#ifndef SELECT_PIPE_REL_OP_H
#define SELECT_PIPE_REL_OP_H

#include "RelOp.h"
#include "Pipe.h"
#include "Record.h"
#include "Comparison.h"

class SelectPipe : public RelationalOp {

public:

	SelectPipe();
	~SelectPipe();

	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

private:

	static void* RunWorker (void *op);
	void DoWork ();

	Pipe *_inPipe;
	Pipe *_outPipe;
	CNF *_selOp;
	Record *_literal;

};

#endif
