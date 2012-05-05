#ifndef DUPLICATE_REMOVAL_REL_OP_H
#define DUPLICATE_REMOVAL_REL_OP_H

#include "RelOp.h"
#include "Pipe.h"
#include "Record.h"
#include "Schema.h"
#include "Comparison.h"

class DuplicateRemoval : public RelationalOp {

public:

	DuplicateRemoval();
	~DuplicateRemoval();

	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

private:

	static void* RunWorker(void *op);
	void DoWork ();

	Pipe *_inPipe;
	Pipe *_outPipe;
	Schema *_mySchema;

};

#endif
