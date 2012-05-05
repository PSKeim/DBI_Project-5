#ifndef WRITEOUT_REL_OP_H
#define WRITEOUT_REL_OP_H

#include "RelOp.h"
#include "Pipe.h"
#include "Record.h"
#include "Comparison.h"

#include <stdio.h>

class WriteOut : public RelationalOp {

public:

	WriteOut();
	~WriteOut();

	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

private:

	static void* RunWorker (void *op);
	void DoWork ();

	Pipe *_inPipe;
	FILE *_outFile;
	Schema *_mySchema;

};

#endif
