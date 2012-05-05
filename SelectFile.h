#ifndef SELECT_FILE_REL_OP_H
#define SELECT_FILE_REL_OP_H

#include "RelOp.h"
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class SelectFile : public RelationalOp { 

public:

	SelectFile ();
	~SelectFile ();

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

private:

	static void* RunWorker (void *op);
	void DoWork ();

	DBFile *_inFile;
	Pipe *_outPipe;
	CNF *_selOp;
	Record *_literal;

};

#endif
