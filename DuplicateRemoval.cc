#include "RelOp.h"
#include "Pipe.h"
#include "BigQ.h"
#include "Record.h"
#include "Schema.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DuplicateRemoval.h"

DuplicateRemoval::DuplicateRemoval (){

}

DuplicateRemoval::~DuplicateRemoval (){

}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {

  _inPipe = &inPipe;
  _outPipe = &outPipe;
  _mySchema = &mySchema;

  pthread_create(&thread, NULL, RunWorker, (void*) this);

}

void* DuplicateRemoval::RunWorker (void *op) {

  DuplicateRemoval *distinct = static_cast<DuplicateRemoval*>(op);

  distinct->DoWork();

  pthread_exit(NULL);

}

void DuplicateRemoval::DoWork () {

  Record *readIn = new Record;
  Record *toPipe = new Record;
  Record *prev = NULL;

  int pipeSize = 100;

  Pipe sortedPipe(pipeSize);
  OrderMaker order(_mySchema);

  BigQ sort(*_inPipe, sortedPipe, order, pageSize);

  ComparisonEngine cEngine;

    // Pull records from the pipe and check them against the CNF
  while (sortedPipe.Remove(readIn)){

    if (prev == NULL){

	prev = new Record;
	  // Copy the current record into prev
	prev->Copy(readIn);

	  // Now send the read-in record to the pipe
	toPipe->Consume(readIn);
	_outPipe->Insert(toPipe);

    }
    else {

      if (cEngine.Compare(readIn, prev, &order) == 0){
	// do nothing
      }

      else {

	  // Copy the current record into prev
	prev->Copy(readIn);

	  // Now send the read-in record to the pipe
	toPipe->Consume(readIn);
	_outPipe->Insert(toPipe);

      } // end if cEngine

    } // end if..else

  } // end while outPipe

    // Clean up temp variables 
  delete prev;
  delete readIn;
  delete toPipe;

    // Signal that pipe no longer is taking records
  _outPipe->ShutDown();

}

void DuplicateRemoval::WaitUntilDone () {
  pthread_join (thread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int runlen) {
  pageSize = runlen;
}
