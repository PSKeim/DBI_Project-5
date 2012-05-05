#include "RelOp.h"
#include "Pipe.h"
#include "Record.h"
#include "SelectPipe.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

SelectPipe::SelectPipe (){


}

SelectPipe::~SelectPipe(){

}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {

  _inPipe = &inPipe;
  _outPipe = &outPipe;
  _selOp = &selOp;
  _literal = &literal;

  pthread_create(&thread, NULL, RunWorker, (void *) this);

}

void* SelectPipe::RunWorker (void *op) {

  SelectPipe *select = static_cast<SelectPipe*>(op);

  select->DoWork();

  pthread_exit(NULL);

}

void SelectPipe::DoWork () {

  Record *readIn = new Record;
  Record *toPipe = new Record;

  ComparisonEngine cEngine;

    // Pull records from the pipe and check them against the CNF
  while(_inPipe->Remove(readIn)){

    if (cEngine.Compare(readIn, _literal, _selOp)){
      toPipe->Consume(readIn);
      _outPipe->Insert(toPipe);
    }

  } // end while outPipe

    // Clean up temp variables 
  delete readIn;
  delete toPipe;

    // Signal that pipe no longer is taking records
  _outPipe->ShutDown();

}

void SelectPipe::WaitUntilDone () {
  pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
  // This does nothing, no need to keep track of pages for
  // this operator.
}
