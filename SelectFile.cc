 #include "RelOp.h"
#include "Pipe.h"
#include "Record.h"
#include "DBFile.h"
#include "SelectFile.h"
#include "Comparison.h"

SelectFile::SelectFile (){


}

SelectFile::~SelectFile(){

}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {

  _inFile = &inFile;
  _outPipe = &outPipe;
  _selOp = &selOp;
  _literal = &literal;

  pthread_create(&thread, NULL, RunWorker, (void*) this);

}

void* SelectFile::RunWorker (void *op){

  SelectFile *select = static_cast<SelectFile*>(op);

  select->DoWork();

  pthread_exit(NULL);

}

void SelectFile::DoWork () {

  Record *readIn = new Record;
  Record *toPipe = new Record;

    // Put records into pipe so long as records are accepted

  if(_selOp == 0){
  	while(_inFile->GetNext(*readIn)){
	    toPipe->Consume(readIn);
 	   _outPipe->Insert(toPipe);
	} // end while outPipe

  }
  else{
	  while(_inFile->GetNext(*readIn, *_selOp, *_literal)){
	    toPipe->Consume(readIn);
	    _outPipe->Insert(toPipe);
	  } // end while outPipe
  }
    // Clean up temp variables 
  delete readIn;
  delete toPipe;

    // Signal that pipe no longer is taking records
  _outPipe->ShutDown();

}

void SelectFile::WaitUntilDone () {
  pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
  // This does nothing, no need to keep track of pages for
  // this operator.
}
