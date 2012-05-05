#include "RelOp.h"
#include "Pipe.h"
#include "Record.h"
#include "Schema.h"
#include "WriteOut.h"

#include <iostream>
#include <sstream>
#include <stdio.h>

WriteOut::WriteOut (){

}

WriteOut::~WriteOut (){

}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {

  _inPipe = &inPipe;
  _outFile = outFile;
  _mySchema = &mySchema;

  pthread_create(&thread, NULL, RunWorker, (void*) this);

}

void* WriteOut::RunWorker (void *op){

  WriteOut *print = static_cast<WriteOut*>(op);

  print->DoWork();

  pthread_exit(NULL);

}

void WriteOut::DoWork (){

  Record *readIn = new Record;

  if (_outFile == NULL){
    // do nothing, file stream isn't open
  }
  else {

    std::stringstream s;

      // Print the record to the file as long as there are records
      // in the pipe AND the file isn't at the end AND there has been
      // no error
    while (_inPipe->Remove(readIn) && !feof(_outFile) && !ferror(_outFile)){

	// Print out the record to the stringstream
      readIn->Print(_mySchema, s);

	// Write record out to the file
      fputs(s.str().c_str(), _outFile);

	// Reset the buffer used by s
      s.str("");

    } // end while inPipe.Remove()

  } // end if..else

  delete readIn;

}

void WriteOut::WaitUntilDone () {
  pthread_join (thread, NULL);
}

void WriteOut::Use_n_Pages (int runlen) {
  // This does nothing, no need to keep track of pages for
  // this operator.
}
