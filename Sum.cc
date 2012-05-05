#include "RelOp.h"
#include "Sum.h"
#include "Pipe.h"
#include "Record.h"
#include "Function.h"
#include "Comparison.h"

#include <iostream>
#include <sstream>

Sum::Sum (){

}

Sum::~Sum (){

}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){

  _inPipe = &inPipe;
  _outPipe = &outPipe;
  _computeMe = &computeMe;

  pthread_create(&thread, NULL, RunWorker, (void*) this);

}

void* Sum::RunWorker (void *op){

  Sum *sum = static_cast<Sum*>(op);

  sum->DoWork();

  pthread_exit(NULL);

}

void Sum::DoWork (){

  Type type;
  Record *sum = new Record;
  Record *readIn = new Record;

    // Store the overall computation results
  int intResult = 0;
  double doubleResult = 0.0;

    // Store the computation results for the current record
  int intIncrement = 0;
  double doubleIncrement = 0.0;


    // Compute the sum from the records in the pipe
  while (_inPipe->Remove(readIn)){

      // Compute the local value
    type = _computeMe->Apply(*readIn, intIncrement, doubleIncrement);

      // Increment the counts
    intResult += intIncrement;
    doubleResult += doubleIncrement;

  } // end while _inPipe->Remove()

    // Build the schema that will be used to construct the record

    // Schema will only have one attribute
  Attribute *atts = new Attribute[1];

    // Conversion from numeric value to string will happen in
    // a stringstream
  std::stringstream s;

    // Convert the appropriate sum into a string
  if (type == Int){
    s << intResult;
  }
  else {
    s << doubleResult;
  }

    // Set the name and type of the Attribute
  atts[0].name = "SUM";
  atts[0].myType = type;

    // Now create the schema for the record
  Schema sumSchema("boogityboogity", 1, atts);

    // Build the record from the schema

    // Dobra's code expects a "|" to deliminate where an
    // attribute ends. Adding this to the sum should properly
    // utilize his code
  s << "|";

    // Create the result record based on the schema
  sum->ComposeRecord(&sumSchema, s.str().c_str());

    // Add record to the output pipe
  _outPipe->Insert(sum);
  _outPipe->ShutDown();

  delete sum;
  delete readIn;

  delete[] atts;

}

void Sum::WaitUntilDone () {
  pthread_join (thread, NULL);
}

void Sum::Use_n_Pages (int runlen) {
  pageSize = runlen;
}
