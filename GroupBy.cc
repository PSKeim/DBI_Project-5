#include "RelOp.h"
#include "BigQ.h"
#include "GroupBy.h"
#include "Pipe.h"
#include "Defs.h"
#include "Record.h"
#include "Comparison.h"
#include <iostream>
GroupBy::GroupBy (){

}

GroupBy::~GroupBy (){

}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe,
		  OrderMaker &groupAtts, Function &computeMe) {

  _inPipe = &inPipe;
  _outPipe = &outPipe;
  _groupAtts = &groupAtts;
  _computeMe = &computeMe;

  pthread_create(&thread, NULL, RunWorker, (void*) this);

}

void* GroupBy::RunWorker (void *op){

  GroupBy *order = static_cast<GroupBy*>(op);

  order->DoWork();
  order->_outPipe->ShutDown();

  pthread_exit(NULL);

}

void GroupBy::DoWork (){


  Type type;
  Record *prev = NULL;
  Record *readIn = new Record;

    // Store the overall computation results
  int intResult = 0;
  double doubleResult = 0.0;

    // Set up a pipe for the BigQ
  int pipeSize = 100;
  Pipe sortedPipe(pipeSize);

  BigQ sort(*_inPipe, sortedPipe, *_groupAtts, pageSize);

  ComparisonEngine cEngine;

    // Pull records from the pipe and check them against the CNF
  while (sortedPipe.Remove(readIn)){

    if (prev == NULL){

	prev = new Record;

	  // Copy the current record into prev
	prev->Copy(readIn);

	type = IncrementSum(readIn, intResult, doubleResult);

    }
    else {

	// Just increment sum
      if (cEngine.Compare(readIn, prev, _groupAtts) == 0){
	type = IncrementSum(readIn, intResult, doubleResult);
      }
      else {

	  // Create a new Record and put it into the pipe
	CreateAndInsertRecord(prev, type, intResult, doubleResult);

	  // Copy the current record into prev
	prev->Copy(readIn);

	intResult = 0;
	doubleResult = 0.0;

	type = IncrementSum(readIn, intResult, doubleResult);

      } // end if cEngine

    } // end if..else

  } // end while outPipe

    // Create a new Record and put it into the pipe
  CreateAndInsertRecord(prev, type, intResult, doubleResult);

    // Clean up temp variables 
  delete prev;
  delete readIn;

}

Type GroupBy::IncrementSum(Record *rec, int &isum, double &dsum){

  Type type;

  int iIncrement = 0;
  double dIncrement = 0.0;

  type = _computeMe->Apply(*rec, iIncrement, dIncrement);

  isum += iIncrement;
  dsum += dIncrement;

  return type;

}

void GroupBy::CreateAndInsertRecord(Record *rec, Type type,
				    int isum, double dsum){

  Record *aggregate = new Record;
  Record *toPipe = new Record;

    // Build the schema that will be used to construct the record
    // Schema will have sum attribute and ordered attributes

    // First, build a record with the sum. Second, merge it with the
    // record that was passed in.

    // Build the aggregate column
  Attribute *atts = new Attribute[1];

    // Conversion from numeric value to string will happen in
    // a stringstream
  std::stringstream s;

    // Convert the appropriate sum into a string
  if (type == Int){
    s << isum;
  }
  else {
    s << dsum;
  }

    // Dobra's code expects a "|" to deliminate where an
    // attribute ends. Adding this to the sum should properly
    // utilize his code
  s << "|";

    // Set the name and type of the Attribute
  atts[0].name = "SUM";
  atts[0].myType = type;

    // Now create the schema for the record
  Schema sumSchema("GROOVY", 1, atts);

    // Build the record from the schema
    // Create the result record based on the schema
  aggregate->ComposeRecord(&sumSchema, s.str().c_str());

    // Now we have to modify rec and then merge the two
    // records together

    // Build the array of attributes to keep in the record
  int numAttsToKeep = 1 + _groupAtts->numAtts;
  int *attsToKeep = new int[numAttsToKeep];

    // Sum record only has one column
  attsToKeep[0] = 0;

    // Loop through ordermaker and get attributes to keep
  for (int i = 1; i < numAttsToKeep; i++){
    attsToKeep[i] = _groupAtts->whichAtts[i-1];
  }

    // Now merge the two records
  toPipe->MergeRecords(aggregate, rec, 1, _groupAtts->GetNumAtts(),
		      attsToKeep, numAttsToKeep, 1);

    // Add record to the output pipe
  _outPipe->Insert(toPipe);

  delete toPipe;
  delete aggregate;

  delete[] atts;
  delete[] attsToKeep;

}

void GroupBy::WaitUntilDone () {
  pthread_join (thread, NULL);
}

void GroupBy::Use_n_Pages (int runlen) {
  pageSize = runlen;
}
