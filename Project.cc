#include "RelOp.h"
#include "Pipe.h"
#include "Record.h"
#include "Project.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

Project::Project (){

}

Project::~Project (){

}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe,
		  int numAttsInput, int numAttsOutput) {

  _inPipe = &inPipe;
  _outPipe = &outPipe;
  _keepMe = keepMe;
  _numAttsInput = numAttsInput;
  _numAttsOutput = numAttsOutput;

  pthread_create(&thread, NULL, RunWorker, (void*) this);

}

void* Project::RunWorker (void *op){

  Project *project = static_cast<Project*>(op);

  project->DoWork();

  pthread_exit(NULL);

}

void Project::DoWork (){

  Record *readIn = new Record;
  Record *toPipe = new Record;

    // Pull records from the pipe
  while (_inPipe->Remove(readIn)){

      // Copy the record, project the columns, and move on
    toPipe->Consume(readIn);
    toPipe->Project(_keepMe, _numAttsOutput, _numAttsInput);
    _outPipe->Insert(toPipe);

  } // end while outPipe

    // Clean up temp variables 
  delete readIn;
  delete toPipe;

    // Signal that pipe no longer is taking records
  _outPipe->ShutDown();

}

void Project::WaitUntilDone () {
  pthread_join (thread, NULL);
}

void Project::Use_n_Pages (int runlen) {
  // This does nothing, no need to keep track of pages for
  // this operator.
}
