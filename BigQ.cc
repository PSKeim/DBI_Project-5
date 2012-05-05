#include "BigQ.h"
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Comparator.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

#include <vector>
#include <algorithm>

#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <iostream>

using std::vector;

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
: sortorder(sortorder), runLength(runlen){

    // Keep track of the pipes
  this->in = &in;
  this->out = &out;

    // Initalize the counts
  runs = 0;

    // Calculate the time in nanoseconds - need for BigQ temp file
  timespec time;
  clock_gettime(CLOCK_REALTIME, &time);
  int timestamp = time.tv_sec*1000000000 + time.tv_nsec;
  if (timestamp < 0) timestamp *= -1;

    // Initialize the file that will contain all run information
  sprintf(filename, "bigq_externsort_%d.bin", timestamp);
  runfile.Open(0, filename);

    // Lastly create the pthread
  pthread_create(&worker, NULL, SortWorker, (void*)this);

}

BigQ::~BigQ () {


}

void* BigQ::SortWorker(void *args){

  BigQ *self = static_cast<BigQ*>(args);

    // Partition data from input pipe into runs and sort the runs
  self->ExternalSortRunPhase();

    // Merge all the runs and push to output pipe
  self->ExternalSortMergePhase();

    // Close the temp file used to store the runs
  self->runfile.Close();

  self->DeleteTempFile();

    // finally shut down the out pipe. This signals the
    // pipe so it knows that it won't be getting more
    // data.
  self->out->ShutDown();

    // Exit the thread
  pthread_exit(NULL);

}

void BigQ::ExternalSortRunPhase(){

    // read data from in pipe sort them into runlen pages

  off_t offset = 0;
  struct comparator compare(false, sortorder);

  Record *record = new Record;
  Record *temp = new Record;

  Page *buffer = new Page;

  vector<Record*> run;
  int pageNo = 0;
  int records = 0;

      // Get next record from pipe
  while (in->Remove(record)){

      // Keep track of the number of records in the run.
      // Useful when determining if a shorter run
      // needs to be written.
    records++;

      // If error occurred, need to move to new page or new run
    if (!buffer->Append(record)){

	// Empty the buffer's contents into the current run
      while (buffer->GetFirst(temp)){
	run.push_back(new Record);
	run[run.size()-1]->Consume(temp);
      }

	// If we have reached the page limit, sort+write out run
	// and then restart
      if (pageNo == runLength - 1){

	  // Increment run count
	runs++;
	  // Sort the run
	sort(run.begin(), run.end(), compare);

	  // Write out the run
	offset = WriteRunToFile(run, offset);

	  // Remove records from the run
        for (int i = 0; i < run.size(); i++){
          delete run[i];
        }

	run.clear();

	  // Reset page count
	pageNo = 0;
	records = 1;
      }

	// Else put everything on a new page
      else {
	pageNo++;
      }

	// Add the failed record again - should work this time!
      buffer->Append(record);

    } // end if !buffer

  } // end while in.Remove()

    // If any records are left over, write them to 
  if (records > 0){

    while (buffer->GetFirst(temp)){
      run.push_back(new Record);
      run[run.size()-1]->Consume(temp);
    }

    sort(run.begin(), run.end(), compare);
    WriteRunToFile(run, offset);

    for (int i = 0; i < run.size(); i++){
      delete run[i];
    }

    run.clear();

  } // end if records
  
    // Free temporary variables
  delete record;
  delete temp;
  delete buffer;

} // end ExternalSortRunPhase

void BigQ::ExternalSortMergePhase() {

    // construct priority queue over sorted runs and dump sorted data 
    // into the out pipe

    // Build the comparator objects
  comparator pq_cmp(true, sortorder);
  pq_comparator pq_compare(pq_cmp);

    // Create the priority queue
  priority_queue< HeapRun*, vector<HeapRun*>, pq_comparator > pqueue(pq_compare);

  vector<Page*> pages;
  int bucket = 0;

  HeapRun *temp;
  HeapRun *run;
  Record *tempRec = new Record;

    // Initialize all the buckets before initiating sort
  while (bucket < startPos.size()){

      // Get a page for the current run
    pages.push_back(new Page);
    runfile.GetPage(pages[bucket], startPos[bucket]);

      // Get the first record
    pages[bucket]->GetFirst(tempRec);

      // Push record onto heap
    temp = new HeapRun(bucket, tempRec);
    pqueue.push(temp);

      // Increment the page offset by runLength
    bucket++;

  } // end while init

  int runNum = 0;


  while (!pqueue.empty()){

      // Get the run object
    run = pqueue.top();
    pqueue.pop();

      // Insert into the out pipe
    out->Insert(run->getRecord());

      // Get the run number and delete the run
    runNum = run->getRunNum();
    delete run;

      // If it can get a new record quickly, do so
    if (pages[runNum]->GetFirst(tempRec)){
      temp = new HeapRun(runNum, tempRec);
      pqueue.push(temp);
    }

      // Else page is now exhausted
    else {

	// Increment starting position
      startPos[runNum]++;

	// If the end hasn't been reached, read in a new
	// record
      if (startPos[runNum] < endPos[runNum]){

	  // Read in a new page
	pages[runNum]->EmptyItOut();
	runfile.GetPage(pages[runNum], startPos[runNum]);

	  // Get the first record and push onto heap
	pages[runNum]->GetFirst(tempRec);

	temp = new HeapRun(runNum, tempRec);
	pqueue.push(temp);

      } // end if startPos[runNum]
      else {
	delete pages[runNum];
      }

    } // end if..else

  } // end while !pqueue.empty()

    // run and temp are deleted during normal execution
  delete tempRec;

  pages.clear();


} // end ExternalSortMergePhase


/**
 * Delete's the BigQ's temporary file.
 *
 * @return
 *    1 if the file was successfully deleted, 0 otherwise
 */
int BigQ::DeleteTempFile(){
  return remove(filename);
}

/**
 * Writes out a run's worth of information to the
 * file specified in the BigQ constructor. This function
 * also calculates the start and end position of the run.
 *
 * @param &run
 *    A vector containing records to write out to file
 * @param offset
 *    The starting page offset to which to write
 *
 * @return
 *    The first offset after which the data has been written
 *
 */
off_t BigQ::WriteRunToFile(vector<Record*> &run, off_t offset){

  int i = 0;
  int pages = 0;

  off_t startOffset = offset;
  Page *fileBuffer = new Page;

	  // For each record in the run, write out to the temp file
  while (i < run.size()){

      // Write out page if buffer couldn't add a record
    if (!fileBuffer->Append(run[i])){

	// Increment page offset and # of pages
      runfile.AddPage(fileBuffer, offset);
      offset++;
      pages++;

	// Add record to empty buffer
      fileBuffer->EmptyItOut();
      fileBuffer->Append(run[i]);

    } // end if !fileBuffer

    i++;

  } // end while i

    // Write out remaining records to file
  runfile.AddPage(fileBuffer, offset);
  offset++;
  pages++;

  delete fileBuffer;

    // Save the starting and ending offsets
  startPos.push_back(startOffset);
  endPos.push_back(startOffset+pages);

  return offset;

} // end WriteRunToFile

/**
 * Fills a priority queue with HeapRun objects. This is
 * useful for loading many records into a heap at once.
 *
 * @param &pqueue
 *    The priority queue to fill
 * @param run
 *    The run from which the records came.
 * @param
 *    The page offset from which to read
 *
 * @return
 *    The number of records inserted into the priority queue
 *
 */
int BigQ::FillQueue(priority_queue< HeapRun*, vector<HeapRun*>, pq_comparator > &pqueue, int run, off_t offset){

  HeapRun *tempRun;
  Page *tempBuf = new Page;
  Record *tempRec = new Record;

    // Get the page
  runfile.GetPage(tempBuf, offset);

  int numRecs = 0;

      // While there are records, insert objects into the heap
  while (tempBuf->GetFirst(tempRec)){
    tempRun = new HeapRun(run, tempRec);
    pqueue.push(tempRun);
    numRecs++;
  }

  delete tempBuf;
  delete tempRec;

  return numRecs;

} // end FillUpBuffer


void BigQ::TestSort(){

    off_t lastPage = runfile.GetLength() - 1;
  for(off_t curPage = 0; curPage < lastPage; curPage++)
    {
      Page tp;
      runfile.GetPage(&tp,curPage);
      Record temp;
      while(1 == tp.GetFirst(&temp))
        {
          out->Insert(&temp);
          // cout << "put a record in the pipe" << endl;
        }
    }
  cout << "phase two complete" << endl;
out->ShutDown();
}
