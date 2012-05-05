#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "SortedDB.h"
#include "InternalDB.h"
#include "Comparison.h"
#include "Comparator.h"
#include "ComparisonEngine.h"
#include <iostream>
#include <string>
#include <algorithm>

SortedDB::SortedDB (File *file, OrderMaker _sortorder, char *f_path, int runlen)
: runLength(runlen), f_path(f_path), InternalDB(file) {

  PIPE_SIZE = 100;
  currentRecord = 0;

    // Create the pipes
  in = new Pipe(PIPE_SIZE);
  out = new Pipe(PIPE_SIZE);

    // Copy the sortorder
  _sortorder.CopyMe(&sortorder);

    // differential file is NULL to start
  diff = NULL;

    // query information is NULL to start
  query = NULL;
  lom = NULL;

    // Set mode to READING
  mode = READING;

    // No records in pipe!
  recordsInPipe = false;

}

SortedDB::~SortedDB (){

  Close();

  delete in;
  delete out;

  if (diff != NULL){
    delete diff;
  }

  if (query != NULL){
    delete query;
  }

  if (lom != NULL) {
    delete lom;
  }

}

/*********************************
 *
 * File I/O
 *
 **********************************/

/**
 * @public
 *
 * Performs DB-specific cleanup before the
 * parent DBFile shuts down.
 * 
 */
void SortedDB::Close (){

  SetMode(READING);

} // end SortedDB::Close


/**
 * @public
 *
 * Bulk loads relation data into this SortedDB's .bin file.
 * If the .bin does not exist, the subroutine exits with
 * nothing done.
 *
 * @param &f_schema
 *    The schema for the given relation.
 * @param *loadpath
 *    The path to the relation data.
 */
void SortedDB::Load (Schema &f_schema, char *loadpath) {

    // Set this mode to WRITING, records are being added
  SetMode(WRITING);

    // Open the file from which to get records
  FILE *fp = fopen(loadpath, "r");
  int counter = 0;

  if (fp != NULL){

    Record rec;
    Record *temp = new Record;

      // Get the next record
    while (rec.SuckNextRecord(&f_schema, fp) == 1) {
      temp->Consume(&rec);
      in->Insert(temp);
      counter++;
    }

    delete temp;

      // Close file
    fclose(fp);

  } // end if file

  if (counter > 0){
    recordsInPipe = true;
  }

} // end SortedDB::Load

/*********************************
 *
 * File Modification
 *
 **********************************/

/**
 * @public
 * 
 * Adds a record to the end of the SortedDB.
 * The record is consumed after being added
 * to the file.
 *
 * @param &rec
 *     The record to add to the file.
 *
 * @return
 *    1 on success, 0 otherwise.
 */
void SortedDB::Add (Record &rec) {

    // Set mode to WRITING, a record is being added
  SetMode(WRITING);

  in->Insert(&rec);
  recordsInPipe = true;

} // end SortedDB::Add


/*********************************
 *
 * File Traversal
 *
 **********************************/

/**
 * @public
 * 
 * Get the next record in the SortedDB.
 * 
 * @param &fetchme
 *    The next record to return.
 *
 * @return
 *    1 if a record was returned, 0 otherwise.
 */
int SortedDB::GetNext (Record &fetchme) {

    // Set mode to READING
  SetMode(READING);

  int success = 0;

      // If successful, set return value and break
  if (page->GetFirst(&fetchme) == 1){
      success = 1;
  }
      // Else if there are still pages to search, move
      // to the next page
  else if (pIndex < GetEndOfFileIndex() - 1){
    pIndex++;
    page->EmptyItOut();
    LoadPage(page, pIndex);
    currentRecord = 0;
    success = page->GetFirst(&fetchme);
  }
    // Else there are no more records.
  else {
    success = 0;
  }

  if (success == 1){
    currentRecord++;
  }

  return success;

} // end SortedDB::GetNext


/**
 * Get the next record in the list that satisfies the
 * given CNF and record literal. The records are checked
 * until one comparison yields true.
 *
 * @param &fetchme
 *    The record to return.
 * @param &cnf
 *    The CNF on which to perform the comparison.
 * @param &literal
 *    The literal value against which to check.
 *
 * @return
 *    1 if a record was returned, 0 otherwise.
 *
 * @see ComparisonEngine.h
 * @see Record.h
 */
int SortedDB::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

    // Set mode to READING
  SetMode(READING, false);

  int success = 0;
  
  Record *foundRec = new Record;
  bool found = false;

    // First, build the OrderMaker used in the binary search
    // If the OrderMaker is empty, no binary search will occur!
  if (query == NULL){

    query = new OrderMaker;
    lom = new OrderMaker;

    int numAtts = cnf.CreateQueryOrderMaker(sortorder, *query, *lom);

    Page *foundPage = new Page;

    if (numAtts > 0) {

	// Now perform binary search, if applicable
      found = QuerySearch(literal, foundRec, foundPage);

      if (found){
	delete page;
	page = foundPage;
      }
      else {
	delete foundPage;
	foundPage = NULL;
      }

    } // end if numAtts
    else {
      found = (bool) GetPage(foundRec);
    }

  } // end if query
  else {
    found = (bool) GetPage(foundRec);
  }

    // And now determine record to return
  ComparisonEngine cEngine;

    // If the literal was found, continue looking for 
    // the record
  if (found){

      // While the record matches the literal, try to match
      // against the CNF
    while (cEngine.Compare(foundRec, query, &literal, lom) == 0){

	// If the record matches the CNF, break out
      if (cEngine.Compare(foundRec, &literal, &cnf) != 0){
	fetchme.Copy(foundRec);
	success = 1;
	currentRecord++;
	break;
      }
      else {

	  // If a record couldn't be retreived,
	  // must move to next page
	if (!GetPage(foundRec)){
	    break;
	} // end if page->GetFirst

      } // end if..else

    } // end while search

  } // end if result

  delete foundRec;

  return success;

} // end SortedDB::GetNext

/**
 * @public
 *
 * Move to the first record on the first page.
 */
void SortedDB::MoveFirst() {

  SetMode(READING);

    // Reset the current page and page index
  page->EmptyItOut();
  pIndex = (off_t) 0;

    // Get the first page of data
  LoadPage(page, pIndex);
  currentRecord = 0;

} // end SortedDB::MoveFirst

/**
 * @private
 *
 * Sets the mode of the sorted db. If the mode changes,
 * then data is written out or BigQ instance is generated,
 * depending on the new mode selected.
 *
 * @param newMode
 *    The mode to which to switch
 * @param deleteQuery
 *    Flag that indicates if the query OrderMaker should be
 *    deleted. If true, and if query is not a NULL pointer,
 *    query is deleted and the pointer assigned to NULL.
 *    Defaults to true.
 *
 * @return
 *    1 if the mode changed, 0 otherwise.
 */
int SortedDB::SetMode(FileMode newMode, bool deleteQuery){

    // Get the previous mode
  FileMode prevMode = mode;
    // Set the new mode
  mode = newMode;

    // Check to see if mode was switched
  int modeSwitched = (int) !(prevMode == newMode);

    // If mode was switched, perform some function
  if (modeSwitched){

    switch (mode){

	// Switched to READING: write out data in BigQ
      case READING:  

	  MergeAndWrite();
	  MoveFirst();
	break;

	// Switched to WRITING: instantiate BigQ
      case WRITING:

	StartBigQ();
	break;

    } // end switch mode

  } // end if modeSwitched

  if (deleteQuery && query != NULL){
    delete query;
    query = NULL;

    delete lom;
    lom = NULL;
  }

  return modeSwitched;

} // end SortedDB::SetMode

/********************************************

 SortedDB Helper Methods

*********************************************/

void SortedDB::MergeAndWrite(){

    // Shut down the input pipe
  in->ShutDown();

  comparator merge(false, sortorder);

    // Create a temp file for the merge
  string path = "mergetemp.bin";
  File tempFile;
  tempFile.Open(0, (char*) path.c_str());

    // Create temporary buffer for grabbing records
    // from the existing file
  Page *filePage = new Page;
  off_t fileIndex = (off_t) 0;
  off_t lastPageIndex = GetLastPageIndex();

    // Set condition flags
  bool fileIsDone = (GetFileLength() == 0);
  bool pipeIsDone = false;

    // Create temp vars to hold a record from the
    // pipe and from the file
  Record *pipeRec = new Record;
  Record *fileRec = new Record;
  Record *winner;

  char winnerFlag = ' ';

    // Create buffer for the "new" file
  Page *mergedPage = new Page;
  off_t mergedIndex = (off_t) 0;

  bool endEarly = false;

    // Get the first pipe record
  out->Remove(pipeRec);

    // If the file isn't empty, read in a page
  if (!fileIsDone){
    LoadPage(filePage, fileIndex);
    filePage->GetFirst(fileRec);
  }

  while (!fileIsDone || !pipeIsDone){

    if (fileIsDone){
      winner = pipeRec;
      winnerFlag = 'p';
      endEarly = true;
    }
    else if (pipeIsDone){
      winner = fileRec;
      winnerFlag = 'f';
      endEarly = true;
    }

    if (!endEarly){

	// If pipeRec < fileRec in sorting order, winner is pipeRec
      if (merge(pipeRec, fileRec)){
	winner = pipeRec;
	winnerFlag = 'p';
      }
	// Else fileRec is the winner
      else {
	winner = fileRec;
	winnerFlag = 'f';
      }

    } // end if !endEarly

      // If the current buffer space for the file can't
      // add the record, add a new page and retry
    if (!mergedPage->Append(winner)){
      tempFile.AddPage(mergedPage, mergedIndex);
      mergedIndex += (off_t) 1;
      mergedPage->EmptyItOut();
      mergedPage->Append(winner);
    } // end !pages

    if (winnerFlag == 'p'){
      pipeIsDone = !(bool) out->Remove(pipeRec);
    }
    else {

      int recExists = filePage->GetFirst(fileRec);

      if (!recExists && fileIndex < lastPageIndex){
	fileIndex += (off_t) 1;
	filePage->EmptyItOut();
	LoadPage(filePage, fileIndex);
	filePage->GetFirst(fileRec);
      }
      else if (!recExists && fileIndex == lastPageIndex){
	fileIsDone = true;
      }

    } // end if..else

    endEarly = false;

  } // end while !fileIsDone

  tempFile.AddPage(mergedPage, mergedIndex);
  tempFile.Close();
  UpdateFile(path, f_path);

  delete filePage;
  delete pipeRec;
  delete fileRec;
  delete mergedPage;

    // Delete the BigQ
  delete diff;
  diff = NULL;

    // Indicate that there are no records in the pipes
  recordsInPipe = false;

} // end SortedDB::MergeAndWrite

void SortedDB::StartBigQ(){

  delete in;
  delete out;

  in = new Pipe(PIPE_SIZE);
  out = new Pipe(PIPE_SIZE);

  diff = new BigQ(*in, *out, sortorder, runLength);

} // end SortedDB::StartBigQ

/**
 * Perform a binary search, looking for the literal
 * record. The first exact match, if found, is returned.
 *
 * @param literal
 *    The literal record for which to search
 * @param foundRec
 *    A pointer to the matching record, if it is found
 * @param foundPage
 *    A pointer to the matching page, if it is found
 *
 * @return
 *    The page on which the record is found, or -1 if a
 *    record is not found
 */
bool SortedDB::QuerySearch(Record &literal, Record *foundRec, Page *foundPage){

  comparator compare(false, *query);
  ComparisonEngine cEngine;

  int pos = -1;
  int begin = (int) pIndex;
  int end = (int) GetLastPageIndex();
  off_t search = (off_t) -1;

  int result = -2;
  int prevResult = -2;
  bool quit = false;

    // Keep iterating until the algorithm converges
  while (begin <= end && !quit){

      // Calculate current position. This is a smart-ass C++ programmer
      // way of writing (begin+end) / 2. I'm becoming a pretentious
      // ass aren't I?

      // In any event, it's written like this for efficiency. Division
      // sucks. No, really, go look it up.
    pos = (begin + end) / 2;

      // Get the page located at pos
    LoadPage(foundPage, (off_t) pos);

    if (pIndex == (off_t) pos){
	for (int i = 0; i < currentRecord; i++){
	  foundPage->GetFirst(foundRec);
	}
    }

      // Needed to ensure that, if the record doesn't exist,
      // then quit
    prevResult = -2;

      // Go through each record on the page
    while (true){

	// If a comparison can be done, do so
	if (foundPage->GetFirst(foundRec)){

	  // Run the comparison
	result = cEngine.Compare(foundRec, query, &literal, lom);

	  // If literal > tempRec, go to the left half of search
	if (result >= 1){

	    // If the previous result was a -1, then no match
	    // will be made. Quit the search.
	  if (prevResult < 0){
	    quit = true;
	  }
	    // Else move down into the left half
	  else {
	    end = pos - 1;
	  }

	  break;
	}
	  // If literal matches this record exactly, need to quit!
	else if (result == 0){
	  search = (off_t) pos;
	  quit = true;
	  break;
	}

      } // end if foundPage

	// literal was < tempRec, but no records on the
	// page matched. Move to right half of search.
      else {
	begin = pos + 1;
	break;
      }

	// Track the previous result
      prevResult = result;

    } // end while true

  } // end while begin

  if (search > (off_t) -1){
    pIndex = search;
  }

  return (search > (off_t) -1);

} // end SortedDB::QuerySearch


int SortedDB::GetPage(Record *rec){

  int success = 0;

  if (page->GetFirst(rec)){
    success = 1;
  }
  else if (pIndex < GetEndOfFileIndex() - 1){
    pIndex += (off_t) 1;
    LoadPage(page, pIndex);
    success = page->GetFirst(rec);
    currentRecord = 0;
  }

  if (success == 1){
    currentRecord++;
  }

} // end SortedDB::GetPage
