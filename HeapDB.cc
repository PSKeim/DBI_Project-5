#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "HeapDB.h"
#include "InternalDB.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <iostream>

HeapDB::HeapDB (File *file): InternalDB(file) {

}

HeapDB::~HeapDB (){

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
void HeapDB::Close (){

} // end HeapDB::Close

/**
 * @public
 *
 * Bulk loads relation data into this HeapDB's .bin file.
 * If the .bin does not exist, the subroutine exits with
 * nothing done.
 *
 * @param &f_schema
 *    The schema for the given relation.
 * @param *loadpath
 *    The path to the relation data.
 */
void HeapDB::Load (Schema &f_schema, char *loadpath) {

    // Open the file from which to get records
  FILE *fp = fopen(loadpath, "r");
    // rec will act as temp var when getting record from loadpath
  Record rec;
    // Use temporary page to write records
  Page temp;

    // Get records if the file opened properly
  if (fp != NULL){

    int counter = 0;
    off_t index = GetEndOfFileIndex();

      // Get the next record
    while (rec.SuckNextRecord(&f_schema, fp) == 1) {

	// Attempt to append record to the page
	// If the page is full, write out page and make new page
      if (temp.Append(&rec) == 0){

	Write(&temp, index);

	temp.EmptyItOut();
	temp.Append(&rec);

	index++;

      } // end if temp.Append()

      counter++;

    } // end while rec.SuckNextRecord()

    Write(&temp, index);
    temp.EmptyItOut();

      // Close file
    fclose(fp);

  } // end if file

} // end HeapDB::Load

/*********************************
 *
 * File Modification
 *
 **********************************/

/**
 * @public
 * 
 * Adds a record to the end of the HeapDB.
 * The record is consumed after being added
 * to the file.
 *
 * @param &rec
 *     The record to add to the file.
 *
 * @return
 *    1 on success, 0 otherwise.
 */
void HeapDB::Add (Record &rec) {

  Page *lastPage;
  off_t index;

  if (GetFileLength() == 0){
    lastPage = page;
    index = (off_t) 0;
  }
  else {
    lastPage = new Page;
    index = GetLastPageIndex();
    LoadPage(lastPage, index);
  }

    // Append the record to the last page
  int success = lastPage->Append(&rec);

    // If the record could not be appended, then
    // add the record to a new page. Write new
    // page to the file.
  if (!success){
    lastPage->EmptyItOut();
    lastPage->Append(&rec);
    index = GetEndOfFileIndex();
  }

    // Write out the data
  Write(lastPage, index);

    // Delete lastPage if it was allocated
  if (lastPage != page){
    delete lastPage;
  }

} // end HeapDB::Add


/*********************************
 *
 * File Traversal
 *
 **********************************/

/**
 * @public
 * 
 * Get the next record in the HeapDB.
 * 
 * @param &fetchme
 *    The next record to return.
 *
 * @return
 *    1 if a record was returned, 0 otherwise.
 */
int HeapDB::GetNext (Record &fetchme) {

  int success = -1;

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
    success = page->GetFirst(&fetchme);
  }
    // Else there are no more records.
  else {
    success = 0;
  }

  return success;

} // end HeapDB::GetNext


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
int HeapDB::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

  int success = 0;

  ComparisonEngine cEngine;

    // Keep moving forward, looking for a record
  do {

    success = GetNext(fetchme);

      // If successful, see if record meets comparison
    if (success){

	// If comparison yields true, get record and return
      if (cEngine.Compare(&fetchme, &literal, &cnf)){
	break;
      } // end if cEngine.Compare

    } // end if success

  }
  while (success);

  return success;

} // end HeapDB::GetNext

/**
 * @public
 *
 * Move to the first record on the first page.
 */
void HeapDB::MoveFirst () {

    // Reset the current page and page index
  page->EmptyItOut();
  pIndex = (off_t) 0;

    // Get the first page of data
  LoadPage(page, pIndex);

} // end HeapDB::MoveFirst
