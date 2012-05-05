#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "DBFile.h"
#include "Defs.h"
#include "InternalDB.h"
#include "HeapDB.h"
#include "SortedDB.h"

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <fstream>
#include <string>
#include <sstream>

DBFile::DBFile(){

    // Set status to initialized
  status = FILE_INITIALIZED;

}

DBFile::~DBFile(){

}

/*********************************
 *
 * File I/O
 *
 **********************************/

/**
 * @public
 *
 * Opens a DBFile. Subsequent calls do nothing
 * to the file.
 *
 * @param *f_path
 *    The file path of the DBFile binary file.
 *
 * @return
 *    1 on success, 0 otherwise.
 */
int DBFile::Open (char *f_path) {

  int success = 0;
  ifstream metadata;

  if (status != FILE_OPEN){

    status = FILE_OPEN;

    file.Open(1, f_path);

    string metaPath(f_path);
    metaPath.append(".meta");
    metadata.open(metaPath.c_str());

      // Check to see if connection is good
    if (metadata.is_open()){

      if (metadata.good()){

	string line;
	getline(metadata, line);

	if (line.compare("heap") == 0){

	  type = heap;
	  internalDB = new HeapDB(&file);

	  success = 1;

	} // end if line.compare("heap")

	else if (line.compare("sorted") == 0){

	  type = sorted;

	  OrderMaker *myOrder = new OrderMaker();
	  int runLength = 0;
	  bool error = false;

	  int numAtts = 0;
	  Type attrType = Int;

	    // Get the runlength
	  getline(metadata, line);
	  runLength = atoi(line.c_str());

	    // Read in number of attributes
	  getline(metadata, line);
	  numAtts = atoi(line.c_str());

	  myOrder->numAtts = numAtts;

	  for (int i = 0; i < numAtts; i++){

	      // Read in an attribute
	    getline(metadata, line);
	    myOrder->whichAtts[i] = atoi(line.c_str());

	      // Read in an attribute type
	    getline(metadata, line);

	    if (line.compare("Int") == 0){
	      attrType = Int;
	    }
	    else if (line.compare("Double") == 0){
	      attrType = Double;
	    }
	    else {
	      attrType = String;
	    }

	    myOrder->whichTypes[i] = attrType;

	  } // end for i

	  internalDB = new SortedDB(&file, *myOrder, f_path, runLength);
	  delete myOrder;

	  success = 1;

	} // end if line.compare("sorted")

      } // end if metadata.good()

      metadata.close();

    } // end if metadata.open()

  } // end if status

  return success;

} // end DBFile::Open

/**
 * @public
 *
 * Closes a DBFile. Calls to an unopened file
 * do nothing.
 *
 * @return
 *    1 on success, 0 otherwise.
 */
int DBFile::Close () {

  int success = 0;
  int length = -1;

  if (status != FILE_CLOSED){

      // Force database to complete any pending tasks
      // and then deallocate the memory
    internalDB->Close();
    delete internalDB;

      // Write out to file
    length = file.Close();

    if (length > 0){
      success = 1;
    }
    else {
      success = 0;
    }

    status = FILE_CLOSED;

  } // end status

  return success;

} // end DBFile::Close()


/**
 * @public
 *
 * Creates the *.bin file for the DBFile. Note that
 * repeated calls to this subroutine will recreate
 * the file.
 *
 * @param *f_path
 *    The file path of the DBFile binary file.
 * @param f_type
 *    The type of DBFile. Can be either heap, sorted, or tree.
 * @param *startup
 *    A SortInfo struct. Used in the construction of a sorted
 *    file.
 *
 * @return
 *    1 on success, 0 otherwise.
 */
int DBFile::Create (char *f_path, fType f_type, void *startup) {

  int success = 0;
  ofstream metadata;

  file.Open(0, f_path);

  string metaPath(f_path);
  metaPath.append(".meta");
  metadata.open(metaPath.c_str());

  SortInfo *info;

  switch (f_type){

    case heap:

      if (metadata.is_open() && metadata.good()){
	metadata << "heap" << std::endl;
      }

      internalDB = new HeapDB(&file);

      break;

    case sorted:

      if (metadata.is_open() && metadata.good()){
	metadata << "sorted" << std::endl;
      }

	// Get the sorting information and pass it to the SortedDB
      info = (SortInfo*) startup;
      internalDB = new SortedDB(&file, *(info->myOrder), f_path,
				info->runLength);

	// Now, write out sort information to the meta file
      if (metadata.is_open() && metadata.good()){

	  // Write out runLength
	metadata << info->runLength << std::endl;

	  // Get the OrderMaker and write out the number of attributes
	OrderMaker *order = info->myOrder;
	metadata << order->numAtts << std::endl;

	  // print out the attributes and their type
	for (int i = 0; i < order->numAtts; i++){

	  metadata << order->whichAtts[i] << std::endl;

	    // Print out the attribute type
	  switch (order->whichTypes[i]){

	    case Int:
	      metadata << "Int" << std::endl;
	      break;

	    case Double:
	      metadata << "Double" << std::endl;
	      break;

	    case String:
	      metadata << "String" << std::endl;
	      break;

	  } // end switch whichTypes[i]

	} // end for i

      } // end if metadata

      break;

    case tree:
      break;

    default:
      success = 0;
      break;

  } // end switch f_type

  if (metadata.is_open() && metadata.good()){
    metadata.close();
    success = 1;
  }

  status = FILE_OPEN;

  return success;

} // end DBFile::Create

/**
 * @public
 *
 * Bulk loads relation data into this DBFile's .bin file.
 * If the .bin does not exist, the subroutine exits with
 * nothing done.
 *
 * @param &f_schema
 *    The schema for the given relation.
 * @param *loadpath
 *    The path to the relation data.
 */
void DBFile::Load (Schema &f_schema, char *loadpath) {

  internalDB->Load(f_schema, loadpath);

} // end DBFile::Load

/*********************************
 *
 * File Modification
 *
 **********************************/

/**
 * @public
 * 
 * Adds a record to the end of the DBFile.
 * The record is consumed after being added
 * to the file.
 *
 * @param &rec
 *     The record to add to the file.
 *
 * @return
 *    1 on success, 0 otherwise.
 */
void DBFile::Add (Record &rec) {

  internalDB->Add(rec);

} // end DBFile::Add


/*********************************
 *
 * File Traversal
 *
 **********************************/

/**
 * @public
 * 
 * Get the next record in the DBFile.
 * 
 * @param &fetchme
 *    The next record to return.
 *
 * @return
 *    1 if a record was returned, 0 otherwise.
 */
int DBFile::GetNext (Record &fetchme) {

  return internalDB->GetNext(fetchme);

} // end DBFile::GetNext


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
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

  return internalDB->GetNext(fetchme, cnf, literal);

} // end DBFile::GetNext

/**
 * @public
 *
 * Move to the first record on the first page.
 */
void DBFile::MoveFirst () {

  internalDB->MoveFirst();

} // end DBFile::MoveFirst
