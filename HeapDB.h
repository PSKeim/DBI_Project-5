#ifndef HEAPDB_H
#define HEAPDB_H

#include "InternalDB.h"
#include "Record.h"
#include "Schema.h"
#include "Comparison.h"

class HeapDB : public InternalDB {

public:

	  HeapDB(File *file);
	  ~HeapDB();

	  /**
	   * The following functions are implementations of
	   * virtual functions defined in InternalDB.h
	   */
	  void Close();

	  void Load(Schema &f_schema, char *loadpath);
	  void Add(Record &addMe);

	  int GetNext(Record &fetchme);
	  int GetNext(Record &fetchme, CNF &cnf, Record &literal);
	  void MoveFirst();

private:


};
#endif
