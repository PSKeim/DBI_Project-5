#ifndef SORTEDDB_H
#define SORTEDDB_H

#include "InternalDB.h"
#include "Record.h"
#include "Schema.h"
#include "Comparison.h"
#include "BigQ.h"

class SortedDB : public InternalDB {

public:

	  SortedDB(File *file, OrderMaker sortorder, char *f_path, int runlen);
	  ~SortedDB();

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

	  typedef enum {READING, WRITING} FileMode;

	  int PIPE_SIZE;

	  Pipe *in;
	  Pipe *out;
	  int runLength;
	  BigQ *diff;
	  string f_path;
	  OrderMaker sortorder;

	  FileMode mode;
	  bool recordsInPipe;
	  OrderMaker *query;
	  OrderMaker *lom;

	  int currentRecord;

	  int SetMode(FileMode newMode, bool deleteQuery = false);
	  void MergeAndWrite();
	  void StartBigQ();
	  bool QuerySearch(Record &literal, Record *foundRec, Page *foundPage);

	  int GetPage(Record *rec);

};
#endif
