#ifndef INTERNALDB_H
#define INTERNALDB_H

#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "Comparison.h"
#include <string>

class InternalDB {

public:

	  InternalDB(File *file);
	  virtual ~InternalDB();

	  /**
           * The following methods must be implemented
	   * by each InternalDB instance. InternalDB does not
	   * implement pure virtual subroutines so that the
	   * DBFile class can have an instance of InternalDB.
           */
	  virtual void Close();

	  virtual void Load(Schema &f_schema, char *loadpath);
	  virtual void Add(Record &addMe);

	  virtual int GetNext(Record &fetchme);
	  virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal);
	  virtual void MoveFirst();


protected:

	  Page *page;
	  off_t pIndex;

	  void LoadPage(Page *loadMe, off_t index);
	  void Write(Page *writeMe, off_t index);

	  int GetFileLength();
	  off_t GetLastPageIndex();
	  off_t GetEndOfFileIndex();

	  void UpdateFile(string temp, string filename);

private:
	  File *dbfile;

};


#endif
