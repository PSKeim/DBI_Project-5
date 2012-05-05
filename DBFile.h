#ifndef DBFILE_H
#define DBFILE_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "InternalDB.h"

typedef enum {heap, sorted, tree} fType;

class DBFile {

public:

	DBFile ();
	~DBFile ();

	int Open (char *fpath);
	int Close ();
	int Create (char *fpath, fType file_type, void *startup);

	void Load (Schema &f_schema, char *loadpath);
	void Add (Record &addme);

	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	void MoveFirst ();

private:

	typedef enum {FILE_INITIALIZED, FILE_OPEN, FILE_CLOSED} Status;

	InternalDB *internalDB;

	File file;
	fType type;
	Status status;

	struct SortInfo {
	  OrderMaker *myOrder;
	  int runLength;
	};

};
#endif
