#include "a3test.h"
#include "BigQ.h"
#include "RelOp.h"
#include "Pipe.h"
#include "Schema.h"
#include "Comparison.h"
#include "SelectFile.h"
#include "SelectPipe.h"
#include "WriteOut.h"

#include <stdio.h>

using namespace std;


// make sure that the file path/dir information below is correct
//char *tpch_dir ="/cise/tmp/dbi_sp11/DATA/10M/"; // dir where dbgen tpch files (extension *.tbl) can be found

char *dbfile_dir = "bin/"; // dir where binary heap files should be stored
char *tpch_dir = "table/";
char *catalog_path = "catalog"; // full path of the catalog file

using namespace std;

relation *rel;

Pipe inPipe(10000000);
Pipe outPipe(10000000);

// test WriteOut
void test1 () {

	WriteOut output;

	DBFile dbfile;
	dbfile.Open (rel->path());
	dbfile.MoveFirst ();

	Record temp;

	while (dbfile.GetNext (temp) == 1) {
	    inPipe.Insert(&temp);
	}

	inPipe.ShutDown();

	FILE *fp;
	if ((fp = fopen("OUTPUT.txt", "w")) != NULL){
	  output.Run(inPipe, fp, *(rel->schema()));
	  output.WaitUntilDone();
	}

	dbfile.Close ();

}

// test SelectFile 
void test2 () {

	SelectFile sf;
	WriteOut o;

	DBFile dbfile;
	dbfile.Open (rel->path());
	dbfile.MoveFirst ();

	Record temp;

	CNF cnf; 
	Record literal;
	rel->get_cnf (cnf, literal);

	sf.Run(dbfile, outPipe, cnf, literal);
	sf.WaitUntilDone();

	FILE *fp;
	if ((fp = fopen("OUTPUT.txt", "w")) != NULL){
	  o.Run(outPipe, fp, *(rel->schema()));
	  o.WaitUntilDone();
	}

	dbfile.Close ();

}

// test SelectPipe
void test3 () {


	SelectPipe sp;
	WriteOut o;

	DBFile dbfile;
	dbfile.Open (rel->path());
	dbfile.MoveFirst ();

	Record temp;

	CNF cnf; 
	Record literal;
	rel->get_cnf (cnf, literal);

	while (dbfile.GetNext (temp) == 1) {
	    inPipe.Insert(&temp);
	}

	inPipe.ShutDown();

	sp.Run(inPipe, outPipe, cnf, literal);
	sp.WaitUntilDone();

	FILE *fp;
	if ((fp = fopen("OUTPUT.txt", "w")) != NULL){
	  o.Run(outPipe, fp, *(rel->schema()));
	}

	dbfile.Close ();

}

// adding to a blank page
void test4(){

  Record *record = new Record;
  relation *rel_ptr[] = {n, r, c, p, ps, o, li};

  char tbl_path[100]; // construct path of the tpch flat text file
  sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 


  DBFile dbfile;
  dbfile.Create(rel->path(), heap, NULL);
  dbfile.Load (*(rel->schema ()), tbl_path);
  dbfile.MoveFirst();
  dbfile.GetNext(*record);
  dbfile.Close();

  dbfile.Create(rel->path(), heap, NULL);
  dbfile.Add(*record);
  dbfile.Close();

  delete record;

}

// add a page to a (semi) full page
void test5(){

  Record *record = new Record;

  char tbl_path[100]; // construct path of the tpch flat text file
  sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 

  DBFile dbfile;
  dbfile.Create(rel->path(), heap, NULL);
  dbfile.Load (*(rel->schema ()), tbl_path);

  dbfile.MoveFirst();
  dbfile.GetNext(*record);
  dbfile.Add(*record);

  dbfile.Close();

  delete record;

}

int main () {

	setup (catalog_path, dbfile_dir, tpch_dir);

	void (*test) ();
	void (*test_ptr[]) () = {&test1, &test2, &test3, &test4, &test5};  
	relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};


	int tindx = 0;
	while (tindx < 1 || tindx > 5) {
		cout << " select test: \n";
		cout << " \t 1. write from pipe \n";
		cout << " \t 2. select from file \n";
		cout << " \t 3. select from pipe \n ";
		cout << " \t 4. project attrs \n ";
		cout << " \t 5. sum attrs \n \t ";
		cin >> tindx;
	}

	int findx = 0;
	while (findx < 1 || findx > 7) {
		cout << "\n select table: \n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. suppliers \n";
		cout << "\t 7. orders \n";
		cout << "\t 8. lineitem \n \t ";
		cin >> findx;
	}

	rel = rel_ptr [findx - 1];
	test = test_ptr [tindx - 1];

	test ();

	cleanup ();
}
