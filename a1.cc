#include <iostream>
#include "DBFile.h"
#include "a1.h"
#include "StopWatch.h"

// make sure that the file path/dir information below is correct
//char *tpch_dir ="/cise/tmp/dbi_sp11/DATA/10M/"; // dir where dbgen tpch files (extension *.tbl) can be found

char *dbfile_dir = "bin/"; // dir where binary heap files should be stored
char *tpch_dir = "table/";
char *catalog_path = "catalog"; // full path of the catalog file

using namespace std;

relation *rel;

// load from a tpch file
void test1 () {

	DBFile dbfile;
	cout << " DBFile will be created at " << rel->path () << endl;
	dbfile.Create (rel->path(), heap, NULL);

	char tbl_path[100]; // construct path of the tpch flat text file
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 
	cout << " tpch file will be loaded from " << tbl_path << endl;

	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.Close ();
}

// sequential scan of a DBfile 
void test2 () {

	DBFile dbfile;
	dbfile.Open (rel->path());
	dbfile.MoveFirst ();

	Record temp;

	int counter = 0;
	while (dbfile.GetNext (temp) == 1) {
		counter += 1;
		temp.Print (rel->schema());
		if (counter % 10000 == 0) {
			cout << counter << "\n";
		}
	}
	cout << " scanned " << counter << " recs \n";
	dbfile.Close ();
}

// scan of a DBfile and apply a filter predicate
void test3 () {

	cout << " Filter with CNF for : " << rel->name() << "\n";

	CNF cnf; 
	Record literal;
	rel->get_cnf (cnf, literal);

	DBFile dbfile;
	dbfile.Open (rel->path());
	dbfile.MoveFirst ();

	Record temp;

	int counter = 0;
	while (dbfile.GetNext (temp, cnf, literal) == 1) {
		counter += 1;
		temp.Print (rel->schema());
		if (counter % 10000 == 0) {
			cout << counter << "\n";
		}
	}
	cout << " selected " << counter << " recs \n";
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

	StopWatch watch;

	setup (catalog_path, dbfile_dir, tpch_dir);

	void (*test) ();
	void (*test_ptr[]) () = {&test1, &test2, &test3, &test4, &test5};  
	relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};


	int tindx = 0;
	while (tindx < 1 || tindx > 5) {
		cout << " select test: \n";
		cout << " \t 1. load file \n";
		cout << " \t 2. scan \n";
		cout << " \t 3. scan & filter \n ";
		cout << " \t 4. add record (blank file) \n ";
		cout << " \t 5. add record (full file) \n \t ";
		cin >> tindx;
	}

	int findx = 0;
	while (findx < 1 || findx > 8) {
		cout << "\n select table: \n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. supplier \n";
		cout << "\t 7. orders \n";
		cout << "\t 8. lineitem \n \t ";
		cin >> findx;
	}

	rel = rel_ptr [findx - 1];
	test = test_ptr [tindx - 1];

//	watch.Start();
	test ();
//	double time = watch.End();

//	std::cout << "Time taken to execute (in seconds) = ";
//	std::cout << time << std::endl;

	cleanup ();
}
