#include <ios>
#include <map>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <cstdlib>

#include "Record.h"
#include "Schema.h"
#include "DBFile.h"
#include "QueryTree.h"
#include "QueryType.h"
#include "ParseTree.h"
#include "ParseTreeExtension.h" 
#include "QueryTreeNode.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

extern int queryType;
extern struct InOutPipe *io;
extern struct CreateTableArgs *createArgs;
extern string createTableName;
extern string createTableType;

extern vector<string> atts;
extern vector<string> attTypes;

// donot change this information here
char *catalog_path, *dbfile_dir, *tpch_dir = NULL;

void selectQuery(int output, string outputFile);

void ReadInFileLocations(){

    // test settings file should have the 
    // catalog_path, dbfile_dir and tpch_dir information in separate lines
  const char *settings = "test.cat";

 	FILE *fp = fopen (settings, "r");
	if (fp) {
		char *mem = (char *) malloc (80 * 3);
		catalog_path = &mem[0];
		dbfile_dir = &mem[80];
		tpch_dir = &mem[160];
		char line[80];
		fgets (line, 80, fp);
		sscanf (line, "%s\n", catalog_path);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", dbfile_dir);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", tpch_dir);
		fclose (fp);
		if (! (catalog_path && dbfile_dir && tpch_dir)) {
			cerr << " Test settings file 'test.cat' not in correct format.\n";
			free (mem);
			exit (1);
		}
	}
	else {
		cerr << " Test settings files 'test.cat' missing \n";
		exit (1);
	}
	cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
	cout << " catalog location: \t" << catalog_path << endl;
	cout << " tpch files dir: \t" << tpch_dir << endl;
	cout << " heap files dir: \t" << dbfile_dir << endl;
	cout << " \n\n";
}

void GetLoadPath(char *loadPath, char *table, char *prefix, char *extension){
  sprintf(loadPath, "%s%s.%s", prefix, table, extension);
}

void CreateTable(){

  string schema;
  string type;
  int size = (int) atts.size();

  ofstream create("catalog", ios::out | ios::app);

    // Put in the initial data
  schema = "BEGIN\n";
  schema.append(createTableName.c_str());
  schema.append("\n");
  schema.append(createTableName.c_str());
  schema.append(".tbl\n");

    // Add the attributes
  for (int i = 0; i < size; i++){
    schema.append(atts[i]);

    if (attTypes[i].compare("INTEGER") == 0){
      type = " Int";
    }
    else if (attTypes[i].compare("DOUBLE") == 0){
      type = " Double";
    }
    else {
      type = " String";
    }

    schema.append(type);
    schema.append("\n");

  }

    // Append END flag
  schema.append("END\n\n");

    // Write out data
  if (create.is_open() && create.good()){
    create << schema;
  } // end if catalog

  create.close();

  char *binary = new char[100];

    // Load the path to the binary files
  GetLoadPath(binary, (char*) createTableName.c_str(), dbfile_dir, "bin");

  fType ftype = heap;

  if (createTableType.compare("SORTED") == 0){
    ftype = sorted;
  }

  DBFile newTable;
  newTable.Create(binary, ftype, NULL);
  newTable.Close();

  delete[] binary;

}

void RemoveTableFromCatalog(string table){

  bool copyData = true;

  string line;
  string accumulator = "";
  ifstream catalog("catalog");
  ofstream new_catalog("new_catalog");

  if (catalog.is_open()){

    while (catalog.good()){

      getline(catalog, line);

      if (line.compare("BEGIN") == 0 && accumulator.compare("") != 0){

	if (new_catalog.good() && copyData){
	  new_catalog << accumulator;
	} // end if new_catalog

	accumulator = "";
	accumulator.append(line);
	accumulator.append("\n");

      }
      else if (copyData == false && line.compare("END") == 0){
	copyData = true;
	continue;
      }
      else if (line.compare(table) == 0){
	accumulator = "";
	copyData = false;
      }
      else if (copyData){
	accumulator.append(line);
	accumulator.append("\n");
      }

    } // end while catalog

    if (new_catalog.good() && copyData){
      new_catalog << accumulator;
    } // end if new_catalog

    rename("new_catalog", "catalog");

    catalog.close();

      // Delete binary file
    char *binary = new char[100];
      // Load the paths to the files
    GetLoadPath(binary, (char*)table.c_str(), dbfile_dir, "bin");
    remove(binary);
    delete[] binary;

  } // end if catalog


}

int main () {

  bool quit = false;

    // Keep track of current output values
  int output = PIPE_STDOUT;
  string outputFile = "";

  DBFile table;
  char *loadPath;

    // Load the default TPC-H and Bin folder paths
  ReadInFileLocations();

    cout << "Please enter a query and press <Ctrl-D> 3 times "
	 << "or <Enter> and <Ctrl-D> to execute...\n" << endl;

    // Keep going until shutdown occcurs
  while (!quit){

    cout << "ready to enter a query!" << endl;

      // Parse the input
    yyparse();

      // Perform a SELECT
    if (queryType == QUERY_SELECT) {
	selectQuery(output, outputFile);
    }

      // Perform a CREATE TABLE
    else if (queryType == QUERY_CREATE) {

      CreateTable();

      cout << "table '" << createTableName.c_str()  << "' has been created.";

      atts.clear();
      attTypes.clear();

    }

      // Perform an INSERT
    else if (queryType == QUERY_INSERT) {

      loadPath = new char[100];
      char *file = new char[100];

	// Load the paths to the files
      GetLoadPath(loadPath, io->src, dbfile_dir, "bin");

      Schema toLoad("catalog", io->src);

      ifstream testFile(io->file);

      if (testFile.is_open()){

	  // Open the database, load the data, and close the database
	table.Open(loadPath);
	table.Load(toLoad, file);
	table.Close();

	cout << "data successfully loaded into " << io->src
	     << " from " << io->file <<  "." << endl;

      }
      else {
	cout << "could not open " << io->file << ", load not done!" << endl;
      }

      testFile.close();

      free(io);

      delete[] loadPath;
      delete[] file;

    }

      // Perform a DROP TABLE
    else if (queryType == QUERY_DROP) {

	// Remove table from catalog and remove .bin file
      RemoveTableFromCatalog(tables->tableName);

      cout << "table '" << tables->tableName << "' has been dropped.";

      free(tables);

    }

      // Perform a SET OUTPUT
    else if (queryType == QUERY_SET) {

      output = io->type;

      if (output == PIPE_FILE){
	outputFile = io->file;
      }

      free(io);

    }

      // Quit the program
    else if (queryType == QUERY_QUIT){
      quit = true;
      cout << "shutting down..." << endl;
    }

    if (queryType != QUERY_QUIT){
      cout << "\n" << endl;
    }

  } // end while !quit

  return 0;

} // end main

