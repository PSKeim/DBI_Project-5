#ifndef ParseFunc_Extension
#define ParseFunc_Extension

#define PIPE_STDOUT 1
#define PIPE_NONE 2
#define PIPE_FILE 3

#include <vector>
#include <string>

using std::vector;
using std::string;

struct InOutPipe {

	// this indicates if the output is PIPE_STDOUT, PIPE_NONE, or
	// a specific file
	int type;

        // this indicates the file to which the output should go.
        // it has dummy information if type is PIPE_STDOUT or
        // PIPE_NONE
	char *file;

        // this indicates from where the output should come.
        // it has dummy information if type is PIPE_STDOUT or
        // PIPE_NONE
	char *src;

};

struct CreateTableArgs {

	// This is the name of the table
	string name;

        // Keep track of all the attribute names
  //	vector<string> atts;

        // Keep track of all the attribute types
  //	vector<string> attTypes;

	// What type of database file?
	string dbType;

};

#endif
