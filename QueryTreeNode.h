#ifndef QUERY_TREE_NODE_H
#define QUERY_TREE_NODE_H

#define P_SIZE 100

#include "Schema.h"
#include "Record.h"
#include "Function.h"
#include "Comparison.h"
#include "RelOp.h"
#include "SelectFile.h"
#include "SelectPipe.h"
#include "Join.h"
#include "GroupBy.h"
#include "Project.h"
#include "Sum.h"
#include "DuplicateRemoval.h"
#include "WriteOut.h"
#include "DBFile.h"
#include "HeapDB.h"
#include "SortedDB.h"
#include "InternalDB.h"

#include <vector>


enum QueryNodeType {SELECTF, SELECTP, PROJECT, JOIN, SUM, GROUP_BY, DISTINCT, WRITE};

class QueryTreeNode {

 public:

	      //REL OP SECTION:
		SelectFile *sf;
		SelectPipe *sp;
		Join *j;
		GroupBy *gb;
		Project *p;
		Sum *s;
		DuplicateRemoval *d;
		WriteOut *h;
	
		DBFile *db;

	      QueryNodeType type;
	      QueryTreeNode *parent;
	      QueryTreeNode *left;
	      QueryTreeNode *right;

	      //Pipe identifiers ( can be replaced by actual pipes later, or something)
		int lChildPipeID;
		Pipe *lInputPipe;
		int rChildPipeID;
		Pipe *rInputPipe;
		int outPipeID;
		Pipe *outPipe;

	      QueryTreeNode();
	 	~QueryTreeNode();

	    //PRINT FUNCTIONs
		void PrintInOrder();
	      	void PrintNode ();
	      	void PrintCNF();
		void PrintFunction();
	
		//INITIALIZATION FUNCTION (...why?)
		void SetType(QueryNodeType setter);
	
	
		//RUN FUNCTIONs
		void RunInOrder();
		void Run();
		void WaitUntilDone();

	      string GetTypeName ();
	      QueryNodeType GetType ();
		

		//Used for JOIN
		void GenerateSchema();
		//Used for SUM
		void GenerateFunction();
		//Used for GROUP_BY
		void GenerateOM(int numAtts, vector<int> whichAtts, vector<int> whichTypes);
		

// private:

		// For a PROJECT
		int numAttsIn;
		int numAttsOut;
		vector<int> aTK;
	  	int *attsToKeep;
	  	int numAttsToKeep;
		//NameList *projectAtts;
		// For various operations
	      AndList *cnf;
	      CNF *opCNF;
	      Schema *schema;
	      Record *literal;
		// For GROUP BY
	      OrderMaker *order;
		// For aggregate
	      FuncOperator *funcOp;
		Function *func;
		// For Write and SelectFile (identifies where files are located)
		string path;

		string lookupKey(string path);

	      vector<string> relations;

};

#endif
