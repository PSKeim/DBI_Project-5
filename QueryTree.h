#ifndef QUERY_TREE_H
#define QUERY_TREE_H

#include "Schema.h"
#include "Function.h"
#include "Comparison.h"
#include "QueryTreeNode.h"

class QueryTree {

 public:

	    QueryTree();
	    ~QueryTree();

	    void PrintTree();

// private:

	    QueryTreeNode *root;
//	    Schema schema;
	    Function aggregate;

	    double weight;

	    void PrintTreeInorder(QueryTreeNode *node);
	    void PrintTreeCountPipes();
	    void PrintTreeInorderCountPipes(QueryTreeNode *node, int &i);

};

#endif
