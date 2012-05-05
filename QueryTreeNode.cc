#include "Schema.h"
#include "Function.h"
#include "Comparison.h"
#include "QueryTreeNode.h"

#include "SelectFile.h"
#include "SelectPipe.h"
#include "Join.h"
#include "GroupBy.h"
#include "Project.h"
#include "Sum.h"
#include "DuplicateRemoval.h"
#include "WriteOut.h"

#include <iostream>
#include <vector>

using namespace std;
/*
using std::clog;
using std::endl;
using std::vector;
*/

QueryTreeNode::QueryTreeNode(){

  lChildPipeID = 0;
  rChildPipeID = 0;
  outPipeID = 0;

  parent = NULL;
  left = NULL;
  right = NULL;

  attsToKeep = NULL;
  numAttsToKeep = NULL;

  cnf = NULL;
  opCNF = NULL;
  schema = NULL;
  order = NULL;
  funcOp = NULL;
  func = NULL;
  literal = NULL;

	sf = NULL;
	sp = NULL;
	j = NULL;
	gb = NULL;
	p = NULL;
	s = NULL;
	d = NULL;
	h = NULL;

	db = NULL;


}


QueryTreeNode::~QueryTreeNode(){

}


QueryNodeType QueryTreeNode::GetType(){
  return type;
}

void QueryTreeNode::SetType(QueryNodeType setter){

	type = setter;

	outPipe = new Pipe(P_SIZE);

	switch (type){

	    case SELECTF:
		//string form = lookupKey(path);
		db = new DBFile();
		
		db->Open((char*)(path.c_str()));

		sf = new SelectFile();
		
		break;

	    case SELECTP:

		sp = new SelectPipe();

		opCNF = new CNF();
			opCNF->GrowFromParseTree(cnf, schema, *literal);

	      break;

	    case PROJECT:

		p = new Project();

		numAttsToKeep = (int) aTK.size();
		attsToKeep = new int[numAttsToKeep]; //Need to set this to the, well, indicies of the attributes we're keeping

		for (int i = 0; i < numAttsToKeep; i++){
		  attsToKeep[i] = aTK[i];
		}

	      break;

	    case JOIN:

	      j = new Join();

	      lInputPipe = new Pipe(P_SIZE);
	      rInputPipe = new Pipe(P_SIZE);

	      break;

	    case SUM:

	      s = new Sum();

	      lInputPipe = new Pipe(P_SIZE);

	      break;

	    case GROUP_BY:

	      gb = new GroupBy();

	      lInputPipe = new Pipe(P_SIZE);

	      break;

	    case DISTINCT:

	      d = new DuplicateRemoval();

	      lInputPipe = new Pipe(P_SIZE);

	      break;

	    case WRITE:

	      break;
	  } // end switch
}

/*

RUN FUNCTION

*/

void QueryTreeNode::RunInOrder(){
	if(NULL != left){
		left->RunInOrder();
	}
	if(NULL != right){
		right->RunInOrder();
	}
	Run();
	WaitUntilDone();
}

void QueryTreeNode::Run(){

	switch (type){

		    case SELECTF:
			sf->Run(*db, *outPipe, *opCNF, *literal);
		      break;

		    case SELECTP:
			sp->Run(*lInputPipe, *outPipe, *opCNF, *literal);
		      break;

		    case PROJECT:
			p->Run(*lInputPipe, *outPipe, attsToKeep, numAttsIn, numAttsOut);
		      break;

		    case JOIN:

		      break;

		    case SUM:

		      break;

		    case GROUP_BY:

		      break;

		    case DISTINCT:

		      break;

		    case WRITE:

		      break;
		  } // end switch

}

void QueryTreeNode::WaitUntilDone(){
	switch (type){

		    case SELECTF:
			sf->WaitUntilDone();
		      break;

		    case SELECTP:
			sp->WaitUntilDone();
		      break;

		    case PROJECT:
			p->WaitUntilDone();
		      break;

		    case JOIN:

		      break;

		    case SUM:

		      break;

		    case GROUP_BY:

		      break;

		    case DISTINCT:

		      break;

		    case WRITE:

		      break;
	} // end switch
}


/*

PRINT FUNCTIONS

*/

std::string QueryTreeNode::GetTypeName(){

  string name;

  switch (type){

    case SELECTF:
      name = "SELECT FILE";
      break;

    case SELECTP:
      name = "SELECT PIPE";
      break;

    case PROJECT:
      name = "PROJECT";
      break;

    case JOIN:
      name = "JOIN";
      break;

    case SUM:
      name = "SUM";
      break;

    case GROUP_BY:
      name = "GROUP BY";
      break;

    case DISTINCT:
      name = "DISTINCT";
      break;

    case WRITE:
      name = "WRITE";
      break;
  } // end switch

  return name;

} // end GetTypeName


void QueryTreeNode::PrintInOrder(){
	if(NULL != left){
		left->PrintInOrder();
	}
	if(NULL != right){
		right->PrintInOrder();
	}
	PrintNode();
}

void QueryTreeNode::PrintNode(){
  clog << " *********** " << endl;
  clog << GetTypeName() << " operation" << endl;

  switch (type){

    case SELECTF:

	//clog << "SELECT FILE OPERATION" << endl;
	clog << "INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT PIPE " << outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;
	schema->Print();
	PrintCNF();
      break;

    case SELECTP:
	//clog << "SELECT PIPE OPERATION" << endl;
	clog << "INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT PIPE " << outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;
	schema->Print();
	clog << "SELECTION CNF :" << endl;
	PrintCNF();
      break;

    case PROJECT:
	clog << "INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT PIPE "<< outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;
	schema->Print();
	clog << endl << "************" << endl;
//      PrintCNF();
      break;

    case JOIN:
	clog << "LEFT INPUT PIPE " << lChildPipeID << endl;
	clog << "RIGHT INPUT PIPE " << rChildPipeID << endl;
	clog << "OUTPUT PIPE " << outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;
	schema->Print();
	clog << endl << "CNF: " << endl;
	PrintCNF();
      break;

    case SUM:
	clog << "LEFT INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT PIPE " << outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;
	schema->Print();
	clog << endl << "FUNCTION: " << endl;
	PrintFunction();
      break;

    case DISTINCT:
	clog << "LEFT INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT PIPE " << outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;	
	schema->Print();
	clog << endl << "FUNCTION: " << endl;
	PrintFunction();
	break;

    case GROUP_BY:
	clog << "LEFT INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT PIPE " << outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;	
	schema->Print();
	clog << endl << "GROUPING ON " << endl;
	order->Print();
	clog << endl << "FUNCTION " << endl;
	PrintFunction();
      break;

    case WRITE:
	clog << "LEFT INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT FILE " << path << endl;
      break;

  } // end switch type

} // end Print


void QueryTreeNode::PrintCNF(){

  if (cnf){

    struct AndList *curAnd = cnf;
    struct OrList *curOr;
    struct ComparisonOp *curOp;

      // Cycle through the ANDs
    while (curAnd){

      curOr = curAnd->left;

      if (curAnd->left){
	clog << "(";
      }

	// Cycle through the ORs
      while (curOr){

	curOp = curOr->left;

	  // If op is not null, then parse
	if (curOp){

	  if (curOp->left){
	    clog << curOp->left->value;
	  }

	  //clog << endl << "CUR OP CODE IS " << curOp->code << endl;
	  switch (curOp->code){
	    case 5:
	      clog << " < ";
	      break;

	    case 6:
	      clog << " > ";
	      break;

	    case 7:
	      clog << " = ";
	      break;

	  } // end switch curOp->code

	  if (curOp->right){
	    clog << curOp->right->value;
	  }


	} // end if curOp

	if (curOr->rightOr){
	  clog << " OR ";
	}

	curOr = curOr->rightOr;

      } // end while curOr

      if (curAnd->left){
	clog << ")";
      }

      if (curAnd->rightAnd) {
	clog << " AND ";
      }

      curAnd = curAnd->rightAnd;

    } // end while curAnd

  } // end if parseTree
	clog << endl;

}

void QueryTreeNode::PrintFunction(){
	func->Print();
}


/*
GENERATOR FUNCTIONS (SCHEMA & FUNCTION & GROUP BY)
*/

void QueryTreeNode::GenerateSchema(){
	//Here, the schema is basically a shoved together version of the previous two schemas
	//clog << "Attempting to generate schema" << endl;
	Schema *lChildSchema = left->schema;
	Schema *rChildSchema = right->schema;
	
	schema = new Schema(lChildSchema, rChildSchema);
}

void QueryTreeNode::GenerateFunction(){
	//clog << "Attempting to grow parse tree " << endl;
	func = new Function();
	//clog << "Main thing I'm looking for is at: " << funcOp->leftOperand->value << endl;

	func->GrowFromParseTree(funcOp, *schema);
}

void QueryTreeNode::GenerateOM(int numAtts, vector<int> whichAtts, vector<int> whichTypes){
			/*
				char *str_sum = "(ps_supplycost)";
				get_cnf (str_sum, &join_sch, func);
				func.Print ();
				OrderMaker grp_order;
				grp_order.numAtts = 1;
				grp_order.whichAtts[0] = 3;
				grp_order.whichTypes[0] = Int;
		*/

		order = new OrderMaker();
		
		order->numAtts = numAtts;
		for(int i = 0; i < whichAtts.size(); i++){
			order->whichAtts[i] = whichAtts[i];
			order->whichTypes[i] = (Type)whichTypes[i];
		}
		
		//cout << "GENERATED " << endl;
		//order->Print();
}
