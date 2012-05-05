#include <map>
#include <vector>
#include <iostream>
#include <assert.h>
#include <string.h>

#include "QueryTree.h"
#include "ParseTree.h"
#include "ParseTreeExtension.h"
#include "QueryTreeNode.h"
#include "Statistics.h"


using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;// 1 if there is a DISTINCT in an aggregate query

extern int queryType;
extern struct InOutPipe *io;
extern struct CreateTableArgs *createArgs;
extern string createTableName;
extern string createTableType;

extern vector<string> atts;
extern vector<string> attTypes;

extern char *catalog_path;
extern char *dbfile_dir;
extern char *tpch_dir;



void GetLoadPath(char *loadPath, char *table, char *prefix, char *extension);

void GetTables(vector<string> &relations){

  TableList *list = tables;

  while (list){

    if (list->aliasAs){
      relations.push_back(list->aliasAs);
	//clog << "Pushed back " << list->aliasAs << endl;
    }
    else {
      relations.push_back(list->tableName);
	//clog << "Pushed back " << list->tableName << endl;
    }

    list = list->next;

  } // end while list

} // end GetTables


void GetJoinsAndSelects(vector<AndList> &joins, vector<AndList> &selects, vector<AndList> &joinDepSel, Statistics s){

 // struct AndList curAnd = *boolean;
  struct OrList *curOr;


  while (boolean != 0){

    curOr = boolean->left;

    if (curOr && curOr->left->code == EQUALS && curOr->left->left->code == NAME
	&& curOr->left->right->code == NAME){
	AndList newAnd = *boolean;
	newAnd.rightAnd = 0;
	joins.push_back(newAnd);
    }
    else {
	curOr = boolean->left;
	if(0 == curOr->left){ //Either a (x.y = z.y) join, handled above, or a (something = blah), can only be on one table. No worries
		AndList newAnd = *boolean;
		newAnd.rightAnd = 0;
		selects.push_back(newAnd);
	}
	else{
		//Otherwise, this might be a (a.b = c OR d.e = f) situation
		vector<string> involvedTables;
		//cout << "Potential multiple tables involved!" << endl;
		
		while(curOr != 0){
			Operand *op = curOr->left->left;
			if(op->code != NAME){
				op = curOr->left->right;
			}
			string rel;
			s.ParseRelation(op, rel);
			//cout << "Parsed out relation " << rel << endl;
			if(involvedTables.size() == 0){
				involvedTables.push_back(rel);
			}
			else if(rel.compare(involvedTables[0]) != 0){
					involvedTables.push_back(rel);
			}
	
			curOr = curOr->rightOr;
		}

		if(involvedTables.size() > 1){
			AndList newAnd = *boolean;
			newAnd.rightAnd = 0;
			joinDepSel.push_back(newAnd);
		}
		else{
			AndList newAnd = *boolean;
			newAnd.rightAnd = 0;
			selects.push_back(newAnd);
		}		
	}	
    }

    boolean = boolean->rightAnd;

  } // end while curAnd

} // end GetJoinsAndSelect


/**
 
 JOIN OPTIMIZATION(ish) FUNCTION
 
 **/

vector<AndList> optimizeJoinOrder(vector<AndList> joins,  Statistics *s){
	
	vector<AndList> newOrder;
	newOrder.reserve(joins.size());
	AndList join;
	
	double smallest = -1.0;
	double guess = 0.0;
	
	string rel1;
	string rel2;
	int i = 0;
	int smallestIndex = 0;
	int count = 0;
	vector<string> joinedRels;
	
	//Okay, we're going to repeat ourselves until joins.size = 1 (because then we know what the largest join is
	//clog << "Joins size is " << joins.size() << endl;
	while(joins.size() > 1){
		//clog << "ENTERING WHILE LOOP" << endl;
		while(i < joins.size()){
			//cout << "Iterating over " << joins.size() << " joins" << endl;
			join = joins[i];
			
			s->ParseRelation(join.left->left->left, rel1);
			s->ParseRelation(join.left->left->right, rel2);	
			//clog << "Rel1 is " << rel1 << " Rel2 is " << (char*)rel2.c_str() << endl;
			if(smallest == -1.0){
				char* rels[] = {(char*)rel1.c_str(), (char*)rel2.c_str()};
				smallest = s->Estimate(&join, rels, 2);
				//clog << "Smallest estimate is starting at " << smallest << " tuples" << endl;
				smallestIndex = i;
			}
			else{
				char* rels[] = {(char*)rel1.c_str(), (char*)rel2.c_str()};
				guess = s->Estimate(&join, rels, 2);
				if(guess < smallest){
					smallest = guess;
					//clog << "Smallest estimate is now " << smallest << " tuples" << endl;
					smallestIndex = i;
				}
			}
			//clog << "Incrementing i" << endl;
			i++;
		}
		//clog << "Exited the loop" << endl;
		//clog << "SMALLEST POSSIBLE JOIN WAS FOUND TO HAVE " << smallest << " RECORDS AS A UNION BETWEEN " << rel1 << " AND " << rel2 << endl;
		joinedRels.push_back(rel1);
		joinedRels.push_back(rel2);
		newOrder.push_back(joins[smallestIndex]);
		count++;
		smallest = -1.0;
		i = 0;
		joins.erase(joins.begin()+smallestIndex);
	}
	
	//When joins is size 1, the last one in it must be the largest join, so we just have to do:
	
	newOrder.insert(newOrder.begin()+count, joins[0]);
	//clog << "NewOrder size is " << newOrder.size() << endl;
	
	return newOrder;
}



void selectQuery (int output, string outputFile) {

	
	int pipeID = 1;
	
	Statistics *s = new Statistics();
	s->Read("tcp-h_Statistics.txt");
	//initStatistics(s);
	//clog << "Statistics has supposedly read in something. Maybe." << endl;
	
	/*clog << "Welcome to KA Database System" << endl;
	clog << "*****************************" << endl;
	clog << "All important files are assumed to exist in this directory" << endl;
	clog << "~%: ";*/
	map<string, double> joinCosts;
	
	vector<string> relations;
	
	vector<AndList> joins;
	vector<AndList> selects;
	vector<AndList> joinDepSels;
	string projectStart; //I use this later, sorta hacky.
	
	//yyparse();
	
	// Get the table names from the query
	GetTables(relations);
	
	GetJoinsAndSelects(joins, selects, joinDepSels, *s);
	
	clog << endl << "Number of selects: " << selects.size() << endl;
	clog << "Number of joins: " << joins.size() << endl;
	clog << "Number of join dependent selects: " << joinDepSels.size() << endl << endl;
	
	//assert(0==1); //USED TO TEST THE ABOVE 3 STATEMENTS!
	
	map<string, QueryTreeNode*> leafs;
	QueryTreeNode *insert = NULL; //holder variable for when we need to insert stuff.
	QueryTreeNode *traverse;
	QueryTreeNode *topNode = NULL;
	/*
	 
	 What to do here:
	 Create the n leaf nodes of the tree
	 
	 How:
	 - We have the list of tables that are in the tree (and their aliases, which one will we use as the key? I dunno)
	 - So we iterate over that, and make a node (Select File) for each of these
	 - Once we have that, we can move on to doing the selects
	 */
	
	TableList *iterTable = tables;
	
	//clog << "Generating SF Nodes" << endl;
	while(iterTable != 0){
		if(iterTable->aliasAs != 0){
			leafs.insert(std::pair<string,QueryTreeNode*>(iterTable->aliasAs, new QueryTreeNode()));
			s->CopyRel(iterTable->tableName, iterTable->aliasAs);
			//clog << "Copied statistics object from " << iterTable->tableName << " to " << iterTable->aliasAs << endl;
			//		clog << "Node generated for " << iterTable->aliasAs << endl;
		}
		else{
			leafs.insert(std::pair<string,QueryTreeNode*>(iterTable->tableName, new QueryTreeNode()));
			//		clog << "Node generated for " << iterTable->tableName << endl;
		}
		//Here we need to insert code that makes insert into a "Select File" type node
		//and deals with the relevant information
		
		insert = leafs[iterTable->aliasAs];
		
		//customization of the node
		insert->schema = new Schema ("catalog", iterTable->tableName);
		if(iterTable->aliasAs != 0){
			insert->schema->updateName(string(iterTable->aliasAs));
		}
		
		
		topNode = insert;
		insert->outPipeID = pipeID++;
		
		char binPath[100];
		GetLoadPath(binPath, iterTable->tableName, dbfile_dir, "bin");
		
		//string base (iterTable->tableName);
		//string path ("bin/"+base+".bin");
		
		insert->path = binPath;
		
		insert->SetType(SELECTF);
		
		//insert->PrintNode();
		iterTable = iterTable->next;	
	}
	//	clog << "Done generating SF Nodes" << endl;	
	
	//Code above is not guaranteed to work!
	//Standard debugging thingies apply
	
	AndList selectIter;
	string table;
	string attribute;
	//After that, we can iterate over the selects
	//clog << "Generating S nodes" << endl;
	for(unsigned i = 0; i < selects.size(); i++){
		selectIter = selects[i];
		//	clog << "Is the problem before this?" << endl;
		if(selectIter.left->left->left->code == NAME){
			s->ParseRelation(selectIter.left->left->left, table);
		}
		else{
			s->ParseRelation(selectIter.left->left->right, table);
		}
		
		clog << "Select on " << table << endl;
		
		traverse = leafs[table]; //Get the root node (Select File)
		projectStart = table;
		while(traverse->parent != NULL){
			traverse = traverse->parent;
		}
		insert = new QueryTreeNode();
		//clog << "Select node on " << attribute << " generated." << endl;
		//clog << "Selection was on " << selectIter.left->left->left->value << "." << endl;
		/*
		 insert customization here
		 Traverse's new parent is insert
		 Insert's left child is traverse
		 Insert's CNF is the AND list in selectIter
		 
		 */
		traverse->parent = insert;
		insert->left = traverse;
		//clog << "New node is inserted." << endl;
		//Customizing select node:
		insert->schema = traverse->schema; //Schemas are the same throughout selects, only rows change

		//clog << "Schema is inserted." << endl;
		insert->cnf = &selects[i]; //Need to implement CreateCNF in QueryTreeNode
		insert->lChildPipeID = traverse->outPipeID;
		insert->lInputPipe = traverse->outPipe;
		insert->outPipeID = pipeID++;
		
		insert->SetType(SELECTP);
		
		//Because we made a selection, this may impact our decision on which tables to choose first for the join order
		//So we should update the statistics object to reflect the change
		char *statApply = strdup(table.c_str());
		//clog << "Applying select to the statistics object" << endl;
		//clog << "Should result in "<< s->Estimate(&selectIter, &statApply,1) << " tuples in " << table << endl;
		s->Apply(&selectIter, &statApply,1);
		//clog << "Applying second select to test" << endl;
		//clog << "Should result in "<< s->Estimate(&selectIter, &statApply,1) << " tuples in " << table << endl; //testing to see if apply was actually applied
		
		topNode = insert;
		//clog << "CNF has been created." << endl;
		//	insert->PrintNode();
	}
	//	assert(0==1);
	//clog << "Done generating S Nodes" << endl;
	
	/*
	 So at this point, we've got the initial selects and the non-initial selects out of the way.
	 What do we have to do now?
	 We have to do the joins...
	 Which means we have to optimize the joins
	 */
	
	//Function that optimizes join stuff here
	
	
	if(joins.size() > 1){
		joins = optimizeJoinOrder(joins, s);
	}
	
	//assert(0==1);
	//Now we have to add the joins to the tree
	QueryTreeNode *lTableNode;
	QueryTreeNode *rTableNode;
	AndList curJoin;
	string rel1;
	string rel2;
	for(unsigned i = 0; i < joins.size(); i++){
		//clog << "Generating Join nodes" << endl;
		curJoin = joins[i];
		//Okay, what do we need to know?
		//We need to know the two relations involved
		//AND->OR->COM->OP->VALUE
		//So curJoin->left->left->left->value
		// and curJoin->left->left->right->value
		rel1 = "";//curJoin.left->left->left->value;
		s->ParseRelation(curJoin.left->left->left, rel1);
		rel2 = "";//curJoin.left->left->right->value;
		s->ParseRelation(curJoin.left->left->right, rel2);
		//clog << "Join on " << rel1 << " and " << rel2 << "."<<endl;
		table = rel1; //done for testing purposes. will remove later
		
		//So, now we can get the top nodes for each of these
		lTableNode = leafs[rel1];
		rTableNode = leafs[rel2];
		while(lTableNode->parent != NULL) lTableNode = lTableNode->parent;
		while(rTableNode->parent != NULL) rTableNode = rTableNode->parent;
		
		//At this point, we have the top node for the left, and for the right
		//Now we join! MWAHAHAHA
		
		insert = new QueryTreeNode();
		
		insert->type = JOIN;
		insert->lChildPipeID = lTableNode->outPipeID;
		insert->rChildPipeID = rTableNode->outPipeID;
		insert->outPipeID = pipeID++;
		insert->cnf = &joins[i];
		
		insert->left = lTableNode;
		insert->right = rTableNode;
		
		lTableNode->parent = insert;
		rTableNode->parent = insert;
		
		//clog << "Basic customization done. Now attempting to generate schema." << endl;
		//Okay, now we have to deal with the schema
		insert->GenerateSchema();
		
		//	clog << "Schema generated, now attemping to print the node." << endl;		
		//insert->PrintNode();
		topNode = insert;	
	}
	
	//clog << "Printing an in-order of pre join-dep-sels and such tree" << endl;
	
	
	//	clog << "Done generating join nodes" << endl;
	
	//assert(0==1);
	
	//clog << "Generating Join Dependent Select nodes" << endl;
	for(unsigned i = 0; i < joinDepSels.size(); i++){
		/*selectIter = joinDepSels[i];
		 if(selectIter->left->left->left->code == NAME){
		 s.ParseRelation(selectIter->left->left->left, table);
		 }
		 else{
		 s.ParseRelation(selectIter->left->left->right, table);
		 }
		 
		 clog << "Select on " << table << endl;
		 
		 traverse = leafs[table]; //Get the root node (Select File)
		 projectStart = table;
		 while(traverse->parent != NULL){
		 traverse = traverse->parent;
		 }*/
		
		traverse = topNode;
		insert = new QueryTreeNode();
		
		/*
		 insert customization here
		 Traverse's new parent is insert
		 Insert's left child is traverse
		 Insert's CNF is the AND list in selectIter
		 
		 */
		traverse->parent = insert;
		insert->left = traverse;
		//clog << "New node is inserted." << endl;
		//Customizing select node:
		insert->schema = traverse->schema; //Schemas are the same throughout selects, only rows change
		insert->type = SELECTP;
		//clog << "Schema is inserted." << endl;
		insert->cnf = &joinDepSels[i]; //Need to implement CreateCNF in QueryTreeNode
		insert->lChildPipeID = traverse->outPipeID;
		insert->outPipeID = pipeID++;
		topNode = insert;
		//clog << "CNF has been created." << endl;
		//	insert->PrintNode();
	}
	
	//clog << "Done generating Join Dependent Select Nodes" << endl;
	
	
	
	
	if(0 != finalFunction) { //Okay, So if we have a final function, we need to throw it up there
		
		/*
		 extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
		 extern int distinctFunc;// 1 if there is a DISTINCT in an aggregate query
		 
		 */
		if(0 != distinctFunc){
			insert = new QueryTreeNode();
			//clog << "Creating distinct node" << endl;
			/*
			 We need to set: Type, Left, Pipe IDs, FunctionOp, Schema. Anything else? Don't think so.
			 */
			insert->type = DISTINCT;
			insert->left = topNode;
			insert->lChildPipeID = topNode->outPipeID;
			insert->outPipeID = pipeID++;
			insert->schema = topNode->schema;
			topNode->parent = insert;
			topNode = insert;		
		}
		
		/**
		 Morgan:  so, you check for grouping atts is not null, and you make a group node.
		 you should already know you have a function, so put that function in the node.
		 **/
		
		if(0 == groupingAtts){
			insert = new QueryTreeNode();
			//clog << "Creating function node" << endl;
			insert->type = SUM;
			insert->left = topNode;
			topNode->parent = insert;
			insert->lChildPipeID = topNode->outPipeID;
			insert->outPipeID = pipeID++;
			insert->funcOp = finalFunction;
			insert->schema = topNode->schema;
			//clog << "Function schema is " << endl;
			//insert->schema->Print();
			//Now we need to generate the function
			insert->GenerateFunction();
		}
		else{		
			insert = new QueryTreeNode();
			//Standard customization
			insert->type = GROUP_BY;
			insert->left = topNode;
			topNode->parent = insert;
			insert-> lChildPipeID = topNode->outPipeID;
			insert->outPipeID = pipeID++;
			insert->schema = topNode->schema;	
			
			//Group By customization
			insert->order = new OrderMaker();	
			
			int numAttsToGroup = 0;
			vector<int> attsToGroup;
			vector<int> whichType;
			
			NameList *groupTraverse = groupingAtts;
			while(groupTraverse){
				
				numAttsToGroup++;
				
				attsToGroup.push_back(insert->schema->Find(groupTraverse->name));
				whichType.push_back(insert->schema->FindType(groupTraverse->name));
				clog << "GROUPING ON " << groupTraverse->name << endl;
				
				groupTraverse = groupTraverse->next;
			}
			
			insert->GenerateOM(numAttsToGroup, attsToGroup, whichType);
			insert->funcOp = finalFunction;
			insert->GenerateFunction();
			clog << "Done generating Group By" << endl;
		}
		
		topNode = insert;
		
	}
	
	//clog << "Done generating distinct and function nodes" << endl;
	
	//extern struct NameList *groupingAtts;
	
	
	
	if(0 != distinctAtts){
		insert = new QueryTreeNode();
		clog << "Creating distinct node" << endl;
		/*
		 We need to set: Type, Left, Pipe IDs, FunctionOp, Schema. Anything else? Don't think so.
		 */
		insert->type = DISTINCT;
		insert->left = topNode;
		topNode->parent = insert;
		insert->lChildPipeID = topNode->outPipeID;
		insert->outPipeID = pipeID++;
		insert->schema = topNode->schema;
		
		topNode = insert;
	}
	
	
	
	//Now we need to do the project:
	//	clog << "Generating the project" << endl;
	if(attsToSelect != 0){ //If there's no atts to select, then there's no project that needs to be done. I don't think this ever happens.
		//clog << "Projection node insertion should go here." << endl;		
		//How do we find find the place to insert the project?
		// Simple, traverse up the tree until null parent is found. Has to be the top of the tree
		traverse = topNode;
		//clog << "Traverse is set to ... something " << traverse << endl;
		//clog << "Table is " << table << endl;
		//while(traverse->parent != 0) traverse = traverse->parent; //Move traverse up to the top of the tree
		//		clog << "Traversal finished" << endl;
		//Create and customize our project node		
		insert = new QueryTreeNode();
		//	clog << "Customizing node" << endl;
		
		insert->left = traverse;
		traverse->parent = insert;
		insert->lChildPipeID = traverse->outPipeID;
		insert->lInputPipe = traverse->outPipe;
		insert->outPipeID = pipeID++;
		
		vector<int> indexOfAttsToKeep; //Keeps the indicies that we'll be keeping
		Schema *oldSchema = traverse->schema;
		NameList *attsTraverse = attsToSelect;
		string attribute;
		
		insert->numAttsIn = traverse->schema->GetNumAtts();
		
		while(attsTraverse != 0){
			//clog << "Atts traverse provided " << attsTraverse->name << endl;
			//indexOfAttsToKeep.push_back(botSchema.find(attsTraverse->name))
			attribute = attsTraverse-> name;
			//clog << "*************Looking for attribute " << attribute << endl;
			indexOfAttsToKeep.push_back(oldSchema->Find(const_cast<char*>(attribute.c_str())));
			attsTraverse = attsTraverse->next;
		}
		
		//At the end of this, we've found the indicies of the attributes we want to keep from the old schema
		Schema *newSchema = new Schema(oldSchema, indexOfAttsToKeep);
		insert->schema = newSchema;
		insert->numAttsOut = insert->schema->GetNumAtts();
		clog << "Schema is inserted into insert, and is " << insert->schema << endl;
		insert->schema->Print();
		
		insert->SetType(PROJECT);

	}
	//	clog << "DONE WITH PROJECT NODE" << endl;

	if(output == PIPE_NONE){
		if(insert != NULL) topNode->PrintInOrder();
	}
	else if(output == PIPE_STDOUT){
		cout << "ATTEMPTING TO RUN TREE" << endl;
		topNode->RunInOrder();
	}
	else if(output == PIPE_FILE){
		cout << "EXECUTION FUNCTIONALITY IS BEING IMPLEMENTED. PLEASE EXCUSE MY FAIL." << endl;
		//Write out stuff here
	}
} // end SelectQuery

