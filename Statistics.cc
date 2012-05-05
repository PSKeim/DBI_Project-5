#include "Statistics.h"
#include "RelationStatistics.h"

#include <map>
#include <set>
#include <vector>
#include <string>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>

using std::map;
using std::set;
using std::cout;
using std::endl;
using std::vector;
using std::string;
using std::ofstream;
using std::ifstream;
using std::stringstream;

Statistics::Statistics() {

  tables["n"] = "nation";
  tables["r"] = "region";
  tables["p"] = "part";
  tables["s"] = "supplier";
  tables["ps"] = "partsupp";
  tables["c"] = "customer";
  tables["o"] = "orders";
  tables["l"] = "lineitem";

}

Statistics::Statistics(Statistics &copyMe) {

  relations = copyMe.relations;
  relSets = copyMe.relSets;
//  sets = copyMe.sets;

  tables = copyMe.tables;

}

Statistics::~Statistics() {

}

Statistics& Statistics::operator= (Statistics &s){

  if (this != &s){
    relations = s.relations;
    relSets = s.relSets;
//    sets = s.sets;

    tables = s.tables;
  }

  return *this;

}

void Statistics::AddRel(char *relName, int numTuples) {

  string relation(relName);

  if (Exists(relation)){
    relations[relation].UpdateRowCount((double)numTuples);
  }
  else {

    RelationStats stats((double) numTuples);
    RelationSet set(relation);

    relations[relation] = stats;
    sets.push_back(set);
    relSets[relation] = set;

  }

}

void Statistics::AddRel(char *relName, double numTuples) {

  string relation(relName);

  if (Exists(relation)){
    relations[relation].UpdateRowCount(numTuples);
  }
  else {

    RelationStats stats(numTuples);
    RelationSet set(relation);

    relations[relation] = stats;
    sets.push_back(set);
    relSets[relation] = set;

  }

}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {

  string relation(relName);
  string attribute(attName);

  if (Exists(relation)){
    relations[relation].AddAttribute(attribute, numDistincts);
  }

}

void Statistics::CopyRel(char *oldName, char *newName) {

  string oldRelation(oldName);
  string newRelation(newName);

  if (Exists(oldRelation)){

    relations[newRelation] = relations[oldRelation];

    RelationSet set(newName);
    sets.push_back(set);
    relSets[newRelation] = set;

  }

}

void Statistics::Read(char *fromWhere) {

  ifstream reader(fromWhere);

  if (reader.is_open()){

    map<string,RelationStats>::iterator iter;

    stringstream s;

    string line;
    string relation;

    int numStats = 0;
    int numSets = 0;

    int i = 0;

    if (reader.good()) {

      getline(reader, line);
      s.str(line);

      if (! (s >> numStats)){
	numStats = 0;
      }
      s.clear();
      s.str("");

      for (i = 0; i < numStats; i++){

	RelationStats stats;

	  // Get the first line
	getline(reader, line);

	  // If the first line is a newline, continue to the
	  // next line
	if (line.compare("") == 0){
	  getline(reader, line);
	}

	relation = line;

	  // Read in the statistics for this relation
	reader >> stats;

	  // Store the data
	relations[relation] = stats;	

      } // end for i

	// Get the first line
      getline(reader, line);

      while (line.compare("***") != 0){
	getline(reader, line);
      }

      getline(reader, line);

      getline(reader, line);
      s.str(line);

      if (! (s >> numSets)){
	numSets = 0;
      }

	// Get the first line
      getline(reader, line);

      for (i = 0; i < numSets; i++){

	RelationSet rs;
	vector<string> setRel;

	  // Read in the set for this relation
	reader >> rs;
	rs.GetRelations(setRel);
	sets.push_back(rs);

	vector<string>::iterator it = setRel.begin();

	for (; it != setRel.end(); it++){
	  relSets[*it] = rs;
	}

      } // end for i


    } // end while iter

    reader.close();

  } // end if writer.is_open()

}

void Statistics::Write(char *fromWhere) {

  ofstream writer(fromWhere);

  if (writer.is_open() && sets.size() > 0){

      // Get an iterator to all the RelationStats
    map<string,RelationStats>::iterator iterRel;

    writer << relations.size() << endl << endl;

      // Print out the statistics to the stream
    for (iterRel = relations.begin(); iterRel != relations.end(); iterRel++){
      if (writer.good()){
	writer << iterRel->first << endl << iterRel->second << endl;
      }
    } // end while iter


    writer << "***\n" << endl;
    writer << sets.size() << endl << endl;

      // Print out the sets to the stream
    int index = 0;
    int size = (int) sets.size();

    for (; index < size; index++) {
	writer << sets[index] << endl;
    }

    writer.close();

  } // end if writer.is_open()

}

void Statistics::Apply(struct AndList *parseTree,
			char *relNames[], int numToJoin) {

  RelationSet set;
  vector<int> indexes;

  vector<RelationSet> copy;

    // Create a new RelationSet represented by relNames
  for (int i = 0; i < numToJoin; i++){
   	//cout << "APPLY IS LOOKING AT " << relNames[i] << " IN THIS LOOP THINGY" << endl;
	 set.AddRelationToSet(relNames[i]);
  }

  double joinEstimate = 0.0;
  joinEstimate = ParseJoin(parseTree);

    // Run the estimation. We don't care about the value!
  double estimate = Guess(parseTree, set, indexes, joinEstimate);

  
  int index = 0;
  int oldSize = sets.size();
  int newSize = oldSize - (int) indexes.size() + 1;


    // If relNames spans multiple sets, consolidate them
  if (indexes.size() > 1){


      // Only copy over sets not spanned by set
    for (int i = 0; i < oldSize; i++){
      if (i == indexes[index]){
	index++;
      }
      else {
	copy.push_back(sets[i]);
      }
    } // end for int i

      // Clear the global sets list
    sets.clear();

      // Perform the copy
    for (int i = 0; i < newSize-1; i++){
      sets.push_back(copy[i]);
    }

    set.UpdateNumTuples(estimate);
    
	//cout << "Set updated to " << estimate << " tuples" << endl;
	sets.push_back(set);

    copy.clear();

    vector<string> relations;
    set.GetRelations(relations);

    for (int i = 0; i < set.Size(); i++){
      relSets[relations[i]] = set;
      AddRel((char*)relations[i].c_str(), estimate);
	//cout << "relations[i] is " << relations[i] << endl;
	//cout << "relSets[relations[i]] now has " << set << " tuples" << endl;
    }

  } // end if indexes.size()
  else{
 	AddRel(relNames[0], estimate);
 }
 
 //cout << "relSEts[s] has " << relSets["s"] << endl;
}

double Statistics::Estimate(struct AndList *parseTree,
			    char **relNames, int numToJoin) {

  double estimate = 0.0;

  RelationSet set;
  vector<int> indexes;

    // Create a RelationSet represented by relNames
  for (int i = 0; i < numToJoin; i++){
    set.AddRelationToSet(relNames[i]);
  }

  double joinEstimate = 0.0;
  joinEstimate = ParseJoin(parseTree);
  //cout << "Join Estimate is " << joinEstimate << endl;
    // Get the estimate
  estimate = Guess(parseTree, set, indexes, joinEstimate);
  return estimate;

}

double Statistics::Guess(struct AndList *parseTree, RelationSet toEstimate,
			    vector<int> &indexes, double joinEstimate){

  double estimate = 0.0;
/*
    // If parse tree has unknown attributes, exit the program
  if (!CheckParseTree(parseTree)){
    cout << "BAD: attributes in parseTree do not match any given relation!"
	 << endl;
    exit(1);
  }
*/
    // If the given set can be found in the existing sets, create an estimate
  if (CheckSets(toEstimate, indexes)) {
  // cout << "GENERATING ESTIMATE" << endl;
    estimate = GenerateEstimate(parseTree, joinEstimate);
  }

  return estimate;

}

bool Statistics::CheckSets(RelationSet toEstimate, vector<int> &indexes){

  int numRelations = 0;
  
  int intersect = 0;
  int index = 0;

  int size = (int) sets.size();

    // Iterate through all the sets
  for (; index < size; index++){

      // Compute the intersect value
    intersect = sets[index].Intersect(toEstimate);

      // If the computation returned -1, stop - the join is not feasible
    if (intersect == -1){
      indexes.clear();
      numRelations = 0;
      break;
    }

      // Else if computation returned a positive int, keep track of
      // how many relations in the set have been matched
    else if (intersect > 0) {

      numRelations += intersect;
      indexes.push_back(index);

      if (numRelations == toEstimate.Size()){
	break;
      } // end if numRelations

    } // else if intersect

  } // end for index

  return (numRelations > 0);

}

bool Statistics::CheckParseTree(struct AndList *parseTree){

  if (parseTree){

    struct AndList *curAnd = parseTree;
    struct OrList *curOr;
    struct ComparisonOp *curOp;

      // Cycle through the ANDs
    while (curAnd){

      curOr = curAnd->left;

      if (curAnd->left){
	cout << "(";
      }

	// Cycle through the ORs
      while (curOr){

	curOp = curOr->left;

	  // If op is not null, then parse
	if (curOp){

	  if (curOp->left){
	    cout << curOp->left->value;
	  }


	  switch (curOp->code){
	    case 1:
	      cout << " < ";
	      break;

	    case 2:
	      cout << " > ";
	      break;

	    case 3:
	      cout << " = ";
	      break;

	  } // end switch curOp->code

	  if (curOp->right){
	    cout << curOp->right->value;
	  }


	} // end if curOp

	if (curOr->rightOr){
	  cout << " OR ";
	}

	curOr = curOr->rightOr;

      } // end while curOr

      if (curAnd->left){
	cout << ")";
      }

      if (curAnd->rightAnd) {
	cout << " AND ";
      }

      curAnd = curAnd->rightAnd;

    } // end while curAnd

  } // end if parseTree


  return true;

}

double Statistics::ParseJoin(struct AndList *parseTree){

  double value = 0.0;
  double dummy = 0.0;

  if (parseTree){

    struct AndList *curAnd = parseTree;
    struct OrList *curOr;
    struct ComparisonOp *curOp;

    bool stop = false;

    string relation1, relation2, attribute1, attribute2;
    RelationStats r1, r2;

      // Cycle through the ANDs
    while (curAnd && !stop){
      curOr = curAnd->left;

	// Cycle through the ORs
      while (curOr && !stop){
	curOp = curOr->left;

	  // If op is not null, then parse
	if (curOp){

	  if (curOp->code == EQUALS && curOp->left->code == curOp->right->code){

	    stop = true;

	      // Get the relation names and the attributes
	    ParseRelationAndAttribute(curOp->left, relation1, attribute1);
	    ParseRelationAndAttribute(curOp->right, relation2, attribute2);

	      // Get the relevant statistics
	    r1 = relations[relation1];
	    r2 = relations[relation2];

	      // join algorithm: |A|*|B|* (1/max(v(a),v(b)))
	    value = GetRelationCount(relation1, dummy) *
		    GetRelationCount(relation2, dummy);
		//cout << "r1 " << r1 << " :" << GetRelationCount(relation1, dummy) << endl;
		//cout << "r2 " << r2 << " :" << GetRelationCount(relation2, dummy) << endl;
	    if (r1[attribute1] >= r2[attribute2]){
	      value /= (double) r1[attribute1];
	    }
	    else {
	      value /= (double) r2[attribute2];
	    }

	  }

	} // end if curOp

	curOr = curOr->rightOr;

      } // end while curOr

      curAnd = curAnd->rightAnd;

    } // end while curAnd

  } // end if parseTree

  //cout << "Estimate Join returning " << value << endl;
  return value;

}

double Statistics::GenerateEstimate(struct AndList *parseTree, double joinEstimate){
	double estimate = 1.0; //Final estimation storage variable
	if(joinEstimate > 0){
		estimate = joinEstimate;
	}
	//Traversal Variables
	struct AndList *curAnd = parseTree;
  	struct OrList *curOr;
	struct ComparisonOp *curOp;
	//Relation stuff variables
  	RelationStats r1;
	string relation1;
	string attribute1;
	//Keeps track of shit! 
	bool hasJoin = false; //If I've seen a join (technically I could set this to joinEstimate > 0, but I added that in later, so...)
	long numTups = 0.0l; //Number of tuples in the relationship

	while(curAnd){ //Iterate through the andList

		curOr = curAnd->left;

		bool independentOr = CheckIndependence(curOr);
		/*if(independentOr){
			cout << "The or is independent" << endl;
		}
		else{
			cout << "The or is dependent" << endl;
		}*/
		bool singleOr = (curOr ->rightOr == NULL);
		/*if(singleOr){
			cout << "The or is a single or" << endl;
		}
		else{
			cout << "The or is not a single or" << endl;
		}*/

		//Temp var to hold result of each or
		double tempOr = 0.0;
		if(independentOr) tempOr = 1.0;
		while(curOr){ //iterate through the orList
			curOp = curOr->left;
			Operand *op = curOp->left;

			if (op->code != NAME){ //If the left isn't a name, the right must be
				op = curOp->right; //I don't think there are any tests that have this happen
			}

			//Note that we ignore the case of (1=1) or something silly like that. 
			//What would that mean for injection attacks? Huh

			//What have we got now? we've got the op, and the attribute that it's an op on
			//So we grab the relation and attribute, because we'll need those
			ParseRelationAndAttribute(op, relation1, attribute1);
			r1 = relations[relation1];

			//Now, what are our options?
			//We can either have an equality, or an inequality
			//Each of those has 2 different options: Single Or or not
			//If it is not a single or, it can be dependent or independent
			//So:
			if(curOp->code == EQUALS){
				//cout << "EQUALITY USING " << relation1 << " ON ATTRIBUTE " << attribute1 << endl;

				if(singleOr){
					//Need to check that it isn't a join
					if(curOp->right->code == NAME && curOp->left->code == NAME){
						tempOr = 1.0;
						hasJoin = true;
					}
					else{
					//	cout << "SINGLE OR" << endl;
						double const calc = (1.0l / r1[attribute1]);
					//	cout << "Single value is " << calc << endl;
						tempOr += calc;					
					}
				}//End singleOr

				else{

					if(independentOr){
					//	cout << "INDEPENDENT OR" << endl;
						double const calc = 1.0l - (1.0l / r1[attribute1]);
					//	cout << "Single value is " << calc << endl;
						tempOr *= calc;	
					}
					else{
						//cout << "DEPENDENT OR" << endl;
						double const calc = 1.0l / r1[attribute1];
						//cout << "Single value is " << calc << endl;
						tempOr += calc;	
						//cout << "tempOr is now " << tempOr << endl;
					}

				} //End else
			}//end code == EQUALS

			else{
				//cout << "INEQUALITY USING " << relation1 << " ON ATTRIBUTE " << attribute1 << endl;
				if(singleOr){
					//cout << "SINGLE OR" << endl;
					double const calc = 1.0l / 3.0l;
					//cout << "Single value is " << calc << endl;
					tempOr += calc;	
				}

				else{
					if(independentOr){
					//	cout << "INDEPENDENT OR" << endl;
						double const calc = 1.0l - (1.0l / 3.0l);
					//	cout << "Value is " << calc << endl;
						tempOr *= calc;	
					}
					else{
					//	cout << "DEPENDENT OR" << endl;
						double const calc = 1.0l / 3.0l;
					//	cout << "Value is " << calc << endl;
						tempOr += calc;	
					}
				}
			}//End Code != EQUALS
			if(!hasJoin){
				//cout << "NO JOIN FOUND, SETTING NUMTUPS TO NUM ROWS" << endl;
				if(relSets[relation1].GetNumTuples() == -1){
					numTups = r1.GetNumRows();
				}
				else{
					numTups = relSets[relation1].GetNumTuples();
				}
				//cout << "NUM TUPS IS " << numTups << endl;
			}
			curOr = curOr->rightOr;
		}

		//At this point, every Or for the current And has been calculated into tempOr, leaving us to get our current result:
		if(singleOr){
			//cout << "ADDING SINGLE OR TO CALCULATION" << endl;
			estimate *= tempOr;
			//cout << "Calculation is now " << estimate << endl;
		}
		else{
			if(independentOr){
			//	cout << "ADDING INDEPENDENT OR TO CALCULATION" << endl;
				estimate *= (1- tempOr);
			}
			else{
			//	cout << "ADDING DEPENDENT OR TO CALCULATION" << endl;
				estimate *= tempOr;
			}
		}
		curAnd = curAnd->rightAnd;
	}

	if(!hasJoin){
		//cout << "FINAL CALCULATING ESTIMATE" << endl;
		estimate = numTups * estimate;
		//cout << "ESTIMATE IS " << estimate << endl;
	}
	
	return estimate;
}


double Statistics::GetRelationCount(string relation, double joinEstimate){

  double count = 0.0;

  count = joinEstimate;

  if (count == 0.0){
   
	//cout << "In the 0.0 if conditional" << endl;
	 count = relSets[relation].GetNumTuples();
  	//cout << "Count of " << relation << " returned " << relSets[relation].GetNumTuples() << endl;
	}

  if (count == -1.0){
	//cout << "In the -1 if conditional" << endl;
    count = (double) relations[relation].GetNumRows();
  }


 // cout << "Get Relation Count is returning " << count << " for relation " << relation << endl;
  return count;

}

void Statistics::ParseRelationAndAttribute(struct Operand *op,
		  string &relation, string &attribute) {

    string value(op->value);
    string rel;
    stringstream s;

    int i = 0;

    while (value[i] != '_'){

      if (value[i] == '.'){
	relation = s.str();
	break;
      }

      s << value[i];

      i++;

    }

    if (value[i] == '.'){
      attribute = value.substr(i+1);
    }
    else {
      attribute = value;
      rel = s.str();
      relation = tables[rel];
    }

}


bool Statistics::Exists(string relation) {
  return (relations.find(relation) != relations.end());
}


bool Statistics::CheckIndependence (struct OrList *parseTree){

    // Method checks to see if the ORs in this are independent or not.
    // Returns false if they are, true if they are not. If all
    // attributes in list are the same, it is not an independent
    // or list.

    // Independent: Attributes in list are different.
    // Dependent: Attributes in list are the same

  struct OrList *curOr = parseTree;

  string lName;
  vector<string> checkVec;

  while (curOr){

      // When inside an OR, we want to start keeping track of what we
      // see. If we find an attribute that's not the same, we need to
      // mark that or list as dependent.

    if (checkVec.size() == 0){
      lName = curOr->left->left->value;
      checkVec.push_back(lName);
    }
    else{
      if(checkVec[0].compare(curOr->left->left->value) != 0){
	lName = curOr->left->left->value;
	checkVec.push_back(lName);
      }
    }

    curOr = curOr->rightOr;

  } // end while curOr

   // Dependent will have checkVec size 1, independent will have size > 1
  return (checkVec.size() > 1);

}

bool Statistics::CheckTableIndependence (struct OrList *parseTree){
	
    //Method checks to see if an OrList is dependent upon one (independent) or more (dependent) tables
	
	struct OrList *curOr = parseTree;
	struct ComparisonOp *curOp;
	string lName;
	vector<string> checkVec;
	
	RelationStats r1;
	string relation1;
	string attribute1;
	
	while (curOr){
		
		//First, we grab the op
		curOp = curOr->left;
		
		ParseRelationAndAttribute(curOp->left, relation1, attribute1);
		if(checkVec.size() == 0){
			checkVec.push_back(relation1);
		}
		else{
			if(checkVec[0].compare(relation1) != 0){
				return false;
			}
		}
		
	} // end while curOr
	
	// Dependent will have checkVec size 1, independent will have size > 1
	return true;
	
}

/**
 Given a operand, it tells me what relation is used by the operand.
 **/
void Statistics::ParseRelation(struct Operand *op, string &relation) {
	
    string value(op->value); //Will be something like "line.l_tax", or "o_orderkey".
    string rel;
    stringstream s;
	
    int i = 0;
	
    while (value[i] != '_'){
		
		if (value[i] == '.'){
			//If it is something like "line.l_tax", then the relation name (the alias) we want is "line"
			relation = s.str(); //Which is stored in here! 
			return; //So let's return it
		}
		
		s << value[i];
		
		i++;
		
    }
	
	//rel = s.str();
	//relation = tables[rel];	
	
}

