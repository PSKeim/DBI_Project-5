#ifndef STATISTICS_H
#define STATISTICS_H

#include "ParseTree.h"
#include "RelationStatistics.h"

#include <map>
#include <set>
#include <vector>
#include <iostream>

using std::map;
using std::set;
using std::string;
using std::vector;
using std::ostream;

class Statistics
{

public:

	Statistics ();
	Statistics (Statistics &copyMe);	 // Performs deep copy
	~Statistics ();

	Statistics& operator= (Statistics &stat);
	int operator() (char *relation);
	int operator() (char *relation, char *attribute);

	void AddRel (char *relName, int numTuples);
	void AddRel (char *relName, double numTuples);
	void AddAtt (char *relName, char *attName, int numDistincts);
	void CopyRel (char *oldName, char *newName);
	
	void Read (char *fromWhere);
	void Write (char *fromWhere);

	void  Apply (struct AndList *parseTree,
		     char *relNames[], int numToJoin);

	double Estimate (struct AndList *parseTree,
			char **relNames, int numToJoin);

	void ParseRelation(struct Operand *op, string &relation);

	void ParseRelationAndAttribute(struct Operand *op, string &relation,
				       string &attribute);


private:

	map<string,RelationStats> relations;
	map<string,RelationSet> relSets;
	vector<RelationSet> sets;

	map<string,string> tables;

	bool Exists(string relation);

	double Guess (struct AndList *parseTree, RelationSet toEstimate,
		      vector<int> &indexes, double joinEstimate);

	bool CheckParseTree (struct AndList *parseTree);
	double ParseJoin (struct AndList *parseTree);
	bool CheckSets (RelationSet toEstimate, vector<int> &indexes);
	double GenerateEstimate (struct AndList *parseTree,
				 double joinEstimate);
	double GetRelationCount(string relation, double joinEstimate);

        bool CheckIndependence (struct OrList *parseTree);
	bool CheckTableIndependence (struct OrList *parseTree);

};

#endif
