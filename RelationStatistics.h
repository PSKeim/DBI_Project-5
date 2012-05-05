#ifndef RELATION_STATISTICS_H
#define RELATION_STATISTICS_H

#include <map>
#include <set>
#include <vector>
#include <iostream>

using std::map;
using std::set;
using std::vector;
using std::string;
using std::ostream;
using std::istream;

class RelationStats {

 public:

	  RelationStats ();
	  RelationStats (double _numRows);
	  ~RelationStats ();

	    // Update the number of rows represented by the relation
	  void UpdateRowCount (double _numRows);
	    // Add/update the attribute with the number of distinct values
	  void AddAttribute (string attr, int numDistinct);

	  RelationStats& operator= (RelationStats &rs);

	    // Overloaded stream operators
	  friend ostream& operator<< (ostream& os, RelationStats &stats);
	  friend istream& operator>> (istream& is, RelationStats &stats);

	    // Returns -2 if the attribute does not exist, or >= 0
	    // if it does exist.
	  int operator[] (string attr);

	  double GetNumRows ();


 private:

	  double numRows;
	  map<string,int> attributes;

};




class RelationSet {

 public:

	  RelationSet ();
	  RelationSet (string relation);
	  ~RelationSet ();

	    // Add a new relation to the set
	  void AddRelationToSet (string relation);

	    // Calculate whether or not the intersect of the set with this
	    // set is valid. If -1, the intersection is not valid.
	    // If 0, rs has no elements in common with this set.
	    // For any value x > 0, rs shares has all x elements
	    // that are in the current relation.
	  int Intersect (RelationSet rs);

	    // Get the number of relations in the set
	  int Size ();

	    // Merge another set with this set
	  void Merge (RelationSet rs);

	    // Update query count
	  void UpdateNumTuples (double _numTuples);

	    // Update query count
	  double GetNumTuples ();

	  void GetRelations (vector<string> &sets);

	    // Overloaded stream operators
	  friend ostream& operator<< (ostream& os, RelationSet &set);
	  friend istream& operator>> (istream& is, RelationSet &set);

	  RelationSet& operator= (RelationSet &rs);
	  void PrintRelations ();

 private:

	  double numTuples;
	  set<string> joined;

};


#endif
