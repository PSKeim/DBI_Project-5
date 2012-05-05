#include "RelationStatistics.h"

#include <map>
#include <set>
#include <sstream>
#include <iostream>

using std::map;
using std::set;
using std::cout;
using std::endl;
using std::string;
using std::ostream;
using std::istream;

RelationStats::RelationStats () : numRows(){
  numRows = 0;
}

RelationStats::RelationStats (double _numRows) : numRows(_numRows){

}

RelationStats::~RelationStats () {
  attributes.clear();
}

void RelationStats::UpdateRowCount (double _numRows){
  numRows = _numRows;
}

void RelationStats::AddAttribute (string attr, int numDistinct){
  attributes[attr] = numDistinct;
}

RelationStats& RelationStats::operator= (RelationStats &rs){

  if (this != &rs){
    this->numRows = rs.numRows;
    this->attributes.clear();
    this->attributes = rs.attributes;
  }

  return *this;

}

ostream& operator<< (ostream& os, RelationStats &stats){

  map<string,int>::iterator iter = stats.attributes.begin();
  map<string,int>::iterator end = stats.attributes.end();

  os << stats.numRows << endl;
  os << stats.attributes.size() << endl;

  for (; iter != end; iter++){
    os << iter->first << endl << iter->second << endl;
  }

  return os;

}

istream& operator>> (istream& is, RelationStats &stats){

  int numIterations = 0;

  string line;
  std::stringstream s("");

    // Get the first line and put into the stringstream
  getline(is, line);
  s.str(line);

    // Read in an integer
  if (!(s >> stats.numRows)){
    stats.numRows = 0;
  }
  s.clear();
  s.str("");

    // Get the first line and put into the stringstream
  getline(is, line);
  s.str(line);

    // Read in an integer
  if (!(s >> numIterations)){
    numIterations = 0;
  }
  s.clear();
  s.str("");

  string attName;
  int numDistinct = 0;

    // Loop through each (attribute, numDistinct) pair
    // and store the result into the map
  for (int i = 0; i < numIterations; i++){

      // Get the attribute name
    getline(is, line);
    attName = line;

      // Get the number of distinct values
    getline(is, line);
    s.str(line);

    if (!(s >> numDistinct)){
      numDistinct = 0;
    }
    s.clear();
    s.str("");

      // Store the data
    stats.attributes[attName] = numDistinct;

  } // end for

  return is;

}

int RelationStats::operator[] (string attr){

  int result = 0;

  if (attributes.find(attr) == attributes.end()){
    result = -2;
  }
  else if (attributes[attr] == -1) {
    result = numRows;
  }
  else {
    result = attributes[attr];
  }

  return result;

}

double RelationStats::GetNumRows (){
  return numRows;
}




RelationSet::RelationSet () {
  numTuples = -1.0;
}

RelationSet::RelationSet (string relation) {
  joined.insert(relation);
  numTuples = -1.0;
}


RelationSet::~RelationSet () {

}

void RelationSet::AddRelationToSet (string relation){
  joined.insert(relation);
}

void RelationSet::UpdateNumTuples (double _numTuples){
  numTuples = _numTuples;
}

double RelationSet::GetNumTuples (){
  return numTuples;
}

int RelationSet::Intersect (RelationSet rs){

  RelationSet temp;
  int numMatches = 0;

  set<string>::iterator it = rs.joined.begin();

  for (; it != rs.joined.end(); it++){

    if (this->joined.find(*it) != this->joined.end()){
      temp.AddRelationToSet(*it);
    }

  } // end for it

  int resultSize = temp.Size();

  if (resultSize == 0){
    numMatches = 0;
  }
  else if (resultSize == Size()){
    numMatches = resultSize;
  }
  else {
    numMatches = -1;
  }

  return numMatches;

}

void RelationSet::Merge (RelationSet rs){

  set<string>::iterator it = rs.joined.begin();

  for (; it != rs.joined.end(); it++){
    AddRelationToSet(*it);
  } // end for it

}

int RelationSet::Size (){
  return joined.size();
}

ostream& operator<< (ostream& os, RelationSet &set){

  std::set<string>::iterator iter = set.joined.begin();

  os << set.numTuples << endl;
  os << set.joined.size() << endl;

  for (; iter != set.joined.end(); iter++){
    os << *iter << endl;
  } // end for it

  return os;

}

istream& operator>> (istream& is, RelationSet &set){

  string line;
  std::stringstream s;

  double numTuples = 0;
  int numRelations = 0;

    // Get the number of tuples
  getline(is, line);
  s.str(line);

  if (!(s >> numTuples)){
    numTuples = 0.0;
  }
  s.str("");
  s.clear();

  set.UpdateNumTuples(numTuples);

    // Get the number of relations in the set
  getline(is, line);
  s.str(line);

  if (!(s >> numRelations)){
    numRelations = 0;
  }
  s.str("");
  s.clear();

    // Read in the relations that are joined
  for (int i = 0; i < numRelations; i++){
    getline(is, line);
    set.joined.insert(line);
  }

  return is;

}

void RelationSet::GetRelations (std::vector<string> &sets){

  set<string>::iterator iter = joined.begin();

  for (; iter != joined.end(); iter++){
    sets.push_back(*iter);
  }

}

RelationSet& RelationSet::operator= (RelationSet &rs){

  if (this != &rs){
    this->numTuples = rs.numTuples;
    this->joined = rs.joined;
  }

  return *this;

}

void RelationSet::PrintRelations(){

  set<string>::iterator iter = joined.begin();

  for (; iter != joined.end(); iter++){
    cout << *iter << endl;
  }

}
