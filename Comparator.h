#ifndef COMPARATOR_H
#define COMPARATOR_H

#include "Comparison.h"
#include "ComparisonEngine.h"

	/**
	 * comparator is used to compare two records
	 * together.
	 */
	struct comparator {

	  OrderMaker sortorder;
	  ComparisonEngine cEngine;
	  bool reverse;

	  comparator(bool reverse, OrderMaker sortorder)
	  :reverse(reverse), sortorder(sortorder) { 

	  }

	  bool operator() (Record *r1, Record *r2){
	    int result = cEngine.Compare(r1, r2, &sortorder);

	    if (reverse == false)
	      return (result < 0);
	    else
	      return (result > 0);

	  }

	  int Compare(Record *r1, Record *r2){
	    return cEngine.Compare(r1, r2, &sortorder);
	  }

	  int Compare(Record *rec, Record *literal, CNF *cnf){
	    return cEngine.Compare(rec, literal, cnf);
	  }

	  int Compare(Record *rec1, OrderMaker *o1, Record *rec2,
		      OrderMaker *o2){
	    return cEngine.Compare(rec1, o1, rec2, o2);
	  }


	};


#endif
