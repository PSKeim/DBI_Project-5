#ifndef BIGQ_H
#define BIGQ_H

#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Comparator.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

#include <iostream>
#include <vector>
#include <queue>

#include <stdio.h>
#include <pthread.h>

using namespace std;

class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

	/**
	 * HeapRun represents an entry in a priority queue
	 * that determines which record is returned first.
	 */
	class HeapRun {

	  public:
	      HeapRun(int runNum, Record *rec) : runNum(runNum){
		this->rec = new Record;
		this->rec->Consume(rec);
	      }

	      ~HeapRun(){
		delete rec;
	      }

	      int getRunNum() {
		return runNum;
	      }

              Record* getRecord() {
		return rec;
	      }

	  private:
	      Record *rec;
	      int runNum;
	};


private:


	/**
	 * pq_comparator is a special comparator used
         * by the priority queue to determine how to
	 * order the runs represented inside of it.
	 */
	struct pq_comparator {

	  comparator compare;
	  ComparisonEngine cEngine;

	  pq_comparator(comparator compare):compare(compare){

	  }

	  bool operator() (HeapRun *r1, HeapRun *r2){
	    return compare(r1->getRecord(), r2->getRecord());
	  }

	};


	int runs;
	int runLength;
	File runfile;
	OrderMaker sortorder;
	char filename[50];

	vector<off_t> startPos;
	vector<off_t> endPos;

	Pipe *in;
	Pipe *out;

	pthread_t worker;


	void ExternalSortRunPhase();
	void ExternalSortMergePhase();
	static void *SortWorker(void *args);

	int DeleteTempFile();
	off_t WriteRunToFile(vector<Record*> &run, off_t offset);
	int FillQueue(priority_queue< HeapRun*, vector<HeapRun*>, struct pq_comparator > &pq_compare, int run, off_t offset);

	void TestSort();

};

#endif
