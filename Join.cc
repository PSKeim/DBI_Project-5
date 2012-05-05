#include "RelOp.h"
#include "Join.h"
#include "Pipe.h"
#include "BigQ.h"
#include "Record.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

#include <cassert>
#include <vector>
#include <sstream>
#include <time.h>
#include <cstdlib>

using std::vector;
using std::stringstream;

Join::Join (){

}

Join::~Join (){

}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe,
		CNF &selOp, Record &literal) {

  _inPipeL = &inPipeL;
  _inPipeR = &inPipeR;
  _outPipe = &outPipe;
  _selOp = &selOp;
  _literal = &literal;

  pthread_create(&thread, NULL, RunWorker, (void*) this);

}

void* Join::RunWorker (void *op){

  Join *join = static_cast<Join*>(op);

  join->DoWork();

  join->_outPipe->ShutDown();

  pthread_exit(NULL);

}

void Join::DoWork (){

  OrderMaker *left = new OrderMaker();
  OrderMaker *right = new OrderMaker();
  numOutput = 0;
  if (_selOp->GetSortOrders(*left, *right) == 0)
    ExecuteBlockNestJoin();
  else
    ExecuteSortMergeJoin(left, right);

}

void Join::ExecuteBlockNestJoin(){

  //DBFile lDBF;
  //lDBF.Create("leftJoinFile.bin", heap, NULL);

  DBFile rDBF;
  stringstream stream("");

  string tempFile;
  stream << "rightJoinFile_" << (int) time(NULL) << ".bin";
  tempFile = stream.str();
  stream.str("");

  rDBF.Create((char *) tempFile.c_str(), heap, NULL);

    // Now we need to put all the records from the
    // left and right input pipes into 
  Record temp;

  while (_inPipeR->Remove(&temp)){
    rDBF.Add(temp);
  }

  Record GNRec;
	
  while (_inPipeL->Remove(&GNRec)){

    while(rDBF.GetNext(temp)){
      CreateAndInsertRecord(&GNRec, &temp);
    } // end while rDBF.GetNext()

    rDBF.MoveFirst();

  } // end while _inPipeL->Remove()

  remove(tempFile.c_str());

}

void Join::ExecuteSortMergeJoin(OrderMaker *left, OrderMaker *right){
	//Setting up the BigQ pipes
	int pipesize = 1000;
	Pipe lPipe(pipesize);
	Pipe rPipe(pipesize);
	
	//Our bools to see if they're empty
	bool lIsEmpty = false;
	bool rIsEmpty = false;
		
	//Set up the records in the two BigQ instances
	BigQ qLeft(*_inPipeL, lPipe, *left, pageSize);
	BigQ qRight(*_inPipeR, rPipe, *right, pageSize);

	//Key Records for comparison and checking key values
	Record leftKeyRec;
	Record rightKeyRec;

	//Sets we are using for the subset merges
	vector<Record*> leftSet;
	vector<Record*> rightSet;
	
	//Our comparison
	ComparisonEngine cEngine;
	int result;

	//Initial removal from pipe (for now, assume shit is actually in there)
	lPipe.Remove(&leftKeyRec);
	rPipe.Remove(&rightKeyRec);
	
	//use this later
	//Record *oldKey;

	int count = 0;
	int leftC = 0;
	int rightC = 0;
	

	do{

		result = cEngine.Compare(&leftKeyRec, left, &rightKeyRec, right);
		//cout << "Result was " << result << endl;
		count ++;

		if(count % 10000 == 0) cout << count << endl;

		if(result == 0){ //Match between the key records, we need to fill the subset vectors, compute the cartesian product, then advance the key records
			//Fill the subset vectors		
			//cout << "L has " << leftKeyRec.GetNumAtts() << endl;
			//cout << "R has " << rightKeyRec.GetNumAtts() << endl;			
			lIsEmpty = AdvanceJoinSet(leftSet, &lPipe, leftKeyRec, left, 0);
			//cout << "INSIDE L has " << leftKeyRec.GetNumAtts() << endl;
			leftC += leftSet.size();		
			rIsEmpty = AdvanceJoinSet(rightSet, &rPipe, rightKeyRec, right, 1);
			rightC += rightSet.size();

	//		cout << "INSIDe R has " << rightKeyRec.GetNumAtts() << endl;
			//testing
		//	cout << "New result " << cEngine.Compare(&leftKeyRec, left, &rightKeyRec, right) << endl;
			
			// Compute the cartesian product
			//Can optimize this to scan the smaller size more, but we'll worry about that later.
			for (unsigned int i = 0; i < leftSet.size(); i++){
				for (unsigned int j = 0; j < rightSet.size(); j++){
				  CreateAndInsertRecord(leftSet[i], rightSet[j]);
				}
			}
			numOutput += leftSet.size() * rightSet.size();
		 	//cout << " I have output "<<numOutput<<" records" << endl;
			//Advancing the key records is done automatically in AdvanceJoinSet
			//That is, at the end of AJS, leftKeyRec will be the first record that did not match on key values
			//cout << "Inserted the Cartesian product" << endl;
		}

		else if(result < 0){ //Left record is smaller than right record, so we need to advance the left key
				//Advance left key record
			Record *oldKey = new Record;
			oldKey->Copy(&leftKeyRec); //Since we're advancing the key anyways. It's okay if we consume it
			bool empty = false;
			if(lPipe.Remove(&leftKeyRec)){ //Pull something from the pipe (if you can)
				leftC++;
				count++;
				while((cEngine.Compare(oldKey, &leftKeyRec, left) == 0) && !empty){ //While it still matches the old key, and the pipe has stuff in it...
					leftC++;
					count++;
					empty = (0 == lPipe.Remove(&leftKeyRec)); //Get a new record
				}
			}
			if(cEngine.Compare(oldKey, &leftKeyRec, left) == 0 && empty){
				leftKeyRec.bits = NULL;
			}
		//cout << "New Key Selected" << endl;
		}

		else if(result > 0){ //Right record is smaller than left record, so we need to advance the right key
			//Advance the right key record
			//rIsEmpty = AdvanceKeyPointer(&rPipe, &rightKeyRec, right);
			//cout << "Advanced right key record" << endl;
			Record *oldKey = new Record;
			oldKey->Consume(&rightKeyRec); //Since we're advancing the key anyways. It's okay if we consume it
			bool empty = false;
	
			if(rPipe.Remove(&rightKeyRec)){ //Pull something from the pipe (if you can)
				rightC++;
				count++;
				while((cEngine.Compare(oldKey, &rightKeyRec, right) == 0) && !empty){ //While it still matches the old key, and the pipe has stuff in it...
					rightC++;
					count++;
					empty = (0 == rPipe.Remove(&rightKeyRec)); //Get a new record
				}
			}
			if(cEngine.Compare(oldKey, &rightKeyRec, right) == 0 && empty){
				rightKeyRec.bits = NULL;
			}
			//cout << "Updated right key record" << endl;
		}

	}while(!leftKeyRec.isEmpty() && !rightKeyRec.isEmpty()); //Terminate when the pipes are done

	cout << leftC << " left records done" << endl;
	cout << rightC << " right records done" << endl;
	cout << numOutput << " records inserted" << endl;
}
	
bool Join::AdvanceJoinSet(vector<Record*> &buffer, Pipe *pipe, Record & keyValue, OrderMaker *order, int side){
	//Clear out the buffer from our previous use
	for(unsigned int i = 0; i < buffer.size(); i++){
		delete buffer[i];
	}
	buffer.clear();
	//Now we put our key record into the start of the buffer
	buffer.push_back(new Record);
	buffer[0]->Consume(&keyValue);
	int result = 0;
	bool empty = false;
	ComparisonEngine cEngine;

	int counter = 0;

	do{
		empty = (0 == pipe->Remove(&keyValue)); //Get new record that (potentially) matches our key
		if(side == 0 && keyValue.GetNumAtts() == 5){
			clog << "Num atts was not 7. Fuck it, I'm out" << endl;
			clog << "With the left schema: " << endl;
			keyValue.Print(lSchema);
			clog << "With the right schema: " << endl;
			keyValue.Print(rSchema);
			exit(-1);
		}
		counter++;
		if(!empty){
			result = cEngine.Compare(buffer[0], &keyValue, order); //Does it?
		}
		else{
			result = 1;
		}		
		
		if(result == 0){ 
			buffer.push_back(new Record); //IT DOES! Add it to the set and begin again
			buffer[buffer.size()-1]->Consume(&keyValue);
		}
		
	}while(result == 0 && !empty); //We do this until we don't have a match (result != 0) or the pipe is empty

	//cout << "Advance join set added " << counter << " records" << endl;
	if(empty && result == 0){ //The pipe is now empty, and the last record we read matched our previous key. So we make our key record empty
		keyValue.bits = NULL; //So
	}

	return empty;
}
/*
bool Join::AdvanceKeyPointer(Pipe *pipe, Record *keyRec, OrderMaker *order){
	//Variables for temporary storage
	Record *oldKey = new Record;
	oldKey->Consume(keyRec); //Since we're advancing the key anyways. It's okay if we consume it
	ComparisonEngine cEngine;
	bool empty = false;
	
	if(pipe->Remove(keyRec)){ //Pull something from the pipe (if you can)
		while((cEngine.Compare(oldKey, keyRec, order) == 0) && !empty){ //While it still matches the old key, and the pipe has stuff in it...
			empty = (0 == pipe->Remove(keyRec)); //Get a new record
		}
		//cout << "New Key Selected" << endl;
	}

	return empty;
}*/
/*
void Join::ExecuteSortMergeJoin(OrderMaker *left, OrderMaker *right){

  int pipesize = 100;

    // Set up temporary pipes for the BigQ
  Pipe tempL(pipesize);
  Pipe tempR(pipesize);

  Record *leftTempRec = new Record; //Temp records for holding the next key record after the one that we're comparing.
  Record *rightTempRec = new Record;

  Record leftKeyRec ; //records for seeing if a join is possible.
  Record rightKeyRec;

  vector<Record*> leftSet; //These are the sets we're using for 
  vector<Record*> rightSet;

    // Sort the records in two BigQ instances
  BigQ qLeft(*_inPipeL, tempL, *left, pageSize);
  BigQ qRight(*_inPipeR, tempR, *right, pageSize);

  ComparisonEngine cEngine;
  int result = 0;
  bool leftIsNotEmpty = !AdvanceKeyPointer(&tempL, leftKeyRec, left);

  bool rightIsNotEmpty = !AdvanceKeyPointer(&tempR, rightKeyRec, right);

    // What follows is an implementation of the sort-merge join
  while (leftIsNotEmpty && rightIsNotEmpty){

	if(leftKeyRec == NULL){
		cout << "Left record is null" << endl;
		if(rightKeyRec == NULL) cout << "Right record is also null" << endl;
		assert(leftKeyRec != NULL);
	}
    assert(leftKeyRec != NULL);
    assert(rightKeyRec != NULL);
      // Compare the two records
    result = cEngine.Compare(leftKeyRec, left, rightKeyRec, right);

      // If the records match, merge them together and advance both
      // pointers
    if (result == 0) {
      leftIsNotEmpty = !AdvanceJoinSet(leftSet, &tempL, leftKeyRec, left);
      rightIsNotEmpty = !AdvanceJoinSet(rightSet, &tempR, rightKeyRec, right);

	// Cartesian product of two sets

      for (int i = 0; i < leftSet.size(); i++){
	for (int j = 0; j < rightSet.size(); j++){
	  CreateAndInsertRecord(leftSet[i], rightSet[j]);
	}
      }
 	//This might be necessary, might not. Still fiddling with build.
	// Now, replace the key records with
	// the temp records (now the new key).
      //leftKeyRec->Consume(leftTempRec);
      //rightKeyRec->Consume(rightTempRec);

    }

      // If left record is less than the right,  advance the KEY
      // pointer of the left pipe
    else if (result < 0) {
      leftIsNotEmpty = !AdvanceKeyPointer(&tempL, leftKeyRec, left);
    }

      // Otherwise advance the KEY pointer of the right pipe
    else {
      rightIsNotEmpty = !AdvanceKeyPointer(&tempR, rightKeyRec, right);
    }

  } // end while

  delete leftTempRec;
  delete rightTempRec;
  delete leftKeyRec;
  delete rightKeyRec;

}

bool Join::AdvanceJoinSet(vector<Record*> &buffer, Pipe *pipe,
			  Record *temp, OrderMaker *order){

  Record *recBuf = new Record;

  for (int i = 0; i < buffer.size(); i++){
    delete buffer[i];
  }

  buffer.clear();

  buffer.push_back(new Record);
  buffer[0]->Consume(temp);

  int result = 0;
  bool empty = false;
  ComparisonEngine cEngine;

  do {

    if (pipe->Remove(recBuf) != 0){
    
      result = cEngine.Compare(buffer[0], recBuf, order);

      if (result == 0){
	  // If the result is 0, we add the new record to our buffer subset
	buffer.push_back(new Record);
	buffer[buffer.size()-1]->Consume(recBuf);
      }
      else {
	  // If not, we need to keep track of the recBuff, 
	temp->Consume(recBuf);
      }
    }
    else {
      empty = true;
    }

  }
  while (result == 0 && !empty);

  delete recBuf;

  return empty;

}

bool Join::AdvanceKeyPointer(Pipe *pipe, Record &addressOfKeyRec, OrderMaker *order){

  bool empty = false;
  Record *recBuf = new Record;
  Record *keyPointer = &addressOfKeyRec;
  cout << "Pipe remove returned " << pipe->Remove(recBuf) << endl;

  if (temp == NULL) {
    cout << "First time in Advance Key Pointer for something." << endl;
    keyPointer = new Record;
    keyPointer->Consume(recBuf);
    cout << "The temp variable now has consumed something." << endl;
    cout << "Key Pointer is: " << keyPointer << endl;
  }
  else {

    ComparisonEngine cEngine;

    while (cEngine.Compare(keyPointer, recBuf, order) == 0 && !empty){
      empty = !(bool) pipe->Remove(recBuf);
    }

    keyPointer->Consume(recBuf);

  }

  delete recBuf;

  return empty;
}
*/
void Join::CreateAndInsertRecord(Record *left, Record *right){

  Record *toPipe = new Record;

  int numAttsLeft = left->GetNumAtts();
  int numAttsRight = right->GetNumAtts();
  int numAttsTotal = numAttsLeft + numAttsRight;

  int * attsToKeep = (int *) alloca(sizeof(int) * numAttsTotal);
  int index = 0;

  for (int i = 0; i < numAttsLeft; i++){
    attsToKeep[index] = i;
    index++;
  }

  for (int i = 0; i < numAttsRight; i++){
    attsToKeep[index] = i;
    index++;
  }


  //std::cout << std::endl;

  for (int i = 0; i < numAttsTotal; i++){
  //std::cout << attsToKeep[i] << ",";
  }

  //std::cout << std::endl;

    // Merge the left and right records
  toPipe->MergeRecords(left, right, numAttsLeft, numAttsRight,
		       attsToKeep, numAttsTotal, numAttsLeft);

    // Insert into output pipe
  _outPipe->Insert(toPipe);

  delete toPipe;

}

void Join::WaitUntilDone () {
  pthread_join (thread, NULL);
}

void Join::Use_n_Pages (int runlen) {
  pageSize = runlen;
}

void Join::SetSchemas(Schema *l, Schema *r){
	lSchema = l;
	rSchema = r;
}
