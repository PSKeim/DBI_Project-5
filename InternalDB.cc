#include "File.h"
#include "Record.h"
#include "InternalDB.h"
#include "Comparison.h"
#include <string>
#include <stdio.h>

InternalDB::InternalDB(File *file):dbfile(file){

  page = new Page;
  pIndex = (off_t) 0;

  if (GetFileLength() > 0){
    LoadPage(page, pIndex);
  }

}

InternalDB::~InternalDB(){ 
  delete page;
}

void InternalDB::Close(){

}

void InternalDB::Load(Schema &f_schema, char *loadpath){

}

void InternalDB::Add(Record &addMe){

}

int InternalDB::GetNext(Record &fetchme){
  return 0;
}

int InternalDB::GetNext(Record &fetchme, CNF &cnf, Record &literal){
  return 0;
}

void InternalDB::MoveFirst(){

}

/*********************************
 *
 * Metadata
 *
 **********************************/

void InternalDB::LoadPage(Page *loadMe, off_t index){
  dbfile->GetPage(loadMe, index);
}

void InternalDB::Write(Page *writeMe, off_t index){
  dbfile->AddPage(writeMe, index);
}

int InternalDB::GetFileLength(){
  return dbfile->GetLength();
}

off_t InternalDB::GetLastPageIndex() {

  off_t index;

  if (dbfile->GetLength() == 0){
    index = (off_t) 0;
  }
  else {
    index = dbfile->GetLength() - ((off_t) 2);
  }

  return index;

}

off_t InternalDB::GetEndOfFileIndex() {

  off_t index;

  if (dbfile->GetLength() == 0){
    index = (off_t) 0;
  }
  else {
    index = dbfile->GetLength() - ((off_t) 1);
  }

  return index;

}

void InternalDB::UpdateFile(string temp, string filename){

  dbfile->Close();

  rename(temp.c_str(), filename.c_str());

  dbfile->Open(1, (char*) filename.c_str());

}
