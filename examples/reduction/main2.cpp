/*
 * main.cpp
 *
 *  Created on: Dec 14, 2014
 *      Author: bremer5
 */

#include <cstdio>
#include <unistd.h>
#include <cstdlib>
#include <cstring>

#include "mpi.h"

#include "Reduction.h"
#include "ModuloMap.h"
#include "mpi/Controller.h"

class MyString : public Payload {
public:

  MyString(int32_t size=0, std::string* str=NULL) : Payload(size,(char*)str) {}

  MyString(Payload pay) : Payload(pay) {}

  virtual Payload clone()
  {
    if (mSize == -1) { // If we point to an actual std::string
      std::string* str = reinterpret_cast<std::string*>(mBuffer);

      assert(str != NULL);

      char* buffer = (char*)(new std::string(*str));

      return Payload(-1,buffer);
    }
    else if (mSize != 0) {
      return Payload::clone();
    }
    return Payload();
  }

  virtual void serialize()
  {
    if (mSize != -1)
      return;

    std::string* str = reinterpret_cast<std::string*>(mBuffer);
    assert(str != NULL);

    mSize = str->length();
    char* buffer = new char[mSize];

    memcpy(buffer,str->c_str(),mSize);
    delete str;
    mBuffer = buffer;
  }

  virtual void deserialize()
  {
    assert (mSize != -1);

    if (mSize == 0) {
      assert (mBuffer == NULL);

      mBuffer = (char*)(new std::string);
      mSize = -1;
    }
    else {
      std::string* str = new std::string(mBuffer,mSize);

      delete[] mBuffer;
      mSize = -1;
      mBuffer = reinterpret_cast<char*>(str);
    }
  }

};


int attach_string(std::vector<std::string*>& inputs, std::string* output)
{
  (*output) = "";

  for (uint32_t i=0;i<inputs.size();i++) {
    (*output) += (*inputs[i]);
  }


  int r = rand() % 100000;
  usleep(r);

  return 1;
}

int attach_string_mpi(std::vector<Payload>& inputs, std::vector<Payload>& outputs, TaskId task)
{
  std::vector<MyString> strs(inputs.size());
  std::vector<std::string*> convert(inputs.size());

  for (uint32_t i=0;i<inputs.size();i++) {
    strs[i] = MyString(inputs[i]);

    if (strs[i].size() != -1)
      strs[i].deserialize();

    convert[i] = reinterpret_cast<std::string*>(strs[i].buffer());
  }

  std::string* out = new std::string;

  attach_string(convert,out);

  outputs[0] = Payload(-1,reinterpret_cast<char*>(out));
}



int report_sum(std::string* input)
{

  fprintf(stderr,"Total string is \"%s\"\n",input->c_str());

  int r = rand() % 100000;
  usleep(r);

  return 1;
}

int report_sum_mpi(std::vector<Payload>& inputs, std::vector<Payload>& outputs, TaskId task)
{
  MyString str(inputs[0]);

  if (str.size() != -1)
    str.deserialize();

  return report_sum(reinterpret_cast<std::string*>(str.buffer()));
}



int main(int argc, char* argv[])
{
  if (argc < 3) {
    fprintf(stderr,"Usage: %s <nr-of-leafs> <fan-in> \n", argv[0]);
    return 0;
  }

  srand(100);

  MPI_Init(&argc, &argv);

  fprintf(stderr,"After MPI_Init\n");
  // FInd out how many controllers we need
  int mpi_width;
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_width);

  fprintf(stderr,"Using %d processes\n",mpi_width);

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == 0)
    fprintf(stderr,"My rank %d\n",rank);


  uint32_t leafs = atoi(argv[1]);
  uint32_t valence = atoi(argv[2]);

  Reduction graph(leafs,valence);
  ModuloMap task_map(mpi_width,graph.size());

  Controller master;

  FILE* output = fopen("task_graph.dot","w");
  graph.output_graph(mpi_width,&task_map,output);
  fclose(output);

  master.initialize(graph,&task_map);
  master.registerCallback(1,attach_string_mpi);
  master.registerCallback(2,report_sum_mpi);

  std::map<TaskId,Payload> inputs;


  uint32_t count=1;
  uint32_t sum = 0;
  for (TaskId i=graph.size()-graph.leafCount();i<graph.size();i++) {

    std::string* str = new std::string("A");

    Payload data(-1,(char*)str);

    inputs[i] = data;

    sum += count++;
  }

  master.run(inputs);

  fprintf(stderr,"The result was supposed to be %d\n",sum);
  MPI_Finalize();
  return 0;
}


