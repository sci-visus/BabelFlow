//
// Created by Li, Jixian on 2019-05-28.
//

#ifndef BABELFLOW_MAPREDUCECALLBACK_H
#define BABELFLOW_MAPREDUCECALLBACK_H

#include <iostream>
#include <Payload.h>

int split_load(std::vector<BabelFlow::Payload> &inputs, std::vector<BabelFlow::Payload> &outputs,
               BabelFlow::TaskId task)
{
  BabelFlow::Payload input = inputs[0];
//  std::cout << input.size() << std::endl;
  assert(input.size() > sizeof(int) + sizeof(size_t));
  char *input_buffer = input.buffer();
  char *arr_len_ptr = input_buffer + sizeof(int);
  char *data_buffer = input_buffer + sizeof(int) + sizeof(size_t);

  int n_worker = reinterpret_cast<int *>(input_buffer)[0];
  size_t arr_len = reinterpret_cast<size_t *>(arr_len_ptr)[0];
  std::vector<uint32_t> n_nums;
  if (arr_len % n_worker != 0) {
    int n_num = arr_len / n_worker + 1;
    int total = 0;
    while (total < arr_len) {
      if (total + n_num > arr_len) n_num = arr_len - total;
      n_nums.push_back(n_num);
      total += n_num;
    }
  } else {
    int n_num = arr_len / n_worker;
    for (int i = 0; i < n_worker; ++i) n_nums.push_back(n_num);
  }


  size_t offset = 0;
  for (int i = 0; i < n_worker; ++i) {
    size_t buffer_size = n_nums[i] * sizeof(int);
    char * local_buffer = new char[buffer_size];
    memcpy(local_buffer, data_buffer + offset, buffer_size);
    BabelFlow::Payload out(buffer_size, local_buffer);
    outputs[i] = out;
    offset += buffer_size;
  }


  return 1;
}

int map_func(std::vector<BabelFlow::Payload> &inputs, std::vector<BabelFlow::Payload> &outputs,
             BabelFlow::TaskId task)
{

  using namespace std;
  for (auto &input : inputs) {
    BabelFlow::Payload out;
    auto size = input.size();
    auto n_num = size / sizeof(int);
    int *arr = reinterpret_cast<int *>(input.buffer());
    int *result = new int[n_num];
    for (int i = 0; i < n_num; i++) {
      result[i] = arr[i];
    }
    out.initialize(size, reinterpret_cast<char *>(result));
    outputs[0] = out;
  }
  return 1;
}

int reduce_func(std::vector<BabelFlow::Payload> &inputs, std::vector<BabelFlow::Payload> &outputs,
                BabelFlow::TaskId task)
{
  int sum = 0;
  for (auto &input:inputs) {
    auto n_num = input.size() / sizeof(int);
    for (int i = 0; i < n_num; ++i)
      sum += reinterpret_cast<int *>(input.buffer())[i];
  }

  char *buffer = new char(sizeof(int));
  reinterpret_cast<int *>(buffer)[0] = sum;
  outputs[0].initialize(sizeof(int), buffer);
  return 1;
}

int print_func(std::vector<BabelFlow::Payload> &inputs, std::vector<BabelFlow::Payload> &outputs,
               BabelFlow::TaskId task)
{
  BabelFlow::Payload msg = inputs[0];
  using namespace std;
  int *sum = reinterpret_cast<int *>(msg.buffer());
  std::cout << "SUM: " << *sum << std::endl;
  return 1;
}

#endif //BABELFLOW_MAPREDUCECALLBACK_H
