/*
 * Copyright (c) 2017 University of Utah 
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef REDUCE_ALL_CALLBACKS_H_
#define REDUCE_ALL_CALLBACKS_H_

#include <cstdio>

int add_int(std::vector<BabelFlow::Payload> &inputs, std::vector<BabelFlow::Payload> &output,
            BabelFlow::TaskId task) {
  int32_t size = sizeof(int32_t);
  char *buffer = (char *) (new uint32_t[1]);

  uint32_t *result = (uint32_t *) buffer;

//  printf("task %d: ", task);

  *result = 0;
  for (uint32_t i = 0; i < inputs.size(); i++) {
    *result += *((uint32_t *) inputs[i].buffer());
//    printf("%d + ", *(uint32_t *)inputs[i].buffer());
  }
//  printf(" = %d\n", *(uint32_t *)buffer);

  output[0].initialize(size, buffer);

  int r = rand() % 100000;
  usleep(r);

  return 1;
}

int add_int_broadcast(std::vector<BabelFlow::Payload> &inputs, std::vector<BabelFlow::Payload> &output,
                      BabelFlow::TaskId task) {
  size_t size = sizeof(uint32_t);
  char *buffer = (char *) (new uint32_t[1]);

  uint32_t *result = (uint32_t *) buffer;

//  printf("task %d: ", task);
  for (uint32_t i = 0; i < inputs.size(); i++) {
    *result += *((uint32_t *) inputs[i].buffer());
//    printf("%d + ", *(uint32_t *)inputs[i].buffer());
  }
//  printf(" = %d\n", *(uint32_t *)buffer);

  fprintf(stderr, "task %d: Total sum is %d\n", task, *result);

  output[0].initialize(size, buffer);

  int r = rand() % 100000;
  usleep(r);

  return 1;
}

int print_result(std::vector<BabelFlow::Payload> &inputs, std::vector<BabelFlow::Payload> &output,
                 BabelFlow::TaskId task) {
  uint32_t *result = (uint32_t *) inputs[0].buffer();

  fprintf(stderr, "task %d: Broadcast result %d\n", task, *result);

  int r = rand() % 3000000;
  usleep(r);

  return 1;
}

#endif
