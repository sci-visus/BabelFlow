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

#include "CharmTask.h"
#include "Controller.h"

namespace BabelFlow {
namespace charm {



CProxy_CharmTask Controller::initialize(Payload buffer, uint32_t size)
{
  //fprintf(stderr,"Trying to create %d tasks\n", graph.size());
  CProxy_StatusMgr status_mgr = CProxy_StatusMgr::ckNew(size, 1);   // 1 element in StatusMgr array
  int status_ep = CkIndex_StatusMgr::status(NULL);
  CkArrayID aid = status_mgr;
  int status_id = CkGroupID(aid).idx;

  // Create an array to hold all tasks
  CharmPayload ch_payl(buffer);
  ProxyType tasks = ProxyType::ckNew(ch_payl, status_ep, status_id, size);
 
  // Create the status mgr chare
  // CharmTask::initStatusMgr(size);

  // As per convention, this function now owns the buffer and must
  // free the memory
  delete[] buffer.buffer();

  return tasks;
}

void Controller::initializeAsync(Payload buffer, uint32_t size, int ep, int aid_v, int status_ep, int status_id)
{
  // CProxy_StatusMgr status_mgr = CProxy_StatusMgr::ckNew(1);   // 1 element in StatusMgr array
  // int status_ep = CkIndex_StatusMgr::status(NULL);
  // CkArrayID aid = status_mgr;
  // int status_id = CkGroupID(aid).idx;

  CkArrayID stat_aid = CkGroupID{status_id};
  CkCallback status_cb(status_ep, CkArrayIndex1D(0), stat_aid);
  status_cb.send(new StatusMsg(StatusMgr::StatusCode::NCYCLE, size));

  CharmPayload ch_payl(buffer);

  CkArrayID aid = CkGroupID{aid_v};
  ProxyType::ckNew(ch_payl, status_ep, status_id, size, CkCallback(ep, aid));

  // Create the status mgr chare
  // CharmTask::initStatusMgr(size);

  // As per convention, this function now owns the buffer and must
  // free the memory
  delete[] buffer.buffer();
}

}
}

