/*
 * KWayMerge.h
 *
 *  Created on: Dec 15, 2014
 *      Author: bremer5
 */

#ifndef KWAYMERGE_H_
#define KWAYMERGE_H_

#include <stdint.h>
#include <vector>
#include <cmath>

//#include "TypeDefinitions.h"
#include "TaskGraph.h"
#include "KWayTaskMap.h"

class KWayTaskMap;

/*! This class implements a k-way merge tree algorithm assuming
 *  a regular grid of data blocks as input. The graph will merge
 *  across the longest spatial boundary first and merge until
 *  the given k is achieved. Currently, only power-of-2 merges are
 *  supported. The graph assume four unique operations:
 *
 *  Callback 1: Local compute (one outside input), two outputs the first
 *              to the merge and the next to the first correction stage
 *
 *  Callback 2: Merge computation (up to k inputs), two outputs, the
 *              first to the next merge the second to the relay towards
 *              the next correction
 *
 *  Callback 3: Local corrections with two inputs and one output. The
 *              first input is the current local tree, the second the
 *              boundary tree. The output is the new local tree.
 *
 *  Callback 4: Output of the final local trees with one input and no
 *              outputs
 *
 *  The numbering of the nodes will be as follows. The main reduction
 *  flow is numbered 0 - pow(k,level)-1 as the default reduction. The
 *  various scatter flows will be numbered 0-n as the standard broadcast
 *  but their number will be prefixed by their level in the reduction
 *  using the first m bits (m typically = 4) of the taskid
 *
 */
class KWayMerge : public DataFlow::TaskGraph
{
public:

  friend class KWayTaskMap;

  //! The number of bits used for prefixing scatter tasks
  static const uint8_t sPrefixSize = 4;

  //! The number of non-prefix bits
  static const uint8_t sPostfixSize = sizeof(DataFlow::TaskId)*8 - sPrefixSize;

  //! Bit mask for scatter tasks
  static const DataFlow::TaskId sPrefixMask = ((1 << sPrefixSize) - 1) << sPostfixSize;


  /*! Create a k-way merge graph from the given grid of local tasks
   *  and given merge factor. The leaf task will be numbered in
   *  row major order.
   *
   * @param dim
   * @param factor
   */
  KWayMerge(uint32_t dim[3], uint32_t factor);

  KWayMerge(std::string config);

  KWayMerge(){};

  //! Destructor
  virtual ~KWayMerge() {}

  //! Compute the fully specified tasks for the
  //! given controller id and task map
  virtual std::vector<DataFlow::Task> localGraph(DataFlow::ShardId id, const DataFlow::TaskMap* task_map) const;

  //! Return the total number of tasks
  /*! This function computes the total number of tasks in the graph.
   *  However, it is important to note that the tasks are *not*
   *  numbered consecutively, so "size" cannot be used in a modulo
   *  task map, for example.
   *
   * @return The total number of tasks
   */
  DataFlow::TaskId size() const;

  //! Return the total number of rounds needed to merge
  uint8_t rounds() const {return mRounds;}

  //! Output the entire graph as dot file
  virtual int output_graph(DataFlow::ShardId count, const DataFlow::TaskMap* task_map, FILE* output);

  //! Return the taskId for the given global task id
  virtual DataFlow::Task task(uint64_t gId) const;

  DataFlow::TaskId toTId(uint64_t this_gId) const;

  //! Return the global id of the given task id
  virtual uint64_t gId(DataFlow::TaskId tId) const {

    uint8_t k;

    uint64_t gId = 0;
    DataFlow::TaskId leaf_count = lvlOffset()[1];

    // For all leafs assigned to this controller
    for (DataFlow::TaskId leaf=0;leaf<leaf_count;leaf++) {
      if(leaf == tId)
        return gId;
      else 
        gId++;

      // Now take its local copies for all rounds
      for (k=1;k<=rounds();k++) {
        if(roundId(leaf,k) == tId)
          return gId;
        else 
          gId++;
      }
      // Walk down the tree until your child is no longer
      // assigned to the same controller
      uint8_t lvl = 0;
      DataFlow::TaskId down = leaf;
      DataFlow::TaskId next = reduce(down);
      while ((down != next) &&  (down == expand(next)[0])) {
        lvl++;
        down = next;

        if (lvl < rounds()-1)
          next = reduce(next);

        if(down == tId)
          return gId;
        else 
          gId++;

        // All lower nodes exist for all levels after this one
        for (k=lvl+1;k<rounds();k++){
          if(roundId(down,k) == tId)
            return gId;
          else 
            gId++;
        }
      }// end-while
    } // end-for all leafs
#if 0
    DataFlow::TaskId base = baseId(tId);
    DataFlow::TaskId rnd = round(tId);

    bool gather = gatherTask(tId);

    const std::vector<uint32_t>& offset = lvlOffset();
    uint32_t max_offset = offset[offset.size()-1];

    uint32_t leaves = mLvlOffset[1];

    uint32_t curr_lvl = leaves;

    uint32_t this_offset = 0;
    // while (base > this_offset){
    //   this_offset += curr_lvl*log2(curr_lvl);
    //   curr_lvl = log2(curr_lvl);

    // }

    std::vector<uint32_t> sizes;
    //printf("offs %d\n", offset.size());

    //sizes.push_back(0);
    //printf("sizes: ");
    for(int i=1; i< offset.size(); i++){
      sizes.push_back(offset[i]-offset[i-1]);
      //printf("%d ",sizes.back());
    }
    //printf("\n");
    
    uint32_t l = 0;
    while (base >= offset[l+1]){
      uint32_t off = offset[l+1]-offset[l];
      this_offset +=  off;
      l++;
    }

    //printf("lvl %d\n", l);

    // if(gather){
    //   uint8_t lvl = level(tId);
      
    //   printf("base %d round %d level %d [%d] tId %d max offset %d\n", base, rnd, lvl, offset[lvl],tId, max_offset);
    //   return tId;
    // }
    // else{
    //   DataFlow::TaskId scatterId = max_offset+base+this_offset+rnd;
    //   printf("base %d round %d tId %d USE %d\n", base, rnd, tId, scatterId);
    //   return scatterId;
    // }
    DataFlow::TaskId gId = (base-offset[l])*sizes[l]+this_offset+rnd;

    printf("gId %d tId %d rnd %d base %d level %d offset %d\n", gId, tId, rnd, base, l, this_offset);
    
#endif
   return gId;
    
  };

  //! Serialize a task graph
  virtual DataFlow::Payload serialize() const;

  //! Deserialize a task graph. This will consume the payload
  virtual void deserialize(DataFlow::Payload buffer);
  
private:

  //! The dimension of the input grid
  std::vector<std::vector<uint32_t> > mLvlDim;

  //! The merge factor
  uint32_t mFactor;

  //! How many rounds of merging
  uint32_t mRounds;

  //! A vector storing how many tasks there are for each level and tis children
  std::vector<uint32_t> mLvlOffset;

  //! Simplification factors on a per level basis
  std::vector<std::vector<uint8_t> > mFactors;

  void init(uint32_t dim[3], uint32_t factor);

  /*******************************************************************
   ******* Internal convenience functions
   *******************************************************************/
  // Const access to the level sizes
  const std::vector<uint32_t>& lvlOffset() const {return mLvlOffset;}

  //! Return the level of a reduction task
  uint8_t level(DataFlow::TaskId id) const;

  //! Return the base id (in the reduction)
  DataFlow::TaskId baseId(DataFlow::TaskId id) const {return id &= ~sPrefixMask;}

  //! Return thecomputation round this task is part of
  DataFlow::TaskId round(DataFlow::TaskId id) const {return id >> sPostfixSize;}

  //! Compute the id of a task in a certain round
  DataFlow::TaskId roundId(DataFlow::TaskId id, uint8_t round) const;

  //! Return whether this is a gather or scatter task
  bool gatherTask(DataFlow::TaskId id) const {return (id & sPrefixMask) == 0;}

  uint32_t gatherTasks(DataFlow::TaskId id) const;

  //! Function to map a global id in the reduction to its parent
  DataFlow::TaskId reduce(DataFlow::TaskId source) const;

  //! Function to compute the children of the given task id
  std::vector<DataFlow::TaskId> expand(DataFlow::TaskId source) const;

  //! Function to map row-major indices from grid of round n to grid of round n+1
  DataFlow::TaskId gridReduce(DataFlow::TaskId source, uint8_t lvl) const;

  //! Function to map row-major indices from grid of round n+1 to round n
  std::vector<DataFlow::TaskId> gridExpand(DataFlow::TaskId source, uint8_t lvl) const;


};




#endif /* KWAYMERGE_H_ */
