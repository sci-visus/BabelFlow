//
// Created by Sergei Shudler on 2020-06-09.
//

#ifndef _COMP_UTILS_H
#define _COMP_UTILS_H

#include <string>
#include <vector>

#include "TypeDefinitions.h"
#include "reduce/BinarySwap.h"
#include "reduce/BinarySwapTaskMap.h"
#include "reduce/KWayReduction.h"
#include "reduce/KWayReductionTaskMap.h"
#include "reduce/RadixKExchange.h"
#include "reduce/RadixKExchangeTaskMap.h"
#include "reduce/SingleTaskGraph.h"
#include "ComposableTaskGraph.h"
#include "ComposableTaskMap.h"
#include "DefGraphConnector.h"
#include "PreProcessInputTaskGraph.hpp"
#include "ModTaskMap.hpp"
#include "ModuloMap.h"



//-----------------------------------------------------------------------------
// -- begin comp_utils:: --
//-----------------------------------------------------------------------------
namespace comp_utils
{

struct ImageData
{
  using PixelType = float;

  static const uint32_t sNUM_CHANNELS = 4;
  constexpr static const PixelType sOPAQUE = 0.f;
  
  PixelType* image; 
  PixelType* zbuf;
  uint32_t* bounds;
  uint32_t* rend_bounds;     // Used only for binswap and k-radix
  
  ImageData() : image( nullptr ), zbuf( nullptr ), bounds( nullptr ), rend_bounds( nullptr ) {}
  
  void writeImage(const char* filename, uint32_t* extent);
  void writeDepth(const char* filename, uint32_t* extent);
  BabelFlow::Payload serialize() const;
  void deserialize(BabelFlow::Payload buffer);
  void delBuffers();
};
  
// void compose_images(const std::vector<ImageData>& input_images, 
//                     std::vector<ImageData>& out_images, 
//                     int id,
//                     bool flip_split_side,
//                     bool skip_z_check);
                        
// void split_and_blend(const std::vector<ImageData>& input_images,
//                      std::vector<ImageData>& out_images,
//                      uint32_t* union_box,
//                      bool flip_split_side,
//                      bool skip_z_check);

void register_callbacks();

//-----------------------------------------------------------------------------

class BabelGraphWrapper
{
public:
  static std::string sIMAGE_NAME;
   
  BabelGraphWrapper(int32_t rank_id,
                    int32_t n_ranks,
                    int32_t fanin);
  virtual ~BabelGraphWrapper() {}
  virtual void Initialize(std::map<BabelFlow::TaskId, BabelFlow::Payload>& inputs) = 0;
  virtual void Execute(std::map<BabelFlow::TaskId, BabelFlow::Payload>& inputs) = 0;
  
protected:
  uint32_t m_rankId;
  uint32_t m_nRanks;
  uint32_t m_fanin;

  // BabelFlow::mpi::Controller m_master;
  // BabelFlow::ControllerMap m_contMap;
};

//-----------------------------------------------------------------------------

class BabelCompReduce : public BabelGraphWrapper
{
public:
  BabelCompReduce(int32_t rank_id,
                  int32_t n_blocks,
                  int32_t fanin);
  virtual ~BabelCompReduce() {}
  virtual void Initialize(std::map<BabelFlow::TaskId, BabelFlow::Payload>& inputs) override;

protected:
  BabelFlow::KWayReduction m_graph;
  BabelFlow::KWayReductionTaskMap m_taskMap; 
  BabelFlow::PreProcessInputTaskGraph m_modGraph;
  BabelFlow::ModTaskMap m_modMap;
};

//-----------------------------------------------------------------------------

class BabelCompBinswap : public BabelGraphWrapper
{
public:
  BabelCompBinswap(int32_t rank_id,
                   int32_t n_blocks,
                   int32_t fanin);
  virtual ~BabelCompBinswap() {}
  virtual void Initialize(std::map<BabelFlow::TaskId, BabelFlow::Payload>& inputs) override;

protected:
  BabelFlow::BinarySwap m_graph;
  BabelFlow::BinarySwapTaskMap m_taskMap; 
  BabelFlow::PreProcessInputTaskGraph m_modGraph;
  BabelFlow::ModTaskMap m_modMap;
};

//-----------------------------------------------------------------------------

class BabelCompRadixK : public BabelGraphWrapper
{
public:
  BabelCompRadixK(int32_t rank_id,
                  int32_t n_blocks,
                  int32_t fanin,
                  const std::vector<uint32_t>& radix_v);
  virtual ~BabelCompRadixK();
  
  virtual void InitRadixKGraph();
  virtual void InitGatherGraph();
  virtual void Initialize(std::map<BabelFlow::TaskId, BabelFlow::Payload>& inputs) override;
  
protected:
  std::vector<uint32_t> m_Radices;
  
  BabelFlow::SingleTaskGraph m_preProcTaskGr;
  BabelFlow::ModuloMap m_preProcTaskMp;

  BabelFlow::RadixKExchange m_radixkGr;
  BabelFlow::RadixKExchangeTaskMap m_radixkMp; 

  BabelFlow::KWayReduction m_gatherTaskGr;
  BabelFlow::KWayReductionTaskMap m_gatherTaskMp; 

  BabelFlow::ComposableTaskGraph m_radGatherGraph;
  BabelFlow::ComposableTaskMap m_radGatherTaskMap;

  BabelFlow::DefGraphConnector m_defGraphConnector;
  BabelFlow::DefGraphConnector m_defGraphConnectorPreProc;
};

//-----------------------------------------------------------------------------



//-----------------------------------------------------------------------------
}
//-----------------------------------------------------------------------------
// -- end comp_utils --
//-----------------------------------------------------------------------------






#endif    // _COMP_UTILS_H
