
#ifndef BABELFLOW_TYPEDEFINITIONS_H_
#define BABELFLOW_TYPEDEFINITIONS_H_

#include <stdint.h>


namespace BabelFlow 
{

//! The local index type
typedef uint32_t LocalIndexType;

//! The signed local index type
typedef int32_t SignedLocalIndexType;

//! The NULL element for local indices
const LocalIndexType LNULL = (LocalIndexType)(-1);

//! The global index type for large grids
typedef uint32_t GlobalIndexType;

//! The NULL element for global indices
const GlobalIndexType GNULL = (GlobalIndexType)(-1);

//! The function type of the values
typedef double FunctionType;


}   // namespace BabelFlow


#endif /* BABELFLOW_TYPEDEFINITIONS_H_ */
