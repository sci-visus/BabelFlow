
#include <iostream>
#include <string>
#include <fstream>
#include <cstring>

#if 1
#include "read_block.h"
#include <hana/math.h>
#include <hana/idx.h>
#include <hana/idx_file.h>

using namespace hana;
using namespace std;

static int mycounter = 0;

char* read_block(char* filename, GlobalIndexType* low, GlobalIndexType* high)
{
    IdxFile idx_file;

    //printf("filename %s\n", filename);
    Error error = read_idx_file(filename, &idx_file);
    if (error.code != Error::NoError) {
        cout << "Error: " << error.get_error_msg() << "\n";
        return NULL;
    }

    // printf("read %d %d %d - %d %d %d\n", low[0], low[1], low[2],high[0], high[1], high[2]);

    int hz_level = idx_file.get_max_hz_level();
    int field = 0;// ONLY ONE FIELD IN THE DATASET //idx_file.get_field_index("o2");
    int time = idx_file.get_min_time_step();

    Grid grid;
    grid.extent.from = Vector3i(low[0], low[1], low[2]);
    grid.extent.to = Vector3i(high[0], high[1], high[2]);
    //grid.extent = idx_file.get_logical_extent();
    grid.data.bytes = idx_file.get_size_inclusive(grid.extent, field, hz_level);
    
    grid.data.ptr = (char*)malloc(grid.data.bytes);
    memset(grid.data.ptr,0,grid.data.bytes);

    Vector3i from, to, stride;
    idx_file.get_grid_inclusive(grid.extent, hz_level, from, to, stride);
    Vector3i dim = (to - from) / stride + 1;
    //cout << "Resulting grid dim = " << dim.x << " x " << dim.y << " x " << dim.z << "\n";

    error = read_idx_grid_inclusive(idx_file, field, time, hz_level, &grid);
    //idx::deallocate_memory();

    // ofstream out;
    // out.open("test.raw");
    // out.write(grid.data.ptr, grid.data.bytes);
    // out.close();

    // exit(0);

    if (error.code != Error::NoError) {
        cout << "Error: " << error.get_error_msg() << "\n";
        return NULL;
    }
    /*
    std::ofstream out;
    char name[128];
    sprintf(name, "data_block%d.raw", mycounter++);
    printf("data_block%d.raw\n", mycounter-1);
    out.open(name,std::ofstream::binary);
    out.write(grid.data.ptr, grid.data.bytes);
    out.close();
    */// exit(0);

    return grid.data.ptr;
}

#else

#include "visus_simpleio.h"

static int mycounter = 0;

char* read_block(char* filename, uint32_t* low, uint32_t* high)
{
    SimpleIO reader;

    reader.openDataset(filename);

    SimpleBox box;
    box.p1 = SimplePoint3d(low[0],low[1],low[2]);
    box.p2 = SimplePoint3d(high[0]+1,high[1]+1,high[2]+1);

    printf("read field %s\n", reader.getFields()[0].name.c_str());
    char* data = (char*)reader.getData(box, 0, reader.getFields()[0].name.c_str());
    /*    
    std::ofstream out;
    int datasize = (high[0]-low[0]+1)*(high[1]-low[1]+1)*(high[2]-low[2]+1);
    char name[128];
    sprintf(name, "data_block%d.raw", mycounter++);
    printf("data_block%d.raw\n", mycounter-1);
    out.open(name,std::ofstream::binary);
    out.write(data, datasize);
    out.close();
    // exit(0);
    */
    return data;
}

#endif
