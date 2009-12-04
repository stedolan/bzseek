#ifndef BZ_SEEK_H
#define BZ_SEEK_H

#include <stdio.h>
#include <bzlib.h>
#include <stdint.h>

typedef enum{
  BZSEEK_OK = 0,
  BZSEEK_OUT_OF_MEM,
  BZSEEK_BAD_INDEX,
  BZSEEK_BAD_DATA,
  BZSEEK_USAGE_ERR,
  BZSEEK_IO_ERR,
  BZSEEK_EOF,
} bzseek_err;



bzseek_err bzseek_build_index(FILE* bzip_data, FILE* idx_file);


typedef struct{
  FILE* f_data;
  FILE* f_idx;

  int blocksz;

  int idx_nitems;
  uint64_t* idx_data;


  char* buf;
  int buflen, bufsize;
  int curr_block;

  bz_stream bz;
  
} bzseek_file;

bzseek_err bzseek_open(bzseek_file* file, FILE* bzip_data, FILE* idx_file);

uint64_t bzseek_len(bzseek_file* file);
bzseek_err bzseek_read(bzseek_file* file, uint64_t start, int len, char* buf);
const char* bzseek_errmsg(bzseek_err err);

void bzseek_close(bzseek_file*);

#endif
