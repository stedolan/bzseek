#ifndef BZ_SEEK_H
#define BZ_SEEK_H

#include <stdio.h>
#include <bzlib.h>
#include <stdint.h>

typedef enum{
  BZSEEK_OK = 0,     /* everything worked */
  BZSEEK_OUT_OF_MEM, /* out of memory (malloc failed) */
  BZSEEK_BAD_INDEX,  /* index file is corrupted */
  BZSEEK_BAD_DATA,   /* data file is corrupted */
  BZSEEK_USAGE_ERR,  /* misuse of api */
  BZSEEK_IO_ERR,     /* IO error reading data file or index, see errno */
  BZSEEK_EOF,        /* end of file (read may have returned a partial result) */
} bzseek_err;


/* create an index file for the given bzip file.
   if idx_file is NULL, store the index directly in the bzip file.
   if this returns BZSEEK_EOF, it means that the bzip file ended
   sooner than expected and is probably corrupt. */
bzseek_err bzseek_build_index(FILE* bzip_data, FILE* idx_file);




/* a seekable bzip file. All of the fields of this structure are private */
typedef struct bzseek_file bzseek_file;

/* open a file, initialise a bzseek_file.
   if idx_file is NULL, read the index directly from the bzip_data file */
bzseek_err bzseek_open(bzseek_file* file, FILE* bzip_data, FILE* idx_file);

/* return the length of the uncompressed file */
uint64_t bzseek_len(bzseek_file* file);

/* read a range of bytes into the supplied buffer.
   if this returns BZSEEK_EOF, it means that end of file was reached and 
   only (bzseek_len(file) - start) bytes were written into the buffer */
bzseek_err bzseek_read(bzseek_file* file, uint64_t start, int len, char* buf);

/* get a string representation of an error message.
   for BZSEEK_IO_ERR, errno will have more details */
const char* bzseek_errmsg(bzseek_err err);

/* close a bzseek file, freeing any resources
   this function also closes the filehandles passed to bzseek_open */
void bzseek_close(bzseek_file*);

#endif
