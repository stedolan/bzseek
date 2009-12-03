#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <unistd.h>

#include <assert.h>

#include <bzlib.h>

typedef uint64_t filepos_t;


int fileread(int fd, char* buf, filepos_t start_byte, filepos_t len){
  if (lseek(fd, start_byte, SEEK_SET) == -1) return -1;
  while (len > 0){
    ssize_t r = read(fd, buf, len);
    if (r < 0) return -1;
    len -= r;
    buf += r;
  }
  return 0;
}

void do_write(int fd, char* buf, size_t len){
  while (len > 0){
    ssize_t r = write(fd, buf, len);
    if (r < 0) perror("write");
    len -= r;
    buf += r;
  }
}

#define ROUND_UP_BYTES(nbits) (((nbits)+7)/8)
void bunzip_block(int blocksz, int fd, filepos_t start, filepos_t end){
  filepos_t startbyte = start / 8;
  int start_off = start % 8;
  filepos_t nbits = end - start;
  int nbytes = ROUND_UP_BYTES(nbits);
  int nread = ROUND_UP_BYTES(end - startbyte * 8);
  int i;
  
  unsigned char* buf = malloc(nbytes + 100);
  memcpy(buf, "BZh0", 4);
  buf[3] = '0' + blocksz;
  
  unsigned char* bit_data = buf+4;
  if (fileread(fd, bit_data, startbyte, nread) < 0){
    perror("read");
    return;
  }
  
  unsigned char blk_header[10];
  for (i=0;i<10;i++){
    blk_header[i] = 
      (bit_data[i] << start_off) | 
      (bit_data[i+1] >> (8-start_off));
  }


  assert(!memcmp(blk_header, "\x31\x41\x59\x26\x53\x59", 6));

  memcpy(bit_data + nread, "\x17\x72\x45\x38\x50\x90", 6); 
  memcpy(bit_data + nread + 6, blk_header + 6, 4);

  int trail_off = end % 8;
  bit_data[nread + 10] = 0;
  bit_data[nread-1] = 
    ((bit_data[nread-1] >> (8-trail_off)) << (8-trail_off)) | 
    (bit_data[nread] >> trail_off);
  unsigned char* trailer = bit_data + nread;
  for (i=0;i<10;i++){
    trailer[i] = 
      (trailer[i] << (8-trail_off)) | 
      (trailer[i+1] >> trail_off);
  }


  for (i=0;i<nread+10;i++){
    bit_data[i] = 
      (bit_data[i] << start_off) |
      (bit_data[i+1] >> (8-start_off));
  }


  do_write(1, buf, 4 + nbytes + 10);
}

/*
  .magic:16                       = 'BZ' signature/magic number
  .version:8                      = 'h' for Bzip2 ('H'uffman coding), '0' for Bzip1 (deprecated)
  .hundred_k_blocksize:8          = '1'..'9' block-size 100 kB-900 kB

  .compressed_magic:48            = 0x314159265359 (BCD (pi))
  .crc:32                         = checksum for this block
  .randomised:1                   = 0=>normal, 1=>randomised (deprecated)
  .origPtr:24                     = starting pointer into BWT for after untransform
  .huffman_used_map:16            = bitmap, of ranges of 16 bytes, present/not present
  .huffman_used_bitmaps:0..256    = bitmap, of symbols used, present/not present (multiples of 16)
  .huffman_groups:3               = 2..6 number of different Huffman tables in use
  .selectors_used:15              = number of times that the Huffman tables are swapped (each 50 bytes)
  *.selector_list:1..6            = zero-terminated bit runs (0..62) of MTF'ed Huffman table (*selectors_used)
  .start_huffman_length:5         = 0..20 starting bit length for Huffman deltas
  *.delta_bit_length:1..40        = 0=>next symbol; 1=>alter length
  { 1=>decrement length;  0=>increment length } (*(symbols+2)*groups)
  .contents:2..∞                  = Huffman encoded data stream until end of block

  .eos_magic:48                   = 0x177245385090 (BCD sqrt(pi))
  .crc:32                         = checksum for whole stream
  .padding:0..7                   = align to whole byte

*/

int main(int argc, char* argv[]){
  //  bunzip_block(9, open("testfile.bz2", O_RDONLY), 32, 6458889 /*6458866*/);
  bunzip_block(9, open("test3.bz2", O_RDONLY), atoi(argv[1]), atoi(argv[2]));
}
