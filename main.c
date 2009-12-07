#define _ISOC99_SOURCE
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <getopt.h>
#include <ctype.h>
#include <errno.h>
#include "bzseek.h"


void usage(const char* progname){
  const char msg[] = {
    "\n"
    "Usage: \n"
    "%s <file.bz2> [-i indexfile] -g\n"
    "   -- generate an index for the bzip2 <file.bz2>\n"
    "%s <file.bz2> [-i indexfile] <start> <len>\n"
    "   -- read <len> bytes starting at <start> from an indexed file\n"
    "      <start> and <len> may be decimal, 0xhex or 0oct\n"
  };
  fprintf(stderr, msg, progname, progname);
  exit(1);
}

void describe(const char* prog, const char* msg, bzseek_err err){
  if (err == BZSEEK_IO_ERR){
    fprintf(stderr, "%s: ", prog);
    perror(msg);
  }else if (err != BZSEEK_OK){
    fprintf(stderr, "%s: %s: %s\n", prog, msg, bzseek_errmsg(err));
  }
}

int do_build(const char* prog, const char* filename, const char* index){
  bzseek_err err = bzseek_build_index(filename, index);
  if (err){
    describe(prog, "building index", err);
    return 1;
  }else{
    return 0;
  }
}

#define BUFFER_SIZE 1024
int do_read(const char* prog, const char* filename, const char* index, uint64_t start, unsigned int len){
  char buf[BUFFER_SIZE];
  bzseek_err err;
  bzseek_file f;

  err = bzseek_open(&f, filename, index);
  if (err){describe(prog, "opening file", err); return 1;}

  while (len > 0){
    int l = len > BUFFER_SIZE ? BUFFER_SIZE : len;
    err = bzseek_read(&f, start, l, buf);
    if (err){
      if (err == BZSEEK_EOF){
        fprintf(stderr, "%s: Warning: End-of-file reached during read.\n", prog);
        l = len = (int)(bzseek_len(&f) - start);
      }else{
        describe(prog, "reading", err);
        return 1;
      }
    }
    fwrite(buf, 1, l, stdout);
    len -= l;
    start += l;
  }
  return 0;
}

int main(int argc, char* argv[]){
  char* indexfile = NULL;
  struct option longopts[] = {
    {"index", 1, NULL, 'i'},
    {"generate", 0, NULL, 'g'},
    {0,0,0,0}
  };
  int c;
  int mode = 0;
  char* opts[3];
  int nopts = 0;
  while ((c = getopt_long(argc, argv, "-gi:", longopts, NULL)) != -1){
    switch (c){
    case 1:
      if (nopts < 3) opts[nopts++] = optarg;
      break;
    case 'i':
      indexfile = optarg;
      break;
    case 'g':
      mode = 1;
      break;
    case '?':
      if (optopt == 'i')
        fprintf (stderr, "Option -%c requires an argument.\n", optopt);
      else if (isprint (optopt))
        fprintf (stderr, "Unknown option `-%c'.\n", optopt);
      else
        fprintf (stderr,
                 "Unknown option character `\\x%x'.\n",
                 optopt);
      usage(argv[0]);
      break;
    default:
      fprintf(stderr, "Error parsing options\n");
      usage(argv[0]);
    }
  }
  
  if (mode == 0){
    if (nopts != 3){
      fprintf(stderr, "Missing arguments\n");
      usage(argv[0]);
    }
    char* filename = opts[0];
    uint64_t start;
    unsigned int len;

    char* endptr;
    start = strtoull(opts[1], &endptr, 0);
    if (*endptr != '\0'){
      fprintf(stderr, "Can't parse %s as a starting position\n", opts[1]);
      usage(argv[0]);
    }
    len = strtoul(opts[2], &endptr, 0);
    if (*endptr != '\0'){
      fprintf(stderr, "Can't parse %s as a length\n", opts[2]);
      usage(argv[0]);
    }
    return do_read(argv[0], filename, indexfile, start, len);
  }else{
    if (nopts < 1){
      fprintf(stderr, "Missing filename\n");
      usage(argv[0]);
    }else if (nopts > 1){
      fprintf(stderr, "Unexpected options\n");
      usage(argv[0]);
    }
    const char* filename = opts[0];
    return do_build(argv[0], filename, indexfile);
  }
}
