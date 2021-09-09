#ifndef DATASETFUSE_BINDNGS
#define DATASETFUSE_BINDNGS

#ifdef __cplusplus
extern "C"{
#endif
int azs_rust_readlink(const char *path, char *buf, size_t size);
int azs_rust_symlink(const char *from, const char *to);
int azs_rust_open(const char *path, struct fuse_file_info *fi);
int azs_rust_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
void print_custom_string(char* string);


#ifdef __cplusplus
}
#endif

#endif