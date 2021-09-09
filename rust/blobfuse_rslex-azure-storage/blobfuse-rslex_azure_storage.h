#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>


struct ADLSGen1StreamHandler;

struct BlobfuseScopedAccessTokenResolver;


extern "C" {

ADLSGen1StreamHandler *create_adls_gen1_destination_handler_with_token_resolver(BlobfuseScopedAccessTokenResolver *access_token_resolver);

ADLSGen1StreamHandler *create_adls_gen1_handler();

} // extern "C"
