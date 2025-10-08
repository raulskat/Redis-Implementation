#include "sds.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef HAVE_JEMALLOC
#include <jemalloc/jemalloc.h>
#define sds_malloc je_malloc
#define sds_free je_free
#else
#define sds_malloc malloc
#define sds_free free
#endif

typedef struct sds_header {
    size_t len;
    size_t alloc;
    char buf[];
} sds_header_t;

#define SDS_HDR(s) ((sds_header_t *)((unsigned char *)(s) - offsetof(sds_header_t, buf)))

sds sdsnewlen(const void *init, size_t initlen) {
    sds_header_t *hdr =
        (sds_header_t *)sds_malloc(sizeof(sds_header_t) + initlen + 1);
    if (!hdr) {
        return NULL;
    }
    hdr->len = initlen;
    hdr->alloc = initlen;
    if (initlen && init) {
        memcpy(hdr->buf, init, initlen);
    } else if (initlen) {
        memset(hdr->buf, 0, initlen);
    }
    hdr->buf[initlen] = '\0';
    return hdr->buf;
}

sds sdsnew(const char *init) {
    if (!init) {
        return sdsempty();
    }
    return sdsnewlen(init, strlen(init));
}

sds sdsempty(void) {
    sds_header_t *hdr =
        (sds_header_t *)sds_malloc(sizeof(sds_header_t) + 1);
    if (!hdr) {
        return NULL;
    }
    hdr->len = 0;
    hdr->alloc = 0;
    hdr->buf[0] = '\0';
    return hdr->buf;
}

sds sdsdup(const sds s) {
    if (!s) {
        return NULL;
    }
    size_t len = SDS_HDR(s)->len;
    return sdsnewlen(s, len);
}

void sdsfree(sds s) {
    if (!s) {
        return;
    }
    sds_free(SDS_HDR(s));
}

size_t sdslen(const sds s) {
    if (!s) {
        return 0;
    }
    return SDS_HDR(s)->len;
}
